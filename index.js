require('dotenv').config();

const fs = require('fs');
const fsp = fs.promises;
const path = require('path');
const os = require('os');
const http = require('http');
const express = require('express');
const { randomUUID } = require('crypto');
const { chromium } = require('playwright');
const {
  S3Client,
  PutObjectCommand,
  ListObjectsV2Command,
  GetObjectCommand,
  DeleteObjectCommand,
  DeleteObjectsCommand,
} = require('@aws-sdk/client-s3');

const PORT = Number(process.env.PORT || 10000);
const URLS_FILE = process.env.URLS_FILE || './urls.txt';
const SETTINGS_FILE = process.env.SETTINGS_FILE || './recorder-settings.json';
const RECORDINGS_DIR = process.env.RECORDINGS_DIR || './recordings';
const RECORD_SECONDS = Number(process.env.RECORD_SECONDS || 30);
const PAGE_TIMEOUT_MS = Number(process.env.PAGE_TIMEOUT_MS || 60000);
const PAGE_WARMUP_MS = Number(process.env.PAGE_WARMUP_MS || 3000);
const AUTO_START = String(process.env.AUTO_START || 'false').toLowerCase() === 'true';
const DELETE_LOCAL_AFTER_UPLOAD = String(process.env.DELETE_LOCAL_AFTER_UPLOAD || 'false').toLowerCase() === 'true';
const APP_VERSION = '17.0.0';
const B2_PREFIX = 'recordings/';

const QUALITY_PRESETS = Object.freeze({
  '240p': { width: 426, height: 240 },
  '360p': { width: 640, height: 360 },
  '480p': { width: 854, height: 480 },
  '540p': { width: 960, height: 540 },
  '720p': { width: 1280, height: 720 },
  '1080p': { width: 1920, height: 1080 },
});

fs.mkdirSync(RECORDINGS_DIR, { recursive: true });

function cleanEndpoint(value) {
  return String(value || '')
    .trim()
    .replace(/^['"]+|['"]+$/g, '')
    .replace(/\/+$/, '');
}

const B2_ENDPOINT = cleanEndpoint(process.env.B2_ENDPOINT || 'https://s3.us-east-005.backblazeb2.com');
const B2_REGION = String(process.env.B2_REGION || 'us-east-005').trim();
const B2_BUCKET = String(process.env.B2_BUCKET || '').trim();
const B2_KEY_ID = String(process.env.B2_KEY_ID || '').trim();
const B2_APPLICATION_KEY = String(process.env.B2_APPLICATION_KEY || '').trim();

let b2EndpointError = null;
try {
  const parsed = new URL(B2_ENDPOINT);
  if (!['http:', 'https:'].includes(parsed.protocol)) throw new Error('Endpoint must use http:// or https://');
} catch (err) {
  b2EndpointError = `Invalid B2_ENDPOINT: ${B2_ENDPOINT || '(empty)'}. Expected https://s3.us-east-005.backblazeb2.com`;
}

const b2Configured = Boolean(!b2EndpointError && B2_ENDPOINT && B2_REGION && B2_BUCKET && B2_KEY_ID && B2_APPLICATION_KEY);
const s3 = b2Configured ? new S3Client({
  region: B2_REGION,
  endpoint: B2_ENDPOINT,
  forcePathStyle: true,
  credentials: { accessKeyId: B2_KEY_ID, secretAccessKey: B2_APPLICATION_KEY },
}) : null;

function sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }

function loadSettings() {
  const envQuality = QUALITY_PRESETS[process.env.DEFAULT_QUALITY] ? process.env.DEFAULT_QUALITY : '480p';
  const fallback = { quality: envQuality, width: QUALITY_PRESETS[envQuality].width, height: QUALITY_PRESETS[envQuality].height, recordSeconds: clampSeconds(RECORD_SECONDS) };
  try {
    if (!fs.existsSync(SETTINGS_FILE)) return fallback;
    const parsed = JSON.parse(fs.readFileSync(SETTINGS_FILE, 'utf8'));
    const quality = QUALITY_PRESETS[parsed.quality] ? parsed.quality : fallback.quality;
    const seconds = Number(parsed.recordSeconds);
    return { quality, width: QUALITY_PRESETS[quality].width, height: QUALITY_PRESETS[quality].height, recordSeconds: Number.isFinite(seconds) ? clampSeconds(seconds) : fallback.recordSeconds };
  } catch (err) {
    console.log(`Settings load warning: ${err.message}`);
    return fallback;
  }
}

function clampSeconds(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 30;
  return Math.min(3600, Math.max(1, Math.round(n)));
}

let settings = loadSettings();

function saveSettings(next) {
  settings = { ...settings, ...next };
  fs.writeFileSync(SETTINGS_FILE, JSON.stringify(settings, null, 2));
  return settings;
}

function normalizeUrls(input) {
  const values = Array.isArray(input) ? input : String(input ?? '').split(/\r?\n/);
  const out = [];
  const seen = new Set();
  for (const raw of values) {
    const value = String(raw ?? '').trim();
    if (!value || value.startsWith('#')) continue;
    let u;
    try { u = new URL(value); } catch { throw new Error(`Invalid URL: ${value}`); }
    if (!['http:', 'https:'].includes(u.protocol)) throw new Error(`Only http:// and https:// URLs are allowed: ${value}`);
    const normalized = u.toString();
    if (!seen.has(normalized)) { seen.add(normalized); out.push(normalized); }
  }
  return out;
}

function readUrls() {
  if (!fs.existsSync(URLS_FILE)) return [];
  return normalizeUrls(fs.readFileSync(URLS_FILE, 'utf8'));
}

function writeUrls(urls) {
  fs.writeFileSync(URLS_FILE, urls.length ? `${urls.join('\n')}\n` : '');
}

function safeFilePart(value) {
  return String(value).replace(/^https?:\/\//i, '').replace(/[^a-z0-9._-]+/gi, '_').replace(/^_+|_+$/g, '').slice(0, 90) || 'page';
}

function makeFilename(index, url) {
  const stamp = new Date().toISOString().replace(/:/g, '-');
  const host = safeFilePart(new URL(url).hostname);
  return `${String(index + 1).padStart(3, '0')}_${stamp}_${host}.webm`;
}

function validateKey(key) {
  const value = String(key || '');
  if (!value.startsWith(B2_PREFIX) || value.includes('..') || !/\.webm$/i.test(value)) throw new Error('Invalid recording key.');
  return value;
}

async function uploadToB2(localPath, key) {
  if (!s3) throw new Error('B2 is not configured.');
  await s3.send(new PutObjectCommand({
    Bucket: B2_BUCKET,
    Key: key,
    Body: fs.createReadStream(localPath),
    ContentType: 'video/webm',
    ServerSideEncryption: 'AES256',
  }));
}

async function listB2Files() {
  if (!s3) throw new Error('B2 is not configured.');
  const out = await s3.send(new ListObjectsV2Command({ Bucket: B2_BUCKET, Prefix: B2_PREFIX, MaxKeys: 1000 }));
  return (out.Contents || [])
    .filter(x => x.Key && /\.webm$/i.test(x.Key))
    .sort((a, b) => new Date(b.LastModified || 0) - new Date(a.LastModified || 0))
    .map(x => ({
      key: x.Key,
      filename: x.Key.slice(B2_PREFIX.length),
      size: x.Size || 0,
      lastModified: x.LastModified || null,
      previewUrl: `/api/preview?key=${encodeURIComponent(x.Key)}`,
      downloadUrl: `/api/download?key=${encodeURIComponent(x.Key)}`,
    }));
}

let browser = null;
let running = false;
let stopRequested = false;
let currentUrl = null;
let currentIndex = null;
let totalUrls = 0;
let activeRunUrls = [];
let lastResult = null;
let lastError = null;
let livePreviewBuffer = null;
let livePreviewUpdatedAt = null;

async function ensureBrowser() {
  if (!browser) browser = await chromium.launch({ headless: true, args: ['--no-sandbox', '--disable-setuid-sandbox'] });
  return browser;
}

async function captureLivePreview(page) {
  try {
    livePreviewBuffer = await page.screenshot({ type: 'jpeg', quality: 65, animations: 'disabled' });
    livePreviewUpdatedAt = new Date().toISOString();
  } catch (_) {}
}

async function recordOneUrl(url, index, runSettings) {
  const b = await ensureBrowser();
  const tempDir = await fsp.mkdtemp(path.join(os.tmpdir(), `pella-webm-${randomUUID()}-`));
  const localFilename = makeFilename(index, url);
  const localPath = path.resolve(RECORDINGS_DIR, localFilename);
  let context;
  let page;

  try {
    context = await b.newContext({
      viewport: { width: runSettings.width, height: runSettings.height },
      recordVideo: { dir: tempDir, size: { width: runSettings.width, height: runSettings.height } },
    });
    page = await context.newPage();
    currentUrl = url;
    currentIndex = index;
    livePreviewBuffer = null;
    livePreviewUpdatedAt = null;

    page.on('console', msg => { if (msg.type() === 'error') console.log(`Page console error: ${msg.text()}`); });
    page.on('pageerror', err => console.log(`Page error: ${err.message}`));

    console.log(`Loading: ${url}`);
    try {
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: PAGE_TIMEOUT_MS });
      console.log(`Page loaded: ${url}`);
    } catch (err) {
      console.log(`Navigation warning for ${url}: ${err.message}`);
      if (page.url() === 'about:blank') throw err;
      console.log(`Continuing with current page URL: ${page.url()}`);
    }

    const videos = await page.locator('video').count().catch(() => 0);
    const audios = await page.locator('audio').count().catch(() => 0);
    console.log(`Detected media: ${videos} video, ${audios} audio`);

    if (PAGE_WARMUP_MS > 0) await sleep(PAGE_WARMUP_MS);
    await captureLivePreview(page);

    console.log(`Recording for ${runSettings.recordSeconds}s at ${runSettings.quality}: ${url}`);
    const endAt = Date.now() + runSettings.recordSeconds * 1000;
    let nextPreviewAt = 0;
    while (!stopRequested && Date.now() < endAt) {
      if (Date.now() >= nextPreviewAt) {
        await captureLivePreview(page);
        nextPreviewAt = Date.now() + 1000;
      }
      await sleep(200);
    }
  } finally {
    // Closing context finalizes the Playwright WebM file.
    if (page) await page.close().catch(() => {});
    if (context) await context.close().catch(() => {});
    livePreviewBuffer = null;
    livePreviewUpdatedAt = null;
  }

  const names = await fsp.readdir(tempDir);
  const webmNames = names.filter(n => /\.webm$/i.test(n));
  if (!webmNames.length) {
    await fsp.rm(tempDir, { recursive: true, force: true }).catch(() => {});
    throw new Error('Playwright did not produce a WebM file.');
  }

  webmNames.sort((a, b) => fs.statSync(path.join(tempDir, b)).mtimeMs - fs.statSync(path.join(tempDir, a)).mtimeMs);
  const producedPath = path.join(tempDir, webmNames[0]);
  await fsp.copyFile(producedPath, localPath);
  await fsp.rm(tempDir, { recursive: true, force: true }).catch(() => {});

  const key = `${B2_PREFIX}${localFilename}`;
  console.log(`Recording saved locally: ${localPath}`);

  let uploaded = false;
  if (b2Configured) {
    await uploadToB2(localPath, key);
    uploaded = true;
    console.log(`B2 upload OK: ${key}`);
    if (DELETE_LOCAL_AFTER_UPLOAD) await fsp.unlink(localPath).catch(() => {});
  } else {
    console.log('B2 upload skipped: B2 is not configured.');
  }

  return { url, filename: localFilename, key, quality: runSettings.quality, width: runSettings.width, height: runSettings.height, recordSeconds: runSettings.recordSeconds, uploaded, finishedAt: new Date().toISOString() };
}

async function runRecorder(urls, source) {
  if (running) throw new Error('Recorder is already running.');
  const runUrls = normalizeUrls(urls);
  if (!runUrls.length) throw new Error('No URLs provided.');
  running = true;
  stopRequested = false;
  lastError = null;
  lastResult = null;
  activeRunUrls = runUrls.slice();
  totalUrls = runUrls.length;
  const runSettings = { ...settings };
  console.log(`Recorder starting from ${source}. ${runUrls.length} URL(s). Quality: ${runSettings.quality} (${runSettings.width}x${runSettings.height}), ${runSettings.recordSeconds}s each.`);
  runUrls.forEach((u, i) => console.log(`  [${i + 1}/${runUrls.length}] ${u}`));

  try {
    for (let i = 0; i < runUrls.length; i += 1) {
      if (stopRequested) break;
      currentIndex = i;
      try {
        lastResult = await recordOneUrl(runUrls[i], i, runSettings);
      } catch (err) {
        lastError = `${runUrls[i]}: ${err.message}`;
        console.error(`Recording failed: ${lastError}`);
      }
    }
  } finally {
    running = false;
    currentUrl = null;
    currentIndex = null;
    totalUrls = 0;
    activeRunUrls = [];
    stopRequested = false;
    livePreviewBuffer = null;
    livePreviewUpdatedAt = null;
    console.log('Recorder finished.');
  }
}

function safeStatus() {
  let savedUrls = [];
  try { savedUrls = readUrls(); } catch (err) { lastError = `URL file error: ${err.message}`; }
  return {
    version: APP_VERSION,
    running,
    currentUrl,
    currentIndex,
    totalUrls,
    urls: running ? activeRunUrls.slice() : savedUrls,
    savedUrls,
    settings,
    qualityPresets: QUALITY_PRESETS,
    b2Configured,
    bucket: B2_BUCKET || null,
    region: B2_REGION,
    endpoint: B2_ENDPOINT,
    endpointError: b2EndpointError,
    runSource: running ? 'panel' : 'saved',
    lastResult,
    lastError,
    livePreviewUpdatedAt,
  };
}

function renderError(message, code = 500) { return { ok: false, error: message, code }; }

const app = express();
app.use(express.json({ limit: '256kb' }));

app.get('/health', (_req, res) => {
  res.json({
    ok: true,
    version: APP_VERSION,
    running,
    b2Configured,
    bucket: B2_BUCKET || null,
    region: B2_REGION,
    endpoint: B2_ENDPOINT || null,
    endpointError: b2EndpointError,
    settings,
    currentUrl,
    currentIndex,
    totalUrls,
    lastError,
  });
});

app.get('/api/status', (_req, res) => res.json(safeStatus()));

app.get('/api/debug', (_req, res) => {
  let savedUrls = [];
  let urlsError = null;
  try { savedUrls = readUrls(); } catch (err) { urlsError = err.message; }
  res.json({
    ok: true,
    version: APP_VERSION,
    node: process.version,
    cwd: process.cwd(),
    port: PORT,
    b2Configured,
    b2Endpoint: B2_ENDPOINT,
    b2Region: B2_REGION,
    b2Bucket: B2_BUCKET || null,
    hasKeyId: Boolean(B2_KEY_ID),
    hasApplicationKey: Boolean(B2_APPLICATION_KEY),
    urlsFile: path.resolve(URLS_FILE),
    savedUrls,
    urlsError,
    settingsFile: path.resolve(SETTINGS_FILE),
    recordingsDir: path.resolve(RECORDINGS_DIR),
  });
});

app.post('/api/start', async (req, res) => {
  if (running) return res.status(409).json(renderError('Recorder is already running.', 409));
  try {
    const urlsInput = Array.isArray(req.body?.urls) ? req.body.urls : req.body?.text;
    const urls = normalizeUrls(urlsInput);
    if (!urls.length) return res.status(400).json(renderError('Paste at least one valid URL into the panel.', 400));

    // Atomic manual-start snapshot: panel URLs + settings arrive in the same request.
    const quality = String(req.body?.quality || settings.quality);
    const recordSeconds = req.body?.recordSeconds == null ? settings.recordSeconds : clampSeconds(req.body.recordSeconds);
    if (!QUALITY_PRESETS[quality]) return res.status(400).json(renderError(`Unsupported quality: ${quality}`, 400));
    saveSettings({ quality, width: QUALITY_PRESETS[quality].width, height: QUALITY_PRESETS[quality].height, recordSeconds });
    writeUrls(urls);

    console.log(`Panel start requested with ${urls.length} URL(s):`);
    urls.forEach((u, i) => console.log(`  [${i + 1}] ${u}`));
    runRecorder(urls, 'panel').catch(err => { lastError = err.message; console.error(`Recorder fatal error: ${err.message}`); });
    return res.json({ ok: true, started: true, source: 'panel', urls, settings });
  } catch (err) {
    return res.status(400).json(renderError(err.message, 400));
  }
});

app.post('/api/stop', (_req, res) => {
  if (!running) return res.json({ ok: true, stopped: false });
  stopRequested = true;
  res.json({ ok: true, stopped: true });
});

app.get('/api/urls', (_req, res) => {
  try { return res.json({ ok: true, urls: readUrls() }); }
  catch (err) { return res.status(500).json(renderError(err.message, 500)); }
});

app.post('/api/urls', (req, res) => {
  try {
    const input = Array.isArray(req.body?.urls) ? req.body.urls : req.body?.text;
    const urls = normalizeUrls(input);
    writeUrls(urls);
    res.json({ ok: true, urls });
  } catch (err) { res.status(400).json(renderError(err.message, 400)); }
});

app.post('/api/settings', (req, res) => {
  try {
    const quality = String(req.body?.quality || settings.quality);
    if (!QUALITY_PRESETS[quality]) throw new Error(`Unsupported quality: ${quality}`);
    const recordSeconds = clampSeconds(req.body?.recordSeconds ?? settings.recordSeconds);
    saveSettings({ quality, width: QUALITY_PRESETS[quality].width, height: QUALITY_PRESETS[quality].height, recordSeconds });
    res.json({ ok: true, settings });
  } catch (err) { res.status(400).json(renderError(err.message, 400)); }
});

app.get('/api/files', async (_req, res) => {
  try {
    if (!b2Configured) return res.status(503).json(renderError('B2 is not configured. Add B2_ENDPOINT, B2_REGION, B2_BUCKET, B2_KEY_ID and B2_APPLICATION_KEY.', 503));
    res.json({ ok: true, bucket: B2_BUCKET, files: await listB2Files() });
  } catch (err) {
    console.error(`B2 list error: ${err.message}`);
    res.status(502).json(renderError(`B2 list failed: ${err.message}`, 502));
  }
});

async function streamB2Object(req, res, key, attachment) {
  if (!b2Configured) throw new Error('B2 is not configured.');
  const cmd = { Bucket: B2_BUCKET, Key: key };
  if (req.headers.range) cmd.Range = req.headers.range;
  const out = await s3.send(new GetObjectCommand(cmd));
  const isPartial = Boolean(req.headers.range);
  res.status(isPartial ? 206 : 200);
  res.setHeader('Content-Type', out.ContentType || 'video/webm');
  res.setHeader('Accept-Ranges', 'bytes');
  if (out.ContentLength != null) res.setHeader('Content-Length', String(out.ContentLength));
  if (out.ContentRange) res.setHeader('Content-Range', out.ContentRange);
  if (attachment) {
    const filename = path.basename(key).replace(/["\r\n]/g, '');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
  } else res.setHeader('Content-Disposition', 'inline');
  if (out.Body?.pipe) out.Body.pipe(res);
  else res.end(Buffer.from(await out.Body.transformToByteArray()));
}

app.get('/api/preview', async (req, res) => {
  try { await streamB2Object(req, res, validateKey(req.query.key), false); }
  catch (err) { console.error(`B2 preview error: ${err.message}`); if (!res.headersSent) res.status(502).send(`Preview failed: ${err.message}`); }
});

app.get('/api/download', async (req, res) => {
  try { await streamB2Object(req, res, validateKey(req.query.key), true); }
  catch (err) { console.error(`B2 download error: ${err.message}`); if (!res.headersSent) res.status(502).send(`Download failed: ${err.message}`); }
});

function removeLocalByKey(key) {
  const filename = path.basename(key);
  const localPath = path.resolve(RECORDINGS_DIR, filename);
  if (fs.existsSync(localPath)) fs.unlinkSync(localPath);
}

app.post('/api/delete', async (req, res) => {
  try {
    const key = validateKey(req.body?.key);
    if (!b2Configured) throw new Error('B2 is not configured.');
    await s3.send(new DeleteObjectCommand({ Bucket: B2_BUCKET, Key: key }));
    removeLocalByKey(key);
    res.json({ ok: true, deleted: key });
  } catch (err) { console.error(`B2 delete error: ${err.message}`); res.status(502).json(renderError(`Delete failed: ${err.message}`, 502)); }
});

app.post('/api/delete-many', async (req, res) => {
  try {
    const keys = Array.from(new Set((Array.isArray(req.body?.keys) ? req.body.keys : []).map(validateKey)));
    if (!keys.length) return res.status(400).json(renderError('No files selected.', 400));
    if (!b2Configured) throw new Error('B2 is not configured.');
    const result = await s3.send(new DeleteObjectsCommand({ Bucket: B2_BUCKET, Delete: { Objects: keys.map(Key => ({ Key })), Quiet: true } }));
    for (const key of keys) removeLocalByKey(key);
    if (result.Errors?.length) throw new Error(result.Errors.map(e => `${e.Key}: ${e.Message}`).join('; '));
    res.json({ ok: true, deleted: keys });
  } catch (err) { console.error(`B2 bulk delete error: ${err.message}`); res.status(502).json(renderError(`Delete failed: ${err.message}`, 502)); }
});

app.get('/api/live-preview', (_req, res) => {
  if (!livePreviewBuffer) return res.status(404).send('No live preview available.');
  res.setHeader('Content-Type', 'image/jpeg');
  res.setHeader('Cache-Control', 'no-store');
  res.end(livePreviewBuffer);
});

function htmlEscape(value) {
  return String(value ?? '').replace(/[&<>"']/g, c => ({ '&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;' }[c]));
}
function fmtBytes(n) {
  if (!n) return '0 B';
  const units = ['B','KB','MB','GB'];
  const i = Math.min(Math.floor(Math.log(n) / Math.log(1024)), units.length - 1);
  return `${(n / Math.pow(1024, i)).toFixed(i ? 1 : 0)} ${units[i]}`;
}

app.get('/', async (_req, res) => {
  let urls = [];
  try { urls = readUrls(); } catch (_) {}
  let files = [];
  let filesError = '';
  if (b2Configured) {
    try { files = await listB2Files(); }
    catch (err) { filesError = err.message; console.error(`Dashboard B2 list error: ${err.message}`); }
  } else {
    filesError = b2EndpointError || 'B2 is not configured.';
  }

  const optionHtml = Object.entries(QUALITY_PRESETS).map(([q, p]) => `<option value="${q}" ${q === settings.quality ? 'selected' : ''}>${q} — ${p.width}×${p.height}</option>`).join('');
  const initialFiles = files.map(f => ({ ...f, sizeLabel: fmtBytes(f.size), lastLabel: f.lastModified ? new Date(f.lastModified).toLocaleString() : '' }));

  res.type('html').send(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Pella Render Recorder v17</title>
<style>
:root{font-family:Inter,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#111;background:#f5f7fa}*{box-sizing:border-box}body{margin:0}main{max-width:1200px;margin:auto;padding:20px}.card{background:#fff;border:1px solid #dde3e8;border-radius:14px;padding:18px;margin-bottom:16px}.top{display:flex;justify-content:space-between;gap:16px;flex-wrap:wrap}h1{margin:0 0 8px;font-size:27px}h2{margin:0 0 10px;font-size:19px}.muted{color:#66717c;font-size:13px}.status{font-size:14px;color:#5c6670}.status.err{color:#a51d2d}.status.ok{color:#0b6b4f}.actions,.row{display:flex;gap:8px;flex-wrap:wrap;align-items:center}.btn,button,select,input,textarea{font:inherit}.btn,button{padding:10px 13px;border:1px solid #bbc3cb;border-radius:9px;background:#fff;text-decoration:none;color:#111;cursor:pointer}.primary{background:#111827;color:#fff;border-color:#111827}.danger{background:#fff0f0;border-color:#efb5b5;color:#981b1b}.grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}.field{display:grid;gap:6px}input[type=text],input[type=number],select,textarea{width:100%;padding:10px 12px;border:1px solid #c9d0d6;border-radius:9px;background:#fff}textarea{min-height:170px;resize:vertical;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}.help{font-size:13px;color:#69757f}.preview{background:#101419;border-radius:11px;overflow:hidden;min-height:240px;display:flex;align-items:center;justify-content:center}.preview img{display:block;max-width:100%;width:100%;height:auto}.empty{color:#cbd1d7;padding:40px;text-align:center}.files{display:grid;gap:14px}.file{border:1px solid #e0e5ea;border-radius:12px;padding:12px;background:#fbfcfd}.filehead{display:grid;grid-template-columns:auto 1fr auto;gap:10px;align-items:center;margin-bottom:10px}.name{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.meta{font-size:13px;color:#6c7680;white-space:nowrap}.file video{width:100%;max-height:440px;background:#000;border-radius:9px}.errbox{padding:12px;border:1px solid #efb4b4;background:#fff1f1;color:#972020;border-radius:10px}.pill{padding:4px 8px;border-radius:999px;background:#edf1f5;font-size:12px}@media(max-width:760px){.grid{grid-template-columns:1fr}.filehead{grid-template-columns:1fr}.meta{white-space:normal}}
</style></head><body><main>
<section class="card"><div class="top"><div><h1>Pella Render Recorder</h1><div id="status" class="status">Version ${APP_VERSION} · Loading…</div><div id="substatus" class="muted" style="margin-top:6px">Panel URLs are authoritative for Start Recording.</div></div><div class="actions"><button id="startBtn" class="primary">Start Recording</button><button id="stopBtn">Stop</button><button id="refreshBtn">Refresh</button></div></div></section>
<section class="card"><h2>Recording Settings</h2><div class="grid"><div class="field"><label for="quality">Video quality</label><select id="quality">${optionHtml}</select><div class="help">240p–1080p. The selection applies to the next run.</div></div><div class="field"><label for="recordSeconds">Seconds per URL</label><input id="recordSeconds" type="number" min="1" max="3600" value="${settings.recordSeconds}"><div class="help">Each URL is recorded for this many seconds unless Stop is pressed.</div></div></div><div class="actions" style="margin-top:12px"><button id="saveSettingsBtn">Save Settings</button></div></section>
<section class="card"><h2>URLs to Record</h2><div class="field"><label for="urls">Paste multiple links — one per line</label><textarea id="urls" placeholder="https://example.com/video1\nhttps://example.com/video2"></textarea><div class="help"><b>Start Recording uses exactly what is in this box.</b> urls.txt is not read during a manual start.</div></div><div class="row" style="margin-top:10px"><input id="newUrl" type="text" placeholder="https://example.com/video-page"><button id="addUrlBtn">Add URL</button><button id="saveUrlsBtn">Save URL List</button><button id="loadUrlsBtn">Load urls.txt</button><button id="clearUrlsBtn">Clear</button></div></section>
<section class="card"><div class="top"><div><h2>Live Screen Preview</h2><div id="liveText" class="muted">No active recording.</div></div><span id="liveQuality" class="pill">${settings.quality} · ${settings.width}×${settings.height}</span></div><div class="preview" style="margin-top:12px"><div id="liveEmpty" class="empty">Start a recording to see the current page here.</div><img id="livePreview" alt="Live webpage preview" style="display:none"></div></section>
<section class="card"><div class="top"><div><h2>B2 Recordings</h2><div class="muted">Private recordings in ${htmlEscape(B2_BUCKET || 'not configured')}.</div></div><div class="actions"><button id="selectAllBtn">Select All</button><button id="deleteSelectedBtn" class="danger">Delete Selected</button></div></div><div id="files" class="files" style="margin-top:12px"></div></section>
</main>
<script>
const APP=${JSON.stringify({version:APP_VERSION,initialUrls:urls,initialFiles:initialFiles,initialFilesError:filesError,qualityPresets:QUALITY_PRESETS})};
const $=id=>document.getElementById(id);
function escapeHtml(v){return String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
function fmtBytes(n){if(!n)return'0 B';const u=['B','KB','MB','GB'];const i=Math.min(Math.floor(Math.log(n)/Math.log(1024)),u.length-1);return((n/Math.pow(1024,i)).toFixed(i?1:0))+' '+u[i];}
async function api(url,opt){const r=await fetch(url,opt);const t=await r.text();let d;try{d=JSON.parse(t)}catch{d={error:t}}if(!r.ok)throw new Error(d.error||('HTTP '+r.status));return d}
function setUrlsInMemory(urls){$('urls').value=urls.join('\n');localStorage.setItem('pella.panelUrls', $('urls').value);}
function getPanelUrls(){return $('urls').value.split(/\r?\n/).map(s=>s.trim()).filter(Boolean);}
function setStatus(s){$('status').textContent='Running: '+(s.running?'YES':'NO')+(s.currentUrl?' · Current: '+s.currentUrl:'')+' · Progress: '+(s.totalUrls?(s.currentIndex+1)+'/'+s.totalUrls:'—')+' · B2: '+(s.b2Configured?'configured':'not configured')+' · Bucket: '+(s.bucket||'not set')+' · Version: '+APP.version;$('status').className='status '+(s.lastError?'err':'ok');$('substatus').textContent=s.lastError?'Last error: '+s.lastError:'Panel URLs and selected quality stay local until you save them.';const img=$('livePreview');if(s.running&&s.currentUrl){img.src='/api/live-preview?ts='+Date.now();img.style.display='block';$('liveEmpty').style.display='none';$('liveText').textContent='Recording: '+s.currentUrl+' · '+$('quality').value}else{img.style.display='none';$('liveEmpty').style.display='block';$('liveText').textContent='No active recording.'}}
function renderFiles(files,error){if(error){$('files').innerHTML='<div class="errbox">B2 file list error: '+escapeHtml(error)+'</div>';return}if(!files.length){$('files').innerHTML='<div class="empty" style="color:#6c7680">No WebM recordings found.</div>';return}$('files').innerHTML=files.map(f=>'<div class="file" data-key="'+escapeHtml(f.key)+'"><div class="filehead"><label><input class="filecheck" type="checkbox" value="'+escapeHtml(f.key)+'"> Select</label><div class="name" title="'+escapeHtml(f.filename)+'">'+escapeHtml(f.filename)+'</div><div class="meta">'+fmtBytes(f.size)+' · '+(f.lastModified?new Date(f.lastModified).toLocaleString():'')+'</div></div><video controls preload="metadata" src="'+escapeHtml(f.previewUrl)+'"></video><div class="row" style="margin-top:10px"><a class="btn" href="'+escapeHtml(f.downloadUrl)+'">Download</a><button class="danger deleteOne" data-key="'+escapeHtml(f.key)+'">Delete</button></div></div>').join('')}
async function refreshStatus(){try{setStatus(await api('/api/status'))}catch(e){$('status').textContent='Status error: '+e.message;$('status').className='status err'}}
async function refreshFiles(){try{const d=await api('/api/files');renderFiles(d.files,'')}catch(e){renderFiles([],e.message)}}
async function refreshAll(){await Promise.all([refreshStatus(),refreshFiles()])}
function saveSettingsLocal(){localStorage.setItem('pella.quality',$('quality').value);localStorage.setItem('pella.recordSeconds',String($('recordSeconds').value));$('liveQuality').textContent=$('quality').value+' · '+(APP.qualityPresets[$('quality').value]?.width||'')+'×'+(APP.qualityPresets[$('quality').value]?.height||'');}
async function saveSettings(silent=false){saveSettingsLocal();const d=await api('/api/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({quality:$('quality').value,recordSeconds:Number($('recordSeconds').value)})});saveSettingsLocal();if(!silent)alert('Settings saved. Quality '+d.settings.quality+' will be used for the next recording.');return d}
async function start(){if(document.body.dataset.busy==='1')return;const urls=getPanelUrls();if(!urls.length)return alert('Paste at least one URL into the panel.');document.body.dataset.busy='1';$('startBtn').disabled=true;$('startBtn').textContent='Starting…';try{const d=await api('/api/start',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({urls:urls,quality:$('quality').value,recordSeconds:Number($('recordSeconds').value)})});setUrlsInMemory(d.urls);await refreshStatus()}catch(e){alert(e.message)}finally{document.body.dataset.busy='0';$('startBtn').disabled=false;$('startBtn').textContent='Start Recording'}}
async function stop(){try{await api('/api/stop',{method:'POST'});await refreshStatus()}catch(e){alert(e.message)}}
async function saveUrls(showAlert=true){try{const d=await api('/api/urls',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({text:$('urls').value})});setUrlsInMemory(d.urls);if(showAlert)alert('Saved '+d.urls.length+' URL(s) to urls.txt.');return d}catch(e){alert('Save URLs failed: '+e.message);throw e}}
async function loadUrls(){try{const d=await api('/api/urls');setUrlsInMemory(d.urls);alert('Loaded '+d.urls.length+' URL(s) from urls.txt.')}catch(e){alert('Load URLs failed: '+e.message)}}
async function addUrl(){const v=$('newUrl').value.trim();if(!v)return;try{const u=new URL(v);if(!['http:','https:'].includes(u.protocol))throw new Error('Only http:// and https:// URLs are allowed.');const vals=getPanelUrls();if(!vals.includes(u.toString()))vals.push(u.toString());setUrlsInMemory(vals);$('newUrl').value='';await saveUrls(false);$('substatus').textContent='URL added and saved: '+u.toString();}catch(e){alert('Add URL failed: '+e.message)}}
async function clearUrls(){setUrlsInMemory([]);try{await saveUrls(false);$('substatus').textContent='URL list cleared and saved.'}catch(e){}}
function selectAll(){document.querySelectorAll('.filecheck').forEach(x=>x.checked=true)}
async function del(key){if(!confirm('Delete this recording from Backblaze B2?\n\n'+key))return;try{await api('/api/delete',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({key})});await refreshFiles()}catch(e){alert(e.message)}}
async function delSelected(){const keys=[...document.querySelectorAll('.filecheck:checked')].map(x=>x.value);if(!keys.length)return alert('Select at least one recording.');if(!confirm('Delete '+keys.length+' selected recording(s)?'))return;try{await api('/api/delete-many',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({keys})});await refreshFiles()}catch(e){alert(e.message)}}
$('startBtn').onclick=start;$('stopBtn').onclick=stop;$('refreshBtn').onclick=refreshAll;$('saveSettingsBtn').onclick=()=>saveSettings(false);$('addUrlBtn').onclick=addUrl;$('saveUrlsBtn').onclick=()=>saveUrls(true);$('loadUrlsBtn').onclick=loadUrls;$('clearUrlsBtn').onclick=clearUrls;$('selectAllBtn').onclick=selectAll;$('deleteSelectedBtn').onclick=delSelected;$('files').addEventListener('click',e=>{const b=e.target.closest('.deleteOne');if(b)del(b.dataset.key)});
$('urls').addEventListener('input',()=>localStorage.setItem('pella.panelUrls',$('urls').value));
$('quality').addEventListener('change',()=>saveSettingsLocal());
$('recordSeconds').addEventListener('input',()=>saveSettingsLocal());
$('newUrl').addEventListener('keydown',e=>{if(e.key==='Enter'){e.preventDefault();addUrl()}});
(function init(){const local=localStorage.getItem('pella.panelUrls');if(local!=null){$('urls').value=local}else{$('urls').value=APP.initialUrls.join('\n');localStorage.setItem('pella.panelUrls',$('urls').value)}const q=localStorage.getItem('pella.quality');const secs=localStorage.getItem('pella.recordSeconds');if(q&&APP.qualityPresets[q])$('quality').value=q;if(secs&&Number.isFinite(Number(secs)))$('recordSeconds').value=secs;saveSettingsLocal();renderFiles(APP.initialFiles,APP.initialFilesError);refreshStatus();setInterval(refreshStatus,1500);setInterval(refreshFiles,7000)})();
</script></body></html>`);
});

const server = http.createServer(app);
server.listen(PORT, '0.0.0.0', () => {
  console.log(`Pella Render Recorder ${APP_VERSION} listening on 0.0.0.0:${PORT}`);
  console.log(`B2 configured: ${b2Configured}`);
  console.log(`B2 endpoint: ${B2_ENDPOINT || '(not set)'}`);
  console.log(`B2 bucket: ${B2_BUCKET || '(not set)'}`);
  console.log(`B2 region: ${B2_REGION}`);
  console.log(`URLS_FILE=${URLS_FILE}`);
  console.log(`Default settings: ${settings.quality} ${settings.width}x${settings.height}, ${settings.recordSeconds}s`);
  if (b2EndpointError) console.log(b2EndpointError);
  if (AUTO_START) runRecorder(readUrls(), 'auto').catch(err => { lastError = err.message; console.error(`Auto-start error: ${err.message}`); });
});

async function shutdown() {
  stopRequested = true;
  try { if (browser) await browser.close(); } catch (_) {}
  process.exit(0);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
