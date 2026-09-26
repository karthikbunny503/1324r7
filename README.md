# Pella Render Recorder v17

Render-hosted Playwright webpage recorder with a Backblaze B2 dashboard.

## v17 fixes
- Quality selection is kept in the browser and is always sent with Start Recording.
- Background status refresh no longer overwrites the quality or duration controls.
- Add URL immediately saves the updated URL list to `urls.txt`.
- Save URL List explicitly saves the textarea contents to `urls.txt`.
- Load urls.txt reads the saved server-side URL list into the panel.
- Clear clears both the panel and saved URL list.
- Panel URL text is stored in browser localStorage so refreshes do not erase unsaved links.
- Multiple URLs are recorded sequentially.
- 240p, 360p, 480p, 540p, 720p and 1080p presets are supported as recording viewport/video sizes.
- Download, preview, delete, and bulk-delete B2 recordings remain available.

## Render
Build:
`npm install && PLAYWRIGHT_BROWSERS_PATH=0 npx playwright install chromium`

Start:
`npm start`

Health:
`/health`
