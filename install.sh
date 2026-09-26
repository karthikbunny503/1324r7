#!/usr/bin/env bash
set -e
npm install
PLAYWRIGHT_BROWSERS_PATH=0 npx playwright install chromium
