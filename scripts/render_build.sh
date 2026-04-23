#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Install Node 22+ if missing (Render Python build may not include npm)
if ! command -v npm >/dev/null 2>&1; then
  if command -v apt-get >/dev/null 2>&1; then
    curl -fsSL https://deb.nodesource.com/setup_22.x | bash -
    apt-get update && apt-get install -y nodejs
  else
    echo "npm not found and apt-get unavailable; install Node.js 22+ in the build environment" >&2
    exit 1
  fi
fi

( cd client && npm ci && npm run build )
pip install -r requirements.txt
playwright install --with-deps chromium
