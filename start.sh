#!/bin/bash

echo "🚀 Starting MMG Agent (Flask)..."

# Install / update Python dependencies
pip3 install -q flask anthropic requests playwright

# Ensure Playwright's Chromium browser is installed (needed for Sunbiz + Google Maps)
python3 -m playwright install chromium --quiet 2>/dev/null || true

# Kill any existing processes on port 8501
lsof -ti:8501 | xargs kill -9 2>/dev/null

# Start Flask in the background
python3 flask_app.py &
FLASK_PID=$!
echo "Flask PID: $FLASK_PID"

# Wait for Flask to start
sleep 2
echo "✅ Flask running at http://localhost:8501"

# Start ngrok tunnel
echo "🌐 Starting ngrok tunnel..."
ngrok http 8501 --log=stdout
