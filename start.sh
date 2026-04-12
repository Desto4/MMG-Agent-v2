#!/bin/bash

echo "🚀 Starting Lead Gen App..."

# Start Streamlit in the background
streamlit run app.py &
STREAMLIT_PID=$!

# Wait for Streamlit to start
sleep 3

# Start ngrok with a random temporary URL
echo "🌐 Starting ngrok tunnel..."
ngrok http 8501 --log=stdout
