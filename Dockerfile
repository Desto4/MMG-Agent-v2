# ── Build React (Vite) client ──────────────────────────────────────────────
FROM node:22-bookworm-slim AS client_build
WORKDIR /client
COPY client/package.json client/package-lock.json ./
RUN npm ci
COPY client/ ./
RUN npm run build

# ── Python app + Playwright ────────────────────────────────────────────────
FROM mcr.microsoft.com/playwright/python:v1.40.0-jammy
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
RUN playwright install chromium
COPY . .
# Production UI from Vite
COPY --from=client_build /client/dist ./client/dist
EXPOSE 8502
CMD gunicorn flask_app:app --bind 0.0.0.0:$PORT --timeout 600 --graceful-timeout 60 --workers 1 --threads 4
