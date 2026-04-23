# MMG Agent — React (Vite) front end

The main UI is a React 19 + TypeScript + Vite + Tailwind app. Flask serves the production build from `client/dist` when that folder exists; otherwise it falls back to `templates/index.html` (vanilla Jinja/JS).

## Development

1. From the **repository root** (where `flask_app.py` lives), start Flask (e.g. `python flask_app.py`, default port often `8502`).

2. In another shell:

   ```bash
   cd client
   npm install
   npm run dev
   ```

   Vite runs on `http://127.0.0.1:5173` and proxies `/api` to the Flask app.

## Production build

```bash
cd client
npm ci
npm run build
```

This writes `client/dist/`. On the next request, `GET /` serves the React `index.html` and `/assets/...` serves hashed JS/CSS from `client/dist/assets`.

`render.yaml` and the root `Dockerfile` run this build as part of deploy.

## Note on Streamlit

`app.py` in the repository root is a legacy **Streamlit** app; the deployed product uses **Flask** + this React UI (or the Jinja template fallback).
