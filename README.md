# Quartr Loader (FastAPI + Playwright)
Scrapes Quartr (press release, presentation, transcript PDFs), extracts text, and writes to Supabase.

## Deploy on Railway (recommended)
1. Create a new project → **Deploy from GitHub** → select this `loader/` repo.
2. Railway auto-detects Dockerfile. Set env vars:
   - `QUARTR_EMAIL`, `QUARTR_PASSWORD`
   - `SUPABASE_URL`
   - `SUPABASE_SERVICE_ROLE_KEY` (server-side only)
   - `SUPABASE_BUCKET=earnings`
3. Deploy → copy your public URL (e.g., `https://quartr-loader.up.railway.app`).

## API
POST `/backfill`
```json
{ "ticker":"PCOR", "start_year":2025, "end_year":2025, "start_q":"Q1", "end_q":"Q1" }
```
