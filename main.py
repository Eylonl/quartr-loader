import os
import time
import logging
from typing import Optional, List

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, PlainTextResponse, JSONResponse
from pydantic import BaseModel
from playwright.sync_api import sync_playwright

# Supabase diag support
from supabase import create_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("main")

app = FastAPI(title="Quartr Loader", version="2.2")

# ------------------ Environment ------------------
QUARTR_EMAIL = os.getenv("QUARTR_EMAIL")
QUARTR_PASSWORD = os.getenv("QUARTR_PASSWORD")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET", "earnings")

def _require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required env var: {name}")
    return val

def _sb_client():
    url = _require_env("SUPABASE_URL")
    key = _require_env("SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)

# ------------------ Debug helpers & endpoints ------------------
def _save_png(page, tag: str) -> str:
    """Save a full-page PNG to /tmp and log a direct URL you can click."""
    fname = f"debug_{tag}_{int(time.time())}.png"
    path = f"/tmp/{fname}"
    try:
        page.screenshot(path=path, full_page=True)
        logger.error("Saved debug PNG: %s", f"/debug/snap/{fname}")
    except Exception as e:
        logger.error("Failed to save PNG: %s", e)
    return fname

def _save_html(page, tag: str) -> str:
    """Save current HTML to /tmp and log a direct URL you can click."""
    fname = f"debug_{tag}_{int(time.time())}.html"
    path = f"/tmp/{fname}"
    try:
        html = page.content()
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
        logger.error("Saved debug HTML: %s", f"/debug/html/{fname}")
    except Exception as e:
        logger.error("Failed to save HTML: %s", e)
    return fname

@app.get("/debug/list_tmp")
def debug_list_tmp():
    files = [f for f in os.listdir("/tmp") if f.endswith(".png") or f.endswith(".html")]
    return {"files": files}

@app.get("/debug/snap/{fname}")
def debug_snap(fname: str):
    safe = os.path.basename(fname)
    path = f"/tmp/{safe}"
    if not os.path.exists(path):
        return JSONResponse({"error": "not found"}, status_code=404)
    return FileResponse(path, media_type="image/png")

@app.get("/debug/html/{fname}")
def debug_html(fname: str):
    safe = os.path.basename(fname)
    path = f"/tmp/{safe}"
    if not os.path.exists(path):
        return JSONResponse({"error": "not found"}, status_code=404)
    with open(path, "r", encoding="utf-8") as f:
        return PlainTextResponse(f.read(), media_type="text/html")

# ------------------ Quartr login ------------------
def login_keycloak(page, email: str, password: str):
    """
    Simple 2-step flow: fill Email -> Enter; then fill Password -> Enter.
    If already logged in, returns immediately.
    """
    page.goto("https://web.quartr.com/home", wait_until="domcontentloaded")
    page.wait_for_timeout(500)

    # already in app?
    if "web.quartr.com" in page.url and "auth" not in page.url:
        return

    # If a 'Log in' link exists, click it (some tenants)
    try:
        page.get_by_role("link", name="Log in").click()
    except Exception:
        pass

    # ---- Step 1: Email ----
    try:
        email_input = page.get_by_label("Email", exact=False)
        if email_input.count() == 0:
            email_input = page.locator("input[type='email'], #username, input[name='username'], input[name='email']")
        email_input.first.fill(email)
    except Exception:
        _save_png(page, "login_no_email_field")
        raise RuntimeError("Couldn't find email field on login page.")

    page.keyboard.press("Enter")
    page.wait_for_timeout(700)

    # ---- Step 2: Password (if prompted) ----
    try:
        pwd_input = page.get_by_label("Password", exact=False)
        if pwd_input.count() == 0:
            pwd_input = page.locator("input[type='password'], #password, input[name='password']")
        if pwd_input.count():
            pwd_input.first.fill(password)
            page.keyboard.press("Enter")
    except Exception:
        # Some tenants auto-complete or do passwordless SSO; proceed
        pass

    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(1500)

    if "web.quartr.com" not in page.url:
        _save_png(page, "login_fail")
        raise RuntimeError(f"Keycloak login failed; final URL: {page.url}")

# ------------------ Company search (robust, '/' first) ------------------
def open_company(page, ticker: str):
    """
    Quartr requires pressing '/' to focus search.
    This presses '/', types ticker, Enter, waits for results containers,
    then clicks the first result mentioning the ticker.
    Falls back to visible search inputs and a direct search URL.
    Dumps PNG + HTML on failure.
    """
    t = ticker.upper()

    # Always start from home
    page.goto("https://web.quartr.com/home", wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(250)

    # Strategy A: '/' hotkey first
    try:
        page.keyboard.press("/")
        page.wait_for_timeout(120)
        page.keyboard.type(t)
        page.keyboard.press("Enter")
        page.wait_for_load_state("networkidle")
    except Exception:
        pass

    # Wait for any plausible results container
    possible_containers = [
        "role=listbox",
        "role=list",
        "role=grid",
        "section:has-text('Search')",
        "[data-testid*='search']",
        "div[role='navigation'] >> .. >> div:has-text('Search')",
    ]
    for sel in possible_containers:
        try:
            page.wait_for_selector(sel, timeout=1500)
            break
        except Exception:
            continue  # not fatal

    # Try clicking a matching result
    def _click_first_match(ctx) -> bool:
        # Try common link/button/card patterns containing the ticker
        candidates = [
            ctx.get_by_role("link", name=t, exact=False),
            ctx.get_by_role("button", name=t, exact=False),
            ctx.locator(f"a:has-text('{t}')"),
            ctx.locator(f"button:has-text('{t}')"),
            ctx.locator(f"[data-testid*='result']:has-text('{t}')"),
            ctx.locator(f"[data-testid*='company']:has-text('{t}')"),
            ctx.locator(f"[class*='card']:has-text('{t}')"),
            ctx.locator(f"text={t}"),
        ]
        for loc in candidates:
            try:
                if loc and loc.count():
                    loc.first.click()
                    ctx.wait_for_load_state("networkidle")
                    return True
            except Exception:
                continue
        # As a last resort: clickable ancestor of any text match
        generic = ctx.get_by_text(t, exact=False)
        if generic and generic.count():
            try:
                el = generic.first
                el.locator("xpath=ancestor-or-self::*[self::a or self::button][1]").first.click()
                ctx.wait_for_load_state("networkidle")
                return True
            except Exception:
                pass
        return False

    if _click_first_match(page):
        return

    # Strategy B: search inputs
    search_boxes = [
        page.get_by_placeholder("Search"),
        page.get_by_role("combobox", name="Search", exact=False),
        page.locator("input[type='search']"),
        page.locator("input[placeholder*='Search' i]"),
        page.locator("input[aria-label*='Search' i]"),
    ]
    for sb in search_boxes:
        if sb.count():
            try:
                sb.first.click()
                sb.first.fill(t)
                page.keyboard.press("Enter")
                page.wait_for_load_state("networkidle")
                page.wait_for_timeout(500)
                if _click_first_match(page):
                    return
            except Exception:
                continue

    # Strategy C: direct route
    try:
        page.goto(f"https://web.quartr.com/search?q={t}", wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle")
        if _click_first_match(page):
            return
    except Exception:
        pass

    # Debug artifacts
    png = _save_png(page, f"open_company_fail_{t}")
    html = _save_html(page, f"open_company_fail_{t}")
    raise RuntimeError(f"Could not open company from search UI. See /debug/snap/{png} and /debug/html/{html}")

# ------------------ Models ------------------
class BackfillRequest(BaseModel):
    ticker: str
    extra: Optional[str] = None

# ------------------ Routes ------------------
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/envcheck")
def envcheck():
    return {
        "QUARTR_EMAIL": bool(QUARTR_EMAIL),
        "QUARTR_PASSWORD": bool(QUARTR_PASSWORD),
        "SUPABASE_URL": bool(SUPABASE_URL),
        "SUPABASE_SERVICE_ROLE_KEY": bool(SUPABASE_SERVICE_ROLE_KEY),
        "SUPABASE_BUCKET": SUPABASE_BUCKET,
    }

@app.get("/diag")
def diag():
    """
    Checks Supabase connectivity and lists the first-level objects in the bucket.
    """
    try:
        sb = _sb_client()
        bucket = SUPABASE_BUCKET
        items: List[dict] = sb.storage.from_(bucket).list() or []
        names = [i.get("name") for i in items][:50]
        return {"ok": True, "bucket": bucket, "count": len(items), "sample": names}
    except Exception as e:
        logger.exception("Diag failed")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.post("/backfill")
def backfill(req: BackfillRequest):
    if not QUARTR_EMAIL or not QUARTR_PASSWORD:
        raise HTTPException(status_code=500, detail="Missing QUARTR_EMAIL or QUARTR_PASSWORD")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"],
            )
            page = browser.new_page()

            # Login and take a screenshot you can click from logs
            login_keycloak(page, QUARTR_EMAIL, QUARTR_PASSWORD)
            _save_png(page, "after_login_home")

            # Go to company page (now uses '/' first, waits for containers, many selectors)
            open_company(page, req.ticker)

            # TODO: add your downloads & Supabase uploads here.

            browser.close()
            return {"status": "ok"}
    except Exception as e:
        logger.exception("Backfill failed")
        raise HTTPException(status_code=500, detail=f"Backfill failed: {e}")