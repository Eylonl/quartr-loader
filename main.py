import os
import time
import logging
from typing import Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from playwright.sync_api import sync_playwright
import httpx

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Environment variables
QUARTR_EMAIL = os.getenv("QUARTR_EMAIL")
QUARTR_PASSWORD = os.getenv("QUARTR_PASSWORD")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET", "earnings")

if not QUARTR_EMAIL or not QUARTR_PASSWORD or not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    logger.warning("Missing one or more required environment variables.")


# ------------------ Debug helpers ------------------

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


# ------------------ Quartr login ------------------

def login_keycloak(page, email, password):
    """Log in to Quartr via Keycloak."""
    page.goto("https://web.quartr.com/home", wait_until="domcontentloaded")
    page.wait_for_timeout(500)

    # If already logged in
    if "home" in page.url and "auth" not in page.url:
        return

    try:
        page.get_by_role("link", name="Log in").click()
    except Exception:
        pass

    # Email
    email_input = page.get_by_label("Email", exact=False)
    if email_input.count() == 0:
        email_input = page.locator("input[type='email']")
    email_input.fill(email)
    page.keyboard.press("Enter")
    page.wait_for_timeout(500)

    # Password
    pwd_input = page.get_by_label("Password", exact=False)
    if pwd_input.count() == 0:
        pwd_input = page.locator("input[type='password']")
    pwd_input.fill(password)
    page.keyboard.press("Enter")
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(1500)

    if "home" not in page.url:
        _save_png(page, "login_fail")
        raise RuntimeError(f"Keycloak login failed; final URL: {page.url}")


# ------------------ Company search ------------------

def open_company(page, ticker: str):
    """
    Quartr requires pressing '/' to focus search.
    This tries that first, then falls back to clicking visible search inputs.
    Finally it clicks the first result that mentions the ticker.
    Saves a debug PNG with a clickable URL if it fails.
    """
    t = ticker.upper()

    # Always start from home
    page.goto("https://web.quartr.com/home", wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(400)

    # Strategy A: '/' hotkey
    try:
        page.keyboard.press("/")
        page.wait_for_timeout(150)
        page.keyboard.type(t)
        page.keyboard.press("Enter")
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(800)
    except Exception:
        pass

    def _click_first_match(ctx) -> bool:
        for loc in (
            ctx.get_by_role("link", name=t, exact=False),
            ctx.locator(f"a:has-text('{t}')"),
        ):
            if loc.count():
                try:
                    loc.first.click()
                    ctx.wait_for_load_state("networkidle")
                    return True
                except Exception:
                    pass
        generic = ctx.get_by_text(t, exact=False)
        if generic.count():
            try:
                el = generic.first
                try:
                    el.click()
                except Exception:
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
                page.wait_for_timeout(800)
                if _click_first_match(page):
                    return
            except Exception:
                continue

    # Strategy C: direct URL
    try:
        page.goto(f"https://web.quartr.com/search?q={t}", wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(800)
        if _click_first_match(page):
            return
    except Exception:
        pass

    fname = _save_png(page, f"open_company_fail_{t}")
    raise RuntimeError(f"Could not open company from search UI. See /debug/snap/{fname}")


# ------------------ Models ------------------

class BackfillRequest(BaseModel):
    ticker: str
    extra: Optional[str] = None


# ------------------ Routes ------------------

@app.get("/envcheck")
def envcheck():
    return {
        "QUARTR_EMAIL": bool(QUARTR_EMAIL),
        "QUARTR_PASSWORD": bool(QUARTR_PASSWORD),
        "SUPABASE_URL": bool(SUPABASE_URL),
        "SUPABASE_SERVICE_ROLE_KEY": bool(SUPABASE_SERVICE_ROLE_KEY),
        "SUPABASE_BUCKET": SUPABASE_BUCKET
    }


@app.post("/backfill")
def backfill(req: BackfillRequest):
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            login_keycloak(page, QUARTR_EMAIL, QUARTR_PASSWORD)
            _save_png(page, "after_login_home")

            open_company(page, req.ticker)

            # TODO: implement download + upload logic here

            browser.close()
            return {"status": "ok"}
    except Exception as e:
        logger.exception("Backfill failed")
        raise HTTPException(status_code=500, detail=f"Backfill failed: {e}")
