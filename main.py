import os
import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from playwright.sync_api import sync_playwright

# --- Logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("main")

# --- Config ---
PW_DEFAULT_TIMEOUT_MS = 10000
QUARTR_EMAIL = os.getenv("QUARTR_EMAIL", "")
QUARTR_PASSWORD = os.getenv("QUARTR_PASSWORD", "")

DEBUG_PNG_DIR = "/debug/snap"
DEBUG_HTML_DIR = "/debug/html"
os.makedirs(DEBUG_PNG_DIR, exist_ok=True)
os.makedirs(DEBUG_HTML_DIR, exist_ok=True)

def _save_png(page, tag: str):
    path = os.path.join(DEBUG_PNG_DIR, f"{tag}.png")
    page.screenshot(path=path, full_page=True)
    logger.error(f"Saved debug PNG: {path}")
    return os.path.basename(path)

def _save_html(page, tag: str):
    path = os.path.join(DEBUG_HTML_DIR, f"{tag}.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(page.content())
    logger.error(f"Saved debug HTML: {path}")
    return os.path.basename(path)

# --- FastAPI ---
app = FastAPI()

class BackfillRequest(BaseModel):
    ticker: str
    start_year: int
    end_year: int
    start_q: str
    end_q: str

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/envcheck")
def envcheck():
    return {
        "QUARTR_EMAIL": bool(QUARTR_EMAIL),
        "QUARTR_PASSWORD": bool(QUARTR_PASSWORD),
    }

@app.get("/diag")
def diag():
    return {"status": "diag ok"}

# --- Login ---
def login_keycloak(page, email: str, password: str):
    logger.info("Logging in...")
    page.goto("https://web.quartr.com", wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")

    # Try input by id or name
    try:
        email_input = page.wait_for_selector("input#username, input[name='email']", timeout=15000)
        email_input.fill(email)
        logger.info("Filled email.")
    except Exception as e:
        _save_png(page, "login_fail")
        _save_html(page, "login_fail")
        raise RuntimeError("Could not find email input") from e

    # Password
    try:
        pw_input = page.query_selector("input[type='password']")
        if pw_input:
            pw_input.fill(password)
            logger.info("Filled password.")
    except Exception:
        pass

    # Submit
    try:
        page.keyboard.press("Enter")
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(1000)
    except Exception:
        pass

    _save_png(page, "after_login")
    _save_html(page, "after_login")

# --- Open company ---
def open_company(page, ticker: str):
    """
    Open a company in Quartr by typing "/" + ticker and Enter.
    Uses multiple fallbacks and saves debug artifacts.
    """
    page.set_default_timeout(PW_DEFAULT_TIMEOUT_MS)
    t = ticker.upper()

    def snap(tag):
        _save_png(page, tag)
        _save_html(page, tag)

    # Go home
    page.goto("https://web.quartr.com/home", wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(150)
    snap("open_home")

    # Trigger palette
    page.keyboard.press("/")
    page.wait_for_timeout(200)

    # Try input / contenteditable focus
    focused_ok = page.evaluate("""() => {
        const a = document.activeElement;
        if (!a) return false;
        const tag = a.tagName?.toLowerCase();
        const editable = a.getAttribute && a.getAttribute("contenteditable");
        return (tag === "input" || editable === "" || editable === "true");
    }""")

    if not focused_ok:
        # Try clicking placeholders
        for sb in [
            page.get_by_placeholder("Search"),
            page.locator("input[type='search']"),
            page.locator("[contenteditable=''], [contenteditable='true'], [role='textbox']"),
        ]:
            try:
                if sb and sb.count():
                    sb.first.click()
                    page.wait_for_timeout(120)
                    break
            except Exception:
                continue
    snap("after_open_palette")

    # Type ticker
    page.keyboard.type(t, delay=25)
    page.wait_for_timeout(100)
    page.keyboard.press("Enter")
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(500)
    snap(f"after_enter_{t}")

    # Wait for results
    try:
        page.wait_for_selector("[role='listbox'], [role='dialog'], div[role='list']", timeout=2000)
    except Exception:
        pass
    snap(f"after_results_{t}")

    # Click
    try:
        match = page.get_by_text(t, exact=False).first
        if match and match.is_visible():
            match.click()
            page.wait_for_load_state("networkidle")
            snap(f"clicked_{t}")
            return
    except Exception:
        pass

    # Fallback: direct search
    page.goto(f"https://web.quartr.com/search?q={t}", wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(250)
    snap(f"direct_search_{t}")

    match = page.get_by_text(t, exact=False).first
    if match and match.is_visible():
        match.click()
        page.wait_for_load_state("networkidle")
        snap(f"clicked_direct_{t}")
        return

    snap(f"open_company_fail_{t}")
    raise RuntimeError(f"Could not open company {t}")

# --- Backfill ---
@app.post("/backfill")
def backfill(req: BackfillRequest):
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page()

            login_keycloak(page, QUARTR_EMAIL, QUARTR_PASSWORD)
            open_company(page, req.ticker)

            # Placeholder: return ok
            browser.close()
        return {"status": "ok", "ticker": req.ticker}
    except Exception as e:
        logger.error("Backfill failed", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))