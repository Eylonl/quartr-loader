import os
import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from playwright.sync_api import sync_playwright
import httpx

# ------------- Logging -------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("main")

# ------------- Env Vars -------------
QUARTR_EMAIL = os.getenv("QUARTR_EMAIL")
QUARTR_PASSWORD = os.getenv("QUARTR_PASSWORD")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET", "earnings")

# ------------- FastAPI -------------
app = FastAPI()

# ------------- Models -------------
class BackfillRequest(BaseModel):
    ticker: str
    start_year: int
    end_year: int
    start_q: str = "Q1"
    end_q: str = "Q4"

# ------------- Helpers -------------

def _save_png(page, name: str):
    """Save debug screenshot into /debug/snap/"""
    path = f"/debug/snap/{name}.png"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        page.screenshot(path=path, full_page=True)
        logger.error(f"Saved debug PNG: {path}")
    except Exception as e:
        logger.error(f"Could not save PNG {path}: {e}")

def login_keycloak(page, email: str, password: str):
    """Logs into Quartr Keycloak screen with email+pw."""
    logger.info("Logging in...")
    page.goto(
        "https://auth.quartr.com/realms/prod/protocol/openid-connect/auth"
        "?response_type=code&client_id=web"
        "&redirect_uri=https%3A%2F%2Fweb.quartr.com%2Fapi%2Fauth%2Fcallback%2Fkeycloak"
        "&scope=openid+profile+email"
    )
    page.wait_for_selector("input#username", timeout=15000)
    page.fill("input#username", email)
    page.fill("input#password", password)
    _save_png(page, "login_filled")
    page.click("button[type=submit]")
    page.wait_for_load_state("networkidle", timeout=20000)
    if "web.quartr.com" not in page.url:
        raise RuntimeError(f"Keycloak login failed; final URL: {page.url}")
    logger.info("Login successful")
    _save_png(page, "after_login_home")

def open_company(page, ticker: str):
    """Navigates to a company page via search UI."""
    try:
        page.wait_for_selector("input[placeholder='Search']", timeout=15000)
        search = page.locator("input[placeholder='Search']")
        search.click()
        search.fill("/" + ticker)  # Quartr requires "/" prefix
        page.keyboard.press("Enter")
        page.wait_for_timeout(2000)
        _save_png(page, f"open_company_{ticker}")
    except Exception:
        _save_png(page, f"debug_open_company_fail_{ticker}")
        raise RuntimeError("Could not open company from search UI.")

def open_quarter(page, year: int, quarter: str) -> bool:
    """Try to click into quarter results (several label variants)."""
    variants = [f"{quarter} {year}", f"{quarter} FY{year}", f"{quarter} {str(year)[-2:]}"]
    for v in variants:
        try:
            loc = page.get_by_text(v, exact=False)
            if loc and loc.count():
                loc.first.click()
                page.wait_for_load_state("networkidle")
                page.wait_for_timeout(500)
                _save_png(page, f"open_quarter_{year}_{quarter}")
                return True
        except Exception:
            continue
    return False

# ------------- Routes -------------

@app.get("/health")
def health():
    return {"status": "ok"}

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
    return {"status": "ok", "message": "diagnostic endpoint"}

@app.post("/backfill")
def backfill(req: BackfillRequest):
    if not QUARTR_EMAIL or not QUARTR_PASSWORD:
        raise HTTPException(status_code=500, detail="Missing QUARTR_EMAIL or QUARTR_PASSWORD")

    def qnum(q: str) -> int:
        return int(q.replace("Q", ""))

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"],
            )
            page = browser.new_page()

            # 1) Login
            login_keycloak(page, QUARTR_EMAIL, QUARTR_PASSWORD)

            # 2) Go to company
            open_company(page, req.ticker)

            # 3) Iterate quarters
            start_qn = qnum(req.start_q)
            end_qn = qnum(req.end_q)
            for year in range(req.start_year, req.end_year + 1):
                q_from = start_qn if year == req.start_year else 1
                q_to = end_qn if year == req.end_year else 4
                for qi in range(q_from, q_to + 1):
                    qlabel = f"Q{qi}"
                    if not open_quarter(page, year, qlabel):
                        _save_png(page, f"open_quarter_fail_{req.ticker}_{year}_{qlabel}")
                        continue

                    # TODO: Insert download logic (press release, presentation, transcript)
                    logger.info(f"Would download docs for {req.ticker} {qlabel} {year}")

            browser.close()
            return {
                "status": "ok",
                "ticker": req.ticker,
                "window": {
                    "start_year": req.start_year,
                    "end_year": req.end_year,
                    "start_q": req.start_q,
                    "end_q": req.end_q,
                },
            }

    except Exception as e:
        logger.exception("Backfill failed")
        raise HTTPException(status_code=500, detail=f"Backfill failed: {e}")