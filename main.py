import os
import time
import logging
from typing import Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from playwright.sync_api import sync_playwright
from supabase import create_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()


# ------------------------------
# Supabase client creation
# ------------------------------
def supabase_client_server():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set")
    return create_client(url, key)


# ------------------------------
# Robust login
# ------------------------------
def login(page, email: str, password: str):
    """
    Robust login for Quartr:
    - Tries multiple login URLs (override via QUARTR_LOGIN_URL)
    - Handles cookie banners
    - Finds email/password by placeholder, type, label, or role
    - Searches inside iframes
    """
    login_urls = [
        os.getenv("QUARTR_LOGIN_URL") or "https://app.quartr.com/login",
        "https://quartr.com/login",
        "https://app.quartr.com/sign-in",
    ]

    old_timeout = page.get_default_timeout()
    page.set_default_timeout(45000)

    def _dismiss_cookies(p):
        try:
            for text in ["Accept", "I agree", "Agree", "Accept all", "Allow all"]:
                btn = p.get_by_role("button", name=text, exact=False)
                if btn and btn.count():
                    btn.first.click()
                    p.wait_for_timeout(500)
                    break
        except Exception:
            pass

    def _fill_in_login_on(p) -> bool:
        selectors_email = [
            "input[placeholder*='email' i]",
            "input[type='email']",
            "input[name*='email' i]"
        ]
        selectors_pass = [
            "input[placeholder*='password' i]",
            "input[type='password']",
            "input[name*='password' i]"
        ]

        candidates_email = [
            p.get_by_placeholder("Email"),
            p.get_by_label("Email", exact=False),
            p.get_by_role("textbox", name="Email", exact=False),
        ]
        candidates_pass = [
            p.get_by_placeholder("Password"),
            p.get_by_label("Password", exact=False),
            p.get_by_role("textbox", name="Password", exact=False),
        ]

        for sel in selectors_email:
            loc = p.locator(sel)
            if loc.count():
                loc.first.fill(email)
                break
        else:
            for loc in candidates_email:
                if loc and loc.count():
                    loc.first.fill(email)
                    break
            else:
                return False

        for sel in selectors_pass:
            loc = p.locator(sel)
            if loc.count():
                loc.first.fill(password)
                break
        else:
            for loc in candidates_pass:
                if loc and loc.count():
                    loc.first.fill(password)
                    break
            else:
                return False

        buttons = [
            p.get_by_role("button", name="Log in", exact=False),
            p.get_by_role("button", name="Sign in", exact=False),
            p.get_by_role("button", name="Sign In", exact=False),
            p.locator("button[type='submit']"),
            p.locator("input[type='submit']"),
        ]
        for b in buttons:
            if b and b.count():
                b.first.click()
                p.wait_for_load_state("networkidle")
                return True

        try:
            p.keyboard.press("Enter")
            p.wait_for_load_state("networkidle")
            return True
        except Exception:
            return False

    def _try_on_page(p) -> bool:
        _dismiss_cookies(p)
        if _fill_in_login_on(p):
            return True

        try:
            for frame in p.frames:
                if frame == p.main_frame:
                    continue
                _dismiss_cookies(frame)
                if _fill_in_login_on(frame):
                    return True
        except Exception:
            pass
        return False

    last_url_err: Optional[str] = None
    for url in login_urls:
        try:
            page.goto(url, wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle")
            if _try_on_page(page):
                page.wait_for_load_state("networkidle")
                page.wait_for_timeout(800)
                page.set_default_timeout(old_timeout)
                return
        except Exception as e:
            last_url_err = f"{type(e).__name__}: {e}"
            continue

    try:
        path = f"/tmp/login_failure_{int(time.time())}.png"
        page.screenshot(path=path, full_page=True)
        logger.error("Login failed. Saved screenshot: %s", path)
    except Exception:
        pass

    page.set_default_timeout(old_timeout)
    raise RuntimeError(f"Unable to locate login form fields. Last error: {last_url_err or 'none'}")


# ------------------------------
# Request model
# ------------------------------
class BackfillRequest(BaseModel):
    ticker: str
    start_year: int
    end_year: int
    start_q: str
    end_q: str


# ------------------------------
# Routes
# ------------------------------
@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/backfill")
def backfill(req: BackfillRequest):
    email = os.getenv("QUARTR_EMAIL")
    password = os.getenv("QUARTR_PASSWORD")
    if not email or not password:
        raise HTTPException(status_code=500, detail="QUARTR_EMAIL and QUARTR_PASSWORD must be set")

    try:
        SB = supabase_client_server()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Supabase init failed: {e}")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
            ctx = browser.new_context(
                accept_downloads=True,
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                           "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
                viewport={"width": 1366, "height": 900},
                locale="en-US",
            )
            page = ctx.new_page()
            login(page, email, password)

            # Placeholder for actual backfill logic
            logger.info(f"Backfill for {req.ticker} {req.start_year} {req.start_q} → {req.end_year} {req.end_q}")
            browser.close()

        return {"status": "ok", "ticker": req.ticker}
    except Exception as e:
        logger.exception("Backfill failed")
        raise HTTPException(status_code=500, detail=f"Unhandled error: {e}")
