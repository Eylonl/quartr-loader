import os
import time
import logging
import traceback
from typing import Optional, Tuple

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, PlainTextResponse, JSONResponse
from pydantic import BaseModel
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# Optional: comment these out if you’re not using Supabase in this service yet
from supabase import create_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("uvicorn.error")

app = FastAPI(title="Quartr Loader", version="2.0 (envcheck + debug + 2-step login)")


# ------------------------------
# Env / Supabase helpers
# ------------------------------
def require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required env var: {name}")
    return val


def supabase_client_server():
    # Only used by /diag (safe no-op if you haven’t created table/bucket yet)
    url = require_env("SUPABASE_URL")
    key = require_env("SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)


def bucket_name() -> str:
    return os.getenv("SUPABASE_BUCKET", "earnings")


# ------------------------------
# Debug helpers & endpoints
# ------------------------------
def _save_png(page, tag: str) -> str:
    fname = f"debug_{tag}_{int(time.time())}.png"
    path = f"/tmp/{fname}"
    try:
        page.screenshot(path=path, full_page=True)
        logger.error("Saved debug PNG: %s", path)
    except Exception as e:
        logger.error("Failed to save PNG: %s", e)
    return fname


def _save_html(page, tag: str) -> str:
    fname = f"debug_{tag}_{int(time.time())}.html"
    path = f"/tmp/{fname}"
    try:
        html = page.content()
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
        logger.error("Saved debug HTML: %s", path)
    except Exception as e:
        logger.error("Failed to save HTML: %s", e)
    return fname


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


@app.get("/debug/list_tmp")
def debug_list_tmp():
    files = [f for f in os.listdir("/tmp") if f.endswith(".png") or f.endswith(".html")]
    return {"files": files}


# ------------------------------
# Keycloak login (robust 2-step)
# ------------------------------
def login_keycloak(page, email: str, password: str):
    """
    Step 1: enter email -> Next/Continue (or Enter)
    Step 2: enter password (if shown) -> Log in/Sign in (or Enter)
    Works on main page and iframes. Dumps screenshot+HTML on failure.
    """
    page.set_default_timeout(60000)

    def _dismiss_cookies(doc):
        try:
            for txt in ["Accept", "Agree", "Allow all", "OK", "I agree", "Accept all cookies"]:
                btn = doc.get_by_role("button", name=txt, exact=False)
                if btn and btn.count():
                    btn.first.click()
                    doc.wait_for_timeout(250)
                    break
        except Exception:
            pass

    def _press_submit(doc) -> bool:
        buttons = [
            "#kc-login", "button#kc-login", "button[name='login']",
            "button[type='submit']", "input[type='submit']",
        ]
        text_buttons = [
            "Next", "Continue", "Continue with Email",
            "Sign in", "Sign In", "Log in", "Log In",
            "Proceed",
        ]
        for sel in buttons:
            loc = doc.locator(sel)
            if loc and loc.count():
                try:
                    with doc.expect_navigation(wait_until="load", timeout=20000):
                        loc.first.click()
                except Exception:
                    loc.first.click()
                doc.wait_for_load_state("networkidle")
                doc.wait_for_timeout(400)
                return True
        for txt in text_buttons:
            loc = doc.get_by_role("button", name=txt, exact=False)
            if loc and loc.count():
                try:
                    with doc.expect_navigation(wait_until="load", timeout=20000):
                        loc.first.click()
                except Exception:
                    loc.first.click()
                doc.wait_for_load_state("networkidle")
                doc.wait_for_timeout(400)
                return True
        try:
            doc.keyboard.press("Enter")
            doc.wait_for_load_state("networkidle")
            doc.wait_for_timeout(300)
            return True
        except Exception:
            return False

    def _fill_email_then_submit(doc) -> bool:
        _dismiss_cookies(doc)
        # Some themes require clicking an intermediate "Continue with Email"
        try:
            for txt in ["Continue with Email", "Continue", "Sign in with email", "Email"]:
                b = doc.get_by_role("button", name=txt, exact=False)
                if b and b.count():
                    b.first.click()
                    doc.wait_for_timeout(300)
                    break
        except Exception:
            pass

        email_sels = [
            "#username", "input#email",
            "input[name='username']", "input[name='email']",
            "input[type='email']", "input[autocomplete='username']",
            "input[placeholder*='email' i]", "input[placeholder*='username' i]",
        ]
        filled = False
        for sel in email_sels:
            loc = doc.locator(sel)
            if loc and loc.count():
                try:
                    loc.first.click()
                    loc.first.fill(email)
                    filled = True
                    break
                except Exception:
                    pass
        if not filled:
            for loc in (
                doc.get_by_label("Email", exact=False),
                doc.get_by_label("Username", exact=False),
                doc.get_by_placeholder("Email"),
                doc.get_by_placeholder("Username"),
                doc.get_by_role("textbox", name="Email", exact=False),
            ):
                if loc and loc.count():
                    try:
                        loc.first.click()
                        loc.first.fill(email)
                        filled = True
                        break
                    except Exception:
                        pass
        if not filled:
            return False

        _press_submit(doc)
        return True

    def _fill_password_then_submit(doc) -> bool:
        pass_sels = [
            "#password", "input[name='password']",
            "input[type='password']",
            "input[autocomplete='current-password']",
            "input[placeholder*='password' i]",
        ]
        for sel in pass_sels:
            loc = doc.locator(sel)
            if loc and loc.count():
                try:
                    loc.first.click()
                    loc.first.fill(password)
                    _press_submit(doc)
                    return True
                except Exception:
                    pass
        for loc in (
            doc.get_by_label("Password", exact=False),
            doc.get_by_placeholder("Password"),
        ):
            if loc and loc.count():
                try:
                    loc.first.click()
                    loc.first.fill(password)
                    _press_submit(doc)
                    return True
                except Exception:
                    pass
        return False

    def _try_login_in(doc) -> bool:
        try:
            doc.wait_for_selector("input,button,form", timeout=15000)
        except Exception:
            return False
        _fill_email_then_submit(doc)
        if "web.quartr.com" in page.url and "auth.quartr.com" not in page.url:
            return True
        doc.wait_for_timeout(700)
        _fill_password_then_submit(doc)
        return ("web.quartr.com" in page.url) and ("auth.quartr.com" not in page.url)

    # Start at the app and let it redirect to live Keycloak URL
    page.goto("https://web.quartr.com/", wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")
    _dismiss_cookies(page)

    if "web.quartr.com" in page.url and "auth.quartr.com" not in page.url:
        return

    if _try_login_in(page):
        return

    try:
        for fr in page.frames:
            if fr == page.main_frame:
                continue
            if _try_login_in(fr):
                return
    except Exception:
        pass

    png = _save_png(page, "login_fail")
    html = _save_html(page, "login_fail")
    raise RuntimeError(
        f"Login failed; URL: {page.url}. "
        f"Screenshot: /debug/snap/{png} , HTML: /debug/html/{html}"
    )


# ------------------------------
# Minimal company-open (you can replace with your full flow later)
# ------------------------------
def open_company(page, ticker: str):
    """
    Very basic search → open first result that matches ticker.
    Expand selectors later based on your UI screenshot/HTML if needed.
    """
    # Try a few likely search inputs
    cand = [
        page.get_by_placeholder("Search"),
        page.locator("input[type='search']"),
        page.locator("input[placeholder*='Search' i]"),
    ]
    for sb in cand:
        if sb and sb.count():
            sb.first.click()
            sb.first.fill(ticker)
            page.keyboard.press("Enter")
            page.wait_for_load_state("networkidle")
            page.wait_for_timeout(1200)
            # click a link mentioning ticker
            link = page.get_by_role("link", name=ticker.upper(), exact=False)
            if link and link.count():
                link.first.click()
                page.wait_for_load_state("networkidle")
                return
    raise RuntimeError("Could not open company from search UI.")


# ------------------------------
# Models & routes
# ------------------------------
class BackfillRequest(BaseModel):
    ticker: str
    start_year: Optional[int] = None
    end_year: Optional[int] = None
    start_q: Optional[str] = None
    end_q: Optional[str] = None


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/envcheck")
def envcheck():
    present = lambda k: bool(os.getenv(k))
    return {
        "QUARTR_EMAIL": present("QUARTR_EMAIL"),
        "QUARTR_PASSWORD": present("QUARTR_PASSWORD"),
        "SUPABASE_URL": present("SUPABASE_URL"),
        "SUPABASE_SERVICE_ROLE_KEY": present("SUPABASE_SERVICE_ROLE_KEY"),
        "SUPABASE_BUCKET": os.getenv("SUPABASE_BUCKET", "earnings"),
    }


@app.get("/diag")
def diag():
    # Basic Supabase connectivity check (only if env is set)
    try:
        sb = supabase_client_server()
        bucket = bucket_name()
        entries = sb.storage.from_(bucket).list()
        return {"ok": True, "bucket": bucket, "entries": len(entries or [])}
    except Exception as e:
        logger.error("Diag failed: %s\n%s", e, traceback.format_exc())
        return {"ok": False, "error": str(e)}


@app.post("/backfill")
def backfill(req: BackfillRequest):
    # Pull creds from env
    try:
        email = require_env("QUARTR_EMAIL")
        password = require_env("QUARTR_PASSWORD")
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=f"Config error: {e}")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"],
            )
            ctx = browser.new_context(
                accept_downloads=True,
                user_agent=("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
                viewport={"width": 1366, "height": 900},
                locale="en-US",
            )
            page = ctx.new_page()

            # Login
            login_keycloak(page, email, password)

            # Optional: take a post-login shot for debugging once
            _save_png(page, "after_login_home")

            # Navigate to company (skeleton)
            open_company(page, req.ticker)

            ctx.close()
            browser.close()

        return {"status": "ok", "note": "Login + open company succeeded (skeleton). Expand downloads next."}

    except Exception as e:
        logger.error("Backfill failed: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Unhandled error: {e}")
