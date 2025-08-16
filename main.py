import os
import time
import logging
from typing import Optional, List

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, PlainTextResponse, JSONResponse
from pydantic import BaseModel
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# Optional: only used by /diag
from supabase import create_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("main")

app = FastAPI(title="Quartr Loader", version="2.5 (watchdog + bounded timeouts)")

# ------------------ Environment ------------------
QUARTR_EMAIL = os.getenv("QUARTR_EMAIL")
QUARTR_PASSWORD = os.getenv("QUARTR_PASSWORD")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET", "earnings")

# Global runtime cap for /backfill (seconds)
BACKFILL_MAX_SECONDS = int(os.getenv("BACKFILL_MAX_SECONDS", "120"))
# Default Playwright timeout per operation (ms)
PW_DEFAULT_TIMEOUT_MS = int(os.getenv("PW_DEFAULT_TIMEOUT_MS", "15000"))

def _require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v

def _sb_client():
    url = _require_env("SUPABASE_URL")
    key = _require_env("SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)

# ------------------ Debug helpers & endpoints ------------------
def _save_png(page, tag: str) -> str:
    """Save full-page PNG to /tmp and log a direct URL you can click."""
    fname = f"debug_{tag}_{int(time.time())}.png"
    path = f"/tmp/{fname}"
    try:
        page.screenshot(path=path, full_page=True)
        logger.error("Saved debug PNG: %s", f"/debug/snap/{fname}")
    except Exception as e:
        logger.error("Failed to save PNG: %s", e)
    return fname

def _save_html(page, tag: str) -> str:
    """Save HTML to /tmp and log a direct URL you can click."""
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

@app.get("/debug/latest")
def debug_latest():
    try:
        files = sorted(
            [f for f in os.listdir("/tmp") if f.endswith(".png")],
            key=lambda x: os.path.getmtime(os.path.join("/tmp", x)),
            reverse=True,
        )
        if not files:
            return JSONResponse({"error": "no screenshots yet"}, status_code=404)
        fname = files[0]
        return FileResponse(os.path.join("/tmp", fname), media_type="image/png")
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

# ------------------ Login (robust, frames-aware) ------------------
def login_keycloak(page, email: str, password: str):
    """
    Robust Keycloak login with bounded waits and screenshots on failure.
    """
    page.set_default_timeout(PW_DEFAULT_TIMEOUT_MS)

    def _link_png(tag):
        fname = _save_png(page, tag)
        return f"/debug/snap/{fname}"

    def _link_html(tag):
        fname = _save_html(page, tag)
        return f"/debug/html/{fname}"

    logger.info("LOGIN: navigate to app")
    page.goto("https://web.quartr.com/", wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")

    if "web.quartr.com" in page.url and "auth.quartr.com" not in page.url:
        logger.info("LOGIN: already authenticated")
        return

    page.wait_for_timeout(600)  # let SPA redirect

    def _dismiss_cookies(doc):
        try:
            for txt in ["Accept", "Agree", "Allow all", "OK", "I agree", "Accept all cookies"]:
                btn = doc.get_by_role("button", name=txt, exact=False)
                if btn and btn.count():
                    btn.first.click()
                    doc.wait_for_timeout(200)
                    break
        except Exception:
            pass

    def _press_submit(doc) -> bool:
        for sel in ["#kc-login", "button#kc-login", "button[name='login']", "button[type='submit']", "input[type='submit']"]:
            loc = doc.locator(sel)
            if loc and loc.count():
                try:
                    with doc.expect_navigation(wait_until="load", timeout=PW_DEFAULT_TIMEOUT_MS):
                        loc.first.click()
                except Exception:
                    loc.first.click()
                doc.wait_for_load_state("networkidle")
                return True
        for txt in ["Next", "Continue", "Continue with Email", "Sign in", "Sign In", "Log in", "Log In"]:
            loc = doc.get_by_role("button", name=txt, exact=False)
            if loc and loc.count():
                try:
                    with doc.expect_navigation(wait_until="load", timeout=PW_DEFAULT_TIMEOUT_MS):
                        loc.first.click()
                except Exception:
                    loc.first.click()
                doc.wait_for_load_state("networkidle")
                return True
        try:
            doc.keyboard.press("Enter")
            doc.wait_for_load_state("networkidle")
            return True
        except Exception:
            return False

    def _fill_text(doc, value, candidates) -> bool:
        # CSS selectors first
        for sel in candidates:
            try:
                loc = doc.locator(sel)
                if loc and loc.count():
                    for i in range(min(loc.count(), 5)):
                        el = loc.nth(i)
                        if el.is_visible():
                            el.click()
                            el.fill(value)
                            return True
            except Exception:
                continue
        # Fallbacks via label / placeholder / role
        if value == email:
            fallbacks = [
                doc.get_by_label("Email", exact=False),
                doc.get_by_label("Username", exact=False),
                doc.get_by_placeholder("Email"),
                doc.get_by_placeholder("Username"),
                doc.get_by_role("textbox", name="Email", exact=False),
            ]
        else:
            fallbacks = [
                doc.get_by_label("Password", exact=False),
                doc.get_by_placeholder("Password"),
                doc.get_by_role("textbox", name="Password", exact=False),
            ]
        for loc in fallbacks:
            try:
                if loc and loc.count():
                    vis = loc.first
                    if vis.is_visible():
                        vis.click()
                        vis.fill(value)
                        return True
            except Exception:
                continue
        return False

    def _maybe_continue_email(doc):
        for nm in ["Continue with Email", "Continue", "Email"]:
            try:
                b = doc.get_by_role("button", name=nm, exact=False)
                if b and b.count() and b.first.is_visible():
                    b.first.click()
                    doc.wait_for_timeout(250)
                    return
            except Exception:
                continue

    def _attempt(doc) -> bool:
        try:
            doc.wait_for_selector("input,button,form", timeout=PW_DEFAULT_TIMEOUT_MS)
        except Exception:
            return False

        _dismiss_cookies(doc)
        _maybe_continue_email(doc)

        # Email phase
        email_candidates = [
            "#username", "input#username", "input#email",
            "input[name='username']", "input[name='email']",
            "input[type='email']", "input[autocomplete='username']",
            "input[placeholder*='email' i]", "input[placeholder*='username' i]",
        ]
        if _fill_text(doc, email, email_candidates):
            _press_submit(doc)
            if "web.quartr.com" in page.url and "auth.quartr.com" not in page.url:
                return True

        # Password phase
        doc.wait_for_timeout(400)
        pw_candidates = [
            "#password", "input#password", "input[name='password']",
            "input[type='password']", "input[autocomplete='current-password']",
            "input[placeholder*='password' i]",
        ]
        if _fill_text(doc, password, pw_candidates):
            _press_submit(doc)
            if "web.quartr.com" in page.url and "auth.quartr.com" not in page.url:
                return True

        # Maybe email-only redirect happened
        return "web.quartr.com" in page.url and "auth.quartr.com" not in page.url

    logger.info("LOGIN: attempt on main page")
    if _attempt(page):
        logger.info("LOGIN: success on main page")
        return

    logger.info("LOGIN: attempt in iframes")
    frame_urls = []
    for fr in page.frames:
        try:
            frame_urls.append(fr.url)
            if _attempt(fr):
                logger.info("LOGIN: success in iframe")
                return
        except Exception:
            continue

    # Failure diagnostics
    png = _link_png("login_fail")
    html = _link_html("login_fail")
    raise RuntimeError(
        f"Keycloak login failed. URL: {page.url} | Frames tried: {frame_urls} | "
        f"Screenshot: {png} | HTML: {html}"
    )

# ------------------ Company search (press '/' first) ------------------
def open_company(page, ticker: str):
    """
    Press '/', type ticker, Enter, then click a matching result.
    Fallbacks: visible search inputs and direct search route.
    """
    page.set_default_timeout(PW_DEFAULT_TIMEOUT_MS)
    t = ticker.upper()
    logger.info("OPEN: go home")
    page.goto("https://web.quartr.com/home", wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(200)

    logger.info("OPEN: use '/' hotkey then Enter")
    try:
        page.keyboard.press("/")
        page.wait_for_timeout(100)
        page.keyboard.type(t)
        page.keyboard.press("Enter")
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(300)
    except Exception:
        pass

    def _click_first_match(ctx) -> bool:
        candidates = [
            ctx.get_by_role("link", name=t, exact=False),
            ctx.get_by_role("button", name=t, exact=False),
            ctx.locator(f"a:has-text('{t}')"),
            ctx.locator(f"button:has-text('{t}')"),
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
        try:
            el = ctx.get_by_text(t, exact=False).first
            el.locator("xpath=ancestor-or-self::*[self::a or self::button][1]").first.click()
            ctx.wait_for_load_state("networkidle")
            return True
        except Exception:
            return False

    if _click_first_match(page):
        return

    logger.info("OPEN: try visible search inputs")
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
                page.wait_for_timeout(300)
                if _click_first_match(page):
                    return
            except Exception:
                continue

    logger.info("OPEN: try direct /search route")
    try:
        page.goto(f"https://web.quartr.com/search?q={t}", wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle")
        if _click_first_match(page):
            return
    except Exception:
        pass

    png = _save_png(page, f"open_company_fail_{t}")
    html = _save_html(page, f"open_company_fail_{t}")
    raise RuntimeError(f"Could not open company from search UI. See /debug/snap/{png} and /debug/html/{html}")

# ------------------ Quarter open ------------------
def open_quarter(page, year: int, quarter: str) -> bool:
    page.set_default_timeout(PW_DEFAULT_TIMEOUT_MS)
    labels = [f"{quarter} {year}", f"{quarter} FY{year}", f"{quarter} {str(year)[-2:]}"]
    for lb in labels:
        loc = page.get_by_text(lb, exact=False)
        if loc and loc.count():
            try:
                loc.first.click()
                page.wait_for_load_state("networkidle")
                page.wait_for_timeout(300)
                _save_png(page, f"open_quarter_{year}_{quarter}")
                return True
            except Exception:
                continue
    return False

# ------------------ Models ------------------
class BackfillRequest(BaseModel):
    ticker: str
    start_year: int
    end_year: int
    start_q: str = "Q1"
    end_q: str = "Q4"

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
        "BACKFILL_MAX_SECONDS": BACKFILL_MAX_SECONDS,
        "PW_DEFAULT_TIMEOUT_MS": PW_DEFAULT_TIMEOUT_MS,
    }

@app.get("/diag")
def diag():
    try:
        sb = _sb_client()
        items: List[dict] = sb.storage.from_(SUPABASE_BUCKET).list() or []
        names = [i.get("name") for i in items][:50]
        return {"ok": True, "bucket": SUPABASE_BUCKET, "count": len(items), "sample": names}
    except Exception as e:
        logger.exception("Diag failed")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.post("/backfill")
def backfill(req: BackfillRequest):
    if not QUARTR_EMAIL or not QUARTR_PASSWORD:
        raise HTTPException(status_code=500, detail="Missing QUARTR_EMAIL or QUARTR_PASSWORD")

    def qn(q: str) -> int:
        return int(q.replace("Q", ""))

    start = time.monotonic()

    def _check_watchdog(step: str):
        elapsed = time.monotonic() - start
        if elapsed > BACKFILL_MAX_SECONDS:
            # last snapshot for context
            try:
                _save_png(page, f"watchdog_timeout_{int(elapsed)}s")
            except Exception:
                pass
            raise HTTPException(
                status_code=504,
                detail=f"Backfill exceeded {BACKFILL_MAX_SECONDS}s at step: {step}"
            )

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"],
            )
            page = browser.new_page()
            page.set_default_timeout(PW_DEFAULT_TIMEOUT_MS)

            logger.info("STEP 1: login")
            login_keycloak(page, QUARTR_EMAIL, QUARTR_PASSWORD)
            _check_watchdog("login")

            logger.info("STEP 2: open company")
            open_company(page, req.ticker)
            _check_watchdog("open_company")

            logger.info("STEP 3: iterate quarters")
            start_qn = qn(req.start_q)
            end_qn = qn(req.end_q)
            for year in range(req.start_year, req.end_year + 1):
                q_from = start_qn if year == req.start_year else 1
                q_to   = end_qn   if year == req.end_year   else 4
                for qi in range(q_from, q_to + 1):
                    qlabel = f"Q{qi}"
                    ok = open_quarter(page, year, qlabel)
                    _check_watchdog(f"open_quarter {year} {qlabel}")
                    if not ok:
                        _save_png(page, f"open_quarter_fail_{req.ticker}_{year}_{qlabel}")
                        continue
                    # TODO: add download/upload logic here (bounded waits, expect_download with 15s timeout)

            return {"status": "ok"}

    except HTTPException:
        # watchdog already produced a clean HTTP error
        raise
    except PWTimeoutError as e:
        logger.exception("Playwright timeout")
        raise HTTPException(status_code=504, detail=f"Playwright timeout: {e}")
    except Exception as e:
        logger.exception("Backfill failed")
        raise HTTPException(status_code=500, detail=f"Backfill failed: {e}")
    finally:
        # Always try to close the browser to avoid zombie processes
        try:
            browser.close()
        except Exception:
            pass