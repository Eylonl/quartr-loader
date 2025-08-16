import os
import time
import logging
from typing import List

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, PlainTextResponse, JSONResponse
from pydantic import BaseModel
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# ───────────────────────── Logging / Config ─────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("main")

app = FastAPI(title="Quartr Loader", version="3.1")

QUARTR_EMAIL = os.getenv("QUARTR_EMAIL", "")
QUARTR_PASSWORD = os.getenv("QUARTR_PASSWORD", "")

# Playwright timeouts and a hard watchdog for /backfill
PW_DEFAULT_TIMEOUT_MS = int(os.getenv("PW_DEFAULT_TIMEOUT_MS", "15000"))
BACKFILL_MAX_SECONDS = int(os.getenv("BACKFILL_MAX_SECONDS", "150"))

# Where debug artifacts live (served via endpoints below)
TMP_DIR = "/tmp"

# Preferred company names when a ticker is ambiguous
PREFERRED_COMPANY_BY_TICKER = {
    "PCOR": ["Procore"],  # extend as needed, e.g., "SHOP": ["Shopify"], ...
}

# ───────────────────────── Models ─────────────────────────
class BackfillRequest(BaseModel):
    ticker: str
    start_year: int
    end_year: int
    start_q: str = "Q1"
    end_q: str = "Q4"

# ───────────────────────── Debug utils + endpoints ─────────────────────────
def _save_png(page, tag: str) -> str:
    fname = f"debug_{tag}_{int(time.time())}.png"
    path = os.path.join(TMP_DIR, fname)
    try:
        page.screenshot(path=path, full_page=True)
        logger.error("Saved debug PNG: /debug/snap/%s", fname)
    except Exception as e:
        logger.error("Failed to save PNG: %s", e)
    return fname

def _save_html(page, tag: str) -> str:
    fname = f"debug_{tag}_{int(time.time())}.html"
    path = os.path.join(TMP_DIR, fname)
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(page.content())
        logger.error("Saved debug HTML: /debug/html/%s", fname)
    except Exception as e:
        logger.error("Failed to save HTML: %s", e)
    return fname

@app.get("/debug/list_tmp")
def debug_list_tmp():
    files = [f for f in os.listdir(TMP_DIR) if f.endswith(".png") or f.endswith(".html")]
    files.sort()
    return {"files": files}

@app.get("/debug/latest")
def debug_latest():
    files = [f for f in os.listdir(TMP_DIR) if f.endswith(".png")]
    if not files:
        return JSONResponse({"error": "no screenshots yet"}, status_code=404)
    files.sort(key=lambda n: os.path.getmtime(os.path.join(TMP_DIR, n)), reverse=True)
    return FileResponse(os.path.join(TMP_DIR, files[0]), media_type="image/png")

@app.get("/debug/snap/{fname}")
def debug_snap(fname: str):
    path = os.path.join(TMP_DIR, os.path.basename(fname))
    if not os.path.exists(path):
        return JSONResponse({"error": "not found"}, status_code=404)
    return FileResponse(path, media_type="image/png")

@app.get("/debug/html/{fname}")
def debug_html(fname: str):
    path = os.path.join(TMP_DIR, os.path.basename(fname))
    if not os.path.exists(path):
        return JSONResponse({"error": "not found"}, status_code=404)
    with open(path, "r", encoding="utf-8") as f:
        return PlainTextResponse(f.read(), media_type="text/html")

# ───────────────────────── Health / Env ─────────────────────────
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/envcheck")
def envcheck():
    return {
        "QUARTR_EMAIL": bool(QUARTR_EMAIL),
        "QUARTR_PASSWORD": bool(QUARTR_PASSWORD),
        "PW_DEFAULT_TIMEOUT_MS": PW_DEFAULT_TIMEOUT_MS,
        "BACKFILL_MAX_SECONDS": BACKFILL_MAX_SECONDS,
    }

@app.get("/diag")
def diag():
    count = len([f for f in os.listdir(TMP_DIR) if f.startswith("debug_")])
    return {"ok": True, "debug_files": count}

# ───────────────────────── Login (robust, frames-aware) ─────────────────────────
def login_keycloak(page, email: str, password: str):
    page.set_default_timeout(PW_DEFAULT_TIMEOUT_MS)

    def link_png(tag): return f"/debug/snap/{_save_png(page, tag)}"
    def link_html(tag): return f"/debug/html/{_save_html(page, tag)}"

    logger.info("LOGIN: navigate to app")
    page.goto("https://web.quartr.com/", wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")

    # Already logged in?
    if "web.quartr.com" in page.url and "auth.quartr.com" not in page.url:
        logger.info("LOGIN: already authenticated")
        return

    page.wait_for_timeout(600)  # allow SPA redirect

    def dismiss_cookies(doc):
        try:
            for txt in ["Accept", "Agree", "Allow all", "OK", "I agree", "Accept all cookies"]:
                btn = doc.get_by_role("button", name=txt, exact=False)
                if btn and btn.count() and btn.first.is_visible():
                    btn.first.click()
                    doc.wait_for_timeout(150)
                    break
        except Exception:
            pass

    def press_submit(doc) -> bool:
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
        for nm in ["Next", "Continue", "Continue with Email", "Sign in", "Sign In", "Log in", "Log In"]:
            loc = doc.get_by_role("button", name=nm, exact=False)
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

    def fill_text(doc, value, candidates) -> bool:
        # CSS first
        for sel in candidates:
            loc = doc.locator(sel)
            if loc and loc.count():
                for i in range(min(loc.count(), 5)):
                    el = loc.nth(i)
                    if el.is_visible():
                        try:
                            el.click()
                            el.fill(value)
                            return True
                        except Exception:
                            continue
        # label/placeholder fallbacks
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
            if loc and loc.count():
                vis = loc.first
                if vis.is_visible():
                    try:
                        vis.click(); vis.fill(value)
                        return True
                    except Exception:
                        continue
        return False

    def maybe_continue_email(doc):
        for nm in ["Continue with Email", "Continue", "Email"]:
            try:
                b = doc.get_by_role("button", name=nm, exact=False)
                if b and b.count() and b.first.is_visible():
                    b.first.click()
                    doc.wait_for_timeout(250)
                    return
            except Exception:
                continue

    def attempt(doc) -> bool:
        try:
            doc.wait_for_selector("input,button,form", timeout=PW_DEFAULT_TIMEOUT_MS)
        except Exception:
            return False

        dismiss_cookies(doc)
        maybe_continue_email(doc)

        email_candidates = [
            "#username", "input#username", "input#email",
            "input[name='username']", "input[name='email']",
            "input[type='email']", "input[autocomplete='username']",
            "input[placeholder*='email' i]", "input[placeholder*='username' i]",
        ]
        if fill_text(doc, email, email_candidates):
            press_submit(doc)
            if "web.quartr.com" in page.url and "auth.quartr.com" not in page.url:
                return True

        doc.wait_for_timeout(400)
        pw_candidates = [
            "#password", "input#password", "input[name='password']",
            "input[type='password']", "input[autocomplete='current-password']",
            "input[placeholder*='password' i]",
        ]
        if fill_text(doc, password, pw_candidates):
            press_submit(doc)
            if "web.quartr.com" in page.url and "auth.quartr.com" not in page.url:
                return True

        return "web.quartr.com" in page.url and "auth.quartr.com" not in page.url

    if attempt(page):
        return

    frame_urls = []
    for fr in page.frames:
        frame_urls.append(fr.url)
        try:
            if attempt(fr):
                return
        except Exception:
            continue

    png = link_png("login_fail")
    html = link_html("login_fail")
    raise RuntimeError(f"Keycloak login failed. URL: {page.url} | Frames: {frame_urls} | PNG: {png} | HTML: {html}")

# ───────────────────────── Company search (palette, contenteditable, preferences) ─────────────────────────
def open_company(page, ticker: str):
    """
    Opens the company page from the 'Companies' results.
    Prefers mapped names (e.g., PCOR → Procore). Falls back to first company card.
    Saves PNG/HTML at key steps.
    """
    page.set_default_timeout(PW_DEFAULT_TIMEOUT_MS)
    t = ticker.upper()
    preferred_names = PREFERRED_COMPANY_BY_TICKER.get(t, ["Procore"] if t == "PCOR" else [])

    def snap(tag):
        _save_png(page, tag); _save_html(page, tag)

    # 1) Home → open palette ('/' then Enter) → type ticker
    page.goto("https://web.quartr.com/home", wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(150)

    # try to focus the palette
    def focused_is_textual():
        try:
            return page.evaluate("""() => {
              const a = document.activeElement;
              if (!a) return {ok:false};
              const tag = a.tagName?.toLowerCase();
              const editable = a.getAttribute && a.getAttribute('contenteditable');
              const role = a.getAttribute && a.getAttribute('role');
              return { ok: !!(tag==='input' || editable==='' || editable==='true' || role==='textbox') };
            }""")
        except Exception:
            return {"ok": False}

    try:
        page.keyboard.press("/")
        page.wait_for_timeout(100)
    except Exception:
        pass

    if not focused_is_textual().get("ok"):
        # click likely inputs
        for sb in [
            page.get_by_placeholder("Search"),
            page.get_by_role("combobox", name="Search", exact=False),
            page.locator("input[type='search']"),
            page.locator("[contenteditable=''], [contenteditable='true'], [role='textbox']"),
            page.locator("input[placeholder*='Search' i]"),
            page.locator("input[aria-label*='Search' i]"),
        ]:
            if sb and sb.count():
                try:
                    sb.first.click(); page.wait_for_timeout(80); break
                except Exception:
                    continue

    # clear → type → Enter
    try:
        page.keyboard.down("Control"); page.keyboard.press("KeyA"); page.keyboard.up("Control")
        page.keyboard.press("Backspace")
    except Exception:
        pass
    page.keyboard.type(t, delay=20)
    page.keyboard.press("Enter")
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(300)
    snap(f"open_company_after_enter_{t}")

    # 2) Ensure we're on /search page (matches your screenshot layout)
    if "/search" not in page.url:
        page.goto(f"https://web.quartr.com/search?q={t}", wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(200)

    # 3) Locate 'Companies' section container
    companies = None
    for sel in [
        "section:has-text('Companies')",
        "div:has(> h2:has-text('Companies'))",
        "div:has-text('Companies')",
        "main section >> nth=0",
    ]:
        try:
            loc = page.locator(sel)
            if loc and loc.count():
                companies = loc.first
                break
        except Exception:
            continue
    if not companies:
        snap(f"no_companies_section_{t}")
        companies = page  # fallback to entire page

    # 4) Click helpers
    def click_match(ctx, name_contains: str | None = None) -> bool:
        cands = []
        if name_contains:
            cands += [
                ctx.locator(f"[class*='card']:has-text('{name_contains}'):has-text('{t}')"),
                ctx.locator(f"a:has-text('{name_contains}'):has-text('{t}')"),
                ctx.locator(f"button:has-text('{name_contains}'):has-text('{t}')"),
                ctx.get_by_role("link", name=name_contains, exact=False).filter(has_text=t),
                ctx.get_by_role("button", name=name_contains, exact=False).filter(has_text=t),
            ]
        else:
            cands += [
                ctx.locator(f"[class*='card']:has-text('{t}')"),
                ctx.get_by_role("link", name=t, exact=False),
                ctx.get_by_role("button", name=t, exact=False),
                ctx.locator(f"a:has-text('{t}')"),
                ctx.locator(f"button:has-text('{t}')"),
                ctx.locator(f"text=/\\b{t}\\b/"),
            ]
        for loc in cands:
            try:
                if loc and loc.count():
                    el = loc.first
                    el.scroll_into_view_if_needed(timeout=800)
                    try:
                        el.click()
                    except Exception:
                        el.locator("xpath=ancestor-or-self::*[self::a or self::button][1]").first.click()
                    page.wait_for_load_state("networkidle")
                    snap(f"clicked_{(name_contains or 'ticker').replace(' ', '_')}_{t}")
                    return True
            except Exception:
                continue
        return False

    # Priority order:
    # a) Preferred name inside Companies
    for nm in preferred_names:
        if click_match(companies, nm):
            return
    # b) Any ticker match inside Companies
    if click_match(companies, None):
        return
    # c) Preferred name anywhere on page
    for nm in preferred_names:
        if click_match(page, nm):
            return
    # d) Ticker anywhere on page
    if click_match(page, None):
        return

    # e) Final fallback: first company card in Companies section
    try:
        first_card = companies.locator("[class*='card'], [data-testid*='company'], a:has([alt])").first
        if first_card and first_card.is_visible():
            first_card.scroll_into_view_if_needed(timeout=800)
            try:
                first_card.click()
            except Exception:
                first_card.locator("xpath=ancestor-or-self::*[self::a or self::button][1]").first.click()
            page.wait_for_load_state("networkidle")
            snap(f"clicked_first_card_{t}")
            return
    except Exception:
        pass

    # Hard fail with artifacts
    png = _save_png(page, f"open_company_fail_{t}")
    html = _save_html(page, f"open_company_fail_{t}")
    raise RuntimeError(f"Could not open company for {t}. PNG: /debug/snap/{png} HTML: /debug/html/{html}")

# ───────────────────────── Open quarter ─────────────────────────
def open_quarter(page, year: int, quarter: str) -> bool:
    page.set_default_timeout(PW_DEFAULT_TIMEOUT_MS)
    labels = [f"{quarter} {year}", f"{quarter} FY{year}", f"{quarter} {str(year)[-2:]}"]
    for lb in labels:
        loc = page.get_by_text(lb, exact=False)
        if loc and loc.count():
            try:
                loc.first.click()
                page.wait_for_load_state("networkidle")
                page.wait_for_timeout(250)
                _save_png(page, f"open_quarter_{year}_{quarter}")
                return True
            except Exception:
                continue
    return False

# ───────────────────────── Backfill route (with watchdog) ─────────────────────────
@app.post("/backfill")
def backfill(req: BackfillRequest):
    if not QUARTR_EMAIL or not QUARTR_PASSWORD:
        raise HTTPException(status_code=500, detail="Missing QUARTR_EMAIL or QUARTR_PASSWORD")

    def qn(q: str) -> int:
        return int(q.replace("Q", ""))

    start = time.monotonic()

    def watchdog(step: str, page=None):
        elapsed = time.monotonic() - start
        if elapsed > BACKFILL_MAX_SECONDS:
            try:
                if page: _save_png(page, f"watchdog_{int(elapsed)}s_at_{step.replace(' ', '_')}")
            except Exception:
                pass
            raise HTTPException(status_code=504, detail=f"Backfill exceeded {BACKFILL_MAX_SECONDS}s at step: {step}")

    browser = None
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"],
            )
            page = browser.new_page()
            page.set_default_timeout(PW_DEFAULT_TIMEOUT_MS)

            logger.info("STEP 1: login")
            login_keycloak(page, QUARTR_EMAIL, QUARTR_PASSWORD)
            watchdog("login", page)

            logger.info("STEP 2: open company")
            open_company(page, req.ticker)
            watchdog("open_company", page)

            logger.info("STEP 3: iterate quarters")
            start_qn = qn(req.start_q); end_qn = qn(req.end_q)
            for year in range(req.start_year, req.end_year + 1):
                q_from = start_qn if year == req.start_year else 1
                q_to   = end_qn   if year == req.end_year   else 4
                for qi in range(q_from, q_to + 1):
                    qlabel = f"Q{qi}"
                    ok = open_quarter(page, year, qlabel)
                    watchdog(f"open_quarter_{year}_{qlabel}", page)
                    if not ok:
                        _save_png(page, f"open_quarter_fail_{req.ticker}_{year}_{qlabel}")
                        continue
                    # TODO: add download/upload logic here

            return {"status": "ok", "ticker": req.ticker}

    except HTTPException:
        raise
    except PWTimeoutError as e:
        logger.exception("Playwright timeout")
        raise HTTPException(status_code=504, detail=f"Playwright timeout: {e}")
    except Exception as e:
        logger.exception("Backfill failed")
        raise HTTPException(status_code=500, detail=f"Backfill failed: {e}")
    finally:
        try:
            if browser: browser.close()
        except Exception:
            pass