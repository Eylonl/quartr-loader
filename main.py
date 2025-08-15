import os
import time
import logging
import traceback
from typing import Optional, Tuple

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
import fitz  # PyMuPDF
from supabase import create_client

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("uvicorn.error")

app = FastAPI(title="Quartr Loader", version="1.7 (Keycloak robust)")


# ------------------------------
# Env / Supabase helpers
# ------------------------------
def require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required env var: {name}")
    return val


def supabase_client_server():
    url = require_env("SUPABASE_URL")
    key = require_env("SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)


def bucket_name() -> str:
    return os.getenv("SUPABASE_BUCKET", "earnings")


# ------------------------------
# PDF / storage utils
# ------------------------------
def pdf_bytes_to_text(b: bytes) -> str:
    with fitz.open(stream=b, filetype="pdf") as doc:
        return "\n".join(p.get_text() for p in doc).strip()


def path_for(ticker: str, year: int, quarter: str, file_type: str) -> str:
    return f"pdfs/{ticker.upper()}/{year}-{quarter}/{file_type}.pdf"


def file_exists(sb, storage_bucket: str, storage_path: str) -> bool:
    parent, name = storage_path.rsplit("/", 1)
    try:
        entries = sb.storage.from_(storage_bucket).list(path=parent)
        return any(e.get("name") == name for e in (entries or []))
    except Exception:
        return False


def upsert_row(sb, **row):
    sb.table("earnings_files").upsert(
        row, on_conflict="ticker,year,quarter,file_type,file_format"
    ).execute()


# ------------------------------
# Keycloak login flow (robust)
# ------------------------------
def login_keycloak(page, email: str, password: str):
    """
    Logs in via Quartr's Keycloak page.
    Tries explicit QUARTR_LOGIN_URL (or provided URL), then falls back by letting
    web.quartr.com redirect to the active Keycloak URL. Handles iframes and late render.
    """
    provided_url = (
        "https://auth.quartr.com/realms/prod/protocol/openid-connect/auth"
        "?response_type=code&client_id=web"
        "&redirect_uri=https%3A%2F%2Fweb.quartr.com%2Fapi%2Fauth%2Fcallback%2Fkeycloak"
        "&code_challenge=1pq9sKtxWv6EouXakPlyEFXYbuV9sKIkzaGL26g9ss8"
        "&code_challenge_method=S256&scope=openid+profile+email"
    )
    login_url = os.getenv("QUARTR_LOGIN_URL") or provided_url

    page.set_default_timeout(50000)

    def _dismiss_cookies(doc):
        try:
            for txt in ["Accept", "Agree", "Allow all", "OK", "I agree"]:
                btn = doc.get_by_role("button", name=txt, exact=False)
                if btn and btn.count():
                    btn.first.click()
                    doc.wait_for_timeout(300)
                    break
        except Exception:
            pass

    def _fill_on(doc) -> bool:
        """Try to fill on a given document context (page or frame)."""
        try:
            doc.wait_for_selector("input, button[type='submit'], #kc-login", timeout=15000)
        except Exception:
            return False

        # Some Keycloak themes show an intermediate “Continue with email/password” step
        try:
            for txt in ["Continue with Email", "Continue", "Sign in with email", "Email"]:
                b = doc.get_by_role("button", name=txt, exact=False)
                if b and b.count():
                    b.first.click()
                    doc.wait_for_timeout(400)
        except Exception:
            pass

        user_sel = [
            "#username", "input[name='username']",
            "input[type='email']", "input[placeholder*='email' i]",
            "input[placeholder*='username' i]",
        ]
        pass_sel = [
            "#password", "input[name='password']",
            "input[type='password']", "input[placeholder*='password' i]",
        ]

        # Username/email
        for sel in user_sel:
            loc = doc.locator(sel)
            if loc and loc.count():
                try:
                    loc.first.fill(email)
                    break
                except Exception:
                    pass
        else:
            for loc in (
                doc.get_by_label("Email", exact=False),
                doc.get_by_label("Username", exact=False),
                doc.get_by_placeholder("Email"),
                doc.get_by_placeholder("Username"),
                doc.get_by_role("textbox", name="Email", exact=False),
            ):
                if loc and loc.count():
                    try:
                        loc.first.fill(email)
                        break
                    except Exception:
                        pass
            else:
                return False

        # Password
        for sel in pass_sel:
            loc = doc.locator(sel)
            if loc and loc.count():
                try:
                    loc.first.fill(password)
                    break
                except Exception:
                    pass
        else:
            for loc in (
                doc.get_by_label("Password", exact=False),
                doc.get_by_placeholder("Password"),
                doc.get_by_role("textbox", name="Password", exact=False),
            ):
                if loc and loc.count():
                    try:
                        loc.first.fill(password)
                        break
                    except Exception:
                        pass
            else:
                return False

        # Submit
        for btn in (
            doc.locator("#kc-login"),
            doc.get_by_role("button", name="Sign in", exact=False),
            doc.get_by_role("button", name="Log in", exact=False),
            doc.locator("button[type='submit']"),
            doc.locator("input[type='submit']"),
        ):
            if btn and btn.count():
                try:
                    # Some Keycloak themes navigate fully, others SPA-transition
                    with doc.expect_navigation(wait_until="load", timeout=20000):
                        btn.first.click()
                except Exception:
                    btn.first.click()
                doc.wait_for_load_state("networkidle")
                doc.wait_for_timeout(800)
                return True

        # Fallback: Enter key
        try:
            doc.keyboard.press("Enter")
            doc.wait_for_load_state("networkidle")
            doc.wait_for_timeout(800)
            return True
        except Exception:
            return False

    def _attempt(url: str) -> bool:
        page.goto(url, wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle")
        _dismiss_cookies(page)

        # Try on the main page
        if _fill_on(page):
            if "web.quartr.com" in page.url:
                return True

        # Try any iframes (Keycloak can render in an inner frame)
        try:
            for fr in page.frames:
                if fr == page.main_frame:
                    continue
                _dismiss_cookies(fr)
                if _fill_on(fr):
                    if "web.quartr.com" in page.url:
                        return True
        except Exception:
            pass

        if "auth.quartr.com" in page.url:
            page.wait_for_timeout(2000)
        return "web.quartr.com" in page.url

    # 1) Try explicit URL
    if _attempt(login_url):
        return

    # 2) Let app push us to active auth URL then attempt there
    page.goto("https://web.quartr.com/", wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")
    if _attempt(page.url):
        return

    # Debug screenshot
    try:
        snap = f"/tmp/keycloak_fail_{int(time.time())}.png"
        page.screenshot(path=snap, full_page=True)
        logger.error("Keycloak login failed. Screenshot saved: %s", snap)
    except Exception:
        pass

    raise RuntimeError(f"Keycloak login failed; final URL: {page.url}")


# ------------------------------
# Quartr app navigation
# ------------------------------
def open_home(page):
    page.goto("https://web.quartr.com/home", wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")
    try:
        btn = page.get_by_role("button", name="Accept", exact=False)
        if btn and btn.count():
            btn.first.click()
            page.wait_for_timeout(300)
    except Exception:
        pass


def open_company(page, ticker: str):
    """Search for ticker from the app header search."""
    candidates = [
        page.get_by_placeholder("Search"),
        page.get_by_role("combobox", name="Search", exact=False),
        page.locator("input[type='search']"),
        page.locator("input[placeholder*='Search' i]"),
    ]
    for loc in candidates:
        try:
            if loc and loc.count():
                loc.first.click()
                loc.first.fill(ticker)
                page.keyboard.press("Enter")
                page.wait_for_load_state("networkidle")
                page.wait_for_timeout(1200)
                res = page.get_by_text(ticker.upper(), exact=False)
                if res and res.count():
                    res.first.click()
                    page.wait_for_load_state("networkidle")
                    return
        except Exception:
            continue
    raise RuntimeError("Could not open company from search UI.")


def open_quarter(page, year: int, quarter: str) -> bool:
    patterns = [f"{quarter} {year}", f"{quarter} FY{year}", f"{quarter} {str(year)[-2:]}"]
    for pat in patterns:
        loc = page.get_by_text(pat, exact=False)
        if loc and loc.count():
            loc.first.click()
            page.wait_for_load_state("networkidle")
            page.wait_for_timeout(600)
            return True
    return False


def download_label(page, label_text: str) -> Tuple[Optional[bytes], Optional[str]]:
    locator = page.get_by_text(label_text, exact=False).first
    if not locator or not locator.count():
        return None, None
    try:
        with page.expect_download() as dl_info:
            locator.click()
        dl = dl_info.value
        return dl.read(), dl.url
    except PWTimeoutError:
        return None, None


# ------------------------------
# API models & routes
# ------------------------------
class BackfillRequest(BaseModel):
    ticker: str
    start_year: int
    end_year: int
    start_q: str = "Q1"
    end_q: str = "Q4"


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
    # Validate env first
    try:
        email = require_env("QUARTR_EMAIL")
        password = require_env("QUARTR_PASSWORD")
        sb = supabase_client_server()
        bucket = bucket_name()
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=f"Config error: {e}")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                ],
            )
            ctx = browser.new_context(
                accept_downloads=True,
                user_agent=("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
                viewport={"width": 1366, "height": 900},
                locale="en-US",
            )
            page = ctx.new_page()

            # Login via Keycloak
            login_keycloak(page, email, password)

            # Land on home to ensure SPA has loaded
            open_home(page)

            LABELS = [
                ("Transcript", "transcript"),
                ("Press Release", "press_release"),
                ("Presentation", "presentation"),
            ]

            def qn(q: str) -> int:
                return int(q.replace("Q", ""))

            # Navigate to ticker and iterate quarters
            open_company(page, req.ticker)

            for year in range(req.start_year, req.end_year + 1):
                q_start = qn(req.start_q) if year == req.start_year else 1
                q_end = qn(req.end_q) if year == req.end_year else 4
                for qi in range(q_start, q_end + 1):
                    q = f"Q{qi}"
                    if not open_quarter(page, year, q):
                        logger.warning("Quarter not found: %s %s %s", req.ticker, year, q)
                        continue

                    for label, ftype in LABELS:
                        key = path_for(req.ticker, year, q, ftype)
                        if file_exists(sb, bucket, key):
                            logger.info("Skip existing: %s", key)
                            continue

                        b, url = download_label(page, label)
                        if not b:
                            logger.info("No %s for %s %s %s", ftype, req.ticker, year, q)
                            continue

                        # Upload PDF
                        sb.storage.from_(bucket).upload(
                            key, b, {"content-type": "application/pdf", "upsert": True}
                        )

                        # Extract and upsert text + metadata
                        text = pdf_bytes_to_text(b)
                        upsert_row(
                            sb,
                            ticker=req.ticker.upper(),
                            year=year,
                            quarter=q,
                            file_type=ftype,
                            file_format="pdf",
                            storage_path=key,
                            source_url=url,
                            text_content=None,
                        )
                        upsert_row(
                            sb,
                            ticker=req.ticker.upper(),
                            year=year,
                            quarter=q,
                            file_type=ftype,
                            file_format="text",
                            storage_path=None,
                            source_url=url,
                            text_content=text,
                        )

            ctx.close()
            browser.close()
        return {"status": "ok"}

    except Exception as e:
        logger.error("Backfill failed: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Unhandled error: {e}")
