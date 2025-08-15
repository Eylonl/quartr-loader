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

app = FastAPI(title="Quartr Loader", version="1.6 (Keycloak login)")


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
# Keycloak login flow
# ------------------------------
def login_keycloak(page, email: str, password: str):
    """
    Logs in via Quartr's Keycloak page.
    Uses QUARTR_LOGIN_URL if set; otherwise tries your provided URL first,
    then falls back to letting web.quartr.com redirect to the current auth URL.
    """
    provided_url = (
        "https://auth.quartr.com/realms/prod/protocol/openid-connect/auth"
        "?response_type=code&client_id=web"
        "&redirect_uri=https%3A%2F%2Fweb.quartr.com%2Fapi%2Fauth%2Fcallback%2Fkeycloak"
        "&code_challenge=1pq9sKtxWv6EouXakPlyEFXYbuV9sKIkzaGL26g9ss8"
        "&code_challenge_method=S256&scope=openid+profile+email"
    )
    login_url = os.getenv("QUARTR_LOGIN_URL") or provided_url

    def _try_login_at(url: str) -> bool:
        page.set_default_timeout(45000)
        page.goto(url, wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle")

        # Dismiss cookie banners if any
        try:
            for txt in ["Accept", "Agree", "Allow all", "OK", "I agree"]:
                btn = page.get_by_role("button", name=txt, exact=False)
                if btn and btn.count():
                    btn.first.click()
                    page.wait_for_timeout(250)
                    break
        except Exception:
            pass

        # Typical Keycloak input names/ids; include robust fallbacks
        user_candidates = [
            "#username", "input[name='username']",
            "input[type='email']", "input[placeholder*='email' i]",
            "input[placeholder*='username' i]",
        ]
        pass_candidates = [
            "#password", "input[name='password']",
            "input[type='password']", "input[placeholder*='password' i]",
        ]

        def fill_one(cands, value) -> bool:
            for sel in cands:
                loc = page.locator(sel)
                if loc and loc.count():
                    loc.first.fill(value)
                    return True
            for loc in (
                page.get_by_label("Email", exact=False),
                page.get_by_label("Username", exact=False),
                page.get_by_placeholder("Email"),
                page.get_by_placeholder("Username"),
                page.get_by_role("textbox", name="Email", exact=False),
            ):
                if loc and loc.count():
                    loc.first.fill(value)
                    return True
            return False

        if not fill_one(user_candidates, email):
            return False
        if not fill_one(pass_candidates, password):
            return False

        # Submit
        submitted = False
        for btn in (
            page.get_by_role("button", name="Sign in", exact=False),
            page.get_by_role("button", name="Log in", exact=False),
            page.locator("input[type='submit']"),
            page.locator("button[type='submit']"),
        ):
            if btn and btn.count():
                btn.first.click()
                submitted = True
                break
        if not submitted:
            page.keyboard.press("Enter")

        # Wait for redirect back to web.quartr.com
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(800)

        # Heuristic: ensure we’re on web.quartr.com now (or already logged in)
        return "web.quartr.com" in page.url

    # 1) Try provided/explicit Keycloak URL
    if _try_login_at(login_url):
        return

    # 2) Fallback: let the app redirect us to the active Keycloak URL
    page.goto("https://web.quartr.com/", wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")
    if _try_login_at(page.url):  # page.url should now be the live Keycloak URL
        return

    raise RuntimeError(f"Keycloak login failed; final URL: {page.url}")


# ------------------------------
# Quartr app navigation
# ------------------------------
def open_home(page):
    page.goto("https://web.quartr.com/home", wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")
    # best-effort cookie banner dismiss
    try:
        btn = page.get_by_role("button", name="Accept", exact=False)
        if btn and btn.count():
            btn.first.click()
            page.wait_for_timeout(300)
    except Exception:
        pass


def open_company(page, ticker: str):
    """Search for ticker from the app header search."""
    # Try multiple search entry points
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
                # click the first matching result
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
    try:
        email = require_env("QUARTR_EMAIL")
        password = require_env("QUARTR_PASSWORD")
        sb = supabase_client_server()  # validates SB env
        bucket = bucket_name()
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=f"Config error: {e}")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"]
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

            # What to pull
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
