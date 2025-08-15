import os
import io
import json
import base64
import time
import logging
import traceback
from typing import Optional, Tuple, List, Dict

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
import fitz  # PyMuPDF
from supabase import create_client

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("uvicorn.error")

app = FastAPI(title="Quartr Loader", version="1.4 (uses saved session)")


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


def load_storage_state_to_file() -> Optional[str]:
    """
    If QUARTR_STORAGE_STATE is set (raw JSON or base64 of JSON),
    write it to /app/quartr_state.json and return that path.
    """
    state_env = os.getenv("QUARTR_STORAGE_STATE", "").strip()
    if not state_env:
        return None
    try:
        # Try base64 first
        try:
            decoded = base64.b64decode(state_env).decode("utf-8")
            data = json.loads(decoded)
        except Exception:
            # Fallback: assume raw JSON string
            data = json.loads(state_env)
        path = "/app/quartr_state.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f)
        logger.info("Loaded storage_state into %s", path)
        return path
    except Exception as e:
        logger.error("Failed to parse QUARTR_STORAGE_STATE: %s", e)
        return None


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
# Login (fallback) — only used if storage_state is missing/invalid
# ------------------------------
def login_with_credentials(page, email: str, password: str):
    login_urls = [
        os.getenv("QUARTR_LOGIN_URL") or "https://app.quartr.com/login",
        "https://quartr.com/login",
        "https://app.quartr.com/sign-in",
    ]

    page.set_default_timeout(45000)  # assume default 30s otherwise

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
            "input[name*='email' i]",
        ]
        selectors_pass = [
            "input[placeholder*='password' i]",
            "input[type='password']",
            "input[name*='password' i]",
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

        # email
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

        # password
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

        # submit
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

    last_err = None
    for url in login_urls:
        try:
            page.goto(url, wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle")
            _dismiss_cookies(page)
            if _fill_in_login_on(page):
                page.wait_for_load_state("networkidle")
                page.wait_for_timeout(800)
                return
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"

    try:
        path = f"/tmp/login_failure_{int(time.time())}.png"
        page.screenshot(path=path, full_page=True)
        logger.error("Login failed. Screenshot: %s", path)
    except Exception:
        pass

    raise RuntimeError(f"Login fallback failed. Last error: {last_err or 'unknown'}")


# ------------------------------
# Quartr navigation helpers
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
    """
    Search for ticker from the web.quartr.com app header search.
    """
    # Try a few common search entry points
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
# Models & Routes
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
        "QUARTR_STORAGE_STATE": present("QUARTR_STORAGE_STATE"),
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
        sb = supabase_client_server()  # validates SB env
        bucket = bucket_name()
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=f"Config error: {e}")

    storage_state_path = load_storage_state_to_file()
    use_saved_session = storage_state_path is not None

    # If no saved session provided, we can optionally fall back to credentials
    email = os.getenv("QUARTR_EMAIL")
    password = os.getenv("QUARTR_PASSWORD")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"]
            )
            ctx_kwargs = dict(accept_downloads=True,
                              user_agent=("Mozilla/5.0 (X11; Linux x86_64) "
                                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                                          "Chrome/124.0 Safari/537.36"),
                              viewport={"width": 1366, "height": 900},
                              locale="en-US")
            if use_saved_session:
                ctx_kwargs["storage_state"] = storage_state_path
            ctx = browser.new_context(**ctx_kwargs)
            page = ctx.new_page()

            # Start at /home to leverage existing session
            open_home(page)

            # If we still see a login form, fall back (only if creds exist)
            try:
                # Heuristic: look for "Log in" button visible on the page
                needs_login = False
                btn = page.get_by_role("button", name="Log in", exact=False)
                if btn and btn.count():
                    needs_login = True
                if needs_login and email and password:
                    login_with_credentials(page, email, password)
            except Exception:
                pass

            # ---- Actual collection flow ----
            LABELS = [
                ("Transcript", "transcript"),
                ("Press Release", "press_release"),
                ("Presentation", "presentation"),
            ]

            def qn(q: str) -> int:
                return int(q.replace("Q", ""))

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


# ------------------------------
# (Optional) endpoint to read back the storage_state presence
# ------------------------------
@app.get("/statecheck")
def statecheck():
    return {"storage_state_exists": os.path.exists("/app/quartr_state.json")}
