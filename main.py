import os
import logging
import traceback
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
import fitz  # PyMuPDF

load_dotenv()

app = FastAPI(title="Quartr Loader", version="1.2")
logger = logging.getLogger("uvicorn.error")

# ---------- Env helpers ----------
def get_env(name: str, required: bool = True, default: str | None = None) -> str | None:
    val = os.getenv(name, default)
    if required and not val:
        raise RuntimeError(f"Missing required env var: {name}")
    return val

def supabase_client_server():
    # Lazy-create client so app still boots even if env is misconfigured.
    from supabase import create_client
    url = get_env("SUPABASE_URL")
    key = get_env("SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)

def bucket_name() -> str:
    return os.getenv("SUPABASE_BUCKET", "earnings")

# ---------- Utility ----------
def pdf_bytes_to_text(b: bytes) -> str:
    with fitz.open(stream=b, filetype="pdf") as doc:
        return "\n".join(p.get_text() for p in doc).strip()

def path_for(ticker: str, year: int, quarter: str, file_type: str) -> str:
    return f"pdfs/{ticker.upper()}/{year}-{quarter}/{file_type}.pdf"

def file_exists(storage_path: str) -> bool:
    SB = supabase_client_server()
    BUCKET = bucket_name()
    parent, name = storage_path.rsplit("/", 1)
    try:
        entries = SB.storage.from_(BUCKET).list(path=parent)
        return any(e.get("name") == name for e in (entries or []))
    except Exception:
        return False

def upsert_row(**row):
    SB = supabase_client_server()
    SB.table("earnings_files").upsert(
        row, on_conflict="ticker,year,quarter,file_type,file_format"
    ).execute()

# ---------- Quartr steps ----------
def login(page, email, password):
    page.goto("https://quartr.com/login", wait_until="networkidle")
    page.get_by_placeholder("Email").fill(email)
    page.get_by_placeholder("Password").fill(password)
    page.get_by_role("button", name="Log in").click()
    page.wait_for_load_state("networkidle")

def open_company(page, ticker: str):
    page.get_by_placeholder("Search").click()
    page.get_by_placeholder("Search").fill(ticker)
    page.keyboard.press("Enter")
    page.wait_for_timeout(1200)
    page.get_by_text(ticker.upper(), exact=False).first.click()
    page.wait_for_load_state("networkidle")

def open_quarter(page, year: int, quarter: str) -> bool:
    patterns = [f"{quarter} {year}", f"{quarter} FY{year}", f"{quarter} {str(year)[-2:]}"]
    for pat in patterns:
        loc = page.get_by_text(pat, exact=False)
        if loc.count():
            loc.first.click()
            page.wait_for_load_state("networkidle")
            page.wait_for_timeout(600)
            return True
    return False

def download_label(page, label_text: str):
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

# ---------- Health & diagnostics ----------
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/envcheck")
def envcheck():
    present = lambda k: bool(os.getenv(k))
    return {
        # booleans only; no secrets are returned
        "QUARTR_EMAIL": present("QUARTR_EMAIL"),
        "QUARTR_PASSWORD": present("QUARTR_PASSWORD"),
        "SUPABASE_URL": present("SUPABASE_URL"),
        "SUPABASE_SERVICE_ROLE_KEY": present("SUPABASE_SERVICE_ROLE_KEY"),
        "SUPABASE_BUCKET": os.getenv("SUPABASE_BUCKET", "earnings"),
    }

@app.get("/diag")
def diag():
    try:
        SB = supabase_client_server()
        bucket = bucket_name()
        entries = SB.storage.from_(bucket).list()  # list root
        return {"ok": True, "bucket": bucket, "entries": len(entries or [])}
    except Exception as e:
        logger.error("Diag failed: %s\n%s", e, traceback.format_exc())
        return {"ok": False, "error": str(e)}

# ---------- API ----------
class BackfillRequest(BaseModel):
    ticker: str
    start_year: int
    end_year: int
    start_q: str = "Q1"
    end_q: str = "Q4"

@app.post("/backfill")
def backfill(req: BackfillRequest):
    try:
        # Validate required env upfront (but don't crash the server)
        email = get_env("QUARTR_EMAIL")
        password = get_env("QUARTR_PASSWORD")
        SB = supabase_client_server()  # ensures SUPABASE_URL + SERVICE_ROLE are set/valid
        BUCKET = bucket_name()

        def qn(q): return int(q.replace("Q", ""))
        headless = True
        args = ["--no-sandbox", "--disable-dev-shm-usage"]

        LABELS = [
            ("Transcript", "transcript"),
            ("Press Release", "press_release"),
            ("Presentation", "presentation"),
        ]

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless, args=args)
            ctx = browser.new_context(accept_downloads=True)
            page = ctx.new_page()

            # Login + navigate
            login(page, email, password)
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
                        if file_exists(key):
                            logger.info("Skip existing: %s", key)
                            continue
                        b, url = download_label(page, label)
                        if not b:
                            logger.info("No %s for %s %s %s", ftype, req.ticker, year, q)
                            continue
                        SB.storage.from_(BUCKET).upload(
                            key, b, {"content-type": "application/pdf", "upsert": True}
                        )
                        text = pdf_bytes_to_text(b)
                        upsert_row(
                            ticker=req.ticker.upper(), year=year, quarter=q,
                            file_type=ftype, file_format="pdf", storage_path=key,
                            source_url=url, text_content=None
                        )
                        upsert_row(
                            ticker=req.ticker.upper(), year=year, quarter=q,
                            file_type=ftype, file_format="text", storage_path=None,
                            source_url=url, text_content=text
                        )

            ctx.close()
            browser.close()
        return {"status": "ok"}

    except RuntimeError as e:
        logger.error("Config error: %s", e)
        raise HTTPException(status_code=500, detail=f"Config error: {e}")
    except Exception as e:
        logger.error("Backfill failed: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Unhandled error: {e}")
