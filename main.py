import os
from fastapi import FastAPI
from pydantic import BaseModel
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
import fitz  # PyMuPDF
from supabase import create_client
from typing import Optional, List, Dict, Any

load_dotenv()
app = FastAPI(title="Quartr Loader", version="1.0")

SB = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_ROLE_KEY"])
BUCKET = os.getenv("SUPABASE_BUCKET", "earnings")

LABELS = [("Transcript","transcript"),("Press Release","press_release"),("Presentation","presentation")]

def pdf_bytes_to_text(b: bytes) -> str:
    with fitz.open(stream=b, filetype="pdf") as doc:
        return "\n".join(p.get_text() for p in doc).strip()

def path_for(ticker: str, year: int, quarter: str, file_type: str) -> str:
    return f"pdfs/{ticker.upper()}/{year}-{quarter}/{file_type}.pdf"

def file_exists(storage_path: str) -> bool:
    parent, name = storage_path.rsplit("/", 1)
    try:
        entries = SB.storage.from_(BUCKET).list(path=parent)
        return any(e.get("name") == name for e in entries)
    except Exception:
        return False

def upsert_row(**row):
    SB.table("earnings_files").upsert(row, on_conflict="ticker,year,quarter,file_type,file_format").execute()

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

class BackfillRequest(BaseModel):
    ticker: str
    start_year: int
    end_year: int
    start_q: str = "Q1"
    end_q: str = "Q4"

@app.post("/backfill")
def backfill(req: BackfillRequest):
    email = os.environ["QUARTR_EMAIL"]
    password = os.environ["QUARTR_PASSWORD"]

    def qn(q): return int(q.replace("Q",""))
    headless = True
    args = ["--no-sandbox","--disable-dev-shm-usage"]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, args=args)
        ctx = browser.new_context(accept_downloads=True)
        page = ctx.new_page()
        login(page, email, password)

        open_company(page, req.ticker)
        for year in range(req.start_year, req.end_year+1):
            q_start = qn(req.start_q) if year == req.start_year else 1
            q_end = qn(req.end_q) if year == req.end_year else 4
            for qi in range(q_start, q_end+1):
                q = f"Q{qi}"
                if not open_quarter(page, year, q):
                    continue
                for label, ftype in LABELS:
                    key = path_for(req.ticker, year, q, ftype)
                    if file_exists(key):
                        continue
                    b, url = download_label(page, label)
                    if not b:
                        continue
                    SB.storage.from_(BUCKET).upload(key, b, {"content-type":"application/pdf","upsert":True})
                    text = pdf_bytes_to_text(b)
                    upsert_row(ticker=req.ticker.upper(), year=year, quarter=q,
                               file_type=ftype, file_format="pdf", storage_path=key,
                               source_url=url, text_content=None)
                    upsert_row(ticker=req.ticker.upper(), year=year, quarter=q,
                               file_type=ftype, file_format="text", storage_path=None,
                               source_url=url, text_content=text)
        ctx.close(); browser.close()
    return {"status":"ok"}
