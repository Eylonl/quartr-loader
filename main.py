import os
import json
import base64
import traceback
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from playwright.sync_api import sync_playwright

app = FastAPI()

# ---------------------------
# Utility functions
# ---------------------------

def _save_png(page, name: str):
    os.makedirs("/debug/snap", exist_ok=True)
    path = f"/debug/snap/{name}.png"
    page.screenshot(path=path, full_page=True)
    return f"{name}.png"

def _save_html(page, name: str):
    os.makedirs("/debug/html", exist_ok=True)
    path = f"/debug/html/{name}.html"
    with open(path, "w", encoding="utf-8") as f:
        f.write(page.content())
    return f"{name}.html"

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

# ---------------------------
# Keycloak login
# ---------------------------

def login_keycloak(page, email: str, password: str):
    """
    Robust 2-step Keycloak login:
      Step 1: enter email -> click Next/Continue (or press Enter)
      Step 2 (if shown): enter password -> click Log in/Sign in (or press Enter)
      Works in main page or iframes. Dumps screenshot+HTML on failure.
    """
    page.set_default_timeout(60000)

    def _press_submit(doc) -> bool:
        buttons = [
            "#kc-login", "button#kc-login", "button[name='login']",
            "button[type='submit']", "input[type='submit']",
        ]
        text_buttons = [
            "Next", "Continue", "Continue with Email",
            "Sign in", "Sign In", "Log in", "Log In",
            "Proceed", "Continue >", "Continue →",
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
                return True
        try:
            doc.keyboard.press("Enter")
            doc.wait_for_load_state("networkidle")
            return True
        except Exception:
            return False

    def _fill_email_then_submit(doc) -> bool:
        _dismiss_cookies(doc)
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
            "#username", "input#email", "input[name='username']", "input[name='email']",
            "input[type='email']", "input[autocomplete='username']",
            "input[placeholder*='email' i]", "input[placeholder*='username' i]",
        ]
        filled = False
        for sel in email_sels:
            loc = doc.locator(sel)
            if loc and loc.count():
                loc.first.click()
                loc.first.fill(email)
                filled = True
                break
        if not filled:
            for loc in (
                doc.get_by_label("Email", exact=False),
                doc.get_by_label("Username", exact=False),
                doc.get_by_placeholder("Email"),
                doc.get_by_placeholder("Username"),
            ):
                if loc and loc.count():
                    loc.first.click()
                    loc.first.fill(email)
                    filled = True
                    break
        if not filled:
            return False
        _press_submit(doc)
        return True

    def _fill_password_then_submit(doc) -> bool:
        pass_sels = [
            "#password", "input[name='password']", "input[type='password']",
            "input[autocomplete='current-password']", "input[placeholder*='password' i]",
        ]
        for sel in pass_sels:
            loc = doc.locator(sel)
            if loc and loc.count():
                loc.first.click()
                loc.first.fill(password)
                _press_submit(doc)
                return True
        for loc in (
            doc.get_by_label("Password", exact=False),
            doc.get_by_placeholder("Password"),
        ):
            if loc and loc.count():
                loc.first.click()
                loc.first.fill(password)
                _press_submit(doc)
                return True
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

    # Go to Quartr
    page.goto("https://web.quartr.com/", wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")
    _dismiss_cookies(page)

    if "web.quartr.com" in page.url and "auth.quartr.com" not in page.url:
        return

    if _try_login_in(page):
        return

    for fr in page.frames:
        if fr == page.main_frame:
            continue
        if _try_login_in(fr):
            return

    png = _save_png(page, "login_fail")
    html = _save_html(page, "login_fail")
    raise RuntimeError(f"Login failed; URL: {page.url} Screenshot: {png} HTML: {html}")

# ---------------------------
# Open company after login
# ---------------------------

def open_company(page, ticker: str):
    search = page.locator("input[placeholder*='Search' i]")
    if not search.count():
        raise RuntimeError("Could not find search bar.")
    search.fill(ticker)
    page.keyboard.press("Enter")
    page.wait_for_timeout(2000)
    if ticker.lower() not in page.content().lower():
        raise RuntimeError("Could not open company from search UI.")

# ---------------------------
# Models
# ---------------------------

class BackfillRequest(BaseModel):
    email: str
    password: str
    ticker: str

# ---------------------------
# Routes
# ---------------------------

@app.post("/backfill")
def backfill(req: BackfillRequest):
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            login_keycloak(page, req.email, req.password)
            open_company(page, req.ticker)
            browser.close()
        return {"status": "success"}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Backfill failed: {e}")

@app.get("/health")
def health():
    return {"status": "ok"}
