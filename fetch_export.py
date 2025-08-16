import os, asyncio, pathlib, re
from datetime import datetime, timezone
from playwright.async_api import async_playwright
from dotenv import load_dotenv
from logger import log_to_db  # <-- NEW

ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
load_dotenv()

BASE_URL = os.getenv("BASE_URL", "https://letterboxd.com")
USER = os.getenv("LETTERBOXD_USER")
PASS = os.getenv("LETTERBOXD_PASS")
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "./exports")
STATE_PATH   = os.getenv("STATE_PATH", "./state/letterboxd_state.json")

EXPORT_SETTINGS_PATH = f"{BASE_URL}/settings/data/"

PROJECT_NAME = "letterboxd_fetch"

async def ensure_signed_in(page):
    async def has_login_inputs():
        try:
            await page.locator('input[name="username"]').first.wait_for(state="visible", timeout=1500)
            await page.locator('input[name="password"]').first.wait_for(state="visible", timeout=1500)
            return True
        except:
            return False

    if "settings/data" in page.url and not await has_login_inputs():
        return

    if await has_login_inputs():
        log_to_db(PROJECT_NAME, "INFO", "Logging in via inline form")
        await page.locator('input[name="username"]').fill(USER)
        await page.locator('input[name="password"]').fill(PASS)
        await page.get_by_role("button", name=re.compile(r"sign in", re.I)).click()
        await page.wait_for_load_state("networkidle")
        return

    log_to_db(PROJECT_NAME, "INFO", "Navigating to sign-in page")
    await page.goto(f"{BASE_URL}/signin/", wait_until="domcontentloaded")
    await page.locator('input[name="username"]').fill(USER)
    await page.locator('input[name="password"]').fill(PASS)
    await page.get_by_role("button", name=re.compile(r"sign in", re.I)).click()
    await page.wait_for_load_state("networkidle")

async def run():
    pathlib.Path(DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)
    pathlib.Path(os.path.dirname(STATE_PATH)).mkdir(parents=True, exist_ok=True)

    async with async_playwright() as p:
        try:
            browser = await p.chromium.launch(headless=True)
            ctx_kwargs = dict(accept_downloads=True)
            if os.path.exists(STATE_PATH):
                ctx_kwargs["storage_state"] = STATE_PATH
            ctx = await browser.new_context(**ctx_kwargs)
            page = await ctx.new_page()

            log_to_db(PROJECT_NAME, "INFO", "Opening data settings page")
            await page.goto(EXPORT_SETTINGS_PATH, wait_until="domcontentloaded")
            await ensure_signed_in(page)

            if "settings/data" not in page.url:
                await page.goto(EXPORT_SETTINGS_PATH, wait_until="domcontentloaded")

            # Step 1
            try:
                export_trigger = page.get_by_role("link", name=re.compile(r"export your data", re.I))
                await export_trigger.first.wait_for(state="visible", timeout=15000)
            except:
                export_trigger = page.get_by_text("Export your data", exact=False)
            await export_trigger.first.click()
            log_to_db(PROJECT_NAME, "INFO", "Triggered export modal")

            # Step 2
            try:
                modal_export = page.get_by_role("link", name=re.compile(r"export data", re.I))
                await modal_export.first.wait_for(state="visible", timeout=15000)
            except:
                modal_export = page.locator("a.export-data-button")

            async with page.expect_download() as dl_info:
                await modal_export.first.click()
            download = await dl_info.value

            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            suggested = download.suggested_filename or "letterboxd-export.zip"
            out_path = os.path.join(DOWNLOAD_DIR, f"{ts}-{suggested}")
            await download.save_as(out_path)

            await ctx.storage_state(path=STATE_PATH)
            await browser.close()

            log_to_db(PROJECT_NAME, "INFO", f"Downloaded: {out_path}")
            return out_path

        except Exception as e:
            log_to_db(PROJECT_NAME, "ERROR", f"Export failed: {e}")
            raise

if __name__ == "__main__":
    if not USER or not PASS:
        raise SystemExit("Set LETTERBOXD_USER and LETTERBOXD_PASS (via .env or env vars).")
    asyncio.run(run())
