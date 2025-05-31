import asyncio
from pathlib import Path
import polars as pl
from playwright.async_api import async_playwright
from tqdm.asyncio import tqdm_asyncio
from typing import Optional

# ─── PATH CONFIG ─────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_PATH = BASE_DIR / "list_bursa_ids" / "bursa_company_list.csv"
OUTPUT_PATH = BASE_DIR / "csvs" / "cleaned" / "company_urls.csv"
NO_FINANCIALS_PATH = BASE_DIR / "csvs" / "rejected" / "no_financials.csv"
DEBUG_HTML_DIR = BASE_DIR / "debug_html"
SCREENSHOT_DIR = BASE_DIR / "screenshots"

for path in [SCREENSHOT_DIR, DEBUG_HTML_DIR, OUTPUT_PATH.parent, NO_FINANCIALS_PATH.parent]:
    path.mkdir(parents=True, exist_ok=True)

BURSA_URL = "https://my.bursamalaysia.com/market/assets/equities/stocks"

# ─── SEARCH FUNCTION ─────────────────────────
async def run_search_and_navigate(page, search_term: str, retries: int = 3) -> Optional[str]:
    for attempt in range(1, retries + 1):
        try:
            await page.goto(BURSA_URL, timeout=30000, wait_until="domcontentloaded")
            await page.wait_for_timeout(3000 * attempt)

            await page.locator('#stocklistingRef i').click()
            await page.get_by_text('Stock Name').click()
            await page.get_by_text('Stock Name').click()

            search_box = page.locator('#stocklistingRef').get_by_role('textbox', name='Search')
            await search_box.click()
            await search_box.fill(search_term)
            await page.wait_for_timeout(1500)

            match = page.locator(f"a:has(span:text-is('{search_term}'))").first
            if await match.count() == 0:
                return None

            href = await match.get_attribute("href")
            return f"https://my.bursamalaysia.com{href}" if href else None

        except Exception as e:
            print(f"⚠️ Error ({search_term}) attempt {attempt}: {e}")
            await page.screenshot(path=SCREENSHOT_DIR / f"{search_term}_fail.png")
            DEBUG_HTML_DIR.joinpath(f"{search_term}.html").write_text(await page.content())
            await page.wait_for_timeout(3000 * attempt)

    print(f"❌ Failed after retries: {search_term}")
    return None

# ─── SINGLE TASK ─────────────────────────────
async def scrape_single(pw, cid: str, existing_map: dict, bad_ids: set, sem: asyncio.Semaphore, failed: list) -> dict:
    if cid in bad_ids:
        print(f"🚫 Skipping {cid}, known to have no financials.")
        return {"company_id": cid, "new_url": None}

    if cid in existing_map and existing_map[cid]:
        print(f"⏭️ Skipping {cid}, already scraped.")
        return {"company_id": cid, "new_url": existing_map[cid]}

    async with sem:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        url = None
        for _ in range(3):
            url = await run_search_and_navigate(page, cid.zfill(4))
            if url:
                print(f"✅ Success: {cid} → {url}")
                break
            await page.wait_for_timeout(1000)

        await context.close()
        await browser.close()

        if not url:
            failed.append(cid)
        return {"company_id": cid, "new_url": url}

# ─── MAIN ────────────────────────────────────
async def main(retry_mode=False):
    if retry_mode and NO_FINANCIALS_PATH.exists():
        try:
            retry_df = pl.read_csv(NO_FINANCIALS_PATH, schema_overrides={"company_id": pl.Utf8})
            all_ids = [cid.strip().zfill(4) for cid in retry_df.get_column("company_id").unique().to_list()]
            print(f"🔁 Retrying {len(all_ids)} previously failed companies.")
        except Exception as e:
            print(f"⚠️ Failed to read no_financials.csv: {e}")
            return
    else:
        try:
            df = pl.read_csv(INPUT_PATH, schema_overrides={"company_id": pl.Utf8})
            all_ids = [cid.strip().zfill(4) for cid in df.get_column("company_id").unique().to_list()]
        except Exception as e:
            print(f"❌ Failed to read input: {e}")
            return
    existing_map = {}
    if OUTPUT_PATH.exists():
        try:
            old = pl.read_csv(OUTPUT_PATH, schema_overrides={"company_id": pl.Utf8, "new_url": pl.Utf8})
            existing_map = {r["company_id"]: r["new_url"] for r in old.to_dicts() if r["new_url"]}
            print(f"🔁 Resuming with {len(existing_map)} already scraped.")
        except Exception as e:
            print(f"⚠️ Failed to read existing output: {e}")

    bad_ids = set()
    if NO_FINANCIALS_PATH.exists():
        try:
            bad_df = pl.read_csv(NO_FINANCIALS_PATH, schema_overrides={"company_id": pl.Utf8})
            bad_ids = set(bad_df.get_column("company_id").to_list())
            print(f"🚫 Loaded {len(bad_ids)} bad company_ids from {NO_FINANCIALS_PATH}")
        except Exception as e:
            print(f"⚠️ Failed to read bad ids: {e}")

    remaining = [cid for cid in all_ids if (cid not in existing_map or not existing_map[cid]) and cid not in bad_ids]
    
    if not remaining:
        print("✅ All company_ids have already been scraped or rejected. Nothing new to process.")
        return

    results = list(existing_map.items()) if existing_map else []

    sem = asyncio.Semaphore(3)
    failed = []

    try:
        async with async_playwright() as pw:
            tasks = [scrape_single(pw, cid, existing_map, bad_ids, sem, failed) for cid in remaining]
            for fut in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="Scraping Bursa URLs"):
                r = await fut
                if r["new_url"]:
                    results.append((r["company_id"], r["new_url"]))
    except KeyboardInterrupt:
        print("\n⏹️ Interrupted. Saving progress...")
    finally:
        # Save successful results
        unique_results = {}
        for cid, url in results:
            if cid and url:
                unique_results[cid] = url

        if unique_results:
            pl.DataFrame({
                "company_id": list(unique_results.keys()),
                "new_url": list(unique_results.values())
            }).write_csv(OUTPUT_PATH)
            print(f"\n✅ Saved {len(unique_results)} URLs to {OUTPUT_PATH}")
        else:
            print("⚠️ No valid URLs to save.")

        # ─── RETRY MODE: Update no_financials.csv Only If Added ───
        if retry_mode:
            # Load original URLs before run
            old_urls = {}
            if OUTPUT_PATH.exists():
                old_df = pl.read_csv(OUTPUT_PATH, schema_overrides={"company_id": pl.Utf8, "new_url": pl.Utf8})
                old_urls = {r["company_id"]: r["new_url"] for r in old_df.to_dicts() if r["new_url"]}

            # Reload after run
            new_urls = {}
            if OUTPUT_PATH.exists():
                new_df = pl.read_csv(OUTPUT_PATH, schema_overrides={"company_id": pl.Utf8, "new_url": pl.Utf8})
                new_urls = {r["company_id"]: r["new_url"] for r in new_df.to_dicts() if r["new_url"]}

            # Determine which company_ids were truly added this run
            newly_added = {
                cid for cid in new_urls
                if cid not in old_urls or old_urls[cid] != new_urls[cid]
            }

            # Keep only failed ones not added this run
            new_failed = [cid for cid in failed if cid not in newly_added]

            if new_failed:
                pl.DataFrame({"company_id": new_failed}).write_csv(NO_FINANCIALS_PATH)
                print(f"❌ Logged {len(new_failed)} companies with no financials to {NO_FINANCIALS_PATH}")
            else:
                if NO_FINANCIALS_PATH.exists():
                    NO_FINANCIALS_PATH.unlink()
                print("✅ All previously failed companies have now succeeded. no_financials.csv removed.")

# ─── ENTRY ───────────────────────────────────
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--retry", action="store_true", help="Retry previously failed company_ids only")
    args = parser.parse_args()

    asyncio.run(main(retry_mode=args.retry))
    """
    This script:
	•	Searches Bursa by company ID
	•	Finds and stores valid profile URLs
	•	Skips & logs failures
	•	Acts as a precursor step before your financial scrapers (which require the exact URLs)
 
 
    python bursa_url_finder.py           # normal mode
    python bursa_url_finder.py --retry   # retry mode (only from no_financials.csv)
    """