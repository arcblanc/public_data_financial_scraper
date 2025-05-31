import asyncio
import os
import random
from pathlib import Path
import pandas as pd
from tqdm.asyncio import tqdm_asyncio
from playwright.async_api import async_playwright
from datetime import datetime

# ─── CONFIG ───
BASE_DIR = Path(__file__).resolve().parent
OUTPUTS_DIR = BASE_DIR / "outputs" / "marketcap_volume"
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
CSV_PATH = BASE_DIR.parent / "csvs" / "cleaned" / "company_urls.csv"
COMBINED_OUTPUT_PATH = BASE_DIR.parent / "bursa_scrape_sql_inject" / "bursa_data" / "market_info_sample.csv"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)...",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0_0)...",
    "Mozilla/5.0 (X11; Linux x86_64)..."
]

# ─── SCRAPE FUNCTION ───
async def scrape_market_info(page, company_id: str, url: str):
    try:
        await page.goto(url, timeout=60000)

        market_cap, volume = None, None

        try:
            market_cap_locator = page.locator("div.sdt-stockinfo-label", has_text="Market Cap (Mil)").locator("xpath=../div[@class='sdt-stockinfo-text']")
            await market_cap_locator.wait_for(timeout=10000)
            market_cap = await market_cap_locator.inner_text()
        except Exception as e:
            print(f"⚠️ Market Cap not found for {company_id}: {e}")

        try:
            await page.wait_for_selector("div.sdt-stockinfo.value div.sdt-stockinfo-text", timeout=5000)
            volume = await page.locator("div.sdt-stockinfo.value div.sdt-stockinfo-text").inner_text()
        except:
            print(f"⚠️ Volume not found for {company_id}")

        print(f"✅ {company_id.zfill(4)} → Market Cap: {market_cap}, Volume: {volume}")

        return {
            "company_id": company_id.zfill(4),
            "market_cap_mil": market_cap.strip() if market_cap else None,
            "volume": volume.strip() if volume else None,
            "source_url": url
        }

    except Exception as e:
        print(f"❌ Failed for {company_id.zfill(4)}: {e}")
        return {
            "company_id": company_id.zfill(4),
            "market_cap_mil": None,
            "volume": None,
            "source_url": url
        }

# ─── TASK WRAPPER ───
async def scrape_wrapper(pw, sem, entry):
    cid, url = entry["company_id"], entry["new_url"]
    random_user_agent = random.choice(USER_AGENTS)

    async with sem:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=random_user_agent)
        page = await context.new_page()

        # inside scrape_wrapper
        try:
            result = await scrape_market_info(page, cid, url)
            # Save per company_id
            pd.DataFrame([result]).to_csv(OUTPUTS_DIR / f"{cid}.csv", index=False)
        finally:
            await context.close()
            await browser.close()
        return result

# ─── MAIN FUNCTION ───
async def main():
    df_urls = pd.read_csv(CSV_PATH, dtype=str).dropna(subset=["company_id", "new_url"])
    seen_ids = {f.stem for f in OUTPUTS_DIR.glob("*.csv")}
    companies = df_urls[~df_urls["company_id"].isin(seen_ids)].to_dict("records")

    sem = asyncio.Semaphore(3)
    results, bad_ids = [], []

    async with async_playwright() as pw:
        tasks = [scrape_wrapper(pw, sem, entry) for entry in companies]
        for future in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="Scraping Market Info"):
            result = await future
            results.append(result)
            if not result["market_cap_mil"] and not result["volume"]:
                bad_ids.append(result["company_id"])

        # Always combine all CSVs from the output folder
    all_csvs = list(OUTPUTS_DIR.glob("*.csv"))
    if all_csvs:
        combined_df = pd.concat([pd.read_csv(f) for f in all_csvs], ignore_index=True)
        combined_df.to_csv(COMBINED_OUTPUT_PATH, index=False)
        print(f"\n✅ Combined CSV updated at {COMBINED_OUTPUT_PATH} with {len(combined_df)} rows.")
    else:
        print(f"\n⚠️ No CSVs found in {OUTPUTS_DIR}. Nothing to combine.")

if __name__ == "__main__":
    asyncio.run(main())