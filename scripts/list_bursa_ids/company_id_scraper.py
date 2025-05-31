import asyncio
import pandas as pd
from pathlib import Path
from playwright.async_api import async_playwright
import argparse # cli flag

# ── Paths ──
BASE_DIR = Path(__file__).resolve().parent  # Set base directory to current script's folder
OUTPUT_DIR = BASE_DIR  # Output will be saved in the same folder
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)  # Ensure output folder exists

# ── URLs and output file mapping ──
MARKET_URLS = {
    "bursa_main.csv": "https://www.bursamalaysia.com/trade/trading_resources/listing_directory/main_market",
    "ace_market.csv": "https://www.bursamalaysia.com/trade/trading_resources/listing_directory/ace_market",
    "leap_market.csv": "https://www.bursamalaysia.com/trade/trading_resources/listing_directory/leap_market"
}

# Map filename to output path
MARKET_OUTPUTS = {fname: OUTPUT_DIR / fname for fname in MARKET_URLS.keys()}
COMBINED_OUTPUT = OUTPUT_DIR / "bursa_company_list.csv"  # Final combined CSV

# ── Scrape Function ──
async def scrape_market(page, url: str) -> list[dict]:
    await page.goto(url)  # Navigate to Bursa market page

    # Wait and get the embedded iframe containing the data table
    frame = None
    for _ in range(30):
        for f in page.frames:
            if "listing_directory" in f.url:
                frame = f
                break
        if frame:
            break
        await asyncio.sleep(1)  # Retry every second if frame not found

    if not frame:
        raise Exception(f"❌ Could not find data iframe for {url}")

    # Select "All" entries from dropdown to load full table
    await frame.wait_for_selector('select[name="DataTables_Table_0_length"]')
    await frame.select_option('select[name="DataTables_Table_0_length"]', value='-1')
    await frame.wait_for_timeout(2000)  # Wait for table to fully load

    # Extract all company rows from the table
    rows = await frame.query_selector_all("table#DataTables_Table_0 tbody tr")
    print(f"🔍 Found {len(rows)} rows in {url}")

    data = []
    for row in rows:
        try:
            anchor = await row.query_selector("td a")  # Get company link
            name = await anchor.inner_text()  # Extract name
            href = await anchor.get_attribute("href")  # Extract href

            # Parse company_id from stock_code URL
            if href and "stock_code=" in href:
                company_id = href.split("stock_code=")[-1]
                data.append({"company_name": name.strip(), "company_id": company_id.strip()})
        except Exception as e:
            print(f"⚠️ Row parsing failed: {e}")
            continue
    return data

# ── Main Async Entry ──
async def main(update_mode=False, full_mode=False, dry_run=False):
    all_dataframes = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False, slow_mo=200)  # Launch browser with slow motion for debug
        page = await browser.new_page()

        for filename, url in MARKET_URLS.items():
            data = await scrape_market(page, url)  # Scrape each market page
            df = pd.DataFrame(data).drop_duplicates(subset="company_id")  # Remove duplicates
            output_path = MARKET_OUTPUTS[filename]
            df.to_csv(output_path, index=False)  # Save per-market CSV
            all_dataframes.append(df)
            print(f"✅ Saved {len(df)} companies to {output_path.name}")

        # Merge all markets into one CSV
        new_df = pd.concat(all_dataframes, ignore_index=True).drop_duplicates(subset="company_id")

        # ── Update mode: add new companies only ──
        if update_mode and COMBINED_OUTPUT.exists():
            existing_df = pd.read_csv(COMBINED_OUTPUT, dtype=str)
            existing_ids = set(existing_df["company_id"])
            new_entries = new_df[~new_df["company_id"].isin(existing_ids)]

            if not new_entries.empty:
                log_path = OUTPUT_DIR / "log_new_companies.csv"
                if not dry_run:
                    new_entries.to_csv(log_path, index=False)
                    updated_df = pd.concat([existing_df, new_entries], ignore_index=True)
                    updated_df.to_csv(COMBINED_OUTPUT, index=False)
                print(f"📝 Logged {len(new_entries)} new companies to {log_path.name}")
                print(f"🆕 Added {len(new_entries)} new companies. Total: {len(existing_df) + len(new_entries)}")
            else:
                print("✅ No new companies to add.")

        # ── Full mode: overwrite everything ──
        elif full_mode or not COMBINED_OUTPUT.exists():
            if not dry_run:
                new_df.to_csv(COMBINED_OUTPUT, index=False)
            print(f"\n🧾 Full mode: saved {len(new_df)} companies to {COMBINED_OUTPUT.name}")

        # ── Fallback/default behavior ──
        else:
            if not dry_run:
                new_df.to_csv(COMBINED_OUTPUT, index=False)
            print(f"\n🧾 Combined and saved {len(new_df)} companies to {COMBINED_OUTPUT.name}")

        await browser.close()

# ── Run ──
if __name__ == "__main__":
    # setup update instead of blindly scraping
    parser = argparse.ArgumentParser()
    # When passed (--update), it sets update_mode=True.
   
    parser.add_argument("--dry-run", action="store_true", help="Run without writing any files")
    
    
    # add_mutually_exclusive_group prevents both of them being used at the same time
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--update", action="store_true", help="Only add new company_ids to the combined CSV")
    mode_group.add_argument("--full", action="store_true", help="Overwrite everything (default if no flag is passed)")
    args = parser.parse_args()

    asyncio.run(main(update_mode=args.update, full_mode=args.full, dry_run=args.dry_run))


    """
    Scrapes the website for 3 markets that contributes to public bursa list. 
    ace_market
    bursa_main
    leap_market
    -----------
    # List of flags: 
    python company_id_scraper.py                                   -> regular script for start                              
    python company_id_scraper.py --update           -> Only append new companies
    python company_id_scraper.py --full             -> overwrite mode
    python company_id_scraper.py --update --full    -> ❌ Invalid (argparse prevents it)
    python company_id_scraper.py --dry-run          -> Simulate full scrape, no files written
    python company_id_scraper.py --update --dry-run -> Simulate update mode, print what would be added, no writes

    """