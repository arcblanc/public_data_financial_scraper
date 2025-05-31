import asyncio
import os
import random
from pathlib import Path
import pandas as pd
from tqdm.asyncio import tqdm_asyncio
from playwright.async_api import async_playwright
from datetime import datetime
from collections import defaultdict
import argparse
# ─── CONFIG ───
BASE_DIR = Path(__file__).resolve().parent
OUTPUTS_DIR = BASE_DIR / "outputs" / "cash_flow_expanded"
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
CSV_PATH = Path(__file__).resolve().parent.parent / "csvs/cleaned/company_urls.csv"
COMBINED_OUTPUT_PATH = Path(__file__).resolve().parent.parent / "bursa_scrape_sql_inject/bursa_data/complete_cash_flow_statements.csv"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)...",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0_0)...",
    "Mozilla/5.0 (X11; Linux x86_64)..."
]

# ─── SCRAPE FUNCTION ───
async def scrape_company_cashflow(page, company_id: str, url: str):
    try:
        await page.goto(url, timeout=60000)
        await page.get_by_role("button", name="Financials").click()
        await page.get_by_role("button", name="Close").click()

        for attempt in range(2):
            try:
                await page.get_by_role("button", name="Financials").click()
                await page.get_by_role("button", name="Statements").click()
                await page.get_by_role("button", name="Financials").click()
                await page.get_by_role("button", name="Statements").click()
                await page.get_by_role("button", name="Financials").click()
                await page.get_by_role("button", name="Statements").click()
                await page.get_by_role("button", name="Cash Flow").click()
                await page.get_by_role("button", name="Quarterly").click()
                await page.get_by_text("Annual").click()
                await page.wait_for_selector("div.stock-table-body")
                break
            except Exception:
                if attempt == 0:
                    await page.get_by_role("button", name="Profile").click()
                    await asyncio.sleep(1)
                else:
                    raise

        rows = page.locator("div.d-flex.stock-table-flex.w-100")
        row_count = await rows.count()

        raw_data = {}
        fiscal_years = []

        for i in range(row_count):
            row = rows.nth(i)
            cells = await row.locator("div").all_inner_texts()
            cells = [c.strip() for c in cells if c.strip()]
            if not cells:
                continue

            lines = cells[0].splitlines()
            if not lines:
                continue

            metric = lines[0]
            rest = lines[1:]

            if "Amount Standardised" in metric and not fiscal_years:
                for text in rest:
                    text = text.strip()
                    if text.lower() == "5-year trend":
                        continue
                    try:
                        date_label = " ".join(text.strip().split()[-3:])
                        datetime.strptime(date_label, "%d %b %Y")
                        fiscal_years.append(date_label)
                    except Exception:
                        fiscal_years.append(None)
                continue

            if len(rest) % 2 != 0:
                rest.append('')  # prevent misalignment on stray '-'
            """
            •	Each row should have pairs: one value and one %.
            •	If the total number is odd, we append an empty string to make it even.
            •	This avoids zip() mismatches.
            """
            value_list = rest[::2]
            yoy_list = rest[1::2]

            # Normalize dash placeholders
            value_list = [None if v in {'-', '—'} else v for v in value_list]
            yoy_list = [None if p in {'-', '—'} else p for p in yoy_list]
            # 	If you see a dash (- or —), treat it as missing and convert it to None.
            for j, (val, pct) in enumerate(zip(value_list, yoy_list)):
                """
                	You’re looping through value_list and yoy_list together.
                    •	j is the index (0 for latest year, etc).
                    •	val = e.g. "1,200" (actual number).
                    •	pct = e.g. "+5%" (YoY percentage growth).

                
                """ 
                if j >= len(fiscal_years) or fiscal_years[j] is None:
                    continue
                year = fiscal_years[j]
                """
                •	Some rows might not align perfectly with years (e.g. missing data).
	            •	Skip if the j index is beyond your year list, or if the year is None.
                """
                val = val.strip() if isinstance(val, str) else val
                pct = pct.strip() if isinstance(pct, str) else pct

                # Correct misplacement: if val looks like a percentage and pct is empty, swap
                if isinstance(val, str) and val.endswith('%') and (not pct or not (isinstance(pct, str) and pct.endswith('%'))):
                    val, pct = None, val
                """
                •	Sometimes the table puts the YoY % in the wrong cell — into val.
                •	If val ends in % (but pct is empty or wrong), you:
                •	Move the % to pct
                •	Set val = None
                •	Example fix:
                val = '+5%' → None
                pct = ''    → '+5%'
                
                •	Only save val if it’s a proper number (not accidentally a %).
                •	The key is a tuple: e.g. ("2023", "Value")
                •	The metric might be something like "Revenue" or "Net Income"
                """         

                if isinstance(val, str) and not val.endswith('%'):
                    raw_data.setdefault((year, "Value"), {})[metric] = val
                else:
                    raw_data.setdefault((year, "Value"), {})[metric] = None

                if isinstance(pct, str) and pct.endswith('%'):
                    raw_data.setdefault((year, "YoY %"), {})[metric] = pct
                else:
                    raw_data.setdefault((year, "YoY %"), {})[metric] = None


        wide_data = defaultdict(dict)
        for (year, typ), metrics in raw_data.items():
            row_label = f"{year} {typ}"
            for metric, val in metrics.items():
                wide_data[row_label][metric] = val

        df = pd.DataFrame.from_dict(wide_data, orient="index").reset_index()
        df = df.rename(columns={"index": "Year/Type"})
        df.insert(0, "company_id", str(company_id))
        df["source_url"] = url

        for col in df.columns:
            if "Amount Standardised" in col:
                df = df.drop(columns=[col])
                break

        return df

    except Exception as e:
        print(f"\n⚠️ Failed to scrape company_id {company_id} — {url}: {e}")
        return pd.DataFrame()

# ─── TASK WRAPPER ───
async def scrape_wrapper(pw, sem, entry):
    cid, url = entry["company_id"], entry["new_url"]
    random_user_agent = random.choice(USER_AGENTS)

    async with sem:
        browser = await pw.chromium.launch(headless=True, slow_mo=300)
        context = await browser.new_context(user_agent=random_user_agent)
        page = await context.new_page()

        try:
            df_result = await scrape_company_cashflow(page, cid, url)
            if not df_result.empty:
                print(f"\nPreview for company_id {cid} — {url}:")
                print(df_result.head(5))
                df_result.to_csv(OUTPUTS_DIR / f"{cid}.csv", index=False)
        except Exception as e:
            print(f"\n❌ Error scraping {cid}: {e}")
        finally:
            await context.close()
            await browser.close()
            await asyncio.sleep(random.uniform(6, 15))

# ─── MAIN FUNCTION ───

async def main():
    import argparse

    # ─── ARGUMENT PARSER ───
    parser = argparse.ArgumentParser(description="Scrape Bursa cash flow statements.")
    parser.add_argument(
        "--company-id", type=str,
        help="If set, scrape only this company ID, even if its CSV already exists."
    )
    args = parser.parse_args()

    # ─── LOAD COMPANY URL CSV ───
    df_urls = pd.read_csv(CSV_PATH, dtype=str)
    df_urls = df_urls.dropna(subset=["company_id", "new_url"])

    # ─── FILTER TO ONE COMPANY IF SPECIFIED ───
    if args.company_id:
        df_urls = df_urls[df_urls["company_id"] == args.company_id]
        print(f"🔍 Running in single-company mode: {args.company_id}")
    else:
        # Skip companies that have already been scraped (output file exists)
        seen_ids = {f.stem for f in OUTPUTS_DIR.glob("*.csv")}
        df_urls = df_urls[~df_urls["company_id"].isin(seen_ids)]

    # ─── PREPARE SCRAPING TASKS ───
    companies = df_urls.to_dict("records")
    if not companies:
        print("⚠️ No companies to scrape.")
        return

    sem = asyncio.Semaphore(3)  # Controls how many concurrent scrapes happen at once

    async with async_playwright() as pw:
        # For each company, start an async scraping task wrapped in concurrency control
        tasks = [scrape_wrapper(pw, sem, entry) for entry in companies]
        for task in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="Scraping companies"):
            await task

    # ─── MERGE INDIVIDUAL COMPANY CSV FILES ───
    csvs_to_merge = [
        f for f in OUTPUTS_DIR.glob("*.csv")
        if f.name != COMBINED_OUTPUT_PATH.name  # Skip the combined file itself
    ]
    print(f"📦 Merging {len(csvs_to_merge)} CSVs from {OUTPUTS_DIR}")

    combined_df = pd.concat(
        (pd.read_csv(f, dtype={"company_id": str}) for f in csvs_to_merge),
        ignore_index=True
    )
    combined_df.to_csv(COMBINED_OUTPUT_PATH, index=False)

    print(f"\n✅ Saved combined file to {COMBINED_OUTPUT_PATH}")
    print(f"✅ Combined {len(csvs_to_merge)} files into {COMBINED_OUTPUT_PATH}")


if __name__ == "__main__":
    asyncio.run(main())

"""
Usage:
    python3 scrapers_1000/cash_flow.py --company-id 0051   # Single company mode
    python3 scrapers_1000/cash_flow.py                    # Scrape all remaining
"""

