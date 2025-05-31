import asyncio
import random
from pathlib import Path
import pandas as pd
from playwright.async_api import async_playwright
from tqdm.asyncio import tqdm_asyncio



# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
OUTPUTS_DIR = BASE_DIR / "outputs" / "profile_details"
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
CSV_PATH = Path(__file__).resolve().parent.parent / "csvs" / "cleaned" / "company_urls.csv"
# So this should be 5 sections that points to the outputs/profile_details 
SECTIONS = ["profile", "management", "ownership", "top10", "insider"]
# For the combined_csv i want you to take the combined outputs and move them to COMBINED_OUTPUT_PATH = Path(__file__).resolve().parent.parent / "bursa_scrape_sql_inject/bursa_data/market_info_sample.csv"


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)...",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0_0)...",
    "Mozilla/5.0 (X11; Linux x86_64)..."
]
# ─────────────────────────────────────────────
# UTILS
# ─────────────────────────────────────────────

# get_seen_ids() checks which companies have already been scraped. 
# inject_id_url() adds company_id and company_url metadata to each scraped DataFrame.
def get_seen_ids():
    return {f.stem.split(".")[0] for f in OUTPUTS_DIR.glob("*.profile.csv")}

def inject_id_url(df, company_id, company_url):
    df.insert(0, "company_id", company_id)
    df.insert(1, "company_url", company_url)
    return df
# ─────────────────────────────────────────────
# SCRAPER UTILITIES
# ─────────────────────────────────────────────


# Extracts table headers and rows from a given section selector on the page, returning a structured DataFrame.
async def extract_section_table(page, section_selector: str) -> pd.DataFrame:
    header_locator = page.locator(f"{section_selector} .stock-table-head > div")
    row_locator = page.locator(f"{section_selector} .stock-table-body .stock-table-row")

    headers = await header_locator.all_inner_texts()
    row_count = await row_locator.count()

    rows = []
    for i in range(row_count):
        cells = await row_locator.nth(i).locator("div").all_inner_texts()
        if len(cells) == len(headers):
            rows.append(dict(zip(headers, cells)))
        else:
            print(f"⚠️ Row length mismatch at index {i}, skipping row.")

    return pd.DataFrame(rows)


# Loops through paginated tables, clicking “Next” until no more pages, aggregating data into one DataFrame.
async def paginate_section(page, next_button_selector: str, section_selector: str) -> pd.DataFrame:
    all_pages = []
    while True:
        df = await extract_section_table(page, section_selector)
        if not df.empty:
            all_pages.append(df)
        try:
            next_btn = page.locator(next_button_selector)
            if await next_btn.is_enabled():
                await next_btn.click()
                await page.wait_for_timeout(1500)
            else:
                break
        except:
            break
    return pd.concat(all_pages, ignore_index=True) if all_pages else pd.DataFrame()

# ─────────────────────────────────────────────
# PROFILE EXTRACTOR
# ─────────────────────────────────────────────

# Scrapes the “About”, sector, contact info, and address from the profile overview page into a single-row DataFrame.
async def extract_profile_overview(page) -> pd.DataFrame:
    data = {}

    try:
        about = page.locator("div.contactDetails-left .contactInfo-value")
        data["About"] = await about.inner_text()
    except:
        data["About"] = None

    try:
        raw_text = await page.locator("div.contactDetails-right").inner_text()
        lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
        field_map = {
            "Sector": None,
            "Sub Sector": None,
            "Website": None,
            "Phone": None,
            "Fax": None
        }
        for i, label in enumerate(lines):
            if label in field_map and i + 1 < len(lines):
                field_map[label] = lines[i + 1]
        data.update(field_map)
    except Exception as e:
        print(f"⚠️ Failed parsing right column: {e}")

    try:
        address_node = page.locator("a.location_pin")
        if await address_node.count() > 0:
            data["Address"] = await address_node.inner_text()
        else:
            await page.get_by_text("Address", exact=True).click()
            data["Address"] = await page.locator("a.location_pin").inner_text()
    except:
        data["Address"] = None

    return pd.DataFrame([data])

# ─────────────────────────────────────────────
# MANAGEMENT EXTRACTOR
# ─────────────────────────────────────────────
async def extract_management_table(page, section_selector: str) -> pd.DataFrame:
    row_locator = page.locator(f"{section_selector} .stock-table-body .stock-table-row")
    row_count = await row_locator.count()

    data = []
    for i in range(row_count):
        row = row_locator.nth(i)
        data.append({
            "Name": await row.locator(".nameCol").inner_text() if await row.locator(".nameCol").count() else None,
            "Designation": await row.locator(".designationCol").inner_text() if await row.locator(".designationCol").count() else None,
            "Role": await row.locator(".roleCol").inner_text() if await row.locator(".roleCol").count() else None,
            "Since": await row.locator(".sinceCol").inner_text() if await row.locator(".sinceCol").count() else None,
        })

    return pd.DataFrame(data)

async def paginate_management_table(page, section_selector: str) -> pd.DataFrame:
    all_pages = []

    while True:
        df = await extract_management_table(page, section_selector)
        if not df.empty:
            all_pages.append(df)

        try:
            next_btns = page.locator("button", has_text="Next")
            found = False
            for i in range(await next_btns.count()):
                btn = next_btns.nth(i)
                if await btn.get_attribute("disabled") is None:
                    await btn.click()
                    await page.wait_for_timeout(1500)
                    found = True
                    break
            if not found:
                break
        except:
            break

    return pd.concat(all_pages, ignore_index=True) if all_pages else pd.DataFrame()

# Ownership table extractor
async def extract_ownership_table(page, section_selector: str) -> pd.DataFrame:
    row_locator = page.locator(f"{section_selector} .stock-table-body .stock-table-row")
    row_count = await row_locator.count()

    data = []
    for i in range(row_count):
        row = row_locator.nth(i)
        data.append({
            "Investor Name": await row.locator("div.scroll span").inner_text() if await row.locator("div.scroll span").count() else None,
            "No. of Investors": await row.locator(".owner_idCol").inner_text() if await row.locator(".owner_idCol").count() else None,
            "Ownership %": await row.locator(".ownership_percentageCol").inner_text() if await row.locator(".ownership_percentageCol").count() else None,
            "Position (M shares)": await row.locator(".shares_heldCol").inner_text() if await row.locator(".shares_heldCol").count() else None,
            "Position Change (M)": await row.locator(".shares_changedCol").inner_text() if await row.locator(".shares_changedCol").count() else None,
            "Position Change (M) %": await row.locator(".position_changeCol").inner_text() if await row.locator(".position_changeCol").count() else None,
            "Position Value Change (M)": await row.locator(".value_of_shares_changedCol").inner_text() if await row.locator(".value_of_shares_changedCol").count() else None,
            "Value (M USD)": await row.locator(".value_heldCol").inner_text() if await row.locator(".value_heldCol").count() else None,
        })

    return pd.DataFrame(data)

#top investors table extractor
async def extract_top10_table(page, section_selector: str) -> pd.DataFrame:
    row_locator = page.locator(f"{section_selector} .stock-table-body .stock-table-row")
    row_count = await row_locator.count()

    data = []
    for i in range(row_count):
        row = row_locator.nth(i)
        data.append({
            "Investor Name": await row.locator(".ownerCol span").inner_text() if await row.locator(".ownerCol span").count() else None,
            "Ownership %": await row.locator(".ownership_percentageCol").inner_text() if await row.locator(".ownership_percentageCol").count() else None,
            "Position (M Shares)": await row.locator(".shares_heldCol").inner_text() if await row.locator(".shares_heldCol").count() else None,
            "Position Change (M)": await row.locator(".shares_changedCol").inner_text() if await row.locator(".shares_changedCol").count() else None,
            "Position Change (M) %": await row.locator(".position_changeCol").inner_text() if await row.locator(".position_changeCol").count() else None,
            "Position Value Change (M)": await row.locator(".value_of_shares_changedCol").inner_text() if await row.locator(".value_of_shares_changedCol").count() else None,
            "Value (M USD)": await row.locator(".value_heldCol").inner_text() if await row.locator(".value_heldCol").count() else None,
            "Filing Date": await row.locator(".report_dateCol").inner_text() if await row.locator(".report_dateCol").count() else None,
            "Filing Source": await row.locator(".sourceCol").inner_text() if await row.locator(".sourceCol").count() else None,
        })

    return pd.DataFrame(data)

# Latest insider table extractor
async def extract_insider_table(page, section_selector: str) -> pd.DataFrame:
    row_locator = page.locator(f"{section_selector} .stock-table-body .stock-table-row")
    row_count = await row_locator.count()

    data = []
    for i in range(row_count):
        row = row_locator.nth(i)
        data.append({
            "Investor Name": await row.locator(".ownerCol span").inner_text() if await row.locator(".ownerCol span").count() else None,
            "Ownership %": await row.locator(".ownership_percentageCol").inner_text() if await row.locator(".ownership_percentageCol").count() else None,
            "Position (M Shares)": await row.locator(".shares_heldCol").inner_text() if await row.locator(".shares_heldCol").count() else None,
            "Position Change (M)": await row.locator(".shares_changedCol").inner_text() if await row.locator(".shares_changedCol").count() else None,
            "Position Change (M) %": await row.locator(".position_changeCol").inner_text() if await row.locator(".position_changeCol").count() else None,
            "Position Value Change (M)": await row.locator(".value_of_shares_changedCol").inner_text() if await row.locator(".value_of_shares_changedCol").count() else None,
            "Value (M USD)": await row.locator(".value_heldCol").inner_text() if await row.locator(".value_heldCol").count() else None,
            "Filing Date": await row.locator(".report_dateCol").inner_text() if await row.locator(".report_dateCol").count() else None,
            "Filing Source": await row.locator(".sourceCol").inner_text() if await row.locator(".sourceCol").count() else None,
        })

    return pd.DataFrame(data)




# ─────────────────────────────────────────────
# MAIN PROFILE SCRAPER
# ─────────────────────────────────────────────

# scrape_company_profile()

# Main orchestrator that clicks through the profile and all section tabs, 
# runs respective extractors, and returns a dictionary of 5 DataFrames.

async def scrape_company_profile(page, company_id: str, url: str) -> dict:
    print(f"\n🔍 Scraping company_id: {company_id}")
    
    await page.goto(url, timeout=60000)
    await page.get_by_role("button", name="Close").click()
    await page.get_by_role("button", name="Profile").click()

    profile_df = await extract_profile_overview(page)
    print("📄 Profile Extracted:")
    print(profile_df.head(5))

    await page.get_by_text("NameDesignationRoleSince").click()
    await page.get_by_role("button", name="Details").first.click()
    management_df = await paginate_management_table(page, "div.stock-detailed-profile-manegement-table")
    print("📊 Table 'Management' Extracted:")
    print(management_df.head(5))

    # Ownership Section
    try:
        await page.get_by_role("button", name="Details").first.click()

        next_items = page.get_by_role("listitem").filter(has_text="Next")
        if await next_items.count() > 1:
            await next_items.nth(1).click()

        ownership_df = await extract_ownership_table(page, "div:nth-child(3)")
        print("📊 Table 'Ownership Type' Extracted:")
        print(ownership_df.head(5))
    except Exception as e:
        print(f"⚠️ Skipping Ownership section: {e}")
        ownership_df = pd.DataFrame()

    # Top 10 Investors Section
    try:
        await page.get_by_role("button", name="Details").nth(1).click()

        next_items = page.get_by_role("listitem").filter(has_text="Next")
        if await next_items.count() > 2:
            await next_items.nth(2).click()

        top10_df = await extract_top10_table(page, "div:nth-child(4)")
        print("📊 Table 'Top 10 Investors' Extracted:")
        print(top10_df.head(5))
    except Exception as e:
        print(f"⚠️ Skipping Top 10 Investors section: {e}")
        top10_df = pd.DataFrame()

    # Insider Section
    try:
        await page.get_by_role("button", name="Details").nth(2).click()

        next_items = page.get_by_role("listitem").filter(has_text="Next")
        if await next_items.count() > 3:
            await next_items.nth(3).click()

        insider_df = await extract_insider_table(page, "div.latest-insider")
        print("📊 Table 'Latest Insider / Individual Holders' Extracted:")
        print(insider_df.head(5))
    except Exception as e:
        print(f"⚠️ Skipping Insider section: {e}")
        insider_df = pd.DataFrame()

    return {
        "profile": profile_df,
        "management": management_df,
        "ownership": ownership_df,
        "top10": top10_df,
        "insider": insider_df
    }

# ─────────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────────

#Loads company URL list, filters out already scraped ones, creates concurrent scrape tasks with semaphores, and runs them using tqdm_asyncio.
async def scrape_wrapper(pw, sem, entry):
    cid, url = entry["company_id"], entry["new_url"]
    user_agent = random.choice(USER_AGENTS)

    async with sem:
        try:
            # launches a persistent browser profile, meaning it stores session data (cookies, localStorage, etc.) in the user_data_dir.
            # This is different from launch() + context.new_page() (normal Playwright), which uses a fresh, ephemeral context that resets after each run.
            context = await pw.chromium.launch_persistent_context(
                user_data_dir=f"/tmp/{cid}",
                headless=True,
                user_agent=user_agent  # ✅ Set user agent correctly here
            )
            page = await context.new_page()

            results = await scrape_company_profile(page, cid, url)

            for section, df in results.items():
                single_csv_path = OUTPUTS_DIR / f"{cid}.{section}.csv"
                df.to_csv(single_csv_path, index=False)

                COMBINED_DIR = Path(__file__).resolve().parent.parent / "bursa_scrape_sql_inject" / "bursa_data"
                COMBINED_DIR.mkdir(parents=True, exist_ok=True)
                combined_csv_path = COMBINED_DIR / f"combined_{section}.csv"
                df_with_meta = inject_id_url(df.copy(), cid, url)

                if combined_csv_path.exists():
                    existing = pd.read_csv(combined_csv_path, nrows=1)
                    df_with_meta = df_with_meta[existing.columns.intersection(df_with_meta.columns)]
                    df_with_meta.to_csv(combined_csv_path, mode="a", header=False, index=False)
                else:
                    df_with_meta.to_csv(combined_csv_path, index=False)

            await context.close()

        except Exception as e:
            print(f"❌ Error scraping {cid}: {e}")
            # Attempt to close context safely
            try:
                await context.close()
            except:
                pass


async def main():
    df_urls = pd.read_csv(CSV_PATH, dtype=str).dropna(subset=["company_id", "new_url"])
    seen_ids = get_seen_ids()
    companies = df_urls[~df_urls["company_id"].isin(seen_ids)].to_dict("records")

    sem = asyncio.Semaphore(3)

    async with async_playwright() as pw:
        tasks = [scrape_wrapper(pw, sem, entry) for entry in companies]

        # tqdm_asyncio with progress bar
        for future in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="Scraping profiles"):
            await future

def combine_all_profile_sections():
    print("\n🔄 Combining all profile section CSVs...")

    combined_dir = Path(__file__).resolve().parent.parent / "bursa_scrape_sql_inject" / "bursa_data"
    combined_dir.mkdir(parents=True, exist_ok=True)

    for section in SECTIONS:
        all_dfs = []
        files = list(OUTPUTS_DIR.glob(f"*.{section}.csv"))
        for file in files:
            company_id = file.stem.split(".")[0]
            try:
                df = pd.read_csv(file)
                df.insert(0, "company_id", company_id)
                df.insert(1, "source_file", file.name)
                all_dfs.append(df)
            except Exception as e:
                print(f"⚠️ Error reading {file.name}: {e}")

        if all_dfs:
            combined = pd.concat(all_dfs, ignore_index=True)
            out_path = combined_dir / f"combined_{section}.csv"
            combined.to_csv(out_path, index=False)
            print(f"✅ Merged {len(all_dfs)} CSVs into combined_{section}.csv")
        else:
            print(f"⚠️ No data found for section: {section}")
    
if __name__ == "__main__":
    asyncio.run(main())
    combine_all_profile_sections()

    """
    ✅ Overall Flow Summary
	1.	Input: company_urls.csv → Company ID + URL
	2.	Scrape: Go to each URL, extract 5 profile sections
	3.	Output:
	•	Per-company, per-section CSVs in outputs/profile_details/
	•	Appended combined files in bursa_scrape_sql_inject/bursa_data/
	4.	Post-process: Combines all individual section files into final clean CSVs per section

    """