import subprocess
import argparse
import asyncio
from pathlib import Path
from tqdm import tqdm
import warnings
import pandas as pd
warnings.filterwarnings("ignore")
import sys
# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent

SCRIPT_STEPS = [
    ("company_id_scraper.py", BASE_DIR / "list_bursa_ids" / "company_id_scraper.py"),
    ("bursa_url_finder.py", BASE_DIR / "scrapers_1000" / "bursa_url_finder.py"),
    ("scraper_group_parallel", None),  # placeholder for parallel batch
    ("ssm_api_script.py", BASE_DIR / "bursa_scrape_sql_inject" / "ssm_api_script.py"),
    ("sql_master_run.py", BASE_DIR / "bursa_scrape_sql_inject" / "bursa_data" / "sql_master_run.py"),
]

PARALLEL_SCRIPTS = [
    ("balance_sheet.py", BASE_DIR / "scrapers_1000" / "balance_sheet.py"),
    ("cash_flow.py", BASE_DIR / "scrapers_1000" / "cash_flow.py"),
    ("income_statement.py", BASE_DIR / "scrapers_1000" / "income_statement.py"),
    ("market_capscrape.py", BASE_DIR / "scrapers_1000" / "market_capscrape.py"),
    ("profile_scraper.py", BASE_DIR / "scrapers_1000" / "profile_scraper.py"),
]

COMBINE_TARGETS = {
    "balance_sheet_expanded": "complete_balance_sheets.csv",
    "cash_flow_expanded": "complete_cash_flow_statements.csv",
    "income_statement_expanded": "complete_income_statements.csv",
    "marketcap_volume": "market_info_sample.csv",
    "profile_details": {
        "profile": "combined_profile.csv",
        "management": "combined_management.csv",
        "ownership": "combined_ownership.csv",
        "top10": "combined_top10.csv",
        "insider": "combined_insider.csv"
    }
}

# ─────────────────────────────────────────────
# PARALLEL RUNNER
# ─────────────────────────────────────────────
async def run_script(name, path):
    print(f"\U0001F680 Starting {name}")
    process = await asyncio.create_subprocess_exec("python", str(path), stdout=None, stderr=None)
    await process.wait()
    if process.returncode == 0:
        print(f"✅ {name} completed")
    else:
        print(f"❌ {name} failed with code {process.returncode}")

async def run_parallel_group():
    tasks = []
    for name, path in tqdm(PARALLEL_SCRIPTS, desc="⚙️ Parallel Scripts", unit="script"):
        tasks.append(run_script(name, path))
    await asyncio.gather(*tasks)

# ─────────────────────────────────────────────
# MANUAL COMBINE FUNCTION
# ─────────────────────────────────────────────
def run_manual_combiner():
    print("\n🧩 Running manual CSV combine...")

    OUTPUTS_DIR = BASE_DIR / "scrapers_1000" / "outputs"
    COMBINED_DIR = BASE_DIR / "bursa_data"
    COMBINED_DIR.mkdir(parents=True, exist_ok=True)

    def combine_simple_csvs(folder: Path, output_file: Path):
        csv_files = list(folder.glob("*.csv"))
        if not csv_files:
            print(f"⚠️ No files in {folder}")
            return
        df = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)
        df.to_csv(output_file, index=False)
        print(f"✅ Combined {len(csv_files)} files into {output_file.name} ({len(df)} rows)")

    def combine_profile_sections_fixed(folder: Path, section: str, output_file: Path):
        files = list(folder.glob(f"*.{section}.csv"))
        if not files:
            print(f"⚠️ No {section} files in {folder}")
            return

        all_dfs = []
        for f in files:
            try:
                company_id = f.stem.split(".")[0]
                df = pd.read_csv(f)
                df.insert(0, "company_id", company_id)
                df.insert(1, "source_file", str(f.name))
                all_dfs.append(df)
            except Exception as e:
                print(f"⚠️ Skipping {f.name}: {e}")

        if all_dfs:
            combined_df = pd.concat(all_dfs, ignore_index=True)
            combined_df.to_csv(output_file, index=False)
            print(f"✅ Combined {len(files)} files → {output_file.name} ({len(combined_df)} rows)")
        else:
            print(f"⚠️ No valid files found for {section}")

    for foldername, output in COMBINE_TARGETS.items():
        folder_path = OUTPUTS_DIR / foldername
        if isinstance(output, dict):
            for section, outfile in output.items():
                combine_profile_sections_fixed(folder_path, section, COMBINED_DIR / outfile)
        else:
            combine_simple_csvs(folder_path, COMBINED_DIR / output)

# ─────────────────────────────────────────────
# MAIN LOGIC
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Run selected Bursa pipeline scripts.")
    parser.add_argument("--only", help="Run only a specific script by name (e.g., profile_scraper.py)")
    parser.add_argument("--replace", action="store_true", help="Replace data instead of appending")
    args = parser.parse_args()
    REPLACE_MODE = args.replace

    if args.only:
        if args.only in [s[0] for s in PARALLEL_SCRIPTS]:
            print(f"\n⚡ Running only {args.only} from parallel group...")
            asyncio.run(run_script(args.only, dict(PARALLEL_SCRIPTS)[args.only]))
        else:
            match = [s for s in SCRIPT_STEPS if s[0] == args.only]
            if match:
                name, path = match[0]
                if name == "scraper_group_parallel":
                    print(f"\n🚀 Running scraper group in parallel...")
                    asyncio.run(run_parallel_group())
                else:
                    print(f"\n🚀 Running: {name}")
                    args_to_pass = ["python", str(path)]
                    if REPLACE_MODE:
                        args_to_pass.append("--replace")
                    subprocess.run(args_to_pass, check=True)
        return

    bar = tqdm(SCRIPT_STEPS, desc="🔁 Running Pipeline", unit="script", ncols=80)
    for name, path in bar:
        if name == "scraper_group_parallel":
            print("\n🚀 Running scraper group in parallel...")
            asyncio.run(run_parallel_group())

        elif name == "sql_master_run.py":
            run_manual_combiner()
            print(f"\n🚀 Running: {name}")
            args_to_pass = ["python", str(path)]
            if REPLACE_MODE:
                args_to_pass.append("--replace")
            process = subprocess.Popen(args_to_pass)
            process.communicate()
            if process.returncode != 0:
                print(f"❌ Error in {name}")
            if process.returncode != 0:
                print(f"❌ Error in {name}")

        else:
            print(f"\n🚀 Running: {name}")
            process = subprocess.Popen(["python", str(path)])
            process.communicate()
            if process.returncode != 0:
                print(f"❌ Error in {name}")

    print("\n✅ All requested scripts completed!")

if __name__ == "__main__":
    main()
    
    
    """
    This orchestrator.py script is a central pipeline manager for your Bursa Malaysia scraping project. 
    It sequentially or selectively runs a series of scripts 
    (e.g., ID scraper, URL finder, profile scraper, financial scrapers) 
    including a parallel group of Playwright scrapers, 
    and optionally combines all CSV outputs into final structured files. 
    You can run all steps by default or use --only to target a specific script (like just profile_scraper.py or the full parallel batch).
        python orchestrator.py
    
    python orchestrator.py --only scraper_group_parallel
    python orchestrator.py --only profile_scraper.py
    python3 orchestrator.py --only sql_master_run.py
    
    python orchestrator.py
    python orchestrator.py --replace
    python3 orchestrator.py --only sql_master_run.py --replace
    """