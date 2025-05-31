import pandas as pd
import requests
import time
from pathlib import Path
from tqdm import tqdm

# ─── PATH CONFIG ───
BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_CSV = BASE_DIR / 'list_bursa_ids' / 'bursa_company_list.csv'
OUTPUT_CSV = BASE_DIR / 'bursa_scrape_sql_inject' / 'bursa_data' / 'matched_companies_from_ssm.csv'
API_URL = "https://staging-ssm.onecredit.my/api/search"

# ─── SSM Query Function ───
from rapidfuzz import fuzz

def query_company(name):
    try:
        response = requests.get(API_URL, params={"query": name}, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list) and data:
                # Score all candidates and pick the best
                best_match, best_score = None, 0
                for item in data:
                    candidate_name = item.get("companyName", "")
                    score = fuzz.ratio(name.lower(), candidate_name.lower())
                    if score > best_score:
                        best_score = score
                        best_match = item

                if best_match:
                    return {
                        "company_name": name,
                        "name_db": best_match.get("companyName"),
                        "companyNo": best_match.get("companyNo"),
                        "oldCompanyNo": best_match.get("oldCompanyNo"),
                        "company_type": best_match.get("entityType"),
                        "match_score": best_score
                    }

    except Exception as e:
        print(f"⚠️ Error for '{name}': {e}")

    # Return empty result if no match
    return {
        "company_name": name,
        "name_db": None,
        "companyNo": None,
        "oldCompanyNo": None,
        "company_type": None,
        "match_score": 0
    }
# ─── Main Execution ───
def main():
    df = pd.read_csv(INPUT_CSV)
    company_names = df["company_name"].dropna().unique()

    # Load existing matches (if any)
    if OUTPUT_CSV.exists():
        existing = pd.read_csv(OUTPUT_CSV)
        already_matched = set(existing["company_name"].dropna().unique())
        print(f"🔁 Skipping {len(already_matched)} previously matched companies")
    else:
        existing = pd.DataFrame()
        already_matched = set()

    # Only query new companies
    to_query = [name for name in company_names if name not in already_matched]
    print(f"📦 Querying {len(to_query)} new companies...")

    new_results = []
    for name in tqdm(to_query, desc="🔍 Matching companies"):
        new_results.append(query_company(name))
        time.sleep(1.0)


# ### Manual Input of more companies - From CTOS -> need to put more big companies
# ─── Manual override entries ───


        # Combine new results with existing
    df_new = pd.DataFrame(new_results)
    final_df = pd.concat([existing, df_new], ignore_index=True)

    # ─── Manual override entries ───
    manual_override = {
        "SIME DARBY PROPERTY BERHAD": {
            "company_name": "SIME DARBY PROPERTY BERHAD",
            "name_db": "SIME DARBY PROPERTY BERHAD",
            "companyNo": "197301002148",
            "oldCompanyNo": "0015631P",
            "company_type": "Company",
            "match_score": 100
        }
    }

    # Apply manual overrides
    final_df = final_df[~final_df["company_name"].isin(manual_override.keys())]
    manual_df = pd.DataFrame(manual_override.values())
    final_df = pd.concat([final_df, manual_df], ignore_index=True)

    # Save final result
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    final_df.to_csv(OUTPUT_CSV, index=False)
    print(f"\n✅ Done. Total saved to {OUTPUT_CSV.name}: {len(final_df)} companies")
    
if __name__ == "__main__":
    main()