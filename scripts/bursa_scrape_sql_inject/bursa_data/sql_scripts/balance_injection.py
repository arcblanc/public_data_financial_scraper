from pathlib import Path
import pandas as pd
import re
import sys
import warnings
from pandas.errors import SettingWithCopyWarning
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)

REPLACE_MODE = "--replace" in sys.argv

print(f"🛠️  Running in {'REPLACE' if REPLACE_MODE else 'APPEND'} mode")

BASE_DIR = Path(__file__).resolve().parent.parent
csv = pd.read_csv(BASE_DIR / "complete_balance_sheets.csv", keep_default_na=True,na_values=["None", "none", "NaN", "-"],low_memory=False,dtype={"company_id": str})
csv["company_id"] = csv["company_id"].astype(str)

df = csv
bad_ids = df[~df["company_id"].str.fullmatch(r"\d{4}")]
# Ensure company_id is string
csv["company_id"] = csv["company_id"].astype(str)
# Detect bad IDs (not exactly 4 digits)
bad_mask = ~csv["company_id"].str.fullmatch(r"\d{4}")
# Print them
# Fix only the bad ones
csv.loc[bad_mask, "company_id"] = csv.loc[bad_mask, "company_id"].str.zfill(4)
df = csv
# ---------------

# Step 1: Separate Value and YoY rows
df_value = df[df["Year/Type"].str.contains("Value", na=False)].copy()
df_yoy = df[df["Year/Type"].str.contains("YoY %", na=False)].copy()

# Step 2: Extract full date from "Year/Type" (e.g., '31 Mar 2024')
df_value["Fiscal Date"] = df_value["Year/Type"].str.extract(r"(\d{1,2} \w{3} \d{4})")
df_yoy["Fiscal Date"] = df_yoy["Year/Type"].str.extract(r"(\d{1,2} \w{3} \d{4})")

# Step 3: Merge on 'company_id' and 'Fiscal Date'
merged = pd.merge(
    df_value,
    df_yoy,
    on=["company_id", "Fiscal Date"],
    suffixes=("", "_yoy"),
    how="left"
)

# Drop helper columns
merged = merged.drop(columns=["Year/Type", "Year/Type_yoy"])
merged = merged.rename(columns={"Fiscal Date": "Fiscal Date"})

# Find all _yoy columns
yoy_cols = [col for col in merged.columns if col.endswith('_yoy')]

# Identify _yoy columns that do NOT contain any '%' values
cols_to_drop = [
    col for col in yoy_cols
    if not merged[col].astype(str).str.contains('%', na=False).any()
]

# Drop them
merged = merged.drop(columns=cols_to_drop)



# ---------

bursa_registration = pd.read_csv(BASE_DIR / "matched_companies_from_ssm.csv",dtype={"company_id": str,"companyNo": str})
bursa_registration["companyNo"] = (
    bursa_registration["companyNo"]
    .astype(str)
    .str.replace(r"\.0$", "", regex=True)
    .str.strip()
)
# bursa_registration.head()
# Matching company_ids of the public listed companies on bursa.

company_id = pd.read_csv(BASE_DIR.parent.parent / "list_bursa_ids" / "bursa_company_list.csv",dtype={"company_id": str})
company_id["company_id"] = company_id["company_id"].str.strip().str.zfill(4)

# adding registration to company_ids that match bursa's list of public companies
merge = pd.merge(bursa_registration,company_id,on="company_name",how="inner")
merge = merge.drop(columns="company_type")
merge = merge.rename(columns={
    "company_name": "company_name_bursa",
    "name_db": "company_name_api",
    "companyNo": "registration_number",
    "oldCompanyNo": "old_registration_number"
})

merge["company_id"] = merge["company_id"].str.strip()
merged["company_id"] = merged["company_id"].str.strip()
merge["company_id"] = merge["company_id"].astype(str)
merged["company_id"] = merged["company_id"].astype(str)

# Merge registration number, and other details to balance sheet
final_merge = merge.merge(merged, on="company_id", how="inner")

df=final_merge



## Column Name cleanup

df.columns = df.columns.str.replace("-", " ").str.strip().str.strip().str.lower().str.replace(" ", "_").str.replace("-", "_")
df.columns = (
    df.columns
    .str.replace("-", " ", regex=False)        # Replace hyphens with spaces
    .str.replace("/", " ", regex=False) 
    .str.strip()
    .str.lower()
    .str.replace(r"[^\w\s]", "", regex=True)   # Remove non-word characters like . , &
    .str.replace(r"\s+", "_", regex=True)      # Convert spaces to single underscore
    .str.replace(r"_+", "_", regex=True)       # Collapse multiple underscores to one
)
# Normalize columns again for names like investments_ 
df.columns = (
    df.columns
    .str.strip()
    .str.lower()
    .str.replace(r"[^\w\s]", "", regex=True)
    .str.replace(r"\s+", "_", regex=True)
)

# Drop duplicate columns (if any)
duplicates = df.columns[df.columns.duplicated()]
if not duplicates.empty:
    print("Duplicate columns:", duplicates.tolist())
df = df.loc[:, ~df.columns.duplicated()]

# Set up the column map for total_assets calculation
# By converting these cols to numeric 
column_map = {
    "total_current_assets": "total_current_assets",
    "property_plant_equipment_total_net": "property_plant_equipment_total_net",
    "intangibles_net": "intangibles_net",
    "long_term_investments": "long_term_investments",
    "note_receivable_long_term": "note_receivable_long_term",
    "other_long_term_assets": "other_long_term_assets"
}
cols = [
    "total_current_assets",
    "long_term_investments",
    "note_receivable_long_term",
    "other_long_term_assets",
    "intangibles_net"
]


for col in column_map.values():
    if col not in df.columns:
        print(f"❌ Missing column: {col}")
        df[col] = 0
    else:
        # Remove commas before conversion
        df[col] = (
            df[col].astype(str)
            .str.replace(",", "")
            .pipe(pd.to_numeric, errors="coerce")
            .fillna(0)
        )

#------------ How much of the dataset is missing and what the columns names after cleaning is---

df["registration_number"] = df["registration_number"].astype(str)
df["old_registration_number"] = df["old_registration_number"].astype(str)

# null_ratios = df.isnull().mean().sort_values(ascending=False)

# # Convert to DataFrame with formatted percentage
# summary_df = pd.DataFrame({
#     "column": null_ratios.index,
#     "null_ratio": (null_ratios * 100).map("{:.2f}%".format)
# })
# summary_df.to_csv("../../column_percentages/balance_null_ratio_summary.csv", index=False)

# #total assets = 
# """
# 8,626.02  (Current Assets)
# + 792.50  (PPE Net)
# + 15.48   (Intangibles)
# + 17,812.08 (LT Investments)
# + 263.29  (Other LT Assets)
# = 27,509.37 ✅
# + Note Receivable - Long Term: 94.43
# → Total Assets = 27,603.81 ✔️
# """
# # So we need
# """
# [
#     "total_current_assets",
#     "property_plant_equipment_total_net",
#     "intangibles_net",
#     "long_term_investments",
#     "other_long_term_assets",
#     "note_receivable_long_term"
# ]
# """



# ------------- Total Asset Calculation Bursa



# This is to ensure if that row_data has that col, it will clean the data points with missing values for calc 
if "other_long_term_assets_total" in df.columns:
    df["other_long_term_assets"] = (
        df["other_long_term_assets_total"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .pipe(pd.to_numeric, errors="coerce")
        .fillna(0)
    )
else:
    df["other_long_term_assets"] = 0

# Change to numeric to be used for calc as its a string currently
df["other_long_term_assets"] = pd.to_numeric(df["other_long_term_assets"], errors="coerce").fillna(0)

# Final columns to keep — keep only 'total_assets', no recalculation from components
final_columns = [
    "registration_number",
    "old_registration_number",
    "company_id",
    "fiscal_date",
    "fiscal_year",
    "retained_earnings_accumulated_deficit",
    "total_equity",
    "total_debt",
    "total_current_assets",
    "total_assets", 
    "total_current_liabilities",
    "total_liabilities"
]
# 1. Define asset components to compute total_assets
asset_components = [
    "total_current_assets",
    "long_term_investments",
    "note_receivable_long_term",
    "other_long_term_assets",
    "intangibles_net",
    "property_plant_equipment_total_net",
    "goodwill_net"
]
# 2. Ensure all component columns exist and are numeric
for col in asset_components:
    if col not in df.columns:
        df[col] = 0  # fallback if missing
    df[col] = (
        df[col]
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.strip()
        .pipe(pd.to_numeric, errors="coerce")
        .fillna(0)
    )

# 1. Compute total_assets from components (fallback only)
df["computed_total_assets"] = df[asset_components].sum(axis=1)


# 2. Clean scraped total_assets
# in the 15 companies, only 2 had these the rest were computed but accurate to bursa
df["total_assets"] = (
    df["total_assets"]
    .astype(str)
    .str.replace(",", "", regex=False)
    .pipe(pd.to_numeric, errors="coerce")
) 

# 3. Only fill in missing scraped total_assets
df["total_assets"] = df["total_assets"].fillna(df["computed_total_assets"])

# 4. Drop the helper
df.drop(columns=["computed_total_assets"], inplace=True)

# Convert fiscal_date to datetime for sorting
df["fiscal_date"] = pd.to_datetime(df["fiscal_date"], format="%d %b %Y", errors="coerce")

# Sort descending by fiscal_date within each company_id
df = df.sort_values(by=["company_id", "fiscal_date"], ascending=[True, False])

# Convert back to string format expected by DB
df["fiscal_date"] = df["fiscal_date"].dt.strftime("%d %b %Y")


# Filter only those that exist in df
df = df[[col for col in final_columns if col in df.columns]]

# Ensure registration_number is str
df["registration_number"] = df["registration_number"].astype(str).str.strip()
df = df[~df["registration_number"].isin([None, "", "nan", "NaN", "None"])]


### -- Check --- 


# List of registration numbers
registration_numbers = [
    '198401014370', '201001034084', '196501000672', '199101019838',
    '197301002148', '197901003918', '199801018294', '200901024473',
    '193401000023', '198401016183', '199701009694', '195601000197',
    '197401002663', '200601022130', '202301017784', '196001000142'
]

# Print 4 rows per registration number
for reg_no in registration_numbers:
    subset = df[df["registration_number"] == reg_no].head(5)
    print(f"\n🔍 Registration Number: {reg_no}")
    print(subset[["registration_number", "company_id", "fiscal_date", "total_assets"]])
print(f"Final DataFrame shape: {df.shape}")
print(f"Columns: {df.columns.tolist()}")
# -- SQL APPEND


## - second imports 
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os

# ── Load environment variables ──
load_dotenv()

# ── Read DB credentials ──
user = os.getenv("PG_USER")
password = os.getenv("PG_PASSWORD")
host = os.getenv("PG_HOST")
port = os.getenv("PG_PORT")
database = os.getenv("PG_DATABASE")

# ── Create connection string ──
connection_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
engine = create_engine(connection_url)

# ── Your DataFrame ──
df = df.copy()


# ------- For appending new registration numbers or new dates

from sqlalchemy import text

# Step 1: Load existing (registration_number, fiscal_date) pairs
with engine.connect() as conn:
    existing = conn.execute(text("""
        SELECT registration_number, fiscal_date 
        FROM public.public_complete_balance_sheet
    """)).fetchall()
    existing_pairs = set((str(r[0]).strip(), str(r[1])) for r in existing)

# Step 2: Filter df for only new pairs
df["registration_number"] = df["registration_number"].astype(str).str.strip()
df["fiscal_date"] = df["fiscal_date"].astype(str).str.strip()

df["key"] = list(zip(df["registration_number"], df["fiscal_date"]))
df = df[~df["key"].isin(existing_pairs)].drop(columns="key")

print(f"🆕 Appending {len(df)} new rows to DB...")



from sqlalchemy import text

if REPLACE_MODE:
    # REPLACE MODE: Replace whole table
    print("🚨 Replacing entire table with new data...")
    df.to_sql("public_complete_balance_sheet", engine, schema="public", index=False, if_exists="replace")
    print("✅ Replaced 'public_complete_balance_sheet'.")
else:
    # APPEND MODE: Only add new (registration_number, fiscal_date) pairs
    from sqlalchemy import text
    with engine.connect() as conn:
        existing = conn.execute(text("""
            SELECT registration_number, fiscal_date 
            FROM public.public_complete_balance_sheet
        """)).fetchall()
        existing_pairs = set((str(r[0]).strip(), str(r[1])) for r in existing)

    df["registration_number"] = df["registration_number"].astype(str).str.strip()
    df["fiscal_date"] = df["fiscal_date"].astype(str).str.strip()
    df["key"] = list(zip(df["registration_number"], df["fiscal_date"]))
    df = df[~df["key"].isin(existing_pairs)].drop(columns="key")

    print(f"🆕 Appending {len(df)} new rows to DB...")
    df.to_sql("public_complete_balance_sheet", engine, schema="public", index=False, if_exists="append")
    print("✅ Appended to 'public_complete_balance_sheet'.")

"""
If balance_injection.py --replace it will only update new entries such as new company or new dates
balance_injection.py is append mode.
May make a update values or columns in future.

Orchestrator will use append only.
"""