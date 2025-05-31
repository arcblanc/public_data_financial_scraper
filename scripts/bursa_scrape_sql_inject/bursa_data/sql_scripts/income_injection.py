from pathlib import Path
import pandas as pd
import re
import sys


REPLACE_MODE = "--replace" in sys.argv
BASE_DIR = Path(__file__).resolve().parent.parent

csv = pd.read_csv(BASE_DIR / "complete_income_statements.csv", keep_default_na=True,na_values=["None", "none", "NaN", "-"],low_memory=False,dtype={"company_id": str})
csv["company_id"] = csv["company_id"].astype(str)

df = csv
bad_ids = df[~df["company_id"].str.fullmatch(r"\d{4}")]
# Detect bad IDs (not exactly 4 digits)
bad_mask = ~csv["company_id"].str.fullmatch(r"\d{4}")
# Fix only the bad ones
csv.loc[bad_mask, "company_id"] = csv.loc[bad_mask, "company_id"].str.zfill(4)
# Find malformed IDs that are not 4-digit strings

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

final_merge = merge.merge(merged, on="company_id", how="inner")
df=final_merge


# ── Your DataFrame ── ## Column Name cleanup

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
# Check for duplicate column names
duplicates = df.columns[df.columns.duplicated()]

df = df.loc[:, ~df.columns.duplicated()]

null_ratios = df.isnull().mean().sort_values(ascending=False)

# Convert to DataFrame with formatted percentage
summary_df = pd.DataFrame({
    "column": null_ratios.index,
    "null_ratio": (null_ratios * 100).map("{:.2f}%".format)
})

# # Save to CSV
summary_df.to_csv(BASE_DIR.parent / "column_percentages" / "cash_null_ratio_summary.csv", index=False)

desired_columns = [
    "company_id",  # ✅ Add this
    "registration_number",
    "old_registration_number",
    "revenue",
    "net_income_before_taxes",
    "net_income_after_taxes",
    "fiscal_date",
    "bank_total_revenue",
    "operating_income",
    "gross_profit",
    "cost_of_revenue",
    "basic_eps_including_extraordinary_items"
]

# Clean bank_total_revenue (remove commas, convert to float)
df["bank_total_revenue"] = (
    df["bank_total_revenue"]
    .astype(str)
    .str.replace(",", "", regex=False)
    .str.strip()
    .pipe(pd.to_numeric, errors="coerce")
    .fillna(0)
)

# Replace revenue only if it's NaN or 0
df["revenue"] = df["revenue"].fillna(0)
df["revenue"] = df.apply(
    lambda row: row["bank_total_revenue"] if row["revenue"] == 0 and row["bank_total_revenue"] > 0 else row["revenue"],
    axis=1
)

# Drop the helper column
df.drop(columns=["bank_total_revenue"], inplace=True)


# Convert fiscal_date to datetime for sorting
df["fiscal_date"] = pd.to_datetime(df["fiscal_date"], format="%d %b %Y", errors="coerce")

# Sort descending by fiscal_date within each company_id
df = df.sort_values(by=["company_id", "fiscal_date"], ascending=[True, False])

# Convert back to string format expected by DB
df["fiscal_date"] = df["fiscal_date"].dt.strftime("%d %b %Y")


df = df[[col for col in desired_columns if col in df.columns]]
df = df.dropna(subset=["registration_number"])

## ALL THE BANKS don't use revenue in their financials statements
company_1155_manual_revenue = {
    "31 Dec 2024": 27907,
    "31 Dec 2023": 25650,
    "31 Dec 2022": 23702,
    "31 Dec 2021": 22249,
    "31 Dec 2020": 19670
}

for date, value in company_1155_manual_revenue.items():
    mask = (df["company_id"] == "1155") & (df["fiscal_date"] == date)
    df.loc[mask, "revenue"] = float(value)
#---- manual additions
cimb_manual_revenue = {
    "31 Dec 2024": 22301.154,
    "31 Dec 2023": 21014.482,
    "31 Dec 2022": 19837.516,
    "31 Dec 2021": 19512.940,
    "31 Dec 2020": 17189.003
}

for date, value in cimb_manual_revenue.items():
    mask = (df["company_id"] == "1023") & (df["fiscal_date"] == date)
    df.loc[mask, "revenue"] = float(value)
    
hlb_manual_revenue = {
    "30 Jun 2024": 5884,
    "30 Jun 2023": 5570,
    "30 Jun 2022": 5417,
    "30 Jun 2021": 4803,
    "30 Jun 2020": 4399
}

for date, value in hlb_manual_revenue.items():
    mask = (df["company_id"] == "5819") & (df["fiscal_date"] == date)
    df.loc[mask, "revenue"] = float(value)
    

public_bank_manual_revenue = {
    "31 Dec 2024": 14040,
    "31 Dec 2023": 12949,
    "31 Dec 2022": 13065,
    "31 Dec 2021": 11305,
    "31 Dec 2020": 10045
}
for date, value in public_bank_manual_revenue.items():
    mask = (df["company_id"] == "1295") & (df["fiscal_date"] == date)
    df.loc[mask, "revenue"] = float(value)


# # Optional: normalize fiscal_date if needed
# df["fiscal_date"] = pd.to_datetime(df["fiscal_date"], errors="coerce").dt.strftime("%d %b %Y")

# # Patch revenue values
# for date, value in publicbank_manual_revenue.items():
#     mask = (df["company_id"] == "1295") & (df["fiscal_date"] == date)
#     df.loc[mask, "revenue"] = float(value)
# # Optional: sort
# df = df.sort_values(["company_id", "fiscal_date"])





#---------
# Clean commas before conversion
df["revenue"] = df["revenue"].astype(str).str.replace(",", "").str.strip()
df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")


# Ensure registration_number is str
df["registration_number"] = df["registration_number"].astype(str).str.strip()
df = df[~df["registration_number"].isin([None, "", "nan", "NaN", "None"])]


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
    print(subset[["registration_number", "company_id", "fiscal_date", "revenue"]])
print(f"Final DataFrame shape: {df.shape}")
print(f"Columns: {df.columns.tolist()}")

# Ensure fiscal_date is datetime for proper sorting
df["fiscal_date"] = pd.to_datetime(df["fiscal_date"], errors="coerce")

# Sort by fiscal_date descending (most recent first), then by company_id
df = df.sort_values(["company_id", "fiscal_date"], ascending=[True, False])
df["fiscal_date"] = pd.to_datetime(df["fiscal_date"], errors="coerce").dt.strftime("%d %b %Y")
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

# ── Push to DB ──
df["registration_number"] = df["registration_number"].astype(str)
df["old_registration_number"] = df["old_registration_number"].astype(str)

from sqlalchemy import text

if REPLACE_MODE:
    # REPLACE MODE: Replace whole table
    print("🚨 Replacing entire table with new data...")
    df.to_sql("public_complete_income", engine, schema="public", index=False, if_exists="replace")
    print("✅ Replaced 'public_complete_income'.")
else:
    # APPEND MODE: Only add new (registration_number, fiscal_date) pairs
    with engine.connect() as conn:
        existing = conn.execute(text("""
            SELECT registration_number, fiscal_date 
            FROM public.public_complete_income
        """)).fetchall()
        existing_pairs = set((str(r[0]).strip(), str(r[1])) for r in existing)

    df["registration_number"] = df["registration_number"].astype(str).str.strip()
    df["fiscal_date"] = df["fiscal_date"].astype(str).str.strip()
    df["key"] = list(zip(df["registration_number"], df["fiscal_date"]))
    df = df[~df["key"].isin(existing_pairs)].drop(columns="key")

    print(f"🆕 Appending {len(df)} new rows to DB...")
    df.to_sql("public_complete_income", engine, schema="public", index=False, if_exists="append")
    print("✅ Appended to 'public_complete_income'.")

"""
If income_injection.py  it will only update new entries such as new company or new dates
income_injection.py --replace
May make a update for just values or columns in future.

Orchestrator will default
"""