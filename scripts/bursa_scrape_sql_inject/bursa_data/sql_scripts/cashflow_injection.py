from pathlib import Path
import pandas as pd
import re
import sys

REPLACE_MODE = "--replace" in sys.argv

BASE_DIR = Path(__file__).resolve().parent.parent
csv = pd.read_csv(BASE_DIR / "complete_cash_flow_statements.csv", keep_default_na=True,na_values=["None", "none", "NaN", "-"],low_memory=False,dtype={"company_id": str})
csv["company_id"] = csv["company_id"].astype(str)

df = csv
bad_ids = df[~df["company_id"].str.fullmatch(r"\d{4}")]
csv["company_id"] = csv["company_id"].astype(str)
bad_mask = ~csv["company_id"].str.fullmatch(r"\d{4}")
csv.loc[bad_mask, "company_id"] = csv.loc[bad_mask, "company_id"].str.zfill(4)
df = csv


#---------- 



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

# renaming api cols to match
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






# ── ## Column Name cleanup ----
df.columns = df.columns.str.replace("-", " ").str.strip().str.strip().str.lower().str.replace(" ", "_").str.replace("-", "_")

##--- Normalizing the names

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






#------------ How much of the dataset is missing and what the columns names after cleaning is---

# Get null ratios sorted
null_ratios = df.isnull().mean().sort_values(ascending=False)

# Convert to DataFrame with formatted percentage
summary_df = pd.DataFrame({
    "column": null_ratios.index,
    "null_ratio": (null_ratios * 100).map("{:.2f}%".format)
})


df = df.dropna(subset=["registration_number"])

### -- Check --- 


# # List of registration numbers
# registration_numbers = [
#     '198401014370', '201001034084', '196501000672', '199101019838',
#     '197301002148', '197901003918', '199801018294', '200901024473',
#     '193401000023', '198401016183', '199701009694', '195601000197',
#     '197401002663', '200601022130', '202301017784', '196001000142'
# ]

# # Print 4 rows per registration number
# for reg_no in registration_numbers:
#     subset = df[df["registration_number"] == reg_no].head(5)
#     print(f"\n🔍 Registration Number: {reg_no}")
#     print(subset[["registration_number", "company_id", "fiscal_date", "net_income_starting_line"]])
# print(f"Final DataFrame shape: {df.shape}")
# # print(f"Columns: {df.columns.tolist()}")

# Ensure registration_number is str
df["registration_number"] = df["registration_number"].astype(str).str.strip()
df = df[~df["registration_number"].isin([None, "", "nan", "NaN", "None"])]



# -- SQL APPEND

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



from sqlalchemy import text

if REPLACE_MODE:
    # REPLACE MODE: Replace whole table
    print("🚨 Replacing entire table with new data...")
    df.to_sql("public_complete_cash_flow", engine, schema="public", index=False, if_exists="replace")
    print("✅ Replaced 'public_complete_cash_flow'.")
else:
    # APPEND MODE: Only add new (registration_number, fiscal_date) pairs
    with engine.connect() as conn:
        existing = conn.execute(text("""
            SELECT registration_number, fiscal_date 
            FROM public.public_complete_cash_flow
        """)).fetchall()
        existing_pairs = set((str(r[0]).strip(), str(r[1])) for r in existing)

    df["registration_number"] = df["registration_number"].astype(str).str.strip()
    df["fiscal_date"] = df["fiscal_date"].astype(str).str.strip()
    df["key"] = list(zip(df["registration_number"], df["fiscal_date"]))
    df = df[~df["key"].isin(existing_pairs)].drop(columns="key")

    print(f"🆕 Appending {len(df)} new rows to DB...")
    df.to_sql("public_complete_cash_flow", engine, schema="public", index=False, if_exists="append")
    print("✅ Appended to 'public_complete_cash_flow'.")


    """_summary_
    cashflow_injection.py -> appends
    cashflow_injection.py --replace ->replaces
    """