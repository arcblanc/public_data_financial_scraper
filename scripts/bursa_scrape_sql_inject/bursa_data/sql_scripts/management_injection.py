import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--replace", action="store_true", help="(Optional) Replace mode flag")
args = parser.parse_args()
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
import pandas as pd

# %%
csv = pd.read_csv(BASE_DIR / "combined_management.csv", keep_default_na=True,na_values=["None", "none", "NaN", "-"],low_memory=False,dtype={"company_id": str})
csv["company_id"] = csv["company_id"].astype(str)

# %%
df = csv
bad_ids = df[~df["company_id"].str.fullmatch(r"\d{4}")]


# %%
# Ensure company_id is string
csv["company_id"] = csv["company_id"].astype(str)

# Detect bad IDs (not exactly 4 digits)
bad_mask = ~csv["company_id"].str.fullmatch(r"\d{4}")



# Fix only the bad ones
csv.loc[bad_mask, "company_id"] = csv.loc[bad_mask, "company_id"].str.zfill(4)

# %%
# Ensure company_id is string type
csv["company_id"] = csv["company_id"].astype(str)

# Find malformed IDs that are not 4-digit strings
bad_ids = csv[~csv["company_id"].str.fullmatch(r"\d{4}")]



# %%
csv["company_id"].nunique()

# %%
df = csv

# %%
merged = df

# %%
len(merged.columns)

# %%
merged

# %%
bursa_registration = pd.read_csv(BASE_DIR / "matched_companies_from_ssm.csv",dtype={"company_id": str,"companyNo": str})
bursa_registration["companyNo"] = (
    bursa_registration["companyNo"]
    .astype(str)
    .str.replace(r"\.0$", "", regex=True)
    .str.strip()
)
bursa_registration.head()



# %%
company_id = pd.read_csv(BASE_DIR.parent.parent / "list_bursa_ids" / "bursa_company_list.csv",dtype={"company_id": str})
company_id["company_id"] = company_id["company_id"].str.strip().str.zfill(4)


# %%
merge = pd.merge(bursa_registration,company_id,on="company_name",how="inner")
merge

# %%
len(merge["company_id"].drop_duplicates())

# %%
merge_copy = merge.copy()

# %%
merge = merge.drop(columns="company_type")
merge = merge.rename(columns={
    "company_name": "company_name_bursa",
    "name_db": "company_name_api",
    "companyNo": "registration_number",
    "oldCompanyNo": "old_registration_number"
})
# merge.to_csv("bursa_company_registra.csv", index=False)

# %%
merge

# %%
merged

# %%
merged["company_id"].nunique()

# %%
merge["company_id"] = merge["company_id"].str.strip()
merged["company_id"] = merged["company_id"].str.strip()

# %%
merge["company_id"] = merge["company_id"].astype(str)
merged["company_id"] = merged["company_id"].astype(str)
final_merge = merge.merge(merged, on="company_id", how="inner")

# %%
final_merge['company_id'].nunique()

# %%
merge

# %%
(final_merge.isnull().mean() * 100).sort_values(ascending=False).to_frame("missing_%").style.background_gradient(cmap='Reds')

# %%
df=final_merge

# %%
df

# %%
# ── Your DataFrame ──
df = df.copy()
df.columns = df.columns.str.replace("-", " ").str.strip().str.strip().str.lower().str.replace(" ", "_").str.replace("-", "_")
df

# %%
df.columns

# %%
import re
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

# %%
df

# %%
df.columns.tolist()

# %%
# Check for duplicate column names
duplicates = df.columns[df.columns.duplicated()]
df = df.loc[:, ~df.columns.duplicated()]

# %%
# Get null ratios sorted
null_ratios = df.isnull().mean().sort_values(ascending=False)

# Convert to DataFrame with formatted percentage
summary_df = pd.DataFrame({
    "column": null_ratios.index,
    "null_ratio": (null_ratios * 100).map("{:.2f}%".format)
})

# Save to CSV
# summary_df.to_csv("/insider_null_ratio_summary.csv", index=False)
summary_df.head()

# %%
len(df["registration_number"].drop_duplicates())

# %%
df = df.dropna(subset=["registration_number"])

# %%
df['registration_number'].nunique()

# %%
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

df.to_sql("public_complete_directors_executives", engine, schema="public", index=False, if_exists="replace")

print("✅ Uploaded to 'public_complete_directors_executives' with normalized column names.")
print(df.head())
print('Congrats it works')
# %%



