# %%
from pathlib import Path
import pandas as pd
import sys

REPLACE_MODE = "--replace" in sys.argv
# ── Base path ──
BASE_DIR = Path(__file__).resolve().parent.parent

csv = pd.read_csv(BASE_DIR / "combined_profile.csv", keep_default_na=True, na_values=["None", "none", "NaN", "-"], low_memory=False, dtype={"company_id": str})
csv["company_id"] = csv["company_id"].astype(str)


df = csv



# Ensure company_id is string
csv["company_id"] = csv["company_id"].astype(str)

# Detect bad IDs (not exactly 4 digits)
bad_mask = ~csv["company_id"].str.fullmatch(r"\d{4}")

# Fix only the bad ones
csv.loc[bad_mask, "company_id"] = csv.loc[bad_mask, "company_id"].str.zfill(4)

df = csv
merged = df

bursa_registration = pd.read_csv(BASE_DIR / "matched_companies_from_ssm.csv", dtype={"company_id": str, "companyNo": str})
bursa_registration["companyNo"] = (
    bursa_registration["companyNo"]
    .astype(str)
    .str.replace(r"\.0$", "", regex=True)
    .str.strip()
)

company_id = pd.read_csv(BASE_DIR.parent.parent / "list_bursa_ids" / "bursa_company_list.csv", dtype={"company_id": str})
company_id["company_id"] = company_id["company_id"].str.strip().str.zfill(4)
merge = pd.merge(bursa_registration, company_id, on="company_name", how="inner")

# %%
merge = merge.drop(columns="company_type")
merge = merge.rename(columns={
    "company_name": "company_name_bursa",
    "name_db": "company_name_api",
    "companyNo": "registration_number",
    "oldCompanyNo": "old_registration_number"
})

merged = df
merge["company_id"] = merge["company_id"].str.strip()
merged["company_id"] = merged["company_id"].str.strip()
merge["company_id"] = merge["company_id"].astype(str)
merged["company_id"] = merged["company_id"].astype(str)
final_merge = merge.merge(merged, on="company_id", how="inner")
market_cap = pd.read_csv(BASE_DIR / "market_info_sample.csv", dtype={"company_id": str})
market_cap = market_cap[['company_id', 'market_cap_mil']]

# Ensure company_id is string
market_cap["company_id"] = market_cap["company_id"].astype(str)

# Detect bad IDs (not exactly 4 digits)
bad_mask = ~market_cap["company_id"].str.fullmatch(r"\d{4}")

final_merge = pd.merge(final_merge, market_cap, on="company_id", how="left")

df = final_merge
# ── Your DataFrame ──
df = df.copy()
df.columns = df.columns.str.replace("-", " ").str.strip().str.strip().str.lower().str.replace(" ", "_").str.replace("-", "_")

import re
df.columns = (
    df.columns
    .str.replace("-", " ", regex=False)
    .str.replace("/", " ", regex=False)
    .str.strip()
    .str.lower()
    .str.replace(r"[^\w\s]", "", regex=True)
    .str.replace(r"\s+", "_", regex=True)
    .str.replace(r"_+", "_", regex=True)
)

df = df.dropna(subset=["registration_number"])

# Check for duplicate column names
duplicates = df.columns[df.columns.duplicated()]
df = df.loc[:, ~df.columns.duplicated()]
df = df.drop(columns="source_file")
df = df.drop(columns="match_score")
# %%
from sqlalchemy import create_engine
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

# ── Push to DB ──
dupes = df[df.duplicated(subset=["company_id", "registration_number"], keep=False)]
if not dupes.empty:
    print("⚠️ Found duplicates that will be dropped:")
    print(dupes.sort_values(by=["company_id", "registration_number"]))
# 🧼 Drop rows with null or invalid registration_number
df = df[df["registration_number"].notna()]
df = df[df["registration_number"].str.strip().str.lower() != "nan"]
df = df.drop_duplicates(subset=["company_id", "registration_number"])
# Ensure registration_number is str
df["registration_number"] = df["registration_number"].astype(str).str.strip()
df = df[~df["registration_number"].isin([None, "", "nan", "NaN", "None"])]


# ── Enforce uniqueness on company_id and registration_number ──
from sqlalchemy import text  # Make sure this is imported at the top



if REPLACE_MODE:
    # REPLACE MODE: Replace entire table
    print("🚨 Replacing entire table with new data...")
    df.to_sql("public_complete_company_profile", engine, schema="public", index=False, if_exists="replace")
    print("✅ Replaced 'public_complete_company_profile'.")
else:
    # APPEND MODE: Only add new (registration_number, company_id) pairs
    with engine.connect() as conn:
        existing = conn.execute(text("""
            SELECT registration_number, company_id 
            FROM public.public_complete_company_profile
        """)).fetchall()
        existing_pairs = set((str(r[0]).strip(), str(r[1])) for r in existing)

    df["registration_number"] = df["registration_number"].astype(str).str.strip()
    df["company_id"] = df["company_id"].astype(str).str.strip()
    df["key"] = list(zip(df["registration_number"], df["company_id"]))
    df = df[~df["key"].isin(existing_pairs)].drop(columns="key")

    print(f"🆕 Appending {len(df)} new rows to DB...")
    df.to_sql("public_complete_company_profile", engine, schema="public", index=False, if_exists="append")
    print("✅ Appended to 'public_complete_company_profile'.")






#----- setting the unique key for company_id, registration

# ── Enforce uniqueness on company_id and registration_number ──w
with engine.connect() as conn:
    conn.execute(text("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_indexes 
                WHERE schemaname = 'public' 
                AND tablename = 'public_complete_company_profile' 
                AND indexname = 'uniq_company_profile_pair'
            ) THEN
                CREATE UNIQUE INDEX uniq_company_profile_pair 
                ON public.public_complete_company_profile(company_id, registration_number);
            END IF;
        END
        $$;
    """))
