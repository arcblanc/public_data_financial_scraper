⸻

⚙️ Running the Pipeline

🛑 Note: Due to headless browser dependencies, Playwright scripts will not run inside dev_containers. Please run them locally on your machine.

▶️ Full Pipeline

To execute the entire scraping + SQL injection flow in the correct order:
activate venv
python scripts/orchestrator.py

This will:
	1.	Scrape missing company IDs and URLs.
	2.	Run all Playwright-based scrapers in parallel (income, balance, cashflow, profile, etc.).
	3.	Inject cleaned results into PostgreSQL via SQL scripts.

🔍 Run a Specific Step

To run a single script (e.g., profile_scraper.py):

python scripts/orchestrator.py --only profile_scraper.py


⸻

Let me know if you’d like the README to include .env setup steps or Docker alternatives for Playwright that bypass the dev container issue.
⸻

📊 Bursa Data Pipeline

This repository contains the complete data pipeline used to scrape, clean, and inject financial and profile data of publicly listed companies on Bursa Malaysia into a PostgreSQL database.

⸻

🗂️ Project Structure

bursa_dataset_and_cleaning/
├── list_bursa_ids/             # Source bursa_urls to gather raw company ID data
├── scrapers_1000/              # Playwright & BeautifulSoup scrapers (income, balance, etc.)
├── csvs/cleaned/               # Cleaned CSVs, including resolved URLs
├── debug_html/                 # Debug HTML dumps of failed scrapes
├── scripts/
│   ├── bursa_scrape_sql_inject/
│   │   ├── bursa_data/         # Processed data (combined CSVs, cleaned full tables)
│   │   ├── column_percentages/ # Null ratio summaries for analysis
│   │   ├── mapping/            # Mapping scripts to align DB fields with scraped fields
│   │   └── sql_scripts/        # SQL injection scripts for PostgreSQL
├── screenshots/                # Fail screenshots per company_id


⸻

🔁 Data Flow

Step 1: Company ID Collection (list_bursa_ids)
	•	Parse company data from PDFs and CSVs (List_of_Companies.pdf, etc.).
	•	Output a master list of companies (bursa_company_list.csv).
	•	This list includes company names, IDs, and market board classification (Main, ACE, LEAP).

Step 2: URL Resolution (scrapers_1000/bursa_url_finder.py)
	•	Uses Playwright to locate each company’s page on Bursa Malaysia.
	•	Outputs resolved URLs to csvs/cleaned/company_urls.csv.

Step 3: Web Scraping (scrapers_1000/)
	•	Uses Playwright for dynamic pages and BeautifulSoup for parsing static HTML.
	•	Key scripts:
	•	income_statement.py
	•	balance_sheet.py
	•	cash_flow.py
	•	market_capscrape.py
	•	profile_scraper.py
	•	Extracted data is saved in both outputs/ and combined into scripts/bursa_scrape_sql_inject/bursa_data/.
	•	ssm_api matching script

Step 4: SQL Injection (scripts/bursa_scrape_sql_inject/sql_scripts)
	•	Transforms cleaned data into normalized formats.
	•	Injects data into a PostgreSQL database using SQLAlchemy.
	•	Environment variables in .env are used to connect securely.

⸻

🧪 Technologies Used
	•	Python
	•	Playwright – Browser automation for scraping
	•	BeautifulSoup – HTML parsing
	•	Polars / Pandas – Data processing
	•	PostgreSQL – Data storage
	•	SQLAlchemy – Python DB interface
	•	tqdm – Progress tracking
	•	dotenv – Secure credential loading

⸻

📥 Output Highlights

All cleaned and combined datasets are saved under:

scripts/bursa_scrape_sql_inject/bursa_data/
├── combined_profile.csv
├── combined_management.csv
├── combined_ownership.csv
├── combined_top10.csv
├── combined_insider.csv
├── complete_income_statements.csv
├── complete_balance_sheets.csv
├── complete_cash_flow_statements.csv
└── market_info_sample.csv


⸻

🗃️ Database Tables

Examples of PostgreSQL target tables:
	•	public_complete_income
	•	public_complete_balance_sheet
	•	public_complete_company_profile
	•	public_complete_directors_executives
	•	public_complete_insider

⸻

🧼 Quality Checks

Null ratio summaries for major datasets are saved in:

scripts/bursa_scrape_sql_inject/column_percentages/


⸻

