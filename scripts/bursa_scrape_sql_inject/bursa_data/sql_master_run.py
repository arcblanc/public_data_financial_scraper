import subprocess
from pathlib import Path
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--replace", action="store_true", help="Replace data instead of appending")
args = parser.parse_args()

REPLACE_MODE = args.replace
# Define the path to the sql_scripts folder
scripts_dir = Path(__file__).parent / "sql_scripts"

# List of injection scripts to run
scripts = [
    "profile_injection.py",
    "management_injection.py",
    "ownership_injection.py",
    "top_10_injection.py",
    "insider_injection.py",
    "cashflow_injection.py",
    "income_injection.py",
    "balance_injection.py"
    
]

# Run each script one by one
for script in scripts:
    script_path = scripts_dir / script
    print(f"\n🚀 Running: {script_path}")
    cmd = ["python", str(script_path)]
    if REPLACE_MODE:
        cmd.append("--replace")

    result = subprocess.run(cmd, capture_output=True, text=True)
    
    print(result.stdout)
    if result.stderr:
        print(f"❌ Error in {script}:\n{result.stderr}")