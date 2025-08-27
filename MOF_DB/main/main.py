import os
import sys

# ----------------------
# Make src folder importable
# ----------------------
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_PATH = os.path.join(CURRENT_DIR, "../src")
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

from mof_downloader import download_databases, fetch_databases
from mof_extractor import process_database
from mof_cleaner import clean_isotherms

# ----------------------
# Helper functions
# ----------------------
def print_step(title):
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)

def safe_name(name):
    """Convert spaces/slashes to underscores for folder paths."""
    return name.replace(" ", "_").replace("/", "_")

def list_databases(dbs, base_dir=None, folder_type="JSON"):
    """Print all databases with availability info."""
    for idx, db in enumerate(dbs):
        available = False
        if base_dir:
            folder = os.path.join(base_dir, safe_name(db["name"]))
            available = os.path.exists(folder) and os.listdir(folder)
        status = "✅ Available" if available else "❌ Not available"
        prefix = f"{folder_type} -> "
        print(f"  {idx}. {prefix}{db['name']} ({db['count']} MOFs) {status}")

def parse_indices(idx_input, dbs):
    if idx_input.lower() == "all":
        return list(range(len(dbs)))
    try:
        return [int(i.strip()) for i in idx_input.split(",") if i.strip().isdigit() and 0 <= int(i.strip()) < len(dbs)]
    except ValueError:
        return []

# ----------------------
# Main pipeline
# ----------------------
def main():
    BASE_DIR = os.path.join(os.path.dirname(__file__), "data")
    JSON_DIR = os.path.join(BASE_DIR, "JSON")
    CSV_DIR = os.path.join(BASE_DIR, "MOF_ISO")
    CLEAN_DIR = os.path.join(BASE_DIR, "MOF_ISO_CLEAN")

    # =====================================================
    # Step 1. Download MOFs
    # =====================================================
    print_step("Step 1. Download MOFs")
    databases = fetch_databases()
    for idx, db in enumerate(databases):
        print(f"  {idx}. {db['name']} ({db['count']} MOFs)")

    idx_input = input("Enter the database index to download (e.g., 0,2 or 'all'): ").strip()
    download_indices = parse_indices(idx_input, databases)
    if download_indices:
        download_databases(selected_indices=download_indices, save_dir=BASE_DIR)
    else:
        print("❌ No valid indices entered. Skipping download.")

    # =====================================================
    # Step 2. Extract Isotherm CSV Files
    # =====================================================
    print_step("Step 2. Extract Isotherm CSV Files")
    list_databases(databases, base_dir=JSON_DIR, folder_type="Extract")
    idx_input = input("Enter database indices to extract CSV (e.g., 0,2 or 'all'): ").strip()
    extract_indices = parse_indices(idx_input, databases)
    if extract_indices:
        for idx in extract_indices:
            db_name = databases[idx]["name"]
            json_path = os.path.join(JSON_DIR, safe_name(db_name))
            
            if not os.path.exists(json_path):
                print(f"❌ JSON folder not found: {json_path}")
                continue
            print(f"\n📊 Extracting CSV files from database '{db_name}'...")
            process_database(db_name, JSON_DIR, CSV_DIR)
    else:
        print("❌ No valid indices entered. Skipping extraction.")

    # =====================================================
    # Step 3. Clean CSV Files
    # =====================================================
    print_step("Step 3. Clean CSV Files")
    list_databases(databases, base_dir=CSV_DIR, folder_type="Clean")
    idx_input = input("Enter database indices to clean CSV (e.g., 0,2 or 'all'): ").strip()
    clean_indices = parse_indices(idx_input, databases)
    if clean_indices:
        for idx in clean_indices:
            db_name = databases[idx]["name"]
            csv_path = os.path.join(CSV_DIR, safe_name(db_name))
            if not os.path.exists(csv_path):
                print(f"❌ CSV folder not found: {csv_path}")
                continue
            print(f"\n🧹 Cleaning CSV files from database '{db_name}'...")
            result = clean_isotherms(csv_path, os.path.join(CLEAN_DIR, safe_name(db_name)))
            print(f"📊 File statistics for {db_name}:")
            print(f"   Total CSV files scanned: {result['total_files']}")
            print(f"   Files kept: {result['kept_files']}")
            print(f"   Files removed: {result['removed_files']}")
            if result["summary_path"]:
                print(f"   ✅ Summary saved at: {result['summary_path']}")
            else:
                print("   ⚠️ No entries retained (all Surface_area_m2g values were 0 or NaN)")
    else:
        print("❌ No valid indices entered. Skipping cleaning.")

if __name__ == "__main__":
    main()
