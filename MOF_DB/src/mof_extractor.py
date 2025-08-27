import os
import json
from tqdm import tqdm  # For progress bars during processing
from concurrent.futures import ThreadPoolExecutor  # For parallel processing

# ----------------------------
# Parallel processing settings
# ----------------------------
MAX_WORKERS = 10  # Number of threads to process files concurrently

def safe_name(name: str) -> str:
    """将数据库名或文件名中的空格替换为下划线"""
    return name.replace(" ", "_")

# ----------------------------
# Extract isotherms from a single MOF JSON file
# ----------------------------
def extract_isotherms_from_file(json_file, database, output_dir):
    """
    从单个 MOF JSON 文件中提取等温吸附曲线数据，并保存为 CSV 文件。
    """
    count = 0
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            mof_data = json.load(f)
    except (json.JSONDecodeError, FileNotFoundError) as e:
        print(f"❌ Error reading {json_file}: {e}")
        return 0

    # Generate a safe MOF identifier for filenames
    mofid = str(
        mof_data.get("mofid")
        or mof_data.get("mofkey")
        or mof_data.get("id")
        or mof_data.get("name")
        or "unknown_mof"
    ).replace("/", "_").replace(" ", "_")  # MOF ID 文件名中仍替换空格和斜杠

    # Extract surface areas
    sa_m2g = mof_data.get("surface_area_m2g", 0.0)
    sa_m2cm3 = mof_data.get("surface_area_m2cm3", 0.0)

    # Extract isotherms
    isotherms = mof_data.get("isotherms")
    if not isotherms:
        return 0  # Skip if no isotherms
    if isinstance(isotherms, dict):
        isotherms = [isotherms]  # Convert single dict to list

    # ----------------------------
    # Create output folder for this database
    # ----------------------------
    folder = os.path.join(output_dir, database.replace(" ", "_"))  # 保留数据库文件夹原名
    os.makedirs(folder, exist_ok=True)

    # Loop over all isotherms
    for idx, iso in enumerate(isotherms):
        if not isinstance(iso, dict):
            continue
        try:
            # Determine a unique ID for the isotherm
            iso_id = str(iso.get("id") or iso.get("DOI") or idx)
            # ----------------------------
            # Determine CSV filename
            # ----------------------------
            filename = f"{database.replace(' ', '_')}_{mofid}_{iso_id}.csv"  # 保留数据库文件夹原名
            filepath = os.path.join(folder, filename)

            # Extract metadata
            pressure_unit = iso.get("pressureUnits", "N/A")
            adsorption_unit = iso.get("adsorptionUnits", "N/A")
            temperature = iso.get("temperature", "N/A")

            # Extract adsorbates (chemical species)
            adsorbates_list = iso.get("adsorbates", [])
            adsorbate = ";".join([a.get("name", "N/A") for a in adsorbates_list]) if adsorbates_list else "N/A"

            # Extract data points and sort by pressure
            data_points = iso.get("isotherm_data", [])
            data_points_sorted = sorted(
                [p for p in data_points if p.get("pressure") is not None and p.get("total_adsorption") is not None],
                key=lambda x: x["pressure"]
            )

            # Write CSV file
            with open(filepath, "w", encoding="utf-8") as f:
                # 写入元信息
                f.write(f"MOF_ID,{mofid}\n")
                f.write(f"Database,{database}\n")
                f.write(f"Surface_area_m2g,{sa_m2g}\n")
                f.write(f"Surface_area_m2cm3,{sa_m2cm3}\n")
                f.write(f"Adsorbate,{adsorbate}\n")
                f.write(f"Temperature,{temperature}\n\n")
                f.write("\n")  # 空行隔开元信息和数据
                # 写入数据表头
                f.write(f"Pressure ({pressure_unit}),Adsorption ({adsorption_unit})\n")
                for point in data_points_sorted:
                    # 写入数据点
                    pressure = point.get("pressure")
                    adsorption = point.get("total_adsorption")
                    if pressure is not None and adsorption is not None:
                        f.write(f"{pressure},{adsorption}\n")
            count += 1
        except Exception as e:
            print(f"❌ Error processing isotherm {iso.get('id', idx)} in {json_file}: {e}")
    return count


# ----------------------------
# Process all JSON files in a database
# ----------------------------
def process_database(database, json_dir, output_dir, max_workers=MAX_WORKERS):
    """
    处理指定数据库的所有 JSON 文件，提取等温曲线。
    """
    # 使用 safe_name 确保路径一致
    safe_db_name = safe_name(database)
    json_folder = os.path.join(json_dir, safe_db_name)

    if not os.path.exists(json_folder):
        print(f"❌ JSON folder not found: {json_folder}")
        return 0

    json_files = [os.path.join(json_folder, f) for f in os.listdir(json_folder) if f.endswith(".json")]
    if not json_files:
        print(f"❌ No JSON files found in {json_folder}")
        return 0

    print(f"\nProcessing database '{database}', total {len(json_files)} JSON files...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(
            tqdm(
                executor.map(lambda f: extract_isotherms_from_file(f, database, output_dir), json_files),
                total=len(json_files),
                unit="files",
                desc=f"Extracting from {database}"
            )
        )

    total_isotherms = sum(results)
    print(f"✅ Finished '{database}', total {total_isotherms} adsorption isotherms extracted.")
    return total_isotherms



# ----------------------------
# Batch process multiple databases
# ----------------------------
def extract_isotherms(json_dir, output_dir, selected_databases=None, max_workers=MAX_WORKERS):
    if not os.path.exists(json_dir):
        print(f"❌ JSON root directory not found: {json_dir}")
        return

    databases = selected_databases or [
        d for d in os.listdir(json_dir) if os.path.isdir(os.path.join(json_dir, d))
    ]
    if not databases:
        print(f"❌ No database folders found in {json_dir}.")
        return

    if selected_databases is None:
        print("Available databases:")
        for idx, db in enumerate(databases):
            print(f"  {idx}. {db}")
        idx_input = input("Enter database indices to process (e.g., 0,3,5 or 'all'): ")
        if idx_input.strip().lower() == "all":
            indices = range(len(databases))
        else:
            try:
                indices = [int(i.strip()) for i in idx_input.split(",")]
            except ValueError:
                print("❌ Invalid input, please enter numbers or 'all'")
                return
        databases = [databases[i] for i in indices if 0 <= i < len(databases)]

    for db in databases:
        process_database(db, json_dir, output_dir, max_workers)

    print(f"\n🎉 All selected databases processed. CSV files are saved in: {output_dir}")


# ----------------------------
# Script entry point
# ----------------------------
if __name__ == "__main__":
    JSON_DIR = "../MOF_all_raw/JSON"  # Root folder containing MOF JSON files
    ISOTHERMS_DIR = "../MOF_ISO"      # Folder to save extracted isotherm CSV files
    extract_isotherms(JSON_DIR, ISOTHERMS_DIR)
