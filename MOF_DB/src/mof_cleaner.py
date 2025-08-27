import os
import pandas as pd
from tqdm import tqdm  # For progress bars during batch processing


# ----------------------------
# Process a single CSV file
# ----------------------------
def process_csv_file(input_path, output_path):
    """
    Clean and retain a single CSV file representing an adsorption isotherm.

    Steps:
        1. Read the first 6 lines containing metadata.
        2. Extract MOF ID, database, surface areas, adsorbate, temperature.
        3. Skip the file if Surface_area_m2g is 0 or NaN.
        4. Copy the CSV to the output directory if retained.
        5. Return a dictionary of retained MOF info for summary.

    Args:
        input_path (str): Path to the input CSV file.
        output_path (str): Path to save the cleaned CSV file.

    Returns:
        dict or None: A dictionary with MOF metadata if the file is retained, 
                      otherwise None.
    """
    # Read the first 6 lines (metadata)
    with open(input_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    # Skip if file is too short
    if len(lines) < 6:
        return None

    # Helper function to extract the second column from a CSV line
    def get_value(line, default="N/A"):
        try:
            return line.strip().split(",")[1]
        except:
            return default

    # Extract metadata values
    mof_id = get_value(lines[0], "unknown")
    database = get_value(lines[1], "unknown")
    try:
        sa_m2g = float(get_value(lines[2], 0.0))
    except:
        sa_m2g = 0.0
    try:
        sa_m2cm3 = float(get_value(lines[3], 0.0))
    except:
        sa_m2cm3 = 0.0
    adsorbate = get_value(lines[4], "N/A")
    temperature = get_value(lines[5], "N/A")

    # Skip file if Surface_area_m2g is zero or NaN
    if sa_m2g == 0.0 or pd.isna(sa_m2g):
        return None

    # Ensure output folder exists and copy the CSV file
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(input_path, "r", encoding="utf-8") as src, open(output_path, "w", encoding="utf-8") as dst:
        dst.writelines(src.readlines())

    # Return a dictionary of metadata for summary
    return {
        "File": os.path.relpath(output_path, output_path),  # Relative file path
        "MOF_ID": mof_id,
        "Database": database,
        "Adsorbate": adsorbate,
        "Temperature": temperature,
        "Surface_area_m2g": sa_m2g,
        "Surface_area_m2cm3": sa_m2cm3
    }


# ----------------------------
# Batch clean CSV files for multiple databases
# ----------------------------
def clean_isotherms(input_dir, output_dir, summary_file="MOF_ISO_summary.csv"):
    """
    Batch process all CSV files under `input_dir`, keeping only those with
    Surface_area_m2g > 0, and save cleaned files to `output_dir`.

    Steps:
        1. Walk through all subfolders and CSV files.
        2. Process each file using `process_csv_file`.
        3. Collect metadata for retained files.
        4. Save a summary CSV containing all retained MOF info.

    Args:
        input_dir (str): Root directory containing raw CSV files.
        output_dir (str): Directory to save cleaned CSV files.
        summary_file (str): Name of the summary CSV file.

    Returns:
        dict: A dictionary containing:
            - summary_df: Pandas DataFrame of retained MOFs (or None)
            - summary_path: Path to saved summary CSV (or None)
            - total_files: Total number of CSV files scanned
            - kept_files: Number of files retained
            - removed_files: Number of files skipped
    """
    all_records = []  # List to store metadata of retained MOFs
    total_files = 0
    kept_files = 0

    # Walk through all CSV files recursively
    for root, _, files in os.walk(input_dir):
        for file in tqdm(files, desc="Processing CSV files", unit="file"):
            if not file.endswith(".csv"):
                continue
            total_files += 1
            input_path = os.path.join(root, file)
            rel_path = os.path.relpath(input_path, input_dir)
            output_path = os.path.join(output_dir, rel_path)

            # Process single CSV file
            record = process_csv_file(input_path, output_path)
            if record:
                all_records.append(record)
                kept_files += 1

    removed_files = total_files - kept_files

    # Save summary DataFrame
    summary_path = None
    summary_df = None
    if all_records:
        summary_df = pd.DataFrame(all_records)
        # Specify column order for consistency
        cols_order = ["File", "MOF_ID", "Database", "Adsorbate", "Temperature",
                      "Surface_area_m2g", "Surface_area_m2cm3"]
        summary_df = summary_df[cols_order]

        summary_path = os.path.join(output_dir, summary_file)
        os.makedirs(os.path.dirname(summary_path), exist_ok=True)
        summary_df.to_csv(summary_path, index=False)

    # Return statistics
    return {
        "summary_df": summary_df,
        "summary_path": summary_path,
        "total_files": total_files,
        "kept_files": kept_files,
        "removed_files": removed_files
    }


# ----------------------------
# Interactive mode when running the script directly
# ----------------------------
if __name__ == "__main__":
    # Default input/output directories
    INPUT_DIR = "../MOF_ISO"          # Folder with raw CSV files
    OUTPUT_DIR = "../MOF_ISO_CLEAN"   # Folder for cleaned CSV files
    SUMMARY_FILE = "MOF_ISO_summary.csv"

    # Run batch cleaning
    result = clean_isotherms(INPUT_DIR, OUTPUT_DIR, SUMMARY_FILE)

    # Print summary statistics
    print(f"\n📊 File statistics:")
    print(f"Total scanned CSV files: {result['total_files']}")
    print(f"Kept files: {result['kept_files']}")
    print(f"Removed files: {result['removed_files']}")

    if result["summary_path"]:
        print(f"\n✅ Cleaning completed. Summary saved at: {result['summary_path']}")
    else:
        print("\n⚠️ No entries retained (all Surface_area_m2g values were 0 or NaN)")
