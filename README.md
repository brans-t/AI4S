# AI4S
MOF Agent AI

-----------------------

# MOF Data Processing Pipeline

This project provides a complete workflow for downloading, extracting, and cleaning MOF (Metal-Organic Framework) data, including isotherm data and surface areas.

## Directory Structure

```
project/
│
├─ examplemain.py
├─ main.py
├─ data/
│   ├─ JSON/          # Downloaded MOF JSON files
│   ├─ MOF_ISO/       # Extracted isotherm CSV files
│   └─ MOF_ISO_CLEAN/ # Cleaned CSV files
├─ src/
│   ├─ __init__.py
│   ├─ mof_downloader.py  # Download MOF databases and JSON files
│   ├─ mof_extractor.py   # Extract isotherm CSV files from JSON
│   └─ mof_cleaner.py     # Clean CSV files and create summary
└─ README.md
```

## Setup

1. **Create and activate a virtual environment**:

```bash
python -m venv MOF_env
source MOF_env/bin/activate       # Linux/Mac
MOF_env\Scripts\activate.bat      # Windows
```

2. **Install dependencies**:

```bash
pip install requests tqdm pandas
```

## Usage

### 1. Download MOF Data

Run `main.py` or `examplemain.py`:

```bash
python main.py
```

This will:

- Fetch MOF databases list.
- Download all MOFs from selected databases.
- Save JSON files under `data/JSON/<Database>` and optionally CIF files.

### 2. Extract Isotherm CSV Files

Uncomment and run extraction in `examplemain.py`:

```python
process_database(os.path.join(BASE_DIR, "JSON"), selected_db, CSV_DIR)
```

This will:

- Read JSON files from `data/JSON/<Database>`.
- Extract isotherm curves and metadata.
- Save CSV files under `data/MOF_ISO/<Database>`.

### 3. Clean CSV Files

Uncomment and run cleaning in `examplemain.py`:

```python
total, kept, removed, summary = clean_csv_files(CSV_DIR, CLEAN_DIR)
```

This will:

- Remove CSV files where `Surface_area_m2g == 0` or `NaN`.
- Copy valid files to `data/MOF_ISO_CLEAN/`.
- Generate a summary CSV (`MOF_ISO_summary.csv`) with MOF info.

## Customization

- `BASE_DIR`: Root directory for data.
- `MAX_WORKERS`: Number of threads for parallel processing.

## Notes

- Ensure `src/` is in Python path when running scripts.
- You can select multiple databases by indices or use `"all"`.

## License

Open-source. Free to use and modify.

