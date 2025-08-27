import os
import json
import requests
import time
from tqdm import tqdm  # 进度条显示
from concurrent.futures import ThreadPoolExecutor  # 并行保存 MOF 文件

# ----------------------------
# 基础配置
# ----------------------------
BASE_URL = "https://mof.tech.northwestern.edu"  # MOF 数据库服务器 URL
MOFS_URL = f"{BASE_URL}/mofs.json"  # 获取 MOF 数据的 API
DATABASES_URL = f"{BASE_URL}/databases.json"  # 获取数据库列表的 API

PER_PAGE = 200  # 每页 MOF 数量
MAX_RETRIES = 3  # 请求失败最大重试次数
MAX_WORKERS = 10  # 并行保存文件线程数

# ----------------------------
# 工具函数：安全文件夹名
# ----------------------------
def safe_name(name: str) -> str:
    """
    将数据库名称或 MOF ID 转为安全文件夹/文件名
    替换空格和斜杠
    """
    return name.replace(" ", "_").replace("/", "_")

# ----------------------------
# 获取可用数据库列表
# ----------------------------
def fetch_databases():
    """
    从服务器获取可用 MOF 数据库列表

    Returns:
        list: 每个数据库是一个 dict，包含:
              - "name" (str): 数据库名
              - "count" (int): MOF 数量
    """
    try:
        resp = requests.get(DATABASES_URL)
        resp.raise_for_status()
        data = resp.json()
        databases = []
        for db in data:
            databases.append({"name": db["name"], "count": db.get("mofs", 0)})
        return databases
    except Exception as e:
        print(f"❌ Failed to fetch database list: {e}")
        # 备用数据库列表
        return [
            {"name": "CoREMOF 2014", "count": 4764},
            {"name": "CoREMOF 2019", "count": 12020},
            {"name": "CSD", "count": 0},
            {"name": "hMOF", "count": 137953},
            {"name": "IZA", "count": 216},
            {"name": "PCOD-syn", "count": 70},
            {"name": "Tobacco", "count": 13511},
        ]

# ----------------------------
# 获取指定数据库的所有 MOF
# ----------------------------
def get_all_mofs(database):
    """
    使用分页获取某个数据库的所有 MOF

    Args:
        database (str): 数据库名称

    Returns:
        list: MOF 记录列表，每条是 dict
    """
    all_mofs = []
    page = 1
    print(f"\nFetching MOFs from database '{database}' ...")

    while True:
        url = f"{MOFS_URL}?database={database}&page={page}&per_page={PER_PAGE}"
        for attempt in range(1, MAX_RETRIES + 1):  # 重试机制
            try:
                resp = requests.get(url, timeout=10)
                resp.raise_for_status()
                break
            except Exception as e:
                print(f"⚠️ Page {page} request failed, retry {attempt}/{MAX_RETRIES}: {e}")
                time.sleep(1)
                if attempt == MAX_RETRIES:
                    print(f"❌ Page {page} failed after {MAX_RETRIES} retries, skipping")
                    return all_mofs

        data = resp.json()
        mofs = None
        # 判断响应中 MOF 列表位置
        if isinstance(data, dict):
            for k in ["mofs", "results", "data"]:
                if k in data and isinstance(data[k], list):
                    mofs = data[k]
                    break
        elif isinstance(data, list):
            mofs = data
        else:
            print(f"❌ Unknown data structure on page {page}: {type(data)}")
            break

        if not mofs:
            print(f"⚠️ No MOF data found on page {page}")
            break

        all_mofs.extend(mofs)
        print(f"✅ Page {page}: {len(mofs)} records (total {len(all_mofs)})")

        if len(mofs) < PER_PAGE:  # 最后一页
            break
        page += 1
        time.sleep(0.01)

    print(f"📦 Total {len(all_mofs)} MOF records fetched")
    return all_mofs

# ----------------------------
# 保存单个 MOF
# ----------------------------
def save_one_mof(mof, database, save_dir):
    """
    将单个 MOF 保存为 JSON 和 CIF 文件

    Args:
        mof (dict): MOF 数据
        database (str): 数据库名称
        save_dir (str): 保存路径
    """
    folder_name = safe_name(database)
    json_folder = os.path.join(save_dir, "JSON", folder_name)
    cif_folder = os.path.join(save_dir, "CIF", folder_name)
    os.makedirs(json_folder, exist_ok=True)
    os.makedirs(cif_folder, exist_ok=True)

    # MOF 文件名安全化
    mofid = str(mof.get("mofid") or mof.get("mofkey") or mof.get("id") or mof.get("name") or "mof")
    mofid = safe_name(mofid)

    # 保存 JSON
    json_path = os.path.join(json_folder, f"{folder_name}_{mofid}.json")
    try:
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(mof, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"❌ Failed to save JSON for {mofid}: {e}")

    # 保存 CIF
    cif_data = mof.get("cif")
    if cif_data:
        cif_path = os.path.join(cif_folder, f"{folder_name}_{mofid}.cif")
        try:
            with open(cif_path, "w", encoding="utf-8") as f:
                f.write(cif_data)
        except Exception as e:
            print(f"❌ Failed to save CIF for {mofid}: {e}")

# ----------------------------
# 并行保存多个 MOF
# ----------------------------
def save_mofs_parallel(mofs, database, save_dir):
    """
    使用线程并行保存 MOF

    Args:
        mofs (list): MOF 数据列表
        database (str): 数据库名称
        save_dir (str): 保存路径
    """
    total = len(mofs)
    print(f"\nSaving MOFs for {database} ...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        list(tqdm(
            executor.map(lambda m: save_one_mof(m, database, save_dir), mofs),
            total=total,
            unit="MOF",
            desc=f"Saving {database}"
        ))

    folder_name = safe_name(database)
    json_folder = os.path.join(save_dir, "JSON", folder_name)
    cif_folder = os.path.join(save_dir, "CIF", folder_name)
    json_count = len(os.listdir(json_folder)) if os.path.exists(json_folder) else 0
    cif_count = len(os.listdir(cif_folder)) if os.path.exists(cif_folder) else 0
    missing_cif = total - cif_count

    print("\n📊 Download Summary")
    print(f"Database: {database}")
    print(f"Total MOFs: {total}")
    print(f"JSON files: {json_count}")
    print(f"CIF files: {cif_count}")
    print(f"Missing or unavailable CIF files: {missing_cif}")

# ----------------------------
# 下载指定数据库
# ----------------------------
def download_databases(selected_indices=None, save_dir=None, all_databases=False):
    """
    下载指定数据库的 MOF 数据
    - selected_indices: 数据库索引列表
    - save_dir: 保存目录
    - all_databases: 是否下载全部数据库
    """
    if save_dir is None:
        save_dir = os.path.join(os.path.dirname(__file__), "MOF_all_raw")

    databases = fetch_databases()

    if all_databases:
        target_indices = list(range(len(databases)))
    elif selected_indices is not None:
        target_indices = selected_indices
    else:
        raise ValueError("❌ No databases specified. Provide indices or set all_databases=True.")

    for idx in target_indices:
        if 0 <= idx < len(databases):
            db_name = databases[idx]["name"]
            print(f"\n📥 Downloading database: {db_name}")
            mofs = get_all_mofs(db_name)
            save_mofs_parallel(mofs, db_name, save_dir)
        else:
            print(f"⚠️ Invalid database index {idx}, skipping.")

    print(f"\n🎉 All selected databases have been downloaded to {save_dir}, with JSON and CIF stored separately.")

# ----------------------------
# 脚本入口
# ----------------------------
if __name__ == "__main__":
    download_databases(all_databases=True)
