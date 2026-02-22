"""
Data ingestion: download datasets, convert NPPES/LEIE to slim parquets,
and register DuckDB views for downstream signal computation.
"""

import os
import sys
import zipfile
import glob as globmod

import duckdb
import requests
from tqdm import tqdm

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "fraud.duckdb")

MEDICAID_URL = "https://stopendataprod.blob.core.windows.net/datasets/medicaid-provider-spending/2026-02-09/medicaid-provider-spending.parquet"
LEIE_URL = "https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv"
NPPES_URL = "https://download.cms.gov/nppes/NPPES_Data_Dissemination_February_2026_V2.zip"

NPPES_COLUMNS = [
    "NPI",
    "Entity Type Code",
    "Provider Organization Name (Legal Business Name)",
    "Provider Last Name (Legal Name)",
    "Provider First Name",
    "Provider Business Practice Location Address State Name",
    "Provider Business Practice Location Address Postal Code",
    "Healthcare Provider Taxonomy Code_1",
    "Provider Enumeration Date",
    "Authorized Official Last Name",
    "Authorized Official First Name",
]


def download_file(url: str, dest: str, desc: str = "Downloading") -> None:
    """Download a file with a progress bar, skipping if already present."""
    if os.path.exists(dest):
        print(f"  Already exists: {dest}")
        return
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    resp = requests.get(url, stream=True, timeout=60)
    resp.raise_for_status()
    total = int(resp.headers.get("content-length", 0))
    with open(dest, "wb") as f, tqdm(
        total=total, unit="B", unit_scale=True, desc=desc
    ) as bar:
        for chunk in resp.iter_content(chunk_size=1 << 20):
            f.write(chunk)
            bar.update(len(chunk))


def download_all() -> None:
    """Download all three datasets."""
    print("Downloading Medicaid Provider Spending...")
    download_file(
        MEDICAID_URL,
        os.path.join(DATA_DIR, "medicaid-provider-spending.parquet"),
        "Medicaid Spending",
    )
    print("Downloading OIG LEIE Exclusions...")
    download_file(
        LEIE_URL,
        os.path.join(DATA_DIR, "UPDATED.csv"),
        "LEIE Exclusions",
    )
    print("Downloading NPPES NPI Registry...")
    nppes_zip = os.path.join(DATA_DIR, "nppes.zip")
    download_file(NPPES_URL, nppes_zip, "NPPES Registry")
    # Unzip if needed
    nppes_raw_dir = os.path.join(DATA_DIR, "nppes_raw")
    if not os.path.exists(nppes_raw_dir):
        print("Unzipping NPPES...")
        with zipfile.ZipFile(nppes_zip, "r") as zf:
            zf.extractall(nppes_raw_dir)


def find_nppes_csv(data_dir: str) -> str:
    """Find the main NPPES data CSV in the extracted directory."""
    nppes_raw = os.path.join(data_dir, "nppes_raw")
    # The main file matches npidata_pfile_*.csv (not the header or other endpoint files)
    patterns = [
        os.path.join(nppes_raw, "**", "npidata_pfile_*.csv"),
        os.path.join(nppes_raw, "npidata_pfile_*.csv"),
    ]
    for pat in patterns:
        matches = globmod.glob(pat, recursive=True)
        # Exclude header-only files
        matches = [m for m in matches if "FileHeader" not in m]
        if matches:
            # Return the largest file (the main data file)
            return max(matches, key=os.path.getsize)
    # Fallback: any large CSV in nppes_raw
    all_csvs = globmod.glob(os.path.join(nppes_raw, "**", "*.csv"), recursive=True)
    if all_csvs:
        return max(all_csvs, key=os.path.getsize)
    raise FileNotFoundError(f"Could not find NPPES CSV in {nppes_raw}")


def build_nppes_slim(con: duckdb.DuckDBPyConnection, data_dir: str) -> None:
    """Convert NPPES CSV to slim parquet with only the 11 needed columns."""
    slim_path = os.path.join(data_dir, "nppes_slim.parquet")
    if os.path.exists(slim_path):
        print(f"  NPPES slim parquet already exists: {slim_path}")
        return

    csv_path = find_nppes_csv(data_dir)
    print(f"  Reading NPPES CSV: {csv_path}")

    # Build column selection — quote every column name since they contain spaces/parens
    col_select = ", ".join(f'"{c}"' for c in NPPES_COLUMNS)

    # Let DuckDB auto-detect all 330 columns, then SELECT only the 11 we need
    con.execute(f"""
        COPY (
            SELECT {col_select}
            FROM read_csv('{csv_path}',
                          header=true,
                          auto_detect=true,
                          all_varchar=true,
                          parallel=true,
                          ignore_errors=true)
        ) TO '{slim_path}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    print(f"  NPPES slim parquet written: {slim_path}")


def build_leie_parquet(con: duckdb.DuckDBPyConnection, data_dir: str) -> None:
    """Normalize LEIE CSV and save as parquet."""
    parquet_path = os.path.join(data_dir, "leie.parquet")
    if os.path.exists(parquet_path):
        print(f"  LEIE parquet already exists: {parquet_path}")
        return

    csv_path = os.path.join(data_dir, "UPDATED.csv")
    print(f"  Reading LEIE CSV: {csv_path}")

    con.execute(f"""
        COPY (
            SELECT
                UPPER(TRIM(LASTNAME)) AS LASTNAME,
                UPPER(TRIM(FIRSTNAME)) AS FIRSTNAME,
                UPPER(TRIM(MIDNAME)) AS MIDNAME,
                CASE WHEN TRIM(NPI) IN ('', '0000000000') THEN NULL ELSE TRIM(NPI) END AS NPI,
                TRIM(EXCLTYPE) AS EXCLTYPE,
                TRIM(CAST(EXCLDATE AS VARCHAR)) AS EXCLDATE,
                CASE WHEN CAST(REINDATE AS VARCHAR) IN ('', '0', '00000000')
                     THEN NULL
                     ELSE TRIM(CAST(REINDATE AS VARCHAR))
                END AS REINDATE,
                UPPER(TRIM(STATE)) AS STATE,
                TRIM(SPECIALTY) AS SPECIALTY
            FROM read_csv('{csv_path}',
                          header=true,
                          auto_detect=true,
                          ignore_errors=true)
        ) TO '{parquet_path}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    print(f"  LEIE parquet written: {parquet_path}")


def get_connection(data_dir: str | None = None) -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection with memory settings for MacBook."""
    if data_dir is None:
        data_dir = DATA_DIR
    db_path = os.path.join(data_dir, "fraud.duckdb")
    con = duckdb.connect(db_path)
    con.execute("SET memory_limit = '8GB';")
    con.execute("SET threads TO 8;")
    # Enable progress bar for long queries
    con.execute("SET enable_progress_bar = true;")
    return con


def register_views(con: duckdb.DuckDBPyConnection, data_dir: str | None = None) -> None:
    """Register parquet files as views for downstream queries."""
    if data_dir is None:
        data_dir = DATA_DIR

    medicaid_path = os.path.join(data_dir, "medicaid-provider-spending.parquet")
    nppes_path = os.path.join(data_dir, "nppes_slim.parquet")
    leie_path = os.path.join(data_dir, "leie.parquet")

    con.execute(f"""
        CREATE OR REPLACE VIEW spending AS
        SELECT * FROM read_parquet('{medicaid_path}');
    """)
    con.execute(f"""
        CREATE OR REPLACE VIEW nppes AS
        SELECT
            "NPI" AS npi,
            "Entity Type Code" AS entity_type,
            "Provider Organization Name (Legal Business Name)" AS org_name,
            "Provider Last Name (Legal Name)" AS last_name,
            "Provider First Name" AS first_name,
            "Provider Business Practice Location Address State Name" AS state,
            "Provider Business Practice Location Address Postal Code" AS postal_code,
            "Healthcare Provider Taxonomy Code_1" AS taxonomy,
            "Provider Enumeration Date" AS enumeration_date,
            "Authorized Official Last Name" AS auth_off_last,
            "Authorized Official First Name" AS auth_off_first
        FROM read_parquet('{nppes_path}');
    """)
    con.execute(f"""
        CREATE OR REPLACE VIEW leie AS
        SELECT * FROM read_parquet('{leie_path}');
    """)
    print("  Views registered: spending, nppes, leie")


def main() -> None:
    """Run the full ingestion pipeline."""
    os.makedirs(DATA_DIR, exist_ok=True)

    print("=== Downloading datasets ===")
    download_all()

    print("=== Building parquet files ===")
    con = get_connection()
    build_nppes_slim(con, DATA_DIR)
    build_leie_parquet(con, DATA_DIR)

    print("=== Registering views ===")
    register_views(con)

    # Quick sanity checks
    for view_name in ["spending", "nppes", "leie"]:
        count = con.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
        print(f"  {view_name}: {count:,} rows")

    con.close()
    print("=== Ingestion complete ===")


if __name__ == "__main__":
    main()
