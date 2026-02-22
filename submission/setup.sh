#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$SCRIPT_DIR/data"
mkdir -p "$DATA_DIR"

echo "=== Installing Python dependencies ==="
python3 -m pip install -r "$SCRIPT_DIR/requirements.txt"

MEDICAID_URL="https://stopendataprod.blob.core.windows.net/datasets/medicaid-provider-spending/2026-02-09/medicaid-provider-spending.parquet"
LEIE_URL="https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv"
NPPES_URL="https://download.cms.gov/nppes/NPPES_Data_Dissemination_February_2026_V2.zip"

# Download Medicaid spending parquet
if [ ! -f "$DATA_DIR/medicaid-provider-spending.parquet" ]; then
    echo "=== Downloading Medicaid Provider Spending (2.9GB) ==="
    curl -L --progress-bar -o "$DATA_DIR/medicaid-provider-spending.parquet" "$MEDICAID_URL"
else
    echo "=== Medicaid spending parquet already exists, skipping ==="
fi

# Download LEIE exclusions CSV
if [ ! -f "$DATA_DIR/UPDATED.csv" ]; then
    echo "=== Downloading OIG LEIE Exclusions ==="
    curl -L --progress-bar -o "$DATA_DIR/UPDATED.csv" "$LEIE_URL"
else
    echo "=== LEIE CSV already exists, skipping ==="
fi

# Download and unzip NPPES
if [ ! -f "$DATA_DIR/nppes_slim.parquet" ]; then
    if [ ! -f "$DATA_DIR/NPPES_Data_Dissemination_February_2026_V2.zip" ]; then
        echo "=== Downloading NPPES NPI Registry (~1GB) ==="
        curl -L --progress-bar -o "$DATA_DIR/NPPES_Data_Dissemination_February_2026_V2.zip" "$NPPES_URL"
    fi
    echo "=== Unzipping NPPES ==="
    unzip -o "$DATA_DIR/NPPES_Data_Dissemination_February_2026_V2.zip" -d "$DATA_DIR/nppes_raw/"
    echo "=== NPPES unzipped. Will be converted to slim parquet by ingest.py ==="
else
    echo "=== NPPES slim parquet already exists, skipping ==="
fi

echo "=== Setup complete ==="
