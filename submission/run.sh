#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Step 1: Ingesting data ==="
python -m src.ingest

echo "=== Step 2: Computing fraud signals ==="
python -m src.signals

echo "=== Step 3: Generating output report ==="
python -m src.output

echo "=== Done. Output: fraud_signals.json ==="
