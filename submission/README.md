# Medicaid Provider Fraud Signal Detection Engine

CLI tool that ingests 3 public healthcare datasets and outputs provider-level fraud signal reports as JSON. Designed to run on a MacBook (16GB RAM, Apple Silicon) in under 4 hours.

## Quick Start

```bash
# Install dependencies and download datasets (~4GB total)
./setup.sh

# Run the full pipeline → produces fraud_signals.json
./run.sh

# Run tests
python -m pytest tests/ -v
```

## Architecture

**Engine:** DuckDB — streams through 2.9GB parquet with ~2GB peak RAM via `approx_quantile()` and direct SQL on parquet files.

```
submission/
├── setup.sh          # Downloads data + installs deps
├── run.sh            # Produces fraud_signals.json
├── src/
│   ├── ingest.py     # Download, extract, load into DuckDB
│   ├── signals.py    # All 6 signal implementations
│   └── output.py     # JSON report generation
├── tests/
│   ├── test_signals.py
│   ├── create_fixtures.py
│   └── fixtures/
└── fraud_signals.json
```

## Data Sources

| Dataset | Size | Format |
|---------|------|--------|
| HHS Medicaid Provider Spending | 2.9GB, 227M rows | Parquet |
| OIG LEIE Exclusions | ~5MB, ~70K rows | CSV |
| NPPES NPI Registry | ~1GB zip, ~8M rows | Zipped CSV |

## Fraud Signals

### Signal 1: Excluded Provider Still Billing (Critical)
Matches LEIE exclusion records against billing/servicing NPIs. Tier 1 uses direct NPI match; Tier 2 falls back to name+state matching via NPPES for LEIE records without NPI.

### Signal 2: Billing Volume Outlier (Medium/High)
Flags providers above the 99th percentile of total payments within their taxonomy+state peer group. Uses `approx_quantile()` for memory-efficient percentile computation.

### Signal 3: Rapid Billing Escalation (Medium/High)
Identifies newly enumerated providers (within 24 months) whose rolling 3-month average billing grows more than 200% during their first 12 months.

### Signal 4: Workforce Impossibility (High)
Flags organizations where peak monthly claims imply more than 6 claims per provider-hour (based on 22 working days, 8 hours/day).

### Signal 5: Shared Authorized Official (Medium/High)
Identifies authorized officials controlling 5+ organization NPIs with combined spending exceeding $1M.

### Signal 6: Geographic Implausibility (Medium/High)
Flags home health providers (HCPCS G0151-G0162, G0299-G0300, S9122-S9124, T1019-T1022) with a beneficiary-to-claims ratio below 0.1, indicating a small number of patients receiving an implausible volume of services.

## Output Schema

```json
{
  "metadata": {
    "generated_at": "2026-02-21T...",
    "total_providers_flagged": 1234,
    "total_flags": 5678,
    "total_estimated_overpayment_usd": 123456789.00,
    "severity_distribution": { "critical": 10, "high": 200, "medium": 1024 },
    "signal_counts": { ... }
  },
  "providers": [
    {
      "npi": "1234567890",
      "provider_info": { ... },
      "aggregate_stats": { ... },
      "overall_severity": "critical",
      "total_estimated_overpayment_usd": 500000.00,
      "signal_count": 2,
      "flags": [
        {
          "signal": "excluded_provider_billing",
          "severity": "critical",
          "evidence": { ... },
          "estimated_overpayment_usd": 500000.00,
          "statute_references": [ ... ],
          "suggested_next_steps": [ ... ]
        }
      ]
    }
  ]
}
```

## Requirements

- Python 3.10+
- DuckDB >= 1.2
- ~10GB disk space for datasets
- 16GB RAM recommended
