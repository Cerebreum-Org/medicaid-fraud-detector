"""
JSON report generation.

Merges all signal results, enriches with NPPES data, applies severity rules,
computes overpayment estimates, and writes the final fraud_signals.json.
"""

import json
import os
from datetime import datetime, timezone

import duckdb

from src.ingest import DATA_DIR, get_connection, register_views

# Statute references by signal type
STATUTE_REFS = {
    "excluded_provider_billing": [
        "42 U.S.C. § 1320a-7 (Exclusion of certain individuals and entities)",
        "42 CFR § 1001 (OIG Exclusion Authorities)",
    ],
    "billing_volume_outlier": [
        "31 U.S.C. § 3729 (False Claims Act)",
        "42 U.S.C. § 1320a-7b(a) (Criminal penalties for acts involving Federal health care programs)",
    ],
    "rapid_billing_escalation": [
        "31 U.S.C. § 3729 (False Claims Act)",
        "18 U.S.C. § 1347 (Health care fraud)",
    ],
    "workforce_impossibility": [
        "31 U.S.C. § 3729 (False Claims Act)",
        "42 U.S.C. § 1320a-7b(a)(1) (Filing false claims)",
    ],
    "shared_authorized_official": [
        "18 U.S.C. § 371 (Conspiracy to defraud the United States)",
        "18 U.S.C. § 1349 (Conspiracy to commit health care fraud)",
        "42 U.S.C. § 1320a-7b(b) (Anti-kickback statute)",
    ],
    "geographic_implausibility": [
        "31 U.S.C. § 3729 (False Claims Act)",
        "42 U.S.C. § 1320a-7b(a)(1) (Filing false claims)",
    ],
}

# Suggested next steps by signal type
NEXT_STEPS = {
    "excluded_provider_billing": [
        "Verify exclusion status on OIG LEIE website and confirm NPI match",
        "Request payment records from state Medicaid agency for the post-exclusion period",
        "Initiate recovery action for all payments made after exclusion date",
        "Refer to OIG for potential criminal prosecution under 42 U.S.C. § 1320a-7b",
    ],
    "billing_volume_outlier": [
        "Request detailed claims data and supporting documentation from provider",
        "Compare service patterns against peer group norms for the same taxonomy and state",
        "Conduct desk audit of highest-volume service codes",
        "Consider on-site audit if billing exceeds 5x peer median",
    ],
    "rapid_billing_escalation": [
        "Review provider enrollment application and supporting documentation",
        "Analyze service code distribution for patterns consistent with upcoding",
        "Interview beneficiaries to verify services were rendered",
        "Place provider on prepayment review if growth exceeds 500%",
    ],
    "workforce_impossibility": [
        "Request organizational staffing records and provider schedules",
        "Cross-reference NPI sub-parts and rendering providers",
        "Analyze time-of-day claim patterns for statistical impossibilities",
        "Conduct unannounced site visit to verify staffing levels",
    ],
    "shared_authorized_official": [
        "Map all NPIs controlled by the authorized official and their corporate relationships",
        "Analyze billing patterns across all controlled entities for coordination",
        "Review state corporate filings for common ownership structures",
        "Investigate for potential kickback or self-referral arrangements",
    ],
    "geographic_implausibility": [
        "Map beneficiary addresses against provider service area",
        "Review travel logs and service delivery documentation",
        "Interview a sample of beneficiaries to verify services received",
        "Analyze GPS data from electronic visit verification systems if available",
    ],
}


def compute_severity(signal_name: str, flag: dict) -> str:
    """Determine severity level based on signal type and evidence strength."""
    if signal_name == "excluded_provider_billing":
        return "critical"

    if signal_name == "billing_volume_outlier":
        ratio = flag.get("ratio_to_median", 0)
        return "high" if ratio > 5 else "medium"

    if signal_name == "rapid_billing_escalation":
        growth = flag.get("max_growth_pct", 0)
        return "high" if growth > 500 else "medium"

    if signal_name == "workforce_impossibility":
        return "high"

    if signal_name == "shared_authorized_official":
        combined = flag.get("combined_total_paid", 0)
        return "high" if combined > 5_000_000 else "medium"

    if signal_name == "geographic_implausibility":
        ratio = flag.get("bene_to_claims_ratio", 1)
        return "high" if ratio < 0.05 else "medium"

    return "medium"


def estimate_overpayment(signal_name: str, flag: dict) -> float:
    """
    Estimate overpayment in USD based on signal type and evidence.

    Conservative estimates — these are signals, not determinations.
    """
    if signal_name == "excluded_provider_billing":
        # 100% of payments after exclusion are potentially recoverable
        return float(flag.get("total_paid_after_exclusion", 0))

    if signal_name == "billing_volume_outlier":
        # Excess above peer median as estimated overpayment
        total = flag.get("total_paid", 0)
        median = flag.get("median_paid", 0)
        return max(0.0, float(total) - float(median))

    if signal_name == "rapid_billing_escalation":
        # 50% of total paid in first 12 months as conservative estimate
        return float(flag.get("total_paid_12mo", 0)) * 0.5

    if signal_name == "workforce_impossibility":
        # Cannot estimate without total paid — use 0 as placeholder
        return 0.0

    if signal_name == "shared_authorized_official":
        # 10% of combined spending as conservative estimate
        return float(flag.get("combined_total_paid", 0)) * 0.1

    if signal_name == "geographic_implausibility":
        # 75% of payments for home health services with implausible patterns
        return float(flag.get("total_paid", 0)) * 0.75

    return 0.0


def batch_enrich_providers(con: duckdb.DuckDBPyConnection, npis: set[str]) -> dict[str, dict]:
    """Batch look up provider details from NPPES for all flagged NPIs."""
    if not npis:
        return {}

    print(f"  Enriching {len(npis)} unique providers from NPPES...")
    # Create a temp table with all NPIs to join against
    con.execute("CREATE OR REPLACE TEMP TABLE flagged_npis (npi VARCHAR);")
    # Insert in batches
    batch = [(npi,) for npi in npis]
    con.executemany("INSERT INTO flagged_npis VALUES (?)", batch)

    rows = con.execute("""
        SELECT DISTINCT ON (n.npi)
               n.npi, n.entity_type, n.org_name, n.last_name, n.first_name,
               n.state, n.postal_code, n.taxonomy, n.enumeration_date
        FROM flagged_npis f
        JOIN nppes n ON f.npi = n.npi
    """).fetchall()

    cols = [
        "npi", "entity_type", "org_name", "last_name", "first_name",
        "state", "postal_code", "taxonomy", "enumeration_date",
    ]

    result = {}
    for row in rows:
        d = {k: (str(v) if v is not None else None) for k, v in zip(cols, row)}
        result[d["npi"]] = d

    # Fill in missing NPIs
    for npi in npis:
        if npi not in result:
            result[npi] = {"npi": npi}

    print(f"  Enriched {len(rows)} providers from NPPES")
    return result


def batch_aggregate_stats(con: duckdb.DuckDBPyConnection, npis: set[str]) -> dict[str, dict]:
    """Batch compute aggregate spending stats for all flagged NPIs."""
    if not npis:
        return {}

    print(f"  Computing aggregate stats for {len(npis)} providers...")

    rows = con.execute("""
        SELECT
            s.BILLING_PROVIDER_NPI_NUM AS npi,
            SUM(s.TOTAL_PAID) AS total_paid_all_time,
            SUM(s.TOTAL_CLAIMS) AS total_claims_all_time,
            SUM(s.TOTAL_UNIQUE_BENEFICIARIES) AS total_beneficiaries_all_time,
            MIN(s.CLAIM_FROM_MONTH) AS first_claim,
            MAX(s.CLAIM_FROM_MONTH) AS last_claim
        FROM spending s
        JOIN flagged_npis f ON s.BILLING_PROVIDER_NPI_NUM = f.npi
        GROUP BY 1
    """).fetchall()

    result = {}
    for row in rows:
        npi = str(row[0])
        result[npi] = {
            "total_paid_all_time": float(row[1] or 0),
            "total_claims_all_time": int(row[2] or 0),
            "total_beneficiaries_all_time": int(row[3] or 0),
            "first_claim": str(row[4]) if row[4] else None,
            "last_claim": str(row[5]) if row[5] else None,
        }

    print(f"  Computed stats for {len(rows)} providers")
    return result


def build_report(con: duckdb.DuckDBPyConnection, signal_results: dict) -> dict:
    """
    Build the final fraud signals report.

    Groups flags by NPI, enriches with provider info and aggregate stats,
    applies severity rules, and computes overpayment estimates.
    """
    # Group all flags by NPI
    provider_flags: dict[str, list[dict]] = {}

    for signal_name, flags in signal_results.items():
        for flag in flags:
            # Extract NPI — different signals store it differently
            npi = flag.get("npi")
            if npi is None:
                # Signal 5 has npi_list instead of single npi
                npi_list = flag.get("npi_list", [])
                if npi_list:
                    # Create a flag entry for the first NPI, reference all
                    npi = str(npi_list[0])
                else:
                    continue

            npi = str(npi)
            if npi not in provider_flags:
                provider_flags[npi] = []

            provider_flags[npi].append({
                "signal": signal_name,
                "severity": compute_severity(signal_name, flag),
                "evidence": flag,
                "estimated_overpayment_usd": estimate_overpayment(signal_name, flag),
                "statute_references": STATUTE_REFS.get(signal_name, []),
                "suggested_next_steps": NEXT_STEPS.get(signal_name, []),
            })

    # Build final provider records
    all_npis = set(provider_flags.keys())
    enrichment_cache = batch_enrich_providers(con, all_npis)
    stats_cache = batch_aggregate_stats(con, all_npis)

    providers = []
    for npi, flags in provider_flags.items():
        provider_info = enrichment_cache.get(npi, {"npi": npi})
        agg_stats = stats_cache.get(npi, {})

        # Overall severity is the worst among all flags
        severity_order = {"critical": 3, "high": 2, "medium": 1, "low": 0}
        overall_severity = max(
            (f["severity"] for f in flags),
            key=lambda s: severity_order.get(s, 0),
        )

        total_estimated_overpayment = sum(f["estimated_overpayment_usd"] for f in flags)

        providers.append({
            "npi": npi,
            "provider_info": provider_info,
            "aggregate_stats": agg_stats,
            "overall_severity": overall_severity,
            "total_estimated_overpayment_usd": round(total_estimated_overpayment, 2),
            "signal_count": len(flags),
            "flags": flags,
        })

    # Sort by severity then estimated overpayment
    severity_sort = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    providers.sort(
        key=lambda p: (
            severity_sort.get(p["overall_severity"], 99),
            -p["total_estimated_overpayment_usd"],
        )
    )

    # Compute summary stats
    signal_counts = {}
    for signal_name, flags in signal_results.items():
        signal_counts[signal_name] = len(flags)

    report = {
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "engine": "medicaid-fraud-detector v1.0",
            "data_sources": {
                "medicaid_spending": "HHS Medicaid Provider Spending (2026-02-09)",
                "leie_exclusions": "OIG LEIE Exclusions (UPDATED.csv)",
                "nppes_registry": "CMS NPPES NPI Registry (February 2026 V2)",
            },
            "total_providers_flagged": len(providers),
            "total_flags": sum(len(p["flags"]) for p in providers),
            "total_estimated_overpayment_usd": round(
                sum(p["total_estimated_overpayment_usd"] for p in providers), 2
            ),
            "severity_distribution": {
                "critical": sum(1 for p in providers if p["overall_severity"] == "critical"),
                "high": sum(1 for p in providers if p["overall_severity"] == "high"),
                "medium": sum(1 for p in providers if p["overall_severity"] == "medium"),
            },
            "signal_counts": signal_counts,
        },
        "providers": providers,
    }

    return report


def main() -> None:
    """Generate the final fraud signals report."""
    con = get_connection()
    register_views(con)

    # Load intermediate signal results
    intermediate_path = os.path.join(DATA_DIR, "signal_results.json")
    if not os.path.exists(intermediate_path):
        print("ERROR: signal_results.json not found. Run signals.py first.")
        raise SystemExit(1)

    with open(intermediate_path) as f:
        signal_results = json.load(f)

    print("=== Building fraud signals report ===")
    report = build_report(con, signal_results)

    # Write to submission directory
    output_path = os.path.join(os.path.dirname(DATA_DIR), "fraud_signals.json")
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    print(f"  Report written: {output_path}")
    print(f"  Providers flagged: {report['metadata']['total_providers_flagged']}")
    print(f"  Total flags: {report['metadata']['total_flags']}")
    print(f"  Estimated overpayment: ${report['metadata']['total_estimated_overpayment_usd']:,.2f}")
    print(f"  Severity: {report['metadata']['severity_distribution']}")

    con.close()
    print("=== Report generation complete ===")


if __name__ == "__main__":
    main()
