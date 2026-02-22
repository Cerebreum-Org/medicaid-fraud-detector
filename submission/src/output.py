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

# FCA claim type descriptions by signal type
FCA_CLAIM_TYPES = {
    "excluded_provider": "Presenting false claims — excluded provider cannot legally bill federal healthcare programs",
    "billing_outlier": "Potential overbilling — provider billing far exceeds peer group norms suggesting inflated or fabricated claims",
    "rapid_escalation": "Potential bust-out scheme — new entity with explosive billing growth consistent with fraud-and-flee pattern",
    "workforce_impossibility": "False records — billing volume physically impossible for reported workforce, implying fabricated claims",
    "shared_official": "Conspiracy — coordinated billing through multiple entities controlled by same individual suggests shell company network",
    "geographic_implausibility": "Reverse false claims — repeated billing on same patients with implausible geographic patterns",
}

# Single FCA statute reference by signal type
FCA_STATUTE_REFS = {
    "excluded_provider": "31 U.S.C. § 3729(a)(1)(A)",
    "billing_outlier": "31 U.S.C. § 3729(a)(1)(A)",
    "rapid_escalation": "31 U.S.C. § 3729(a)(1)(A)",
    "workforce_impossibility": "31 U.S.C. § 3729(a)(1)(B)",
    "shared_official": "31 U.S.C. § 3729(a)(1)(C)",
    "geographic_implausibility": "31 U.S.C. § 3729(a)(1)(G)",
}

# Suggested next steps by signal type
NEXT_STEPS = {
    "excluded_provider": [
        "Verify exclusion status on OIG LEIE website and confirm NPI match",
        "Request payment records from state Medicaid agency for the post-exclusion period",
        "Initiate recovery action for all payments made after exclusion date",
        "Refer to OIG for potential criminal prosecution under 42 U.S.C. § 1320a-7b",
    ],
    "billing_outlier": [
        "Request detailed claims data and supporting documentation from provider",
        "Compare service patterns against peer group norms for the same taxonomy and state",
        "Conduct desk audit of highest-volume service codes",
        "Consider on-site audit if billing exceeds 5x peer median",
    ],
    "rapid_escalation": [
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
    "shared_official": [
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

# Severity ordering for comparisons (higher = more severe)
SEVERITY_ORDER = {"critical": 3, "high": 2, "medium": 1, "low": 0}


def compute_severity(signal_name: str, flag: dict) -> str:
    """Determine severity level based on signal type and evidence strength."""
    if signal_name == "excluded_provider":
        return "critical"

    if signal_name == "billing_outlier":
        ratio = flag.get("ratio_to_median", 0)
        return "high" if ratio > 5 else "medium"

    if signal_name == "rapid_escalation":
        growth = flag.get("max_growth_pct", 0)
        return "high" if growth > 500 else "medium"

    if signal_name == "workforce_impossibility":
        return "high"

    if signal_name == "shared_official":
        combined = flag.get("combined_total_paid", 0)
        return "high" if combined > 5_000_000 else "medium"

    if signal_name == "geographic_implausibility":
        ratio = flag.get("bene_to_claims_ratio", 1)
        return "high" if ratio < 0.05 else "medium"

    return "medium"


def estimate_overpayment(signal_name: str, flag: dict) -> float:
    """
    Estimate overpayment in USD based on signal type and evidence.

    Conservative estimates -- these are signals, not determinations.
    """
    if signal_name == "excluded_provider":
        # 100% of payments after exclusion are potentially recoverable
        return float(flag.get("total_paid_after_exclusion", 0))

    if signal_name == "billing_outlier":
        # Excess above p99 as estimated overpayment
        total = float(flag.get("total_paid", 0))
        p99 = float(flag.get("p99_paid", 0))
        return max(0.0, total - p99)

    if signal_name == "rapid_escalation":
        # Total paid in months where growth exceeded 200%
        # Prefer the exact field if available, otherwise fallback
        excess = flag.get("total_paid_excess_growth_months")
        if excess is not None:
            return float(excess)
        return float(flag.get("total_paid_12mo", 0)) * 0.5

    if signal_name == "workforce_impossibility":
        # Sum across all impossible months: for each month, excess = (claims - 1056) * avg_cost
        # Simplified: total_paid_impossible * (1 - 1056 * impossible_months / total_claims_impossible)
        total_paid = float(flag.get("total_paid_impossible", 0))
        total_claims = float(flag.get("total_claims_impossible", 0))
        months = int(flag.get("impossible_months_count", 0))
        plausible_claims = 1056.0 * months  # max plausible = 6 claims/hr * 8hr * 22 days * months
        if total_claims > 0 and total_paid > 0:
            return max(0.0, total_paid * (1 - plausible_claims / total_claims))
        return 0.0

    if signal_name == "shared_official":
        return 0.0

    if signal_name == "geographic_implausibility":
        return 0.0

    return 0.0


def _convert_enumeration_date(raw: str | None) -> str | None:
    """Convert enumeration_date from MM/DD/YYYY to YYYY-MM-DD. Returns None on failure."""
    if not raw:
        return None
    try:
        dt = datetime.strptime(raw, "%m/%d/%Y")
        return dt.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def _make_provider_name(info: dict) -> str:
    """Build provider_name: org_name if available, else 'FIRST LAST'."""
    org = info.get("org_name")
    if org:
        return org
    first = info.get("first_name", "")
    last = info.get("last_name", "")
    name = f"{first} {last}".strip()
    return name if name else "UNKNOWN"


def _convert_entity_type(raw: str | None) -> str:
    """Convert entity_type code: '1' -> 'individual', '2' -> 'organization'."""
    if raw == "1":
        return "individual"
    if raw == "2":
        return "organization"
    return raw or "unknown"


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
            SUM(s.TOTAL_UNIQUE_BENEFICIARIES) AS total_unique_beneficiaries_all_time,
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
            "total_unique_beneficiaries_all_time": int(row[3] or 0),
            "first_claim": str(row[4]) if row[4] else None,
            "last_claim": str(row[5]) if row[5] else None,
        }

    print(f"  Computed stats for {len(rows)} providers")
    return result


def _get_total_providers_scanned(con: duckdb.DuckDBPyConnection) -> int:
    """Count total distinct billing providers in spending data."""
    result = con.execute(
        "SELECT COUNT(DISTINCT BILLING_PROVIDER_NPI_NUM) FROM spending"
    ).fetchone()
    return int(result[0]) if result else 0


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
            # Extract NPIs -- Signal 5 has npi_list, others have single npi
            npis_to_flag = []
            if "npi_list" in flag and flag["npi_list"]:
                npis_to_flag = [str(n) for n in flag["npi_list"]]
            elif flag.get("npi") is not None:
                npis_to_flag = [str(flag["npi"])]
            else:
                continue

            severity = compute_severity(signal_name, flag)
            overpayment = estimate_overpayment(signal_name, flag)

            for npi in npis_to_flag:
                if npi not in provider_flags:
                    provider_flags[npi] = []

                provider_flags[npi].append({
                    "signal_type": signal_name,
                    "severity": severity,
                    "evidence": flag,
                    "overpayment": overpayment,
                })

    # Build final provider records
    all_npis = set(provider_flags.keys())
    enrichment_cache = batch_enrich_providers(con, all_npis)
    stats_cache = batch_aggregate_stats(con, all_npis)

    # Get total providers scanned
    total_providers_scanned = _get_total_providers_scanned(con)

    flagged_providers = []
    for npi, flags_list in provider_flags.items():
        provider_info = enrichment_cache.get(npi, {"npi": npi})
        agg_stats = stats_cache.get(npi, {})

        # Find the most severe signal
        most_severe_signal = max(
            flags_list,
            key=lambda f: SEVERITY_ORDER.get(f["severity"], 0),
        )
        most_severe_type = most_severe_signal["signal_type"]

        # Compute total estimated overpayment across all signals for this provider
        total_overpayment = sum(f["overpayment"] for f in flags_list)

        # Aggregate suggested_next_steps from all signals
        all_next_steps = []
        seen_steps = set()
        for f in flags_list:
            for step in NEXT_STEPS.get(f["signal_type"], []):
                if step not in seen_steps:
                    seen_steps.add(step)
                    all_next_steps.append(step)

        # Build fca_relevance from the most severe signal
        fca_relevance = {
            "claim_type": FCA_CLAIM_TYPES.get(most_severe_type, ""),
            "statute_reference": FCA_STATUTE_REFS.get(most_severe_type, ""),
            "suggested_next_steps": all_next_steps,
        }

        # Build the per-signal list (only signal_type, severity, evidence)
        signals = []
        for f in flags_list:
            signals.append({
                "signal_type": f["signal_type"],
                "severity": f["severity"],
                "evidence": f["evidence"],
            })

        # Build flattened provider record
        provider_record = {
            "npi": npi,
            "provider_name": _make_provider_name(provider_info),
            "entity_type": _convert_entity_type(provider_info.get("entity_type")),
            "taxonomy_code": provider_info.get("taxonomy") or "",
            "state": provider_info.get("state") or "",
            "enumeration_date": _convert_enumeration_date(provider_info.get("enumeration_date")),
            "total_paid_all_time": agg_stats.get("total_paid_all_time", 0.00),
            "total_claims_all_time": agg_stats.get("total_claims_all_time", 0),
            "total_unique_beneficiaries_all_time": agg_stats.get("total_unique_beneficiaries_all_time", 0),
            "signals": signals,
            "estimated_overpayment_usd": round(total_overpayment, 2),
            "fca_relevance": fca_relevance,
        }

        flagged_providers.append(provider_record)

    # Sort by severity (most severe first) then by estimated overpayment descending
    severity_sort = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    flagged_providers.sort(
        key=lambda p: (
            severity_sort.get(
                max(
                    (s["severity"] for s in p["signals"]),
                    key=lambda sv: SEVERITY_ORDER.get(sv, 0),
                ),
                99,
            ),
            -p["estimated_overpayment_usd"],
        )
    )

    # Compute signal_counts
    signal_counts = {
        "excluded_provider": 0,
        "billing_outlier": 0,
        "rapid_escalation": 0,
        "workforce_impossibility": 0,
        "shared_official": 0,
        "geographic_implausibility": 0,
    }
    for signal_name, flags in signal_results.items():
        if signal_name in signal_counts:
            signal_counts[signal_name] = len(flags)

    report = {
        "generated_at": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        "tool_version": "medicaid-fraud-detector v1.0",
        "total_providers_scanned": total_providers_scanned,
        "total_providers_flagged": len(flagged_providers),
        "signal_counts": signal_counts,
        "flagged_providers": flagged_providers,
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
    print(f"  Providers flagged: {report['total_providers_flagged']}")
    print(f"  Total providers scanned: {report['total_providers_scanned']}")
    sig_total = sum(report['signal_counts'].values())
    print(f"  Total signals: {sig_total}")
    print(f"  Signal counts: {report['signal_counts']}")

    con.close()
    print("=== Report generation complete ===")


if __name__ == "__main__":
    main()
