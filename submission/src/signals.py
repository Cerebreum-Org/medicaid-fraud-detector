"""
Fraud signal detection engine.

Six signals computed via DuckDB SQL against Medicaid spending,
NPPES NPI registry, and OIG LEIE exclusion list.
"""

import json
import os
import sys

import duckdb

from src.ingest import DATA_DIR, get_connection, register_views


def signal_1_excluded_provider(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """
    Signal 1: Excluded Provider Still Billing.
    Tier 1 — NPI match between LEIE and spending.
    Tier 2 — Name+State fallback for LEIE records without NPI.
    """
    print("  Computing Signal 1: Excluded Provider Still Billing...")

    # Tier 1: Direct NPI match on billing or servicing NPI
    # CLAIM_FROM_MONTH is 'YYYY-MM' varchar; EXCLDATE is 'YYYYMMDD' varchar
    # Convert EXCLDATE to 'YYYY-MM' for string comparison
    rows = con.execute("""
        WITH leie_with_npi AS (
            SELECT NPI, EXCLDATE::VARCHAR AS EXCLDATE, EXCLTYPE, REINDATE::VARCHAR AS REINDATE,
                   SUBSTR(EXCLDATE::VARCHAR, 1, 4) || '-' || SUBSTR(EXCLDATE::VARCHAR, 5, 2) AS excl_ym,
                   CASE WHEN REINDATE IS NOT NULL
                        THEN SUBSTR(REINDATE::VARCHAR, 1, 4) || '-' || SUBSTR(REINDATE::VARCHAR, 5, 2)
                        ELSE NULL END AS rein_ym
            FROM leie
            WHERE NPI IS NOT NULL AND CAST(NPI AS VARCHAR) != ''
        ),
        billing_matches AS (
            SELECT
                m.BILLING_PROVIDER_NPI_NUM AS npi,
                l.EXCLDATE,
                l.EXCLTYPE,
                'billing' AS match_role,
                SUM(m.TOTAL_PAID) AS total_paid_after_exclusion,
                SUM(m.TOTAL_CLAIMS) AS total_claims_after_exclusion,
                MIN(m.CLAIM_FROM_MONTH) AS first_claim_month,
                MAX(m.CLAIM_FROM_MONTH) AS last_claim_month
            FROM spending m
            JOIN leie_with_npi l ON m.BILLING_PROVIDER_NPI_NUM = l.NPI
            WHERE m.CLAIM_FROM_MONTH > l.excl_ym
              AND (l.rein_ym IS NULL OR m.CLAIM_FROM_MONTH < l.rein_ym)
            GROUP BY 1, 2, 3
        ),
        servicing_matches AS (
            SELECT
                m.SERVICING_PROVIDER_NPI_NUM AS npi,
                l.EXCLDATE,
                l.EXCLTYPE,
                'servicing' AS match_role,
                SUM(m.TOTAL_PAID) AS total_paid_after_exclusion,
                SUM(m.TOTAL_CLAIMS) AS total_claims_after_exclusion,
                MIN(m.CLAIM_FROM_MONTH) AS first_claim_month,
                MAX(m.CLAIM_FROM_MONTH) AS last_claim_month
            FROM spending m
            JOIN leie_with_npi l ON m.SERVICING_PROVIDER_NPI_NUM = l.NPI
            WHERE m.CLAIM_FROM_MONTH > l.excl_ym
              AND (l.rein_ym IS NULL OR m.CLAIM_FROM_MONTH < l.rein_ym)
            GROUP BY 1, 2, 3
        )
        SELECT * FROM billing_matches
        UNION ALL
        SELECT * FROM servicing_matches
    """).fetchall()

    columns = [
        "npi", "excl_date", "excl_type", "match_role",
        "total_paid_after_exclusion", "total_claims_after_exclusion",
        "first_claim_month", "last_claim_month",
    ]

    results = []
    for row in rows:
        d = dict(zip(columns, row))
        # Convert date objects to strings
        for k in ("first_claim_month", "last_claim_month"):
            if d[k] is not None:
                d[k] = str(d[k])
        d["total_paid_after_exclusion"] = float(d["total_paid_after_exclusion"] or 0)
        d["total_claims_after_exclusion"] = int(d["total_claims_after_exclusion"] or 0)
        results.append(d)

    # Tier 2: Name+State fallback via NPPES join for LEIE records without NPI
    rows2 = con.execute("""
        WITH leie_no_npi AS (
            SELECT LASTNAME, FIRSTNAME, STATE,
                   EXCLDATE::VARCHAR AS EXCLDATE, EXCLTYPE, REINDATE::VARCHAR AS REINDATE,
                   SUBSTR(EXCLDATE::VARCHAR, 1, 4) || '-' || SUBSTR(EXCLDATE::VARCHAR, 5, 2) AS excl_ym,
                   CASE WHEN REINDATE IS NOT NULL
                        THEN SUBSTR(REINDATE::VARCHAR, 1, 4) || '-' || SUBSTR(REINDATE::VARCHAR, 5, 2)
                        ELSE NULL END AS rein_ym
            FROM leie
            WHERE NPI IS NULL
        ),
        name_matches AS (
            SELECT
                n.npi,
                l.EXCLDATE,
                l.EXCLTYPE,
                'name_match' AS match_role,
                SUM(m.TOTAL_PAID) AS total_paid_after_exclusion,
                SUM(m.TOTAL_CLAIMS) AS total_claims_after_exclusion,
                MIN(m.CLAIM_FROM_MONTH) AS first_claim_month,
                MAX(m.CLAIM_FROM_MONTH) AS last_claim_month
            FROM leie_no_npi l
            JOIN nppes n ON UPPER(TRIM(n.last_name)) = l.LASTNAME
                        AND UPPER(TRIM(n.first_name)) = l.FIRSTNAME
                        AND UPPER(TRIM(n.state)) = l.STATE
            JOIN spending m ON m.BILLING_PROVIDER_NPI_NUM = n.npi
            WHERE m.CLAIM_FROM_MONTH > l.excl_ym
              AND (l.rein_ym IS NULL OR m.CLAIM_FROM_MONTH < l.rein_ym)
            GROUP BY 1, 2, 3
        )
        SELECT * FROM name_matches
    """).fetchall()

    for row in rows2:
        d = dict(zip(columns, row))
        for k in ("first_claim_month", "last_claim_month"):
            if d[k] is not None:
                d[k] = str(d[k])
        d["total_paid_after_exclusion"] = float(d["total_paid_after_exclusion"] or 0)
        d["total_claims_after_exclusion"] = int(d["total_claims_after_exclusion"] or 0)
        results.append(d)

    print(f"    Signal 1: {len(results)} flags")
    return results


def signal_2_billing_volume_outlier(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """
    Signal 2: Billing Volume Outlier (p99 by taxonomy + state).
    """
    print("  Computing Signal 2: Billing Volume Outlier...")

    rows = con.execute("""
        WITH provider_totals AS (
            SELECT BILLING_PROVIDER_NPI_NUM AS npi,
                   SUM(TOTAL_PAID) AS total_paid,
                   SUM(TOTAL_CLAIMS) AS total_claims
            FROM spending
            GROUP BY 1
        ),
        peer_stats AS (
            SELECT n.taxonomy, n.state,
                   approx_quantile(pt.total_paid, 0.50) AS median_paid,
                   approx_quantile(pt.total_paid, 0.99) AS p99_paid,
                   COUNT(*) AS peer_count
            FROM provider_totals pt
            JOIN nppes n ON pt.npi = n.npi
            WHERE n.taxonomy IS NOT NULL AND n.taxonomy != ''
              AND n.state IS NOT NULL AND n.state != ''
            GROUP BY 1, 2
            HAVING COUNT(*) >= 10
        )
        SELECT pt.npi,
               pt.total_paid,
               pt.total_claims,
               ps.taxonomy,
               ps.state,
               ps.median_paid,
               ps.p99_paid,
               ps.peer_count,
               pt.total_paid / NULLIF(ps.median_paid, 0) AS ratio_to_median
        FROM provider_totals pt
        JOIN nppes n ON pt.npi = n.npi
        JOIN peer_stats ps ON n.taxonomy = ps.taxonomy AND n.state = ps.state
        WHERE pt.total_paid > ps.p99_paid
        ORDER BY ratio_to_median DESC
    """).fetchall()

    columns = [
        "npi", "total_paid", "total_claims", "taxonomy", "state",
        "median_paid", "p99_paid", "peer_count", "ratio_to_median",
    ]

    results = []
    for row in rows:
        d = dict(zip(columns, row))
        d["total_paid"] = float(d["total_paid"] or 0)
        d["total_claims"] = int(d["total_claims"] or 0)
        d["median_paid"] = float(d["median_paid"] or 0)
        d["p99_paid"] = float(d["p99_paid"] or 0)
        d["ratio_to_median"] = float(d["ratio_to_median"] or 0)
        d["peer_count"] = int(d["peer_count"] or 0)
        results.append(d)

    print(f"    Signal 2: {len(results)} flags")
    return results


def signal_3_rapid_escalation(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """
    Signal 3: Rapid Billing Escalation (new entities).
    Providers enumerated within 24 months before first billing, with
    rolling 3-month average growth > 200%.
    """
    print("  Computing Signal 3: Rapid Billing Escalation...")

    # CLAIM_FROM_MONTH is 'YYYY-MM' varchar; enumeration_date is 'MM/DD/YYYY'
    # Convert enum_date to 'YYYY-MM' for comparison
    rows = con.execute("""
        WITH provider_months AS (
            SELECT
                BILLING_PROVIDER_NPI_NUM AS npi,
                CLAIM_FROM_MONTH AS month,
                SUM(TOTAL_PAID) AS monthly_paid
            FROM spending
            GROUP BY 1, 2
        ),
        new_providers AS (
            SELECT
                pm.npi,
                pm.month,
                pm.monthly_paid,
                n.enumeration_date AS enum_date_raw,
                STRFTIME(TRY_STRPTIME(n.enumeration_date, '%m/%d/%Y'), '%Y-%m') AS enum_ym,
                STRFTIME(TRY_STRPTIME(n.enumeration_date, '%m/%d/%Y') + INTERVAL '24 months', '%Y-%m') AS enum_plus_24_ym,
                MIN(pm.month) OVER (PARTITION BY pm.npi) AS first_billing_month,
                ROW_NUMBER() OVER (PARTITION BY pm.npi ORDER BY pm.month) AS month_rank
            FROM provider_months pm
            JOIN nppes n ON pm.npi = n.npi
            WHERE n.enumeration_date IS NOT NULL AND n.enumeration_date != ''
        ),
        filtered AS (
            SELECT *
            FROM new_providers
            WHERE enum_ym IS NOT NULL
              AND first_billing_month <= enum_plus_24_ym
              AND month_rank <= 12
        ),
        with_rolling AS (
            SELECT
                npi,
                month,
                monthly_paid,
                enum_date_raw,
                first_billing_month,
                month_rank,
                AVG(monthly_paid) OVER (
                    PARTITION BY npi ORDER BY month
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ) AS rolling_3mo_avg
            FROM filtered
        ),
        with_growth AS (
            SELECT
                *,
                LAG(rolling_3mo_avg, 3) OVER (
                    PARTITION BY npi ORDER BY month
                ) AS prev_rolling_3mo_avg
            FROM with_rolling
        ),
        flagged AS (
            SELECT
                npi,
                MAX(CASE
                    WHEN prev_rolling_3mo_avg > 0
                    THEN (rolling_3mo_avg - prev_rolling_3mo_avg) / prev_rolling_3mo_avg * 100
                    ELSE 0
                END) AS max_growth_pct,
                MAX(monthly_paid) AS peak_monthly_paid,
                MIN(month) AS first_month,
                MAX(month) AS last_month,
                ANY_VALUE(enum_date_raw) AS enum_date,
                SUM(monthly_paid) AS total_paid_12mo
            FROM with_growth
            WHERE month_rank >= 4
            GROUP BY 1
            HAVING MAX(CASE
                WHEN prev_rolling_3mo_avg > 0
                THEN (rolling_3mo_avg - prev_rolling_3mo_avg) / prev_rolling_3mo_avg * 100
                ELSE 0
            END) > 200
        )
        SELECT * FROM flagged
        ORDER BY max_growth_pct DESC
    """).fetchall()

    columns = [
        "npi", "max_growth_pct", "peak_monthly_paid",
        "first_month", "last_month", "enum_date", "total_paid_12mo",
    ]

    results = []
    for row in rows:
        d = dict(zip(columns, row))
        for k in ("first_month", "last_month", "enum_date"):
            if d[k] is not None:
                d[k] = str(d[k])
        d["max_growth_pct"] = float(d["max_growth_pct"] or 0)
        d["peak_monthly_paid"] = float(d["peak_monthly_paid"] or 0)
        d["total_paid_12mo"] = float(d["total_paid_12mo"] or 0)
        results.append(d)

    print(f"    Signal 3: {len(results)} flags")
    return results


def signal_4_workforce_impossibility(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """
    Signal 4: Workforce Impossibility.
    Organizations (Entity Type Code = 2) with implausibly high claims per hour.
    """
    print("  Computing Signal 4: Workforce Impossibility...")

    rows = con.execute("""
        WITH org_monthly AS (
            SELECT
                m.BILLING_PROVIDER_NPI_NUM AS npi,
                m.CLAIM_FROM_MONTH AS month,
                SUM(m.TOTAL_CLAIMS) AS monthly_claims
            FROM spending m
            JOIN nppes n ON m.BILLING_PROVIDER_NPI_NUM = n.npi
            WHERE n.entity_type = '2'
            GROUP BY 1, 2
        ),
        max_monthly AS (
            SELECT
                npi,
                MAX(monthly_claims) AS max_monthly_claims,
                ANY_VALUE(month) AS peak_month
            FROM org_monthly
            GROUP BY 1
        )
        SELECT
            npi,
            max_monthly_claims,
            peak_month,
            max_monthly_claims / (22.0 * 8.0) AS implied_claims_per_hour
        FROM max_monthly
        WHERE max_monthly_claims / (22.0 * 8.0) > 6
        ORDER BY implied_claims_per_hour DESC
    """).fetchall()

    columns = ["npi", "max_monthly_claims", "peak_month", "implied_claims_per_hour"]

    results = []
    for row in rows:
        d = dict(zip(columns, row))
        if d["peak_month"] is not None:
            d["peak_month"] = str(d["peak_month"])
        d["max_monthly_claims"] = int(d["max_monthly_claims"] or 0)
        d["implied_claims_per_hour"] = float(d["implied_claims_per_hour"] or 0)
        results.append(d)

    print(f"    Signal 4: {len(results)} flags")
    return results


def signal_5_shared_authorized_official(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """
    Signal 5: Shared Authorized Official.
    Officials controlling 5+ NPIs with combined spending > $1,000,000.
    """
    print("  Computing Signal 5: Shared Authorized Official...")

    rows = con.execute("""
        WITH official_npis AS (
            SELECT
                UPPER(TRIM(auth_off_last)) AS off_last,
                UPPER(TRIM(auth_off_first)) AS off_first,
                npi,
                org_name,
                state
            FROM nppes
            WHERE auth_off_last IS NOT NULL AND auth_off_last != ''
              AND auth_off_first IS NOT NULL AND auth_off_first != ''
              AND entity_type = '2'
        ),
        official_groups AS (
            SELECT
                off_last,
                off_first,
                COUNT(DISTINCT npi) AS npi_count,
                LIST(DISTINCT npi) AS npi_list,
                LIST(DISTINCT state) AS states
            FROM official_npis
            GROUP BY 1, 2
            HAVING COUNT(DISTINCT npi) >= 5
        ),
        with_spending AS (
            SELECT
                og.off_last,
                og.off_first,
                og.npi_count,
                og.npi_list,
                og.states,
                SUM(m.TOTAL_PAID) AS combined_total_paid,
                SUM(m.TOTAL_CLAIMS) AS combined_total_claims
            FROM official_groups og
            JOIN official_npis on2 ON og.off_last = on2.off_last
                                   AND og.off_first = on2.off_first
            JOIN spending m ON m.BILLING_PROVIDER_NPI_NUM = on2.npi
            GROUP BY 1, 2, 3, 4, 5
        )
        SELECT *
        FROM with_spending
        WHERE combined_total_paid > 1000000
        ORDER BY combined_total_paid DESC
    """).fetchall()

    columns = [
        "off_last", "off_first", "npi_count", "npi_list", "states",
        "combined_total_paid", "combined_total_claims",
    ]

    results = []
    for row in rows:
        d = dict(zip(columns, row))
        d["combined_total_paid"] = float(d["combined_total_paid"] or 0)
        d["combined_total_claims"] = int(d["combined_total_claims"] or 0)
        d["npi_count"] = int(d["npi_count"] or 0)
        # npi_list and states are already Python lists from DuckDB LIST()
        if isinstance(d["npi_list"], list):
            d["npi_list"] = [str(x) for x in d["npi_list"]]
        if isinstance(d["states"], list):
            d["states"] = [str(x) for x in d["states"]]
        results.append(d)

    print(f"    Signal 5: {len(results)} flags")
    return results


def signal_6_geographic_implausibility(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """
    Signal 6: Geographic Implausibility (home health).
    Home health providers with suspiciously low beneficiary-to-claims ratio.
    """
    print("  Computing Signal 6: Geographic Implausibility...")

    rows = con.execute("""
        WITH home_health AS (
            SELECT
                BILLING_PROVIDER_NPI_NUM AS npi,
                CLAIM_FROM_MONTH AS month,
                SUM(TOTAL_CLAIMS) AS monthly_claims,
                SUM(TOTAL_UNIQUE_BENEFICIARIES) AS monthly_beneficiaries,
                SUM(TOTAL_PAID) AS monthly_paid
            FROM spending
            WHERE (
                HCPCS_CODE BETWEEN 'G0151' AND 'G0162'
                OR HCPCS_CODE BETWEEN 'G0299' AND 'G0300'
                OR HCPCS_CODE BETWEEN 'S9122' AND 'S9124'
                OR HCPCS_CODE BETWEEN 'T1019' AND 'T1022'
            )
            GROUP BY 1, 2
        ),
        high_volume AS (
            SELECT *
            FROM home_health
            WHERE monthly_claims > 100
        ),
        flagged AS (
            SELECT
                npi,
                SUM(monthly_claims) AS total_claims,
                SUM(monthly_beneficiaries) AS total_beneficiaries,
                SUM(monthly_paid) AS total_paid,
                SUM(monthly_beneficiaries) * 1.0 / NULLIF(SUM(monthly_claims), 0) AS bene_to_claims_ratio,
                COUNT(*) AS months_flagged,
                MIN(month) AS first_month,
                MAX(month) AS last_month
            FROM high_volume
            GROUP BY 1
            HAVING SUM(monthly_beneficiaries) * 1.0 / NULLIF(SUM(monthly_claims), 0) < 0.1
        )
        SELECT * FROM flagged
        ORDER BY bene_to_claims_ratio ASC
    """).fetchall()

    columns = [
        "npi", "total_claims", "total_beneficiaries", "total_paid",
        "bene_to_claims_ratio", "months_flagged", "first_month", "last_month",
    ]

    results = []
    for row in rows:
        d = dict(zip(columns, row))
        for k in ("first_month", "last_month"):
            if d[k] is not None:
                d[k] = str(d[k])
        d["total_claims"] = int(d["total_claims"] or 0)
        d["total_beneficiaries"] = int(d["total_beneficiaries"] or 0)
        d["total_paid"] = float(d["total_paid"] or 0)
        d["bene_to_claims_ratio"] = float(d["bene_to_claims_ratio"] or 0)
        d["months_flagged"] = int(d["months_flagged"] or 0)
        results.append(d)

    print(f"    Signal 6: {len(results)} flags")
    return results


def run_all_signals(con: duckdb.DuckDBPyConnection) -> dict[str, list[dict]]:
    """Run all 6 fraud signals and return results keyed by signal name."""
    return {
        "excluded_provider_billing": signal_1_excluded_provider(con),
        "billing_volume_outlier": signal_2_billing_volume_outlier(con),
        "rapid_billing_escalation": signal_3_rapid_escalation(con),
        "workforce_impossibility": signal_4_workforce_impossibility(con),
        "shared_authorized_official": signal_5_shared_authorized_official(con),
        "geographic_implausibility": signal_6_geographic_implausibility(con),
    }


def main() -> None:
    """Run all signals and save intermediate results."""
    con = get_connection()
    register_views(con)

    print("=== Running all fraud signals ===")
    results = run_all_signals(con)

    # Save intermediate results
    intermediate_path = os.path.join(DATA_DIR, "signal_results.json")
    with open(intermediate_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"  Intermediate results saved: {intermediate_path}")

    total_flags = sum(len(v) for v in results.values())
    print(f"=== Total flags across all signals: {total_flags} ===")

    con.close()


if __name__ == "__main__":
    main()
