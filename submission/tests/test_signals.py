"""
Unit tests for all 6 fraud signals using synthetic fixtures.

Each test verifies that the corresponding signal correctly identifies
the planted fraudulent patterns in the fixture data.
"""

import os
import sys

import duckdb
import pytest

# Add submission root to path so we can import src modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


@pytest.fixture
def con():
    """Create a DuckDB connection with test fixture views registered."""
    c = duckdb.connect()

    spending_path = os.path.join(FIXTURES_DIR, "spending.parquet")
    nppes_path = os.path.join(FIXTURES_DIR, "nppes_slim.parquet")
    leie_path = os.path.join(FIXTURES_DIR, "leie.parquet")

    c.execute(f"""
        CREATE OR REPLACE VIEW spending AS
        SELECT * FROM read_parquet('{spending_path}');
    """)
    c.execute(f"""
        CREATE OR REPLACE VIEW nppes AS
        SELECT
            "npi" AS npi,
            "entity_type" AS entity_type,
            "org_name" AS org_name,
            "last_name" AS last_name,
            "first_name" AS first_name,
            "state" AS state,
            "postal_code" AS postal_code,
            "taxonomy" AS taxonomy,
            "enumeration_date" AS enumeration_date,
            "auth_off_last" AS auth_off_last,
            "auth_off_first" AS auth_off_first
        FROM read_parquet('{nppes_path}');
    """)
    c.execute(f"""
        CREATE OR REPLACE VIEW leie AS
        SELECT * FROM read_parquet('{leie_path}');
    """)

    yield c
    c.close()


class TestSignal1ExcludedProvider:
    """Signal 1: Excluded Provider Still Billing."""

    def test_npi_match_flags_excluded_provider(self, con):
        """Provider 1000000099 is in LEIE (excluded 2020-01-01), billing in 2024."""
        from src.signals import signal_1_excluded_provider

        results = signal_1_excluded_provider(con)
        flagged_npis = {r["npi"] for r in results}
        assert "1000000099" in flagged_npis, (
            "Expected excluded provider 1000000099 to be flagged"
        )

    def test_name_match_flags_excluded_provider(self, con):
        """Provider BADACTOR FRANK / NY matched via name+state (no NPI in LEIE)."""
        from src.signals import signal_1_excluded_provider

        results = signal_1_excluded_provider(con)
        flagged_npis = {r["npi"] for r in results}
        assert "1000000098" in flagged_npis, (
            "Expected name-matched excluded provider 1000000098 to be flagged"
        )

    def test_excluded_provider_has_positive_paid(self, con):
        """Flagged excluded providers should have positive total_paid_after_exclusion."""
        from src.signals import signal_1_excluded_provider

        results = signal_1_excluded_provider(con)
        for r in results:
            if r["npi"] == "1000000099":
                assert r["total_paid_after_exclusion"] > 0

    def test_non_excluded_not_flagged(self, con):
        """Normal provider 1000000001 should NOT be flagged."""
        from src.signals import signal_1_excluded_provider

        results = signal_1_excluded_provider(con)
        flagged_npis = {r["npi"] for r in results}
        assert "1000000001" not in flagged_npis


class TestSignal2BillingOutlier:
    """Signal 2: Billing Outlier (p99 by taxonomy+state)."""

    def test_outlier_flagged(self, con):
        """Provider 1000000050 bills 100x peers — should be above p99."""
        from src.signals import signal_2_billing_volume_outlier as signal_2_billing_outlier

        results = signal_2_billing_outlier(con)
        flagged_npis = {r["npi"] for r in results}
        assert "1000000050" in flagged_npis, (
            "Expected volume outlier 1000000050 to be flagged"
        )

    def test_outlier_has_high_ratio(self, con):
        """The outlier's ratio_to_median should be very high."""
        from src.signals import signal_2_billing_volume_outlier as signal_2_billing_outlier

        results = signal_2_billing_outlier(con)
        for r in results:
            if r["npi"] == "1000000050":
                assert r["ratio_to_median"] > 5, (
                    f"Expected high ratio, got {r['ratio_to_median']}"
                )

    def test_normal_providers_not_flagged(self, con):
        """Normal CA providers should not be flagged."""
        from src.signals import signal_2_billing_volume_outlier as signal_2_billing_outlier

        results = signal_2_billing_outlier(con)
        flagged_npis = {r["npi"] for r in results}
        assert "1000000001" not in flagged_npis


class TestSignal3RapidEscalation:
    """Signal 3: Rapid Billing Escalation (new entities)."""

    def test_new_fast_grower_flagged(self, con):
        """Provider 1000000060 enumerated 09/2024, explosive growth → flagged."""
        from src.signals import signal_3_rapid_escalation

        results = signal_3_rapid_escalation(con)
        flagged_npis = {r["npi"] for r in results}
        assert "1000000060" in flagged_npis, (
            "Expected rapidly escalating provider 1000000060 to be flagged"
        )

    def test_growth_exceeds_200_pct(self, con):
        """Flagged provider should have max_growth_pct > 200."""
        from src.signals import signal_3_rapid_escalation

        results = signal_3_rapid_escalation(con)
        for r in results:
            if r["npi"] == "1000000060":
                assert r["max_growth_pct"] > 200, (
                    f"Expected growth > 200%, got {r['max_growth_pct']}%"
                )


class TestSignal4WorkforceImpossibility:
    """Signal 4: Workforce Impossibility."""

    def test_high_volume_org_flagged(self, con):
        """Org 1000000070 with 2000 claims/month → ~11.4 claims/hour → flagged."""
        from src.signals import signal_4_workforce_impossibility

        results = signal_4_workforce_impossibility(con)
        flagged_npis = {r["npi"] for r in results}
        assert "1000000070" in flagged_npis, (
            "Expected workforce-impossible org 1000000070 to be flagged"
        )

    def test_implied_claims_per_hour_above_6(self, con):
        """The flagged org should have > 6 implied claims per hour."""
        from src.signals import signal_4_workforce_impossibility

        results = signal_4_workforce_impossibility(con)
        for r in results:
            if r["npi"] == "1000000070":
                assert r["implied_claims_per_hour"] > 6, (
                    f"Expected > 6 claims/hour, got {r['implied_claims_per_hour']}"
                )


class TestSignal5SharedOfficial:
    """Signal 5: Shared Official."""

    def test_shared_official_flagged(self, con):
        """KINGPIN CARL controls 6 NPIs with >$1M combined → flagged."""
        from src.signals import signal_5_shared_authorized_official as signal_5_shared_official

        results = signal_5_shared_official(con)
        assert len(results) > 0, "Expected at least one shared official flag"
        # Find the KINGPIN entry
        kingpin = [r for r in results if r["off_last"] == "KINGPIN"]
        assert len(kingpin) > 0, "Expected KINGPIN CARL to be flagged"
        assert kingpin[0]["npi_count"] >= 5
        assert kingpin[0]["combined_total_paid"] > 1_000_000

    def test_non_shared_official_not_flagged(self, con):
        """Officials controlling < 5 NPIs should not be flagged."""
        from src.signals import signal_5_shared_authorized_official as signal_5_shared_official

        results = signal_5_shared_official(con)
        for r in results:
            assert r["npi_count"] >= 5


class TestSignal6GeographicImplausibility:
    """Signal 6: Geographic Implausibility (home health)."""

    def test_low_bene_ratio_flagged(self, con):
        """Provider 1000000090 with 500 claims, 5 beneficiaries → 0.01 ratio → flagged."""
        from src.signals import signal_6_geographic_implausibility

        results = signal_6_geographic_implausibility(con)
        flagged_npis = {r["npi"] for r in results}
        assert "1000000090" in flagged_npis, (
            "Expected geographically implausible provider 1000000090 to be flagged"
        )

    def test_ratio_below_threshold(self, con):
        """Flagged provider should have bene_to_claims_ratio < 0.1."""
        from src.signals import signal_6_geographic_implausibility

        results = signal_6_geographic_implausibility(con)
        for r in results:
            if r["npi"] == "1000000090":
                assert r["bene_to_claims_ratio"] < 0.1, (
                    f"Expected ratio < 0.1, got {r['bene_to_claims_ratio']}"
                )


class TestOutputReport:
    """Test the report generation logic."""

    def test_build_report_structure(self, con):
        """Report should have top-level keys with correct structure."""
        from src.signals import run_all_signals
        from src.output import build_report

        signal_results = run_all_signals(con)
        report = build_report(con, signal_results)

        # Top-level keys
        assert "total_providers_flagged" in report
        assert "total_providers_scanned" in report
        assert "tool_version" in report
        assert "generated_at" in report
        assert "flagged_providers" in report
        assert report["total_providers_flagged"] > 0

    def test_provider_schema(self, con):
        """Each flagged provider should have the updated field schema."""
        from src.signals import run_all_signals
        from src.output import build_report

        signal_results = run_all_signals(con)
        report = build_report(con, signal_results)

        for provider in report["flagged_providers"]:
            assert "npi" in provider
            assert "provider_name" in provider
            assert isinstance(provider["provider_name"], str)

            # entity_type should be "individual", "organization", or "unknown"
            assert provider["entity_type"] in ("individual", "organization", "unknown"), (
                f"Expected entity_type 'individual' or 'organization', got '{provider['entity_type']}'"
            )

            assert "taxonomy_code" in provider
            assert "enumeration_date" in provider
            # enumeration_date should be YYYY-MM-DD format or None
            if provider["enumeration_date"] is not None:
                assert len(provider["enumeration_date"]) == 10, (
                    f"Expected YYYY-MM-DD format, got '{provider['enumeration_date']}'"
                )

            assert "signals" in provider
            assert "estimated_overpayment_usd" in provider

            # Provider should have fca_relevance
            assert "fca_relevance" in provider
            fca = provider["fca_relevance"]
            assert "claim_type" in fca
            assert "statute_reference" in fca
            assert "suggested_next_steps" in fca

    def test_severity_assignment(self, con):
        """Excluded providers should have critical severity."""
        from src.signals import run_all_signals
        from src.output import build_report

        signal_results = run_all_signals(con)
        report = build_report(con, signal_results)

        # Find provider 1000000099 (excluded)
        excluded = [p for p in report["flagged_providers"] if p["npi"] == "1000000099"]
        assert len(excluded) == 1
        severities = [s["severity"] for s in excluded[0]["signals"]]
        assert "critical" in severities

    def test_all_signals_produce_results(self, con):
        """Each of the 6 signals should produce at least 1 flag."""
        from src.signals import run_all_signals

        signal_results = run_all_signals(con)
        for signal_name, flags in signal_results.items():
            assert len(flags) > 0, f"Signal {signal_name} produced no flags"

    def test_signal2_overpayment_uses_p99(self, con):
        """Signal 2 overpayment should be (total - p99), not (total - median)."""
        from src.signals import run_all_signals
        from src.output import build_report

        signal_results = run_all_signals(con)
        report = build_report(con, signal_results)

        outlier = [p for p in report["flagged_providers"] if p["npi"] == "1000000050"]
        assert len(outlier) == 1
        # Overpayment should be positive (total_paid - p99_paid)
        assert outlier[0]["estimated_overpayment_usd"] > 0

    def test_signal5_overpayment_is_zero(self, con):
        """Signal 5 (shared official) overpayment should be 0."""
        from src.output import estimate_overpayment

        # Signal 5 overpayment should always be 0
        flag = {"combined_total_paid": 5_000_000, "npi_list": ["1", "2"]}
        assert estimate_overpayment("shared_official", flag) == 0.0

    def test_signal6_overpayment_is_zero(self, con):
        """Signal 6 (geographic implausibility) overpayment should be 0."""
        from src.output import estimate_overpayment

        # Signal 6 overpayment should always be 0
        flag = {"total_paid": 300_000, "bene_to_claims_ratio": 0.05}
        assert estimate_overpayment("geographic_implausibility", flag) == 0.0
