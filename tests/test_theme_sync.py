import unittest
from datetime import datetime

from src.themes.tag_new_tickers import (
    apply_google_sheet_ground_truth,
    apply_validation_decisions,
    filter_sector_inconsistent_themes,
    prune_theme_review_state,
    select_validation_tickers,
    themes_match,
)


class ThemeSyncTests(unittest.TestCase):
    def test_google_sheet_ground_truth_freezes_locked_canonical_tags(self) -> None:
        # git_locked_themes (default true) freezes existing canonical tags: the
        # sheet never overwrites them, but the ticker is still treated as ground
        # truth so it won't fall back into Gemini classification. Re-tagging an
        # existing ticker requires the explicit `python -m src.themes.retag` CLI.
        existing = {"VSTS": ["Aerospace & Defense / Components"]}
        google_sheet = {"VSTS": ["Business Services / Uniform Rental & Workplace Supplies"]}

        updated, ground_truth_tickers, updates = apply_google_sheet_ground_truth(
            ticker_themes=existing,
            tickers={"VSTS"},
            google_sheet_themes=google_sheet,
        )

        self.assertEqual(updated["VSTS"], ["Aerospace & Defense / Components"])
        self.assertEqual(ground_truth_tickers, {"VSTS"})
        self.assertEqual(updates, [])

    def test_first_mismatch_only_creates_pending_review(self) -> None:
        result = apply_validation_decisions(
            ticker_themes={"VSTS": ["Aerospace & Defense / Components"]},
            review_state={},
            validation_tickers=["VSTS"],
            dashboard_tickers=["VSTS"],
            decisions={
                "VSTS": {
                    "action": "candidate_change",
                    "themes": ["Business Services / Uniform Rental & Workplace Supplies"],
                    "note": "uniform rental business",
                }
            },
            validation_time=datetime(2026, 3, 17, 13, 30, 0),
        )

        self.assertEqual(
            result.ticker_themes["VSTS"],
            ["Aerospace & Defense / Components"],
        )
        self.assertEqual(result.applied_retags, [])
        self.assertEqual(len(result.pending_mismatches), 1)
        self.assertEqual(
            result.review_state["VSTS"]["pending_candidate_themes"],
            ["Business Services / Uniform Rental & Workplace Supplies"],
        )
        self.assertEqual(result.review_state["VSTS"]["confirmation_count"], 1)

    def test_second_matching_mismatch_applies_retag(self) -> None:
        review_state = {
            "VSTS": {
                "pending_source_themes": ["Aerospace & Defense / Components"],
                "pending_candidate_themes": ["Business Services / Uniform Rental & Workplace Supplies"],
                "confirmation_count": 1,
                "pending_since": "2026-03-17",
            }
        }

        result = apply_validation_decisions(
            ticker_themes={"VSTS": ["Aerospace & Defense / Components"]},
            review_state=review_state,
            validation_tickers=["VSTS"],
            dashboard_tickers=["VSTS"],
            decisions={
                "VSTS": {
                    "action": "candidate_change",
                    "themes": ["Business Services / Uniform Rental & Workplace Supplies"],
                    "note": "uniform rental business",
                }
            },
            validation_time=datetime(2026, 3, 18, 13, 30, 0),
        )

        self.assertEqual(
            result.ticker_themes["VSTS"],
            ["Business Services / Uniform Rental & Workplace Supplies"],
        )
        self.assertEqual(result.pending_mismatches, [])
        self.assertEqual(len(result.applied_retags), 1)
        self.assertEqual(result.review_state["VSTS"]["pending_candidate_themes"], [])
        self.assertEqual(result.review_state["VSTS"]["confirmation_count"], 0)

    def test_select_validation_tickers_keeps_pending_off_dashboard_items(self) -> None:
        tickers = select_validation_tickers(
            dashboard_tickers=["NVDA"],
            review_state={
                "VSTS": {
                    "pending_candidate_themes": ["Business Services / Uniform Rental & Workplace Supplies"],
                    "confirmation_count": 1,
                }
            },
        )

        self.assertEqual(tickers, ["NVDA", "VSTS"])

    def test_select_validation_tickers_includes_stale_entries(self) -> None:
        tickers = select_validation_tickers(
            dashboard_tickers=[],
            review_state={
                "VSTS": {
                    "last_validated_at": "2025-01-01T00:00:00",
                }
            },
        )

        self.assertEqual(tickers, ["VSTS"])

    def test_prune_uses_supplied_reference_time(self) -> None:
        state = {
            "VSTS": {
                "last_validated_at": "2026-03-18T13:30:00",
                "last_applied_at": "2026-03-18T13:30:00",
                "last_applied_themes": ["Business Services / Uniform Rental & Workplace Supplies"],
            }
        }

        pruned = prune_theme_review_state(
            state,
            reference_time=datetime(2026, 3, 18, 13, 30, 0),
        )

        self.assertIn("VSTS", pruned)

    def test_theme_match_ignores_array_order(self) -> None:
        self.assertTrue(
            themes_match(
                ["AI - Infra / Optics", "AI - Infra / Power/Cooling"],
                ["AI - Infra / Power/Cooling", "AI - Infra / Optics"],
            )
        )


class SectorConsistencyFilterTests(unittest.TestCase):
    """Tests for filter_sector_inconsistent_themes."""

    def test_removes_healthcare_theme_from_energy_ticker(self) -> None:
        tags = {"XOM": ["Oil & Gas / E&P", "Healthcare / Medical Devices"]}
        profiles = {"XOM": {"sector": "Energy"}}
        result = filter_sector_inconsistent_themes(tags, profiles)
        self.assertEqual(result["XOM"], ["Oil & Gas / E&P"])

    def test_removes_logistics_theme_from_consumer_cyclical(self) -> None:
        tags = {"CART": ["E-commerce and Digital Retail", "Logistics / Freight Brokerage"]}
        profiles = {"CART": {"sector": "Consumer Cyclical"}}
        result = filter_sector_inconsistent_themes(tags, profiles)
        self.assertEqual(result["CART"], ["E-commerce and Digital Retail"])

    def test_keeps_first_theme_when_all_blocked(self) -> None:
        # Every theme is sector-inconsistent, but the guard never leaves a
        # ticker themeless — it keeps the first one.
        tags = {"BAD": ["Healthcare / Oncology"]}
        profiles = {"BAD": {"sector": "Energy"}}
        result = filter_sector_inconsistent_themes(tags, profiles)
        self.assertEqual(result["BAD"], ["Healthcare / Oncology"])

    def test_no_change_when_sector_consistent(self) -> None:
        tags = {"AAPL": ["AI - Software & Analytics"]}
        profiles = {"AAPL": {"sector": "Technology"}}
        result = filter_sector_inconsistent_themes(tags, profiles)
        self.assertEqual(result["AAPL"], ["AI - Software & Analytics"])

    def test_no_change_when_sector_missing(self) -> None:
        tags = {"UNKNOWN": ["Financials / Argentina"]}
        profiles = {}
        result = filter_sector_inconsistent_themes(tags, profiles)
        self.assertEqual(result["UNKNOWN"], ["Financials / Argentina"])

    def test_no_change_when_sector_not_in_blocklist(self) -> None:
        tags = {"X": ["Some Theme"]}
        profiles = {"X": {"sector": "Communication Services"}}
        result = filter_sector_inconsistent_themes(tags, profiles)
        self.assertEqual(result["X"], ["Some Theme"])


if __name__ == "__main__":
    unittest.main()
