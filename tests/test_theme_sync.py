import unittest
from datetime import datetime

from src.themes.tag_new_tickers import (
    apply_google_sheet_ground_truth,
    apply_validation_decisions,
    select_validation_tickers,
    themes_match,
)


class ThemeSyncTests(unittest.TestCase):
    def test_google_sheet_ground_truth_updates_upstream_theme(self) -> None:
        existing = {"VSTS": ["Aerospace & Defense / Components"]}
        google_sheet = {"VSTS": ["Business Services / Uniform Rental & Workplace Supplies"]}

        updated, ground_truth_tickers, updates = apply_google_sheet_ground_truth(
            ticker_themes=existing,
            tickers={"VSTS"},
            google_sheet_themes=google_sheet,
        )

        self.assertEqual(
            updated["VSTS"],
            ["Business Services / Uniform Rental & Workplace Supplies"],
        )
        self.assertEqual(ground_truth_tickers, {"VSTS"})
        self.assertEqual(
            updates,
            [
                {
                    "ticker": "VSTS",
                    "previous": ["Aerospace & Defense / Components"],
                    "updated": ["Business Services / Uniform Rental & Workplace Supplies"],
                }
            ],
        )

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

    def test_theme_match_ignores_array_order(self) -> None:
        self.assertTrue(
            themes_match(
                ["AI - Infra / Optics", "AI - Infra / Power/Cooling"],
                ["AI - Infra / Power/Cooling", "AI - Infra / Optics"],
            )
        )


if __name__ == "__main__":
    unittest.main()
