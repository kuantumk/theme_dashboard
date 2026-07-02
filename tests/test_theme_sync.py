import json
import unittest
from datetime import datetime
from tempfile import TemporaryDirectory
from pathlib import Path
from unittest.mock import patch

import src.themes.tag_new_tickers as tagger
from src.themes.tag_new_tickers import (
    CLASSIFICATION_BATCH_SIZE,
    GeminiJSONError,
    ThemeSyncResult,
    apply_google_sheet_ground_truth,
    apply_validation_decisions,
    classify_tickers_with_retries,
    filter_sector_inconsistent_themes,
    prune_theme_review_state,
    select_validation_tickers,
    themes_match,
    write_sync_audit,
)
from src.themes.theme_registry import filter_untagged, is_untagged


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


class ThemeClassificationBatchTests(unittest.TestCase):
    def test_config_uses_smaller_llm_batch_size(self) -> None:
        self.assertEqual(CLASSIFICATION_BATCH_SIZE, 20)

    def test_failed_batch_splits_and_retries_halves(self) -> None:
        calls = []

        def fake_classify(tickers, existing_themes, profiles):
            del existing_themes, profiles
            calls.append(tuple(tickers))
            if len(tickers) > 2:
                raise GeminiJSONError(
                    "Gemini returned invalid JSON",
                    finish_reason="MAX_TOKENS",
                    response_chars=1175,
                    preview="truncated",
                )
            return {ticker: ["Singleton"] for ticker in tickers}

        with patch.object(tagger, "classify_tickers_with_gemini", side_effect=fake_classify):
            tags, failures = classify_tickers_with_retries(
                ["AIM", "AMH", "AMRX", "AMSS"],
                [],
                {},
                batch_num=1,
                total_batches=1,
            )

        self.assertEqual(
            calls,
            [
                ("AIM", "AMH", "AMRX", "AMSS"),
                ("AIM", "AMH"),
                ("AMRX", "AMSS"),
            ],
        )
        self.assertEqual(
            tags,
            {
                "AIM": ["Singleton"],
                "AMH": ["Singleton"],
                "AMRX": ["Singleton"],
                "AMSS": ["Singleton"],
            },
        )
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0]["tickers"], ["AIM", "AMH", "AMRX", "AMSS"])
        self.assertTrue(failures[0]["retried"])
        self.assertFalse(failures[0]["terminal"])
        self.assertEqual(failures[0]["finish_reason"], "MAX_TOKENS")

    def test_terminal_batch_failure_is_recorded(self) -> None:
        def fake_classify(tickers, existing_themes, profiles):
            del tickers, existing_themes, profiles
            raise GeminiJSONError(
                "Gemini returned invalid JSON",
                finish_reason="MAX_TOKENS",
                response_chars=1175,
                preview="truncated",
            )

        with patch.object(tagger, "classify_tickers_with_gemini", side_effect=fake_classify):
            tags, failures = classify_tickers_with_retries(
                ["AIM"],
                [],
                {},
                batch_num=2,
                total_batches=3,
            )

        self.assertEqual(tags, {})
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0]["batch_num"], 2)
        self.assertEqual(failures[0]["total_batches"], 3)
        self.assertEqual(failures[0]["tickers"], ["AIM"])
        self.assertFalse(failures[0]["retried"])
        self.assertTrue(failures[0]["terminal"])

    def test_sync_audit_includes_untagged_and_profile_candidates(self) -> None:
        result = ThemeSyncResult(
            ticker_themes={},
            google_sheet_tickers=[],
            google_sheet_updates=[],
            profile_candidates=["AIM", "SGL"],
            untagged_tickers=["AIM"],
        )

        with TemporaryDirectory() as tmp_dir, patch.object(tagger, "LOG_DIR", Path(tmp_dir)):
            audit_path = write_sync_audit(result, screened_ticker_count=2)
            payload = json.loads(Path(audit_path).read_text(encoding="utf-8"))

        self.assertEqual(payload["untagged_tickers"], ["AIM"])
        self.assertEqual(payload["profile_candidates"], ["AIM", "SGL"])
        self.assertEqual(payload["screened_ticker_count"], 2)


class UntaggedPredicateTests(unittest.TestCase):
    """The shared untagged definition (theme_registry.is_untagged) drives both
    the daily report's 'awaiting audit' list and the audit script's [UNTAGGED]
    check — Singleton-only is a deliberate classification and never counts."""

    def test_missing_empty_and_uncategorized_only_are_untagged(self) -> None:
        self.assertTrue(is_untagged(None))
        self.assertTrue(is_untagged([]))
        self.assertTrue(is_untagged(["Uncategorized"]))

    def test_singleton_only_is_not_untagged(self) -> None:
        self.assertFalse(is_untagged(["Singleton"]))

    def test_canonical_and_mixed_are_not_untagged(self) -> None:
        self.assertFalse(is_untagged(["Space / Launch"]))
        self.assertFalse(is_untagged(["Uncategorized", "Space / Launch"]))

    def test_filter_untagged_sorts_and_normalizes(self) -> None:
        themes = {"AAA": ["Uncategorized"], "BBB": ["Singleton"], "CCC": ["Space / Launch"]}
        self.assertEqual(filter_untagged(["ccc", "bbb", "aaa", "ddd"], themes), ["AAA", "DDD"])


class SlimmedSyncTests(unittest.TestCase):
    def test_sync_surfaces_untagged_without_classifying(self) -> None:
        store = {
            "OLD": ["AI / Data Center / Memory"],
            "UNC": ["Uncategorized"],
            "SGL": ["Singleton"],
        }
        warmed = []

        def fake_save(mapping):
            store.clear()
            store.update({k: list(v) for k, v in mapping.items()})

        with patch.object(tagger, "load_existing_themes", side_effect=lambda: {k: list(v) for k, v in store.items()}), \
             patch.object(tagger, "import_google_sheet_themes", return_value={"SHT": ["Space / Launch"]}), \
             patch.object(tagger, "ensure_company_profiles", side_effect=lambda t: warmed.extend(t) or {}), \
             patch.object(tagger, "save_ticker_themes", side_effect=fake_save), \
             patch.object(tagger, "load_ticker_themes", side_effect=lambda: {k: list(v) for k, v in store.items()}), \
             patch.object(tagger, "write_sync_audit", return_value="unused.json"):
            result = tagger.sync_screened_ticker_themes({"OLD", "UNC", "SGL", "NEW", "SHT"})

        # Untagged worklist: missing entry + Uncategorized-only; Singleton-only excluded.
        self.assertEqual(result.untagged_tickers, ["NEW", "UNC"])
        # Sheet onboarding for a brand-new ticker still works.
        self.assertEqual(store["SHT"], ["Space / Launch"])
        # Existing canonical tags untouched.
        self.assertEqual(store["OLD"], ["AI / Data Center / Memory"])
        # Profile cache warmed for the broader generic set (Singleton included).
        self.assertEqual(sorted(warmed), ["NEW", "SGL", "UNC"])
        # No classification happened: the untagged ticker was not written.
        self.assertNotIn("NEW", store)

    def test_sync_survives_sheet_failure(self) -> None:
        store = {"UNC": ["Uncategorized"]}

        with patch.object(tagger, "load_existing_themes", side_effect=lambda: {k: list(v) for k, v in store.items()}), \
             patch.object(tagger, "import_google_sheet_themes", side_effect=RuntimeError("sheet down")), \
             patch.object(tagger, "ensure_company_profiles", side_effect=lambda t: {}), \
             patch.object(tagger, "save_ticker_themes", side_effect=lambda m: None), \
             patch.object(tagger, "load_ticker_themes", side_effect=lambda: {k: list(v) for k, v in store.items()}), \
             patch.object(tagger, "write_sync_audit", return_value="unused.json"):
            result = tagger.sync_screened_ticker_themes({"UNC", "NEW"})

        self.assertEqual(result.untagged_tickers, ["NEW", "UNC"])
        self.assertEqual(result.google_sheet_tickers, [])


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
