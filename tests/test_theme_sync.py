import json
import unittest
from tempfile import TemporaryDirectory
from pathlib import Path
from unittest.mock import patch

import src.themes.tag_new_tickers as tagger
from src.themes.tag_new_tickers import (
    ThemeSyncResult,
    apply_google_sheet_ground_truth,
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

    def test_theme_match_ignores_array_order(self) -> None:
        self.assertTrue(
            themes_match(
                ["AI - Infra / Optics", "AI - Infra / Power/Cooling"],
                ["AI - Infra / Power/Cooling", "AI - Infra / Optics"],
            )
        )


class SyncAuditTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
