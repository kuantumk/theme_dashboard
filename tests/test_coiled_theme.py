import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from src.reporting import export_dashboard_data
from src.screening.coiled_theme import add_coiled_theme_metrics
from src.screening.screeners.coiled_theme import filter_master_table


def rgti_like_row(**overrides):
    row = {
        "date": "2025-09-08",
        "ticker": "RGTI",
        "open": 15.05,
        "high": 15.49,
        "low": 14.80,
        "close": 15.15,
        "volume": 25_000_000,
        "vol_sma50": 26_000_000,
        "avg_dollar_vol": 650_000_000,
        "adr_pct": 0.077,
        "sma50": 14.96,
        "max252": 58.15,
        "min252": 1.79,
        "perf_1mo": -0.033,
        "perf_6mo": 0.62,
        "perf_12mo": 3.0,
        "rs_sts_pct": 30.8,
        "vars": -7.21,
        "inside_day": True,
        "tight_day": True,
        "close_to_ma": True,
        "range_pct": 0.046,
        "range10_pct": 0.208,
        "range20_pct": 0.300,
        "range_contraction_10_20": 0.69,
        "vol_dry_10_50": 0.95,
        "dist_sma50_pct": 0.013,
        "close_vs_252h": 0.26,
        "nr7": False,
        "nr20": False,
    }
    row.update(overrides)
    return row


class CoiledThemeTests(unittest.TestCase):
    def test_rgti_like_low_rs_negative_vars_setup_qualifies(self) -> None:
        df = pd.DataFrame([rgti_like_row()])

        out = add_coiled_theme_metrics(df)

        self.assertTrue(bool(out.loc[0, "coiled_is_candidate"]))
        self.assertGreaterEqual(out.loc[0, "coiled_theme_score"], 70)
        self.assertIn("blind=1m/RS/VARS", out.loc[0, "coiled_flags"])

    def test_extended_name_is_rejected_even_with_high_setup_score(self) -> None:
        df = pd.DataFrame([
            rgti_like_row(dist_sma50_pct=0.55, close_to_ma=False, close=24.00)
        ])

        out = add_coiled_theme_metrics(df)

        self.assertFalse(bool(out.loc[0, "coiled_is_candidate"]))

    def test_screener_mutates_master_with_score_columns(self) -> None:
        master_df = pd.DataFrame([rgti_like_row()])

        mask = filter_master_table(master_df)

        self.assertTrue(bool(mask.iloc[0]))
        self.assertIn("coiled_theme_score", master_df.columns)
        self.assertIn("coiled_flags", master_df.columns)

    def test_export_groups_coiled_candidates_by_theme_with_reasons(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            screening_dir = root / "screening"
            coiled_dir = screening_dir / "coiled_theme"
            consolidated_dir = screening_dir / "consolidated"
            output_dir = root / "docs_data"
            coiled_dir.mkdir(parents=True)
            consolidated_dir.mkdir(parents=True)
            output_dir.mkdir()

            row = rgti_like_row(coiled_theme_score=87.0, coiled_flags="inside; tight; blind=1m/RS/VARS")
            pd.DataFrame([row]).to_csv(coiled_dir / "coiled_theme_2025-09-08.csv", index=False)
            (consolidated_dir / "_darvas_09082025.txt").write_text("RGTI\nIONQ\n", encoding="utf-8")

            with (
                patch.object(export_dashboard_data, "SCREENING_OUTPUT_DIR", screening_dir),
                patch.object(export_dashboard_data, "OUTPUT_DIR", output_dir),
                patch.object(export_dashboard_data, "FUNDAMENTALS_DB", root / "missing.db"),
                patch(
                    "src.themes.theme_registry.load_ticker_themes",
                    return_value={"RGTI": ["Quantum Computing"], "IONQ": ["Quantum Computing"]},
                ),
            ):
                current = export_dashboard_data.export_coiled_theme(day_flags={})

            self.assertEqual(current["report_date"], "2025-09-08")
            self.assertEqual(current["themes"][0]["name"], "Quantum Computing")
            self.assertEqual(current["themes"][0]["other_screened_count"], 2)
            self.assertEqual(current["themes"][0]["tickers"][0]["ticker"], "RGTI")
            self.assertIn("blind=1m/RS/VARS", current["themes"][0]["tickers"][0]["flags"])
            self.assertTrue((output_dir / "coiled_theme.json").exists())
            self.assertTrue((output_dir / "coiled_theme_history.json").exists())


if __name__ == "__main__":
    unittest.main()
