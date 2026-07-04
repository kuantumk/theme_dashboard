import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import run_daily_workflow
import src.stock_utils as su


class ConsolidateScreenerResultsTests(unittest.TestCase):
    """`consolidate_screener_results` derives the screened union from the
    per-screener parquet outputs (the `.txt` files were removed) and writes the
    latest union to `data/screened_union.json` for the tag-audit routine."""

    def _write_screener(self, root: Path, screener: str, date_str: str, tickers) -> None:
        screener_dir = root / screener
        screener_dir.mkdir(parents=True, exist_ok=True)
        su.save_df_to_parquet(
            pd.DataFrame({"ticker": tickers}),
            screener_dir / f"{screener}_{date_str}.parquet",
        )

    def test_unions_tickers_across_screener_parquet(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            data_dir = root / "data"
            self._write_screener(root, "topdog", "2026-05-01", ["AAA", "BBB"])
            self._write_screener(root, "steady_trend", "2026-05-01", ["BBB", "CCC"])
            self._write_screener(root, "parabolic", "2026-05-01", [])  # 0-match

            with patch.object(run_daily_workflow, "SCREENING_OUTPUT_DIR", root), \
                 patch.object(run_daily_workflow, "DATA_DIR", data_dir), \
                 patch.dict(
                     run_daily_workflow.CONFIG,
                     {"screeners": ["topdog", "steady_trend", "parabolic", "htf"]},
                 ):
                tickers = run_daily_workflow.consolidate_screener_results("2026-05-01")

            self.assertEqual(tickers, {"AAA", "BBB", "CCC"})

            union_file = data_dir / "screened_union.json"
            self.assertTrue(union_file.exists())
            payload = json.loads(union_file.read_text(encoding="utf-8"))
            self.assertEqual(payload["date"], "2026-05-01")
            self.assertEqual(payload["tickers"], ["AAA", "BBB", "CCC"])

    def test_all_empty_returns_empty_set(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            data_dir = root / "data"
            self._write_screener(root, "parabolic", "2026-05-01", [])
            self._write_screener(root, "htf", "2026-05-01", [])

            with patch.object(run_daily_workflow, "SCREENING_OUTPUT_DIR", root), \
                 patch.object(run_daily_workflow, "DATA_DIR", data_dir), \
                 patch.dict(
                     run_daily_workflow.CONFIG,
                     {"screeners": ["parabolic", "htf"]},
                 ):
                tickers = run_daily_workflow.consolidate_screener_results("2026-05-01")

            self.assertEqual(tickers, set())
            payload = json.loads((data_dir / "screened_union.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["tickers"], [])


if __name__ == "__main__":
    unittest.main()
