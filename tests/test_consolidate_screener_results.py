import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import run_daily_workflow


class ConsolidateScreenerResultsTests(unittest.TestCase):
    def test_skips_empty_screener_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            consolidated = output_dir / "consolidated"
            consolidated.mkdir()

            (consolidated / "_parabolic_05012026.txt").write_text("")
            (consolidated / "_topdog_05012026.txt").write_text("AAA\nBBB\n")
            (consolidated / "_steady_trend_05012026.txt").write_text("BBB\nCCC\n")

            with patch.object(run_daily_workflow, "SCREENING_OUTPUT_DIR", output_dir):
                tickers = run_daily_workflow.consolidate_screener_results("2026-05-01")

            self.assertEqual(tickers, {"AAA", "BBB", "CCC"})

            union_file = consolidated / "_union_05012026.txt"
            self.assertTrue(union_file.exists())
            self.assertEqual(
                union_file.read_text().splitlines(),
                ["AAA", "BBB", "CCC"],
            )

    def test_all_empty_returns_empty_set(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            consolidated = output_dir / "consolidated"
            consolidated.mkdir()

            (consolidated / "_parabolic_05012026.txt").write_text("")
            (consolidated / "_htf_05012026.txt").write_text("")

            with patch.object(run_daily_workflow, "SCREENING_OUTPUT_DIR", output_dir):
                tickers = run_daily_workflow.consolidate_screener_results("2026-05-01")

            self.assertEqual(tickers, set())


if __name__ == "__main__":
    unittest.main()
