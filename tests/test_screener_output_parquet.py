"""U3: per-screener numeric outputs are parquet; ticker lists stay .txt.

run_screener writes two kinds of output per screener: a NUMERIC frame (now
parquet, read by export_dashboard_data's per-scan builders) and TICKER LISTS
(.txt, read by the consolidation step and the audit tooling). This test pins
the format contract that U3 establishes without invoking the argparse CLI.
"""
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

import src.stock_utils as su


class ScreenerOutputFormatTests(unittest.TestCase):
    def test_numeric_output_parquet_roundtrips_for_export_reader(self) -> None:
        # The shape a screener writes: filtered master rows with metrics.
        df = pd.DataFrame(
            {
                "date": ["2026-07-01", "2026-07-01"],
                "ticker": ["NVDA", "AVGO"],
                "vars": [8.0, 6.5],
                "vars_20ema": [7.1, 5.9],
                "rs_sts_pct": [99.0, 94.0],
            }
        )
        with TemporaryDirectory() as tmp:
            scan_dir = Path(tmp) / "vars"
            path = scan_dir / "vars_2026-07-01.parquet"
            su.save_df_to_parquet(df, path)

            # export_dashboard_data's per-scan read pattern: load + fillna(0).
            loaded = su.load_df_from_parquet(path).fillna(0)
            pd.testing.assert_frame_equal(loaded, df)
            self.assertEqual(sorted(loaded["ticker"]), ["AVGO", "NVDA"])


if __name__ == "__main__":
    unittest.main()
