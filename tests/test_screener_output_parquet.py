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

    def test_ticker_list_stays_txt_and_reads_as_text(self) -> None:
        # run_screener writes ticker lists with pd.DataFrame(tickers).to_csv(.txt);
        # the consolidation reads them via pd.read_csv(header=None). That path is
        # unchanged by U3 and must keep working alongside the parquet numeric file.
        tickers = pd.Series(["NVDA", "AVGO", "SMCI"])
        with TemporaryDirectory() as tmp:
            scan_dir = Path(tmp)
            txt_path = scan_dir / "_vars_07012026.txt"
            pd.DataFrame(tickers).to_csv(txt_path, index=False, header=False)

            # Consolidation read pattern.
            read_back = set(pd.read_csv(txt_path, header=None)[0].tolist())
            self.assertEqual(read_back, {"NVDA", "AVGO", "SMCI"})
            # It is genuinely text, not parquet.
            self.assertTrue(txt_path.read_text(encoding="utf-8").startswith("NVDA"))

    def test_numeric_and_txt_coexist_in_a_scan_dir(self) -> None:
        # A scan dir holds both the parquet numeric file and the .txt list; a
        # parquet glob picks up only the numeric file.
        df = pd.DataFrame({"ticker": ["NVDA"], "vars": [8.0]})
        with TemporaryDirectory() as tmp:
            scan_dir = Path(tmp) / "volspike"
            su.save_df_to_parquet(df, scan_dir / "volspike_2026-07-01.parquet")
            pd.DataFrame(pd.Series(["NVDA"])).to_csv(
                scan_dir / "volspike_07012026.txt", index=False, header=False
            )
            parquet_files = sorted(scan_dir.glob("volspike_*.parquet"))
            self.assertEqual([p.name for p in parquet_files], ["volspike_2026-07-01.parquet"])


if __name__ == "__main__":
    unittest.main()
