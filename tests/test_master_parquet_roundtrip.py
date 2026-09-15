"""U2: the master table file-naming + read path works in parquet.

Exercises the pattern the pipeline uses — `su.get_latest_file('master_*.parquet')`
then `su.load_df_from_parquet(...)` (run_screener.load_master_table, the workflow
date lookup, the report/analyze __main__ readers, and export's per-day globs all
follow it) — without the heavy create_master_table pickle input.
"""
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

import src.stock_utils as su


def _master_frame(date_str: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": [date_str, date_str],
            "ticker": ["AAPL", "NVDA"],
            "rs_sts_pct": [91.5, 88.2],
            "adr_pct": [3.1, 4.4],
            "vars": [2.1, 3.3],
            "tightness": [0.19, 0.62],
            "tight_base": [True, False],
        }
    )


class MasterParquetRoundTripTests(unittest.TestCase):
    def test_get_latest_file_picks_newest_master_parquet(self) -> None:
        with TemporaryDirectory() as tmp:
            master_dir = Path(tmp) / "master"
            master_dir.mkdir()
            for d in ["2026-06-30", "2026-07-01", "2026-07-02"]:
                su.save_df_to_parquet(_master_frame(d), master_dir / f"master_{d}.parquet")
            # file_index=1 -> most recent (matches load_master_table's default)
            latest = su.get_latest_file(master_dir, "master_*.parquet", 1)
            self.assertEqual(latest.name, "master_2026-07-02.parquet")
            # file_index=2 -> one session back (the offset_days path)
            prior = su.get_latest_file(master_dir, "master_*.parquet", 2)
            self.assertEqual(prior.name, "master_2026-07-01.parquet")

    def test_written_master_reads_back_for_screening(self) -> None:
        with TemporaryDirectory() as tmp:
            master_dir = Path(tmp) / "master"
            path = master_dir / "master_2026-07-02.parquet"
            su.save_df_to_parquet(_master_frame("2026-07-02"), path)
            # The load_master_table read pattern: load + fillna(0), then filter.
            master_df = su.load_df_from_parquet(path).fillna(0)
            row = master_df[master_df["ticker"] == "NVDA"]
            self.assertEqual(len(row), 1)
            self.assertAlmostEqual(float(row["vars"].iloc[0]), 3.3)
            # date column survives as a string (drives the export's date logic)
            self.assertEqual(str(master_df["date"].iloc[0]), "2026-07-02")

    def test_boolean_column_survives_as_bool(self) -> None:
        # `tight_base` is consumed as a truth value by the radar. An object or
        # string round-trip would make every row truthy, flagging the whole
        # universe as coiled with nothing on screen to show it went wrong.
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "master" / "master_2026-07-02.parquet"
            su.save_df_to_parquet(_master_frame("2026-07-02"), path)
            df = su.load_df_from_parquet(path)
            self.assertEqual(df["tight_base"].dtype, bool)
            self.assertTrue(bool(df.loc[df["ticker"] == "AAPL", "tight_base"].iloc[0]))
            self.assertFalse(bool(df.loc[df["ticker"] == "NVDA", "tight_base"].iloc[0]))

    def test_missing_tightness_does_not_read_as_tightest_after_fillna(self) -> None:
        # The radar path deliberately skips .fillna(0) because tightness is
        # inverted — lower is tighter, so a zero-filled absence is the
        # strongest possible reading. This pins the hazard that makes that
        # choice load-bearing, for whoever carries the metric to a tab whose
        # snapshot builder does fill.
        frame = _master_frame("2026-07-02")
        frame.loc[frame["ticker"] == "NVDA", "tightness"] = float("nan")
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "master" / "master_2026-07-02.parquet"
            su.save_df_to_parquet(frame, path)
            unfilled = su.load_df_from_parquet(path)
            self.assertTrue(
                pd.isna(unfilled.loc[unfilled["ticker"] == "NVDA", "tightness"].iloc[0]))
            filled = su.load_df_from_parquet(path).fillna(0)
            self.assertEqual(
                float(filled.loc[filled["ticker"] == "NVDA", "tightness"].iloc[0]), 0.0)

    def test_stem_date_extraction_matches_pipeline(self) -> None:
        # Readers derive the session date via stem.replace('master_', '').
        path = Path("master_2026-07-02.parquet")
        self.assertEqual(path.stem.replace("master_", ""), "2026-07-02")


if __name__ == "__main__":
    unittest.main()
