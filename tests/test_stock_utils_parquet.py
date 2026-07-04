"""Round-trip tests for the parquet I/O helpers in src/stock_utils.py.

Screening numeric outputs (master tables, per-screener results) move from CSV
to parquet; these helpers are the single I/O path. The migration invariant is
that a frame as ``pd.read_csv`` would produce it survives a parquet round-trip
unchanged — on pandas 3.x both give the default ``str`` dtype for text columns,
so the swap is transparent to every downstream consumer.
"""
import io
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd

import src.stock_utils as su


class ParquetHelperTests(unittest.TestCase):
    def test_read_csv_shaped_frame_roundtrips_unchanged(self) -> None:
        # The exact shape the pipeline reads today: string ticker/date, float
        # metrics (with a NaN), integer counters — as read_csv would type them.
        csv = io.StringIO(
            "ticker,date,rs_sts_pct,days_since_hv,vars\n"
            "AAPL,2026-07-01,91.5,8,2.1\n"
            "NVDA,2026-07-01,88.2,403,\n"
            "TSLA,2026-07-01,99.9,1,4.4\n"
        )
        df = pd.read_csv(csv)
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "master_2026-07-01.parquet"
            su.save_df_to_parquet(df, path)
            loaded = su.load_df_from_parquet(path)

        # Same values AND same dtypes as the read_csv frame — a transparent swap.
        pd.testing.assert_frame_equal(loaded, df)

    def test_constructed_frame_roundtrips_values_and_dtypes(self) -> None:
        df = pd.DataFrame(
            {
                "ticker": ["AAPL", "NVDA", "TSLA"],
                "rs_sts_pct": [91.5, 88.2, 99.9],
                "days_since_hv": [8, 403, 1],
                "vars": [2.1, np.nan, 4.4],
            }
        )
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "screener_2026-07-01.parquet"
            su.save_df_to_parquet(df, path)
            loaded = su.load_df_from_parquet(path)
        pd.testing.assert_frame_equal(loaded, df)
        self.assertEqual(loaded["days_since_hv"].dtype, df["days_since_hv"].dtype)
        self.assertEqual(loaded["rs_sts_pct"].dtype, np.dtype("float64"))

    def test_empty_dataframe_roundtrips(self) -> None:
        df = pd.DataFrame({"ticker": pd.Series([], dtype="str"), "vars": pd.Series([], dtype="float64")})
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "empty.parquet"
            su.save_df_to_parquet(df, path)
            loaded = su.load_df_from_parquet(path)
        self.assertEqual(len(loaded), 0)
        self.assertEqual(list(loaded.columns), ["ticker", "vars"])

    def test_save_creates_missing_parent_dirs(self) -> None:
        df = pd.DataFrame({"ticker": ["AAPL"], "vars": [2.1]})
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "nested" / "deeper" / "out.parquet"
            su.save_df_to_parquet(df, path)
            self.assertTrue(path.exists())

    def test_index_not_written(self) -> None:
        # A non-default index must not leak into the parquet as a column.
        df = pd.DataFrame({"ticker": ["AAPL", "NVDA"], "vars": [2.1, 3.3]}, index=[10, 20])
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "noindex.parquet"
            su.save_df_to_parquet(df, path)
            loaded = su.load_df_from_parquet(path)
        self.assertEqual(list(loaded.columns), ["ticker", "vars"])
        self.assertNotIn("__index_level_0__", loaded.columns)
        self.assertEqual(list(loaded.index), [0, 1])


if __name__ == "__main__":
    unittest.main()
