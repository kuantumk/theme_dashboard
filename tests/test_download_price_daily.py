"""Step 1 retries a failed batch once and refuses a thinned universe.

A failed 200-ticker batch used to be skipped, so a partial yfinance outage
wrote a price file missing whole slices of the universe and every tab
published it. The coverage floor is opt-in (`min_coverage`): market_history
calls `download_yf_price_data(start, end)` and retries rate-limited tickers
itself afterwards, so the floor must not fire there.
"""

import contextlib
import io
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import src.data_collection.download_price_daily as dl
import src.stock_utils as su

START, END = date(2026, 9, 1), date(2026, 10, 24)


def bars(index):
    return pd.DataFrame({"Open": 10.0, "High": 11.0, "Low": 9.0, "Close": 10.5, "Volume": 1e6},
                        index=index)


class FakeYahoo:
    def __init__(self, fail_batches=0, missing=()):
        self.fail_batches = fail_batches
        self.missing = set(missing)
        self.batch_calls = 0

    def __call__(self, tickers, start=None, end=None, **_):
        index = pd.bdate_range(start, pd.Timestamp(end) - pd.Timedelta(days=1))
        if isinstance(tickers, str):
            return bars(index)
        self.batch_calls += 1
        if self.fail_batches:
            self.fail_batches -= 1
            raise RuntimeError("connection reset")
        served = {t: bars(index) for t in tickers if t not in self.missing}
        if not served:
            return pd.DataFrame()
        return pd.concat(served, axis=1, names=["Ticker", "Price"])


class DownloadTests(unittest.TestCase):
    def run_download(self, yahoo, universe, **kwargs):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        out = Path(tmp.name) / "price_daily.pkl"
        with patch.object(dl.yf, "download", yahoo), \
             patch.object(dl.time, "sleep"), \
             patch.object(dl, "TODAY", END), \
             patch.object(dl, "PRICE_DATA_FILE", out), \
             patch.object(dl.su, "get_tickers_from_nasdaq", lambda: (list(universe), {})), \
             contextlib.redirect_stdout(io.StringIO()):
            dl.download_yf_price_data(START, END, **kwargs)
        return su.load_object_from_pickle(out) if out.exists() else None

    def test_a_failed_batch_is_retried_once(self):
        yahoo = FakeYahoo(fail_batches=1)
        prices = self.run_download(yahoo, ["AAA", "BBB"])
        self.assertTrue({"AAA", "BBB", "SPY", "^GSPC"} <= set(prices))
        self.assertEqual(yahoo.batch_calls, 2)

    def test_a_batch_that_fails_twice_is_skipped(self):
        yahoo = FakeYahoo(fail_batches=2)
        prices = self.run_download(yahoo, ["AAA", "BBB"])
        self.assertNotIn("AAA", prices)
        self.assertEqual(yahoo.batch_calls, 2)

    def test_missing_tickers_in_a_served_batch_are_not_retried(self):
        # market_history retries rate-limited names itself; one request here.
        yahoo = FakeYahoo(missing={"BBB"})
        prices = self.run_download(yahoo, ["AAA", "BBB"])
        self.assertNotIn("BBB", prices)
        self.assertEqual(yahoo.batch_calls, 1)

    def test_coverage_below_the_floor_raises_and_writes_nothing(self):
        universe = [f"T{i}" for i in range(10)]
        yahoo = FakeYahoo(missing={"T0", "T1"})
        with self.assertRaises(dl.DownloadCoverageError):
            self.run_download(yahoo, universe, min_coverage=0.9)

    def test_coverage_at_the_floor_passes(self):
        universe = [f"T{i}" for i in range(10)]
        prices = self.run_download(FakeYahoo(missing={"T0"}), universe, min_coverage=0.9)
        self.assertEqual(len([t for t in prices if t.startswith("T")]), 9)

    def test_no_floor_without_min_coverage(self):
        universe = [f"T{i}" for i in range(10)]
        prices = self.run_download(FakeYahoo(missing=set(universe[:8])), universe)
        self.assertEqual(sorted(t for t in prices if t.startswith("T")), ["T8", "T9"])


if __name__ == "__main__":
    unittest.main()
