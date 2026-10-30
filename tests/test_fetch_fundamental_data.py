"""Regression tests for Finviz fundamentals parsing.

Guards against the June 2026 breakage where Finviz split the quote snapshot
into several <table class="snapshot-table2"> blocks. The fetcher used
soup.find() (first table only), so every field except those in the first
block (Index..IPO) parsed to None and Float(M)/EPS%/Sales% went blank
dashboard-wide.
"""

import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import src.data_collection.fetch_fundamental_data as ffd


# Mimics today's Finviz layout: the snapshot is rendered as MULTIPLE
# snapshot-table2 blocks. Only Market Cap lives in the first block; the
# fundamentals we care about live in later blocks.
MULTI_TABLE_HTML = """
<html><body>
  <table class="js-snapshot-table snapshot-table2 screener_snapshot-table-body">
    <tr><td>Index</td><td>DJIA, NDX, S&amp;P 500</td><td>Market Cap</td><td>463.91B</td></tr>
    <tr><td>Income</td><td>12.23B</td><td>Sales</td><td>60.75B</td></tr>
  </table>
  <table class="snapshot-table2">
    <tr><td>P/E</td><td>38.37</td><td>Shs Float</td><td>3.93B</td></tr>
    <tr><td>Short Float</td><td>1.57%</td><td>Inst Own</td><td>81.20%</td></tr>
  </table>
  <table class="snapshot-table2">
    <tr><td>EPS this Y</td><td>99.99%</td><td>EPS Q/Q</td><td>12.22%</td></tr>
    <tr><td>Sales Q/Q</td><td>11.96%</td></tr>
    <tr><td>Inst Trans</td><td>-0.31%</td><td>Perf Week</td><td>2.10%</td></tr>
  </table>
</body></html>
"""


class _FakeResponse:
    def __init__(self, content: str):
        self.content = content.encode("utf-8")

    def raise_for_status(self):
        return None


class FinvizMultiTableParseTests(unittest.TestCase):
    def test_extracts_fields_from_non_first_snapshot_tables(self) -> None:
        with patch.object(ffd.requests, "get", return_value=_FakeResponse(MULTI_TABLE_HTML)):
            result = ffd.get_fundamental_data("TEST")

        self.assertIsNotNone(result)
        # Market Cap is in the first table; the regression is about everything else.
        self.assertEqual(result["market_cap"], 463.91e9)
        # These live in the 2nd/3rd snapshot-table2 blocks — find() missed them.
        self.assertEqual(result["shares_float"], 3.93e9)
        self.assertEqual(result["pe_ratio"], 38.37)
        self.assertEqual(result["eps_growth_qq"], 12.22)
        self.assertEqual(result["sales_growth_qq"], 11.96)
        self.assertEqual(result["short_interest"], 1.57)
        self.assertEqual(result["inst_ownership"], 81.20)
        self.assertEqual(result["inst_transactions"], -0.31)

    def test_sends_a_complete_chrome_user_agent(self) -> None:
        # Finviz answers 403 to a User-Agent with no Chrome/ token.
        with patch.object(ffd.requests, "get", return_value=_FakeResponse(MULTI_TABLE_HTML)) as get:
            ffd.get_fundamental_data("TEST")
        self.assertIn("Chrome/", get.call_args.kwargs["headers"]["User-Agent"])

    def test_returns_none_when_no_snapshot_table(self) -> None:
        with patch.object(ffd.requests, "get", return_value=_FakeResponse("<html><body>nope</body></html>")):
            result = ffd.get_fundamental_data("TEST")
        self.assertIsNone(result)


class OldCacheTests(unittest.TestCase):
    def test_a_cache_holding_eps_this_y_is_dropped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "fundamentals.db"
            conn = sqlite3.connect(db)
            conn.execute("CREATE TABLE fundamentals (ticker TEXT PRIMARY KEY, "
                         "eps_growth_yoy REAL, last_updated TIMESTAMP)")
            conn.execute("INSERT INTO fundamentals VALUES ('AAA', 94.77, ?)",
                         (datetime.now(timezone.utc),))
            conn.commit()
            conn.close()
            with patch.object(ffd, "DB_PATH", db):
                self.assertEqual(ffd.get_cached_fundamentals(["AAA"]), {})


if __name__ == "__main__":
    unittest.main()
