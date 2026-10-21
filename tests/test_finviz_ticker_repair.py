"""Ticker extraction from Finviz screener markup.

Finviz's 2026-07-15 redesign put a logo avatar inside the ticker cell. Its
one-letter fallback <span> sits before the real symbol, and finvizfinance's
flat `col.text` parse concatenates the two — "OKLO" arrives as "OOKLO". These
tests pin the fix: reading the symbol back out of the markup. The SI
collector depends on it (measured 2026-09-10: 231 of 231 rows corrupted).

The HTML fixtures below are the real cell shapes, captured live from the
screener before and after the redesign. Moved from
tests/test_ep_scan_ticker_parsing.py when the EP scans were retired.
"""

import unittest
from unittest import mock

import pandas as pd
from bs4 import BeautifulSoup

from src.data_collection.finviz_ticker_repair import (
    _ticker_from_row,
    make_ticker_repair_view,
    repair_ticker_column,
)


def _row(html: str):
    """Parse a <tr> fragment the way finvizfinance hands rows to _get_table."""
    return BeautifulSoup(html, "lxml").find("tr")


def _post_redesign_row(symbol: str, *, with_attribute: bool = True) -> str:
    """A ticker cell carrying the logo avatar (Finviz, 2026-07-15 onward)."""
    attribute = f' data-boxover-ticker="{symbol}"' if with_attribute else ""
    return f"""
    <tr class="styled-row">
      <td align="right"><a href="stock?t={symbol}">1</a></td>
      <td align="left"{attribute} data-boxover-company="Example Corp">
        <span class="flex items-center gap-1 pl-0.5">
          <a class="company-ticker" href="stock?t={symbol}">
            <img alt="{symbol} logo" src="https://logo.finviz.com/{symbol}.svg"/>
            <span>{symbol[0]}</span>
          </a>
          <a class="tab-link" href="stock?t={symbol}">{symbol}</a>
        </span>
      </td>
      <td align="left"><a href="stock?t={symbol}">Example Corp</a></td>
    </tr>
    """


def _pre_redesign_row(symbol: str) -> str:
    """A ticker cell as it looked before the logo avatar was added."""
    return f"""
    <tr class="styled-row">
      <td align="right"><a href="stock?t={symbol}">1</a></td>
      <td align="left"><a class="tab-link" href="stock?t={symbol}">{symbol}</a></td>
      <td align="left"><a href="stock?t={symbol}">Example Corp</a></td>
    </tr>
    """


class TickerFromRowTest(unittest.TestCase):
    def test_reads_symbol_from_boxover_attribute(self):
        self.assertEqual(_ticker_from_row(_row(_post_redesign_row("OKLO"))), "OKLO")

    def test_falls_back_to_tab_link_when_attribute_missing(self):
        row = _row(_post_redesign_row("WEN", with_attribute=False))
        self.assertIsNone(row.find("td", attrs={"data-boxover-ticker": True}))
        self.assertEqual(_ticker_from_row(row), "WEN")

    def test_reads_pre_redesign_markup(self):
        self.assertEqual(_ticker_from_row(_row(_pre_redesign_row("CRMT"))), "CRMT")

    def test_returns_none_for_an_unrecognized_cell(self):
        self.assertIsNone(_ticker_from_row(_row("<tr><td>1</td><td>junk</td></tr>")))


class RepairTickerColumnTest(unittest.TestCase):
    def test_repairs_the_doubled_first_character(self):
        # What finvizfinance's flat col.text parse produces from the new markup.
        table = pd.DataFrame(
            {"Ticker": ["OOKLO", "WWEN", "UUAA"], "Company": ["a", "b", "c"]}
        )
        rows = [_row("<tr><th>hdr</th></tr>")] + [
            _row(_post_redesign_row(s)) for s in ("OKLO", "WEN", "UAA")
        ]

        repaired = repair_ticker_column(table, rows[1:])

        self.assertEqual(repaired, 3)
        self.assertEqual(table["Ticker"].tolist(), ["OKLO", "WEN", "UAA"])

    def test_leaves_already_correct_tickers_untouched(self):
        table = pd.DataFrame({"Ticker": ["CRMT"], "Company": ["a"]})

        repaired = repair_ticker_column(table, [_row(_pre_redesign_row("CRMT"))])

        self.assertEqual(repaired, 0)
        self.assertEqual(table["Ticker"].tolist(), ["CRMT"])

    def test_repairs_only_the_current_pages_trailing_rows(self):
        # finvizfinance accumulates pages into one frame and re-enters
        # _get_table per page; earlier pages must not be re-indexed.
        table = pd.DataFrame(
            {"Ticker": ["AAPL", "MSFT", "OOKLO"], "Company": ["a", "b", "c"]}
        )

        repaired = repair_ticker_column(table, [_row(_post_redesign_row("OKLO"))])

        self.assertEqual(repaired, 1)
        self.assertEqual(table["Ticker"].tolist(), ["AAPL", "MSFT", "OKLO"])

    def test_is_a_no_op_when_the_frame_has_no_ticker_column(self):
        table = pd.DataFrame({"Company": ["a"]})
        self.assertEqual(repair_ticker_column(table, [_row(_post_redesign_row("OKLO"))]), 0)

    def test_is_a_no_op_when_rows_outnumber_the_frame(self):
        table = pd.DataFrame({"Ticker": ["OOKLO"]})
        rows = [_row(_post_redesign_row(s)) for s in ("OKLO", "WEN")]
        self.assertEqual(repair_ticker_column(table, rows), 0)


class TickerRepairViewFactoryTests(unittest.TestCase):
    """The repair must not be welded to one screener view.

    The SI collector reads the Ownership view (v=131), which carries Short
    Float directly. It sees the identical avatar corruption — measured
    2026-09-10, 231 of 231 rows arrived with the first character doubled.
    """

    def test_builds_a_subclass_of_any_screener_view(self):
        from finvizfinance.screener.ownership import Ownership

        cls = make_ticker_repair_view(Ownership)

        self.assertTrue(issubclass(cls, Ownership))
        self.assertEqual(cls.__name__, "_TickerRepairOwnership")

    def test_fresh_instance_starts_with_a_zero_repair_count(self):
        from finvizfinance.screener.ownership import Ownership

        self.assertEqual(make_ticker_repair_view(Ownership)().tickers_repaired, 0)


if __name__ == "__main__":
    unittest.main()
