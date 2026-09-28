"""Float and short-interest lookup for the EP scans.

Finviz's late-June 2026 quote-page redesign removed the ``div.quote-links``
block that finvizfinance's ``ticker_fundament()`` reads first, so that call
raised ``AttributeError`` for every ticker. The EP scans read the failure as
"missing fundamental data" and dropped every candidate that had passed the
price check: 0 of 101 AMC candidates survived 2026-08-10 to 08-13, and the EP
tab stayed empty for the whole Q2 season. The snapshot tables themselves were
unchanged, and the daily fundamentals step already read them directly.

These tests pin the EP lookup to that same parser. The markup below is the
real cell shape, captured live from finviz.com/quote.ashx?t=NNE on 2026-09-27.
"""

import unittest
from unittest import mock

from bs4 import BeautifulSoup

from src.data_collection.fetch_fundamental_data import parse_snapshot_tables
from src.reporting import ep_scan_common


def _cell_pair(label: str, value: str) -> str:
    return f"""
    <tr class="table-dark-row">
      <td align="left" class="snapshot-td2 cursor-pointer max-xl:w-[4%] xl:w-px"
          data-boxover-html="{label}"><div class="snapshot-td-label">{label}</div></td>
      <td align="left" class="snapshot-td2 max-xl:w-[8%] xl:w-full xl:max-w-0" style="">
        <div class="snapshot-td-content"><b>{value}</b></div></td>
    </tr>
    """


def _snapshot_table(*pairs) -> str:
    rows = "".join(_cell_pair(label, value) for label, value in pairs)
    return (
        '<table width="100%" cellpadding="3" cellspacing="0" border="0" '
        'class="js-snapshot-table snapshot-table2 screener_snapshot-table-body '
        f'max-xl:table-fixed xl:table-auto">{rows}</table>'
    )


def _quote_page(float_value: str = "44.82M") -> BeautifulSoup:
    """A post-redesign quote page: six snapshot tables, no div.quote-links."""
    html = f"""
    <html><body>
      <h2 class="quote-header_ticker-wrapper_company">NANO Nuclear Energy Inc</h2>
      <div class="quote-header_categories">
        <a class="quote-header_category" href="screener?v=111&f=sec_industrials">Industrials</a>
      </div>
      {_snapshot_table(("Index", "RUT"), ("Market Cap", "2.31B"))}
      {_snapshot_table(("P/E", "-"), ("Forward P/E", "-"))}
      {_snapshot_table(("EPS (ttm)", "-1.05"), ("EPS next Y", "-0.98"))}
      {_snapshot_table(("Insider Own", "21.10%"), ("Inst Own", "40.02%"))}
      {_snapshot_table(("Shs Outstand", "51.49M"), ("Shs Float", float_value),
                       ("Short Float", "34.49%"), ("Avg Volume", "2.07M"))}
      {_snapshot_table(("Perf Week", "4.02%"), ("Price", "51.60"))}
    </body></html>
    """
    return BeautifulSoup(html, "html.parser")


class _FakeQuote:
    """Stands in for finvizfinance's quote object after the redesign."""

    def __init__(self, soup):
        self.soup = soup

    def ticker_fundament(self):
        # What finvizfinance 1.3.0 raises on the current page.
        raise AttributeError("'NoneType' object has no attribute 'find_all'")


def _get_fundamentals(soup):
    with mock.patch.object(ep_scan_common, "FinvizQuote",
                           side_effect=lambda ticker: _FakeQuote(soup),
                           create=True), \
         mock.patch.object(ep_scan_common, "FINVIZ_AVAILABLE", True):
        return ep_scan_common.get_fundamentals("NNE")


class ParseSnapshotTablesTest(unittest.TestCase):

    def test_merges_every_snapshot_table_not_only_the_first(self):
        data = parse_snapshot_tables(_quote_page())
        self.assertEqual(data["Market Cap"], "2.31B")   # first table
        self.assertEqual(data["Shs Float"], "44.82M")   # fifth table
        self.assertEqual(data["Short Float"], "34.49%")
        self.assertEqual(data["Price"], "51.60")        # last table

    def test_page_without_snapshot_tables_gives_empty_dict(self):
        self.assertEqual(parse_snapshot_tables(BeautifulSoup("<html></html>", "html.parser")), {})


class EpGetFundamentalsTest(unittest.TestCase):

    def test_reads_the_redesigned_quote_page(self):
        got = _get_fundamentals(_quote_page())
        self.assertIsNotNone(got)
        self.assertAlmostEqual(got["float"], 44.82)
        self.assertAlmostEqual(got["short"], 34.49)
        self.assertAlmostEqual(got["avg_volume"], 2_070_000.0, places=3)

    def test_billion_float_converts_to_millions(self):
        self.assertAlmostEqual(_get_fundamentals(_quote_page("14.58B"))["float"], 14580.0)

    def test_page_without_snapshot_tables_returns_none(self):
        with mock.patch("builtins.print"):
            self.assertIsNone(_get_fundamentals(BeautifulSoup("<html></html>", "html.parser")))


class LegacyExportTest(unittest.TestCase):
    """``ep_scan_export.py`` (the old single-scan exporter) had both breaks:
    its own copy of the ``ticker_fundament()`` lookup, and a plain Overview
    screener that doubles each ticker's first letter."""

    def test_uses_the_shared_fundamentals_lookup(self):
        from src.reporting import ep_scan_export
        self.assertIs(ep_scan_export.get_fundamentals, ep_scan_common.get_fundamentals)

    def test_earnings_screener_repairs_tickers(self):
        import pandas as pd
        from finvizfinance.screener.overview import Overview
        from src.reporting import ep_scan_export

        views = []

        def fake_screener_view(self, *args, **kwargs):
            views.append(self)
            return pd.DataFrame({"Ticker": ["OKLO"]})

        with mock.patch.object(Overview, "set_filter", autospec=True), \
             mock.patch.object(Overview, "screener_view", autospec=True,
                               side_effect=fake_screener_view):
            tickers = ep_scan_export.get_earnings_tickers("Today After Market Close")

        self.assertEqual(tickers, ["OKLO"])
        self.assertEqual(len(views), 1)
        # Only the repair view carries this counter.
        self.assertTrue(hasattr(views[0], "tickers_repaired"))


if __name__ == "__main__":
    unittest.main()
