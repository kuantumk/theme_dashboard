"""Ticker extraction from Finviz screener markup, plus the all-dropped alarm.

Finviz's 2026-07-15 redesign put a logo avatar inside the ticker cell. Its
one-letter fallback <span> sits before the real symbol, and finvizfinance's
flat `col.text` parse concatenates the two — "OKLO" arrives as "OOKLO". Every
downstream Yahoo lookup then 404s, so the scan silently exported zero tickers
for three weeks of earnings season. These tests pin both halves of the fix:
reading the symbol back out of the markup, and making a 100% drop rate loud.

The HTML fixtures below are the real cell shapes, captured live from the
screener before and after the redesign.
"""

import unittest
from unittest import mock

import pandas as pd
from bs4 import BeautifulSoup

from src.reporting import ep_scan_common
from src.reporting.ep_scan_common import (
    _ticker_from_row,
    repair_ticker_column,
    send_discord_notification,
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


class AllDroppedAlarmTest(unittest.TestCase):
    """An empty export on a non-zero screened count must not read as a quiet day."""

    def _content(self, tickers, screened_count):
        with mock.patch.object(ep_scan_common, "DISCORD_WEBHOOK_URL", "https://example/hook"), \
             mock.patch.object(ep_scan_common.requests, "post") as post:
            post.return_value = mock.Mock(status_code=204)
            send_discord_notification(
                "Afternoon Earnings", "2026-08-07", tickers, screened_count=screened_count
            )
        return post.call_args.kwargs["json"]["content"]

    def test_warns_when_every_screened_candidate_was_dropped(self):
        content = self._content([], screened_count=68)

        self.assertIn("68", content)
        self.assertIn("dropped", content)
        self.assertNotIn("No qualifying tickers found", content)

    def test_reports_a_genuinely_quiet_day_normally(self):
        content = self._content([], screened_count=0)

        self.assertIn("No qualifying tickers found", content)
        self.assertNotIn("dropped", content)

    def test_omitted_screened_count_keeps_the_original_message(self):
        content = self._content([], screened_count=None)

        self.assertIn("No qualifying tickers found", content)

    def test_results_still_render_the_metrics_table(self):
        content = self._content(
            [{"ticker": "OKLO", "float": 12.0, "short": 15.0, "ah_chg_pct": 4.2}],
            screened_count=68,
        )

        self.assertIn("OKLO", content)
        self.assertIn("AH CHG", content)
        self.assertNotIn("dropped", content)


if __name__ == "__main__":
    unittest.main()
