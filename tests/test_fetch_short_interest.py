"""Guards for the SI tab's Finviz collection step.

Three failure modes are pinned here, all of them silent in production.

**Ticker corruption.** Measured 2026-09-10, 231 of 231 Ownership rows arrived
with the first character doubled. Unrepaired, every downstream master-table
join misses and the tab exports nothing — indistinguishable from a market with
no heavily shorted names.

**A 200 that parses to nothing.** The same shape that hid the NAAIM breakage
for weeks. A fetch failure and an empty parse must log differently.

**The order key.** `'Float Short'` is the column name but not a valid sort key;
`finvizfinance` raises `ValueError` listing the real names. The screener quietly
returning an unsorted page would not be caught by eye.
"""

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

from src.data_collection import fetch_short_interest as si


def _frame(**overrides):
    base = {
        "Ticker": ["AAA", "BBB"],
        "Short Float": ["40.00%", "15.00%"],
        "Float": [1.0e6, 2.0e6],
        "Market Cap": [1.0e9, 2.0e9],
        "Inst Trans": [0.0123, -0.0456],
        "Price": [10.0, 20.0],
    }
    base.update(overrides)
    return pd.DataFrame(base)


class ParseShortFloatTests(unittest.TestCase):
    def test_parses_a_percent_string(self):
        self.assertAlmostEqual(si.parse_short_float("70.54%"), 70.54)

    def test_parses_a_bare_number(self):
        self.assertAlmostEqual(si.parse_short_float("12.3"), 12.3)

    def test_returns_none_for_placeholders(self):
        for raw in ("-", "", "  ", None, float("nan")):
            with self.subTest(raw=raw):
                self.assertIsNone(si.parse_short_float(raw))

    def test_returns_none_for_junk(self):
        self.assertIsNone(si.parse_short_float("n/a"))


class BuildRowsTests(unittest.TestCase):
    def test_drops_rows_with_no_short_interest(self):
        rows = si.build_rows(_frame(**{"Short Float": ["15.00%", "-"]}))

        self.assertEqual([r["ticker"] for r in rows], ["AAA"])
        self.assertAlmostEqual(rows[0]["si"], 15.0)

    def test_sorts_by_short_interest_descending(self):
        rows = si.build_rows(_frame(
            Ticker=["LOW", "HIGH"], **{"Short Float": ["12.00%", "40.00%"]}
        ))

        self.assertEqual([r["ticker"] for r in rows], ["HIGH", "LOW"])

    def test_carries_the_columns_the_tab_renders(self):
        row = si.build_rows(_frame())[0]

        self.assertEqual(row["ticker"], "AAA")
        self.assertAlmostEqual(row["si"], 40.0)
        self.assertAlmostEqual(row["float_shares"], 1.0e6)
        self.assertAlmostEqual(row["market_cap"], 1.0e9)
        self.assertAlmostEqual(row["inst_trans"], 1.23)
        self.assertAlmostEqual(row["price"], 10.0)

    def test_uppercases_tickers(self):
        rows = si.build_rows(_frame(Ticker=["aaa", "bbb"]))

        self.assertEqual([r["ticker"] for r in rows], ["aaa".upper(), "bbb".upper()])

    def test_tolerates_a_missing_optional_column(self):
        frame = _frame().drop(columns=["Inst Trans"])

        self.assertIsNone(si.build_rows(frame)[0]["inst_trans"])

    def test_scales_inst_trans_from_a_fraction_to_a_percent(self):
        """The screener returns a fraction; the quote page, fundamentals.db
        and every other consumer here use percent.

        Verified 2026-09-10 against both sources: WOLF reads 0.4804 on the
        screener and 48.04 on its quote page; BTDR 0.373 against 37.3.
        """
        row = si.build_rows(_frame(**{"Inst Trans": [0.4804, 0.373]}))[0]

        self.assertAlmostEqual(row["inst_trans"], 48.04)

    def test_short_interest_is_not_scaled(self):
        """Short Float is already a percent on both sources."""
        self.assertAlmostEqual(si.build_rows(_frame())[0]["si"], 40.0)

    def test_empty_frame_yields_no_rows(self):
        self.assertEqual(si.build_rows(pd.DataFrame()), [])


class FilterTests(unittest.TestCase):
    def test_filters_match_the_source_screener_url(self):
        """f=sh_avgvol_o1000,sh_curvol_o750,sh_price_o10,sh_short_o10,
        ta_volatility_mo4"""
        self.assertEqual(si.SI_FILTERS, {
            "Average Volume": "Over 1M",
            "Current Volume": "Over 750K",
            "Price": "Over $10",
            "Float Short": "Over 10%",
            "Volatility": "Month - Over 4%",
        })

    def test_order_key_is_the_sortable_name_not_the_column_name(self):
        """'Float Short' is the column; 'Short Interest Share' is the sort key.

        finvizfinance raises ValueError on the column name, which would abort
        the whole step.
        """
        self.assertEqual(si.SI_ORDER, "Short Interest Share")


class FetchTests(unittest.TestCase):
    def _view(self, frame, repaired=2):
        view = mock.MagicMock()
        view.screener_view.return_value = frame
        view.tickers_repaired = repaired
        return view

    def test_returns_rows_and_echoes_the_filters(self):
        view = self._view(_frame())
        with mock.patch.object(si, "_make_view", return_value=view):
            out = si.fetch_short_interest()

        self.assertEqual(len(out["rows"]), 2)
        self.assertEqual(out["filters"], si.SI_FILTERS)
        view.set_filter.assert_called_once_with(filters_dict=si.SI_FILTERS)

    def test_sorts_descending_by_short_interest_share(self):
        view = self._view(_frame())
        with mock.patch.object(si, "_make_view", return_value=view):
            si.fetch_short_interest()

        _, kwargs = view.screener_view.call_args
        self.assertEqual(kwargs["order"], "Short Interest Share")
        self.assertFalse(kwargs["ascend"])

    def test_a_200_that_parses_to_nothing_warns_distinctly(self):
        """An empty parse is an upstream-shape signal, not a quiet market.

        Conflating it with a fetch failure is exactly what hid the NAAIM
        breakage.
        """
        view = self._view(pd.DataFrame(), repaired=0)
        with mock.patch.object(si, "_make_view", return_value=view):
            with self.assertLogs(si.logger, level="WARNING") as logs:
                out = si.fetch_short_interest()

        self.assertEqual(out["rows"], [])
        self.assertTrue(any("no rows" in m.lower() for m in logs.output))

    def test_zero_repairs_on_a_non_empty_table_warns(self):
        """Every row arrived corrupted when this was measured. Zero repairs
        means Finviz changed the ticker cell again."""
        view = self._view(_frame(), repaired=0)
        with mock.patch.object(si, "_make_view", return_value=view):
            with self.assertLogs(si.logger, level="WARNING") as logs:
                si.fetch_short_interest()

        self.assertTrue(any("repair" in m.lower() for m in logs.output))


class WriteTests(unittest.TestCase):
    def test_writes_the_payload_with_the_given_date(self):
        payload = {"date": "", "filters": si.SI_FILTERS, "rows": [{"ticker": "AAA"}]}

        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "short_interest.json"
            si.write_short_interest(payload, "2026-09-10", out)
            written = json.loads(out.read_text(encoding="utf-8"))

        self.assertEqual(written["date"], "2026-09-10")
        self.assertEqual(written["rows"], [{"ticker": "AAA"}])

    def test_stamps_the_callers_dict_so_it_can_report_what_it_wrote(self):
        payload = {"date": "", "filters": {}, "rows": []}

        with tempfile.TemporaryDirectory() as tmp:
            si.write_short_interest(payload, "2026-09-10", Path(tmp) / "si.json")

        self.assertEqual(payload["date"], "2026-09-10")


if __name__ == "__main__":
    unittest.main()
