"""Guards for the SI tab's Finviz collection step.

Each failure pinned here is silent in production: step 7b is non-critical, so
a broken fetch keeps the workflow green and the tab shows an old roster.

**A page with no screener data.** Finviz moved the screener to a React page
whose rows live only in an embedded JSON block. A parser that finds nothing
must raise, not report an empty market.

**The User-Agent.** Finviz answers 403 to an old Chrome agent. That blocked
this step from 2026-09-29 to 2026-11-18.

**Paging.** The page holds 20 rows; the screen held 252 on 2026-11-18. Reading
one page drops most of the roster with nothing on screen to say so.
"""

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from src.data_collection import fetch_short_interest as si


def _row(ticker, short, **numbers):
    base = {"shortInterestShare": short, "sharesFloat": 35.33,
            "marketCap": 1220.5, "instTrans": 62.2, "price": 22.78}
    base.update(numbers)
    return {"text": {"ticker": ticker}, "numbers": base}


def _page(rows, total=None, page_size=20):
    data = {"rows": rows, "totalCount": len(rows) if total is None else total,
            "firstRow": 0, "pageSize": page_size}
    props = json.dumps({"url": "/screener/stocks", "data": data})
    return ('<html><script type="application/json" data-ssr-props="ssr-island-1">'
            f"{props}</script></html>")


class ParsePageTests(unittest.TestCase):
    def test_reads_the_embedded_rows(self):
        data = si.parse_screener_page(_page([_row("WOLF", 82.1)], total=252))
        self.assertEqual(data["totalCount"], 252)
        self.assertEqual(data["rows"][0]["text"]["ticker"], "WOLF")

    def test_a_page_with_no_screener_data_raises(self):
        with self.assertRaises(ValueError):
            si.parse_screener_page("<html><div class='fv-table_row'></div></html>")


class BuildRowsTests(unittest.TestCase):
    def test_scales_float_and_market_cap_from_millions(self):
        row = si.build_rows([_row("WOLF", 82.08)])[0]
        self.assertAlmostEqual(row["float_shares"], 35.33e6)
        self.assertAlmostEqual(row["market_cap"], 1220.5e6)

    def test_keeps_percent_fields_as_percents(self):
        row = si.build_rows([_row("WOLF", 82.08, instTrans=62.21)])[0]
        self.assertAlmostEqual(row["si"], 82.08)
        self.assertAlmostEqual(row["inst_trans"], 62.21)

    def test_drops_rows_with_no_short_interest(self):
        rows = si.build_rows([_row("AAA", None), _row("BBB", 15.0)])
        self.assertEqual([r["ticker"] for r in rows], ["BBB"])

    def test_sorts_by_short_interest_descending(self):
        rows = si.build_rows([_row("LOW", 11.0), _row("HIGH", 40.0)])
        self.assertEqual([r["ticker"] for r in rows], ["HIGH", "LOW"])

    def test_a_missing_optional_field_is_none(self):
        raw = _row("AAA", 20.0)
        del raw["numbers"]["instTrans"]
        self.assertIsNone(si.build_rows([raw])[0]["inst_trans"])


class FetchTests(unittest.TestCase):
    def _fetch(self, pages):
        get = mock.Mock(side_effect=pages)
        with mock.patch.object(si, "_get_page", get), \
                mock.patch.object(si.time, "sleep"):
            return si.fetch_short_interest(), get

    def test_reads_every_page(self):
        first = [_row(f"A{i}", 50.0 - i) for i in range(20)]
        second = [_row("LAST", 10.5)]
        out, get = self._fetch([_page(first, total=21), _page(second, total=21)])

        self.assertEqual(len(out["rows"]), 21)
        self.assertEqual([c.args[0] for c in get.call_args_list], [1, 21])

    def test_an_empty_screen_warns(self):
        with self.assertLogs(si.logger, level="WARNING") as logs:
            out, _ = self._fetch([_page([])])

        self.assertEqual(out["rows"], [])
        self.assertTrue(any("no rows" in m.lower() for m in logs.output))

    def test_requests_the_screen_with_the_configured_user_agent(self):
        response = mock.Mock(text=_page([]))
        with mock.patch.object(si.requests, "get", return_value=response) as get:
            si._get_page(1)

        _, kwargs = get.call_args
        self.assertEqual(kwargs["headers"]["User-Agent"],
                         si.CONFIG["fundamental_data"]["user_agent"])
        self.assertEqual(kwargs["params"]["f"], si.SI_FILTERS)
        self.assertEqual(kwargs["params"]["o"], "-shortinterestshare")


class WriteTests(unittest.TestCase):
    def test_writes_the_payload_with_the_given_date(self):
        payload = {"date": "", "filters": si.SI_FILTERS, "rows": [{"ticker": "AAA"}]}

        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "short_interest.json"
            si.write_short_interest(payload, "2026-09-10", out)
            written = json.loads(out.read_text(encoding="utf-8"))

        self.assertEqual(written["date"], "2026-09-10")
        self.assertEqual(written["rows"], [{"ticker": "AAA"}])

    def test_an_empty_roster_is_not_written(self):
        payload = {"date": "", "filters": "", "rows": []}

        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "si.json"
            out.write_text('{"date": "2026-09-09", "rows": [{"ticker": "OLD"}]}', encoding="utf-8")
            with self.assertRaises(ValueError):
                si.write_short_interest(payload, "2026-09-10", out)
            self.assertIn("2026-09-09", out.read_text(encoding="utf-8"))

    def test_stamps_the_callers_dict_so_it_can_report_what_it_wrote(self):
        payload = {"date": "", "filters": "", "rows": [{"ticker": "AAA"}]}

        with tempfile.TemporaryDirectory() as tmp:
            si.write_short_interest(payload, "2026-09-10", Path(tmp) / "si.json")

        self.assertEqual(payload["date"], "2026-09-10")


if __name__ == "__main__":
    unittest.main()
