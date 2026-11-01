"""COT tab collector: net positions, the crowding score and its tags."""

import unittest
from datetime import date, timedelta

from src.data_collection.fetch_cot import (build_payload, crowding_signal, parse_rows,
                                           percentile_rank, week_change)


def raw_row(code, day, oi, nc=(0, 0), cm=(0, 0), sm=(0, 0)):
    return {
        "cftc_contract_market_code": code,
        "report_date_as_yyyy_mm_dd": f"{day}T00:00:00.000",
        "open_interest_all": str(oi),
        "noncomm_positions_long_all": str(nc[0]), "noncomm_positions_short_all": str(nc[1]),
        "comm_positions_long_all": str(cm[0]), "comm_positions_short_all": str(cm[1]),
        "nonrept_positions_long_all": str(sm[0]), "nonrept_positions_short_all": str(sm[1]),
    }


def weeks_of(nc_values, oi=1000, start=date(2026, 1, 6)):
    return [{"date": (start + timedelta(weeks=i)).isoformat(), "oi": oi,
             "nc": v, "cm": -v, "sm": 0} for i, v in enumerate(nc_values)]


class ParseRowsTests(unittest.TestCase):
    def test_the_nasdaq_report_of_2026_10_27_parses_to_the_published_nets(self):
        row = raw_row("209742", "2026-10-27", 268508, nc=(84567, 35728),
                      cm=(136338, 198430), sm=(41642, 28389))
        week = parse_rows([row])["209742"][0]
        self.assertEqual(week, {"date": "2026-10-27", "oi": 268508,
                                "nc": 48839, "cm": -62092, "sm": 13253})
        self.assertEqual(week["nc"] + week["cm"] + week["sm"], 0)

    def test_a_duplicated_week_is_kept_once_and_weeks_sort_by_date(self):
        rows = [raw_row("X", "2026-02-03", 10), raw_row("X", "2026-01-27", 10),
                raw_row("X", "2026-02-03", 10)]
        self.assertEqual([w["date"] for w in parse_rows(rows)["X"]],
                         ["2026-01-27", "2026-02-03"])

    def test_a_row_without_open_interest_is_dropped(self):
        """Every share-of-OI figure divides by it."""
        self.assertEqual(parse_rows([raw_row("X", "2026-01-27", 0)]), {})


class ScoreTests(unittest.TestCase):
    def test_the_rank_counts_the_current_week(self):
        self.assertEqual(percentile_rank([1, 2, 3, 4], 4), 100.0)
        self.assertEqual(percentile_rank([1, 2, 3, 4], 1), 25.0)

    def test_a_change_across_a_reporting_gap_is_unknown(self):
        weeks = weeks_of([10, 20])
        self.assertEqual(week_change(weeks, "nc"), 10)
        weeks[1]["date"] = (date.fromisoformat(weeks[0]["date"]) + timedelta(days=14)).isoformat()
        self.assertIsNone(week_change(weeks, "nc"))


class CrowdingSignalTests(unittest.TestCase):
    def signal(self, nc_values):
        return crowding_signal(weeks_of(nc_values), min_weeks=5, extreme_pct=10)

    def test_a_crowded_long_that_shrinks_is_fade_risk(self):
        s = self.signal(list(range(100)) + [98])
        self.assertEqual((s["crowd"], s["state"], s["commercials"]), ("long", "fade", "bearish"))
        self.assertTrue(s["summary"].startswith("Speculative positioning is fading"))

    def test_a_crowded_long_that_still_grows_is_extended(self):
        s = self.signal(list(range(100)) + [120])
        self.assertEqual((s["score"], s["crowd"], s["state"]), (100, "long", "extended"))

    def test_a_crowded_short_that_covers_is_fade_risk(self):
        s = self.signal(list(range(100, 0, -1)) + [2])
        self.assertEqual((s["crowd"], s["state"], s["commercials"]), ("short", "fade", "bullish"))

    def test_the_middle_of_the_range_carries_no_tags(self):
        s = self.signal(list(range(100)) + [50])
        self.assertEqual((s["crowd"], s["state"], s["commercials"]), (None, None, None))
        self.assertEqual(s["summary"], "Speculative positioning is not at an extreme.")

    def test_the_score_ranks_share_of_open_interest_not_contracts(self):
        """Open interest that doubles halves the share a fixed net holds."""
        weeks = weeks_of([100] * 20)
        weeks[-1]["oi"] = 2000
        s = crowding_signal(weeks, min_weeks=5, extreme_pct=10)
        self.assertEqual((s["score"], s["crowd"]), (5, "short"))

    def test_too_few_weeks_publishes_no_score(self):
        s = crowding_signal(weeks_of([1, 2, 3]), min_weeks=5, extreme_pct=10)
        self.assertIsNone(s["score"])
        self.assertIsNone(s["crowd"])


class BuildPayloadTests(unittest.TestCase):
    CFG = {"window_weeks": 3, "min_weeks_for_score": 2, "extreme_pct": 10,
           "default_market": "B",
           "markets": [{"code": "B", "name": "Bravo"}, {"code": "A", "name": "Alpha"},
                       {"code": "Z", "name": "Missing"}]}

    def test_markets_keep_config_order_and_the_window_is_trimmed(self):
        payload = build_payload({"A": weeks_of(range(6)), "B": weeks_of(range(6))}, self.CFG)
        self.assertEqual([m["name"] for m in payload["markets"]], ["Bravo", "Alpha"])
        self.assertEqual(payload["report_date"], "2026-02-10")
        self.assertEqual(payload["markets"][0]["dates"],
                         ["2026-01-27", "2026-02-03", "2026-02-10"])
        self.assertEqual(payload["default_market"], "B")

    def test_no_rows_at_all_raises_so_the_last_file_stays(self):
        with self.assertRaises(RuntimeError):
            build_payload({}, self.CFG)


if __name__ == "__main__":
    unittest.main()
