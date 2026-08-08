"""Grouping and extreme-badge tests."""

import unittest

from src.bidask.config import load_config
from src.bidask.grouping import UNCLASSIFIED_GROUP, build_columns
from src.bidask.highs import extreme_badge
from src.bidask.session import TickerState

CFG = load_config()

THEMES = {
    "NVDA": ["AI / Data Center / Cloud & Hyperscalers"],
    "DUAL": ["AI / Data Center / Memory", "Space / Launch"],
    "MEH": ["Uncategorized"],
    "SOLO": ["Singleton"],
}


def state(symbol, ask, bid, **meta):
    st = TickerState(symbol=symbol)
    st.ask_hits, st.bid_hits = ask, bid
    st.delta = float(ask - bid)
    st.meta = meta
    return st


class TestGrouping(unittest.TestCase):
    def test_tagged_ticker_groups_under_theme_leaf(self):
        cols = build_columns([state("NVDA", 10, 2)], THEMES, CFG)
        group = cols["strong"][0]
        self.assertEqual(group["name"], "AI / Data Center / Cloud & Hyperscalers")
        self.assertEqual(group["origin"], "theme")

    def test_untagged_ticker_falls_back_to_industry(self):
        cols = build_columns([state("ZZZ", 8, 1, industry="Semiconductors")],
                             THEMES, CFG)
        group = cols["strong"][0]
        self.assertEqual(group["name"], "Semiconductors")
        self.assertEqual(group["origin"], "industry")

    def test_uncategorized_only_takes_industry_fallback(self):
        cols = build_columns([state("MEH", 8, 1, industry="Biotechnology")],
                             THEMES, CFG)
        self.assertEqual(cols["strong"][0]["name"], "Biotechnology")

    def test_singleton_only_is_not_untagged(self):
        # Singleton is a deliberate terminal classification, not a gap.
        cols = build_columns([state("SOLO", 8, 1, industry="Biotechnology")],
                             THEMES, CFG)
        self.assertEqual(cols["strong"][0]["name"], "Singleton")

    def test_dual_role_ticker_appears_in_both_groups(self):
        cols = build_columns([state("DUAL", 9, 1)], THEMES, CFG)
        names = {g["name"] for g in cols["strong"]}
        self.assertIn("AI / Data Center / Memory", names)
        self.assertIn("Space / Launch", names)
        for group in cols["strong"]:
            self.assertEqual(group["score"], 8)

    def test_untagged_with_no_industry_lands_in_catchall(self):
        cols = build_columns([state("XXX", 8, 1)], THEMES, CFG)
        self.assertEqual(cols["strong"][0]["name"], UNCLASSIFIED_GROUP)

    def test_columns_split_on_margin_sign(self):
        cols = build_columns(
            [state("NVDA", 10, 2), state("ZZZ", 1, 9, industry="Gold")],
            THEMES, CFG,
        )
        self.assertEqual(cols["strong"][0]["members"][0]["symbol"], "NVDA")
        self.assertEqual(cols["weak"][0]["members"][0]["symbol"], "ZZZ")

    def test_zero_margin_ticker_appears_in_neither_column(self):
        cols = build_columns([state("NVDA", 5, 5)], THEMES, CFG)
        self.assertEqual(cols["strong"], [])
        self.assertEqual(cols["weak"], [])

    def test_below_min_hits_is_hidden(self):
        cols = build_columns([state("NVDA", 1, 0)], THEMES, CFG)
        self.assertEqual(cols["strong"], [])

    def test_groups_sort_by_summed_margin(self):
        cols = build_columns(
            [state("AAA", 20, 0, industry="Strong Industry"),
             state("BBB", 6, 0, industry="Weaker Industry")],
            THEMES, CFG,
        )
        self.assertEqual(cols["strong"][0]["name"], "Strong Industry")

    def test_crypto_path_is_flat(self):
        cols = build_columns([state("BTC", 9, 1)], {}, CFG, grouped=False)
        self.assertEqual(len(cols["strong"]), 1)
        self.assertEqual(cols["strong"][0]["name"], "All")
        self.assertEqual(cols["strong"][0]["origin"], "flat")


class TestExtremeBadge(unittest.TestCase):
    def test_above_52_week_high_badges_52w_not_1m(self):
        badge = extreme_badge({
            "close": 100.0, "High.1M": 90.0, "High.3M": 95.0,
            "High.6M": 98.0, "price_52_week_high": 99.0,
        })
        self.assertEqual(badge, {"label": "52W", "direction": "high"})

    def test_above_one_month_high_only_badges_1m(self):
        badge = extreme_badge({
            "close": 92.0, "High.1M": 90.0, "High.3M": 95.0,
            "High.6M": 98.0, "price_52_week_high": 99.0,
        })
        self.assertEqual(badge, {"label": "1M", "direction": "high"})

    def test_below_one_month_low_badges_a_low(self):
        badge = extreme_badge({
            "close": 50.0, "Low.1M": 51.0, "Low.3M": 45.0,
            "Low.6M": 40.0, "price_52_week_low": 35.0,
        })
        self.assertEqual(badge, {"label": "1M", "direction": "low"})

    def test_inside_all_horizons_has_no_badge(self):
        self.assertIsNone(extreme_badge({
            "close": 92.0, "High.1M": 95.0, "Low.1M": 90.0,
        }))

    def test_missing_fields_yield_no_badge(self):
        self.assertIsNone(extreme_badge({"close": 92.0}))


if __name__ == "__main__":
    unittest.main()
