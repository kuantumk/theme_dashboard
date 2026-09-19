"""Grouping, theme scoring, column ordering and extreme-badge tests.

The score these pin replaced a sum of classified margins. The sum made breadth
swamp intensity — Spearman(member count, |score|) was +0.72 and 0 of 67
single-name groups ever reached the screen — so the two halves are now separate
and separately bounded: a capped mean of the top three members carries
intensity, and an additive share term carries breadth.
"""

import unittest
from types import SimpleNamespace

from config.settings import CONFIG
from src.bidask.config import load_config
from src.bidask.grouping import (
    DEFAULT_BREADTH_COEF,
    DEFAULT_BREADTH_MIN_MEMBERS,
    DEFAULT_RVOL_CAP,
    RVOL_FIELD,
    SIDE_STRONG,
    SIDE_WEAK,
    TOP_MEMBERS,
    UNCLASSIFIED_GROUP,
    build_columns,
)
from src.bidask.highs import extreme_badge

CFG = load_config()

THEMES = {
    "NVDA": ["AI / Data Center / Cloud & Hyperscalers"],
    "DUAL": ["AI / Data Center / Memory", "Space / Launch"],
    # A leaf repeated in one ticker's tag list. Rare, but the audit tool exists
    # because the file is hand-edited, and it must count once here.
    "TWIN": ["AI / Data Center / Memory", "AI / Data Center / Memory"],
    "PEER": ["AI / Data Center / Memory"],
    "MEH": ["Uncategorized"],
    "SOLO": ["Singleton"],
}

MEMORY = "AI / Data Center / Memory"


def row(symbol, rvol, *, sides=(SIDE_STRONG,), **meta):
    """One poll row that cleared the relative-volume gate."""
    payload = {"symbol": symbol, RVOL_FIELD: float(rvol), "sides": list(sides)}
    payload.update(meta)
    return payload


def stub_cfg(**knobs):
    """A config carrying the scoring knobs explicitly, caps at their defaults."""
    base = {"max_rows_per_column": CFG.max_rows_per_column,
            "max_rows_per_group": CFG.max_rows_per_group,
            "group_rvol_cap": DEFAULT_RVOL_CAP,
            "group_breadth_coef": DEFAULT_BREADTH_COEF,
            "group_breadth_min_members": DEFAULT_BREADTH_MIN_MEMBERS}
    base.update(knobs)
    return SimpleNamespace(**base)


class TestGrouping(unittest.TestCase):
    """Preserved by R17: every one of these predates the rescore."""

    def test_tagged_ticker_groups_under_theme_leaf(self):
        rows = [row("NVDA", 3.0)]
        cols = build_columns(rows, THEMES, CFG, universe=rows)
        group = cols["strong"][0]
        self.assertEqual(group["name"], "AI / Data Center / Cloud & Hyperscalers")
        self.assertEqual(group["origin"], "theme")

    def test_untagged_ticker_falls_back_to_industry(self):
        rows = [row("ZZZ", 3.0, industry="Semiconductors")]
        cols = build_columns(rows, THEMES, CFG, universe=rows)
        group = cols["strong"][0]
        self.assertEqual(group["name"], "Semiconductors")
        self.assertEqual(group["origin"], "industry")

    def test_uncategorized_only_takes_industry_fallback(self):
        rows = [row("MEH", 3.0, industry="Biotechnology")]
        cols = build_columns(rows, THEMES, CFG, universe=rows)
        self.assertEqual(cols["strong"][0]["name"], "Biotechnology")

    def test_singleton_only_is_not_untagged(self):
        # Singleton is a deliberate terminal classification, not a gap.
        rows = [row("SOLO", 3.0, industry="Biotechnology")]
        cols = build_columns(rows, THEMES, CFG, universe=rows)
        self.assertEqual(cols["strong"][0]["name"], "Singleton")

    def test_dual_role_ticker_appears_in_both_groups(self):
        rows = [row("DUAL", 3.0)]
        cols = build_columns(rows, THEMES, CFG, universe=rows)
        names = {g["name"] for g in cols["strong"]}
        self.assertIn(MEMORY, names)
        self.assertIn("Space / Launch", names)
        for group in cols["strong"]:
            self.assertAlmostEqual(group["score"], 3.0, places=4)

    def test_untagged_with_no_industry_lands_in_catchall(self):
        rows = [row("XXX", 3.0)]
        cols = build_columns(rows, THEMES, CFG, universe=rows)
        self.assertEqual(cols["strong"][0]["name"], UNCLASSIFIED_GROUP)

    def test_one_large_group_does_not_consume_the_whole_column(self):
        # Regression: the column budget was decremented by each group's full
        # member count in score order, so a 70-member industry bucket took every
        # slot and every other narrative vanished.
        big = [row(f"B{i}", 4.0, industry="Biotechnology") for i in range(70)]
        small = [row(f"S{i}", 1.0, industry="Semiconductors") for i in range(5)]
        rows = big + small
        cols = build_columns(rows, THEMES, CFG, universe=rows)
        names = [g["name"] for g in cols["strong"]]
        self.assertIn("Biotechnology", names)
        self.assertIn("Semiconductors", names)
        for group in cols["strong"]:
            self.assertLessEqual(len(group["members"]), CFG.max_rows_per_group)

    def test_column_cap_still_bounds_total_rows(self):
        rows = [row(f"T{i}", 3.0, industry=f"Ind{i // 3}") for i in range(120)]
        cols = build_columns(rows, THEMES, CFG, universe=rows)
        total = sum(len(g["members"]) for g in cols["strong"])
        self.assertLessEqual(total, CFG.max_rows_per_column)


class TestSideAssignment(unittest.TestCase):
    """The two columns are independent tests, never an if/else (R5)."""

    def test_a_row_earning_both_sides_appears_in_both_columns(self):
        rows = [row("GAP", 3.0, sides=(SIDE_STRONG, SIDE_WEAK), industry="Alpha")]
        cols = build_columns(rows, THEMES, CFG, universe=rows)
        self.assertEqual(cols["strong"][0]["members"][0]["symbol"], "GAP")
        self.assertEqual(cols["weak"][0]["members"][0]["symbol"], "GAP")

    def test_sides_may_be_a_mapping_of_side_to_reference(self):
        """R5's per-side marker rides on the same field; the test is `in`."""
        payload = {"symbol": "GAP", RVOL_FIELD: 3.0, "industry": "Alpha",
                   "sides": {"strong": "open", "weak": "prev_close"}}
        cols = build_columns([payload], THEMES, CFG, universe=[payload])
        self.assertEqual(cols["strong"][0]["members"][0]["symbol"], "GAP")
        self.assertEqual(cols["weak"][0]["members"][0]["symbol"], "GAP")

    def test_a_row_with_no_side_reaches_neither_column(self):
        rows = [row("FLAT", 9.0, sides=(), industry="Alpha")]
        cols = build_columns(rows, THEMES, CFG, universe=rows)
        self.assertEqual(cols["strong"], [])
        self.assertEqual(cols["weak"], [])

    def test_the_caller_rows_are_not_mutated(self):
        """The badge is attached to a copy: `universe` is the same list."""
        rows = [row("NVDA", 3.0)]
        build_columns(rows, THEMES, CFG, universe=rows)
        self.assertNotIn("badge", rows[0])


class TestThemeScore(unittest.TestCase):
    def test_three_moderate_members_outrank_one_extreme_member(self):
        """AE4. The cap is what makes this come out the right way round.

        Alpha's 40x member is worth 5.0, so Alpha scores (5.0+1.3+1.3)/3 =
        2.533 against Bravo's 3.5. Uncapped, Alpha reads 14.2 and no theme on
        the board can reach it.
        """
        rows = [row("XA", 40.0, industry="Alpha"),
                row("XB", 1.3, industry="Alpha"),
                row("XC", 1.3, industry="Alpha"),
                row("YA", 3.5, industry="Bravo"),
                row("YB", 3.5, industry="Bravo"),
                row("YC", 3.5, industry="Bravo")]
        cols = build_columns(rows, THEMES, CFG, universe=rows)
        self.assertEqual([g["name"] for g in cols["strong"]], ["Bravo", "Alpha"])

    def test_the_cap_is_applied_before_the_mean(self):
        rows = [row("XA", 40.0, industry="Alpha"),
                row("XB", 1.0, industry="Alpha"),
                row("XC", 1.0, industry="Alpha")]
        cols = build_columns(rows, THEMES, CFG, universe=rows)
        score = cols["strong"][0]["score"]
        intensity = (DEFAULT_RVOL_CAP + 1.0 + 1.0) / 3
        self.assertAlmostEqual(score, intensity + DEFAULT_BREADTH_COEF, places=4)
        # Capping the mean instead reads 5.0 — one member carrying two others.
        self.assertLess(score, DEFAULT_RVOL_CAP)

    def test_only_the_top_three_members_carry_the_intensity(self):
        """A fourth, quieter member must not drag the leaders' mean down."""
        three = [row(f"X{i}", 4.0, industry="Alpha") for i in range(TOP_MEMBERS)]
        four = [row(f"Y{i}", 4.0, industry="Bravo") for i in range(TOP_MEMBERS)]
        four.append(row("YZ", 0.8, industry="Bravo"))
        rows = three + four
        cols = build_columns(rows, THEMES, CFG, universe=rows)
        scores = {g["name"]: g["score"] for g in cols["strong"]}
        self.assertAlmostEqual(scores["Alpha"], scores["Bravo"], places=4)

    def test_a_two_member_group_scores_on_what_it_has(self):
        """Never zero-padded to three: that punishes a small roster twice."""
        rows = [row("XA", 4.0, industry="Alpha"), row("XB", 4.0, industry="Alpha")]
        cols = build_columns(rows, THEMES, CFG, universe=rows)
        self.assertEqual(len(cols["strong"]), 1)
        self.assertAlmostEqual(cols["strong"][0]["score"],
                               4.0 + DEFAULT_BREADTH_COEF, places=4)

    def test_a_single_member_group_is_not_excluded(self):
        rows = [row("XA", 4.0, industry="Alpha")]
        cols = build_columns(rows, THEMES, CFG, universe=rows)
        self.assertEqual(len(cols["strong"]), 1)
        self.assertAlmostEqual(cols["strong"][0]["score"], 4.0, places=4)

    def test_an_unusable_relative_volume_never_fabricates_strength(self):
        """The gate excludes these (R11); a NaN here would cost the payload."""
        rows = [row("XA", 4.0, industry="Alpha"),
                {"symbol": "XB", RVOL_FIELD: None, "sides": [SIDE_STRONG],
                 "industry": "Alpha"}]
        cols = build_columns(rows, THEMES, CFG, universe=rows)
        score = cols["strong"][0]["score"]
        self.assertAlmostEqual(score, (4.0 + 0.0) / 2 + DEFAULT_BREADTH_COEF,
                               places=4)

    def test_the_cap_comes_from_config(self):
        cfg = stub_cfg(group_rvol_cap=2.0)
        rows = [row("XA", 40.0, industry="Alpha")]
        cols = build_columns(rows, THEMES, cfg, universe=rows)
        self.assertAlmostEqual(cols["strong"][0]["score"], 2.0, places=4)


class TestBreadthTerm(unittest.TestCase):
    def test_more_qualifying_members_outrank_fewer_at_equal_intensity(self):
        """Identical leaders, different breadth: the term alone decides."""
        qualifying = [row("XA", 3.0, industry="Alpha"),
                      row("XB", 3.0, industry="Alpha"),
                      row("XC", 3.0, industry="Alpha"),
                      row("YA", 3.0, industry="Bravo")]
        universe = qualifying + [row("YB", 0.2, industry="Bravo"),
                                 row("YC", 0.2, industry="Bravo")]
        cols = build_columns(qualifying, THEMES, CFG, universe=universe)
        self.assertEqual([g["name"] for g in cols["strong"]], ["Alpha", "Bravo"])

    def test_the_denominator_is_taken_before_the_gate(self):
        """Post-gate every surviving row qualifies, so the share is always 1.0.

        Alpha and Bravo field the same two members at the same reading. Only
        their pre-gate rosters differ, and that has to be what separates them.
        """
        qualifying = [row("XA", 3.0, industry="Alpha"),
                      row("XB", 3.0, industry="Alpha"),
                      row("YA", 3.0, industry="Bravo"),
                      row("YB", 3.0, industry="Bravo")]
        quiet = [row(f"YQ{i}", 0.2, industry="Bravo") for i in range(6)]
        cols = build_columns(qualifying, THEMES, CFG, universe=qualifying + quiet)
        scores = {g["name"]: g["score"] for g in cols["strong"]}
        self.assertAlmostEqual(scores["Alpha"], 3.0 + DEFAULT_BREADTH_COEF, places=4)
        self.assertAlmostEqual(scores["Bravo"],
                               3.0 + DEFAULT_BREADTH_COEF * 0.25, places=4)

    def test_an_industry_fallback_group_gets_a_denominator(self):
        """Untagged buckets have no taxonomy roster; the universe is the only one."""
        qualifying = [row("ZA", 3.0, industry="Semiconductors"),
                      row("ZB", 3.0, industry="Semiconductors")]
        quiet = [row(f"ZQ{i}", 0.2, industry="Semiconductors") for i in range(2)]
        cols = build_columns(qualifying, THEMES, CFG, universe=qualifying + quiet)
        self.assertAlmostEqual(cols["strong"][0]["score"],
                               3.0 + DEFAULT_BREADTH_COEF * 0.5, places=4)

    def test_a_group_below_the_minimum_scores_no_breadth_at_all(self):
        """Share alone hands a one-member group the maximum."""
        qualifying = [row("XA", 3.0, industry="Alpha")]
        cols = build_columns(qualifying, THEMES, CFG, universe=qualifying)
        self.assertAlmostEqual(cols["strong"][0]["score"], 3.0, places=4)

    def test_the_minimum_comes_from_config(self):
        cfg = stub_cfg(group_breadth_min_members=3)
        qualifying = [row("XA", 3.0, industry="Alpha"),
                      row("XB", 3.0, industry="Alpha")]
        cols = build_columns(qualifying, THEMES, cfg, universe=qualifying)
        self.assertAlmostEqual(cols["strong"][0]["score"], 3.0, places=4)

    def test_the_term_only_ever_adds(self):
        qualifying = [row("XA", 3.0, industry="Alpha"),
                      row("XB", 3.0, industry="Alpha")]
        quiet = [row(f"Q{i}", 0.2, industry="Alpha") for i in range(98)]
        cols = build_columns(qualifying, THEMES, CFG, universe=qualifying + quiet)
        self.assertGreater(cols["strong"][0]["score"], 3.0)

    def test_a_negative_coefficient_cannot_subtract(self):
        cfg = stub_cfg(group_breadth_coef=-2.0)
        qualifying = [row("XA", 3.0, industry="Alpha"),
                      row("XB", 3.0, industry="Alpha")]
        cols = build_columns(qualifying, THEMES, cfg, universe=qualifying)
        self.assertAlmostEqual(cols["strong"][0]["score"], 3.0, places=4)

    def test_a_ticker_in_two_leaves_of_one_theme_counts_once(self):
        """TWIN carries the same leaf twice. It is still one member.

        Counted twice it would read (4.0+4.0+1.0)/3 = 3.0 on intensity and 3 of
        5 on breadth, instead of (4.0+1.0)/2 = 2.5 and 2 of 4.
        """
        qualifying = [row("TWIN", 4.0), row("PEER", 1.0)]
        # Two more members of the same leaf that never cleared the gate, so the
        # roster is four and the share is a half.
        quiet = [row("PEER2", 0.2, sides=()), row("PEER3", 0.2, sides=())]
        universe = qualifying + quiet
        themes = dict(THEMES, PEER2=[MEMORY], PEER3=[MEMORY])
        cols = build_columns(qualifying, themes, CFG, universe=universe)
        group = next(g for g in cols["strong"] if g["name"] == MEMORY)
        self.assertEqual([m["symbol"] for m in group["members"]], ["TWIN", "PEER"])
        self.assertAlmostEqual(group["score"],
                               2.5 + DEFAULT_BREADTH_COEF * 0.5, places=4)


class TestBothColumnsSortDescending(unittest.TestCase):
    """The score is a capped relative volume and is never negative.

    The weak side used to sort ascending, which was right while the score was a
    signed margin whose most negative value was its strongest reading. Left in
    place it puts the quietest theme at the top of the weak column and spends
    the whole 60-row budget before reaching the ones being distributed.
    """

    def test_the_weak_column_leads_with_its_highest_scoring_theme(self):
        rows = [row("WA", 4.5, sides=(SIDE_WEAK,), industry="Heavy"),
                row("WB", 4.5, sides=(SIDE_WEAK,), industry="Heavy"),
                row("WC", 1.0, sides=(SIDE_WEAK,), industry="Light"),
                row("WD", 1.0, sides=(SIDE_WEAK,), industry="Light")]
        cols = build_columns(rows, THEMES, CFG, universe=rows)
        self.assertEqual([g["name"] for g in cols["weak"]], ["Heavy", "Light"])

    def test_members_run_highest_first_on_both_sides(self):
        rows = [row("SA", 1.5, industry="Alpha"),
                row("SB", 4.0, industry="Alpha"),
                row("WA", 1.5, sides=(SIDE_WEAK,), industry="Bravo"),
                row("WB", 4.0, sides=(SIDE_WEAK,), industry="Bravo")]
        cols = build_columns(rows, THEMES, CFG, universe=rows)
        self.assertEqual([m["symbol"] for m in cols["strong"][0]["members"]],
                         ["SB", "SA"])
        self.assertEqual([m["symbol"] for m in cols["weak"][0]["members"]],
                         ["WB", "WA"])

    def test_the_weak_column_truncates_its_quietest_themes(self):
        """The inverted sort dropped exactly the rows this column is for."""
        rows = []
        for i in range(30):
            for j in range(3):
                rows.append(row(f"W{i}_{j}", 0.5 + i * 0.1,
                                sides=(SIDE_WEAK,), industry=f"Ind{i:02d}"))
        cols = build_columns(rows, THEMES, CFG, universe=rows)
        shown = [g["name"] for g in cols["weak"]]
        self.assertEqual(shown[0], "Ind29")
        self.assertNotIn("Ind00", shown)


class TestTruncationIsReported(unittest.TestCase):
    """The column cap drops most of the board, and used to do so silently.

    Measured 2026-08-14: 13 of 124 strong groups and 111 of 367 in-play tickers
    reached the screen, and nothing on the page said anything had been dropped.
    """

    def test_only_totals_are_published(self):
        """The "shown" half is the browser's to count, after its own sliders."""
        rows = [row("NVDA", 3.0)]
        cols = build_columns(rows, THEMES, CFG, universe=rows)
        self.assertEqual(set(cols["truncated"]["strong"]),
                         {"groups_total", "tickers_total"})

    def test_nothing_hidden_still_reports_the_totals(self):
        rows = [row("NVDA", 3.0)]
        cols = build_columns(rows, THEMES, CFG, universe=rows)
        meta = cols["truncated"]["strong"]
        self.assertEqual(meta["groups_total"], len(cols["strong"]))
        self.assertEqual(meta["tickers_total"], 1)

    def test_dropped_groups_are_counted(self):
        # 20 industries of 5 members each is 100 rows against a 60-row budget.
        rows = [row(f"T{i}", 4.0 - (i % 5) * 0.1, industry=f"Ind{i // 5}")
                for i in range(100)]
        cols = build_columns(rows, THEMES, CFG, universe=rows)
        meta = cols["truncated"]["strong"]
        self.assertEqual(meta["groups_total"], 20)
        self.assertLess(len(cols["strong"]), meta["groups_total"])

    def test_dropped_tickers_are_counted_distinctly(self):
        rows = [row(f"T{i}", 3.0, industry=f"Ind{i // 5}") for i in range(100)]
        cols = build_columns(rows, THEMES, CFG, universe=rows)
        meta = cols["truncated"]["strong"]
        self.assertEqual(meta["tickers_total"], 100)
        rendered = {m["symbol"] for g in cols["strong"] for m in g["members"]}
        self.assertLessEqual(len(rendered), CFG.max_rows_per_column)
        self.assertLess(len(rendered), meta["tickers_total"])

    def test_a_dual_role_ticker_counts_once(self):
        """DUAL sits in two theme leaves; it is still one ticker."""
        rows = [row("DUAL", 3.0)]
        cols = build_columns(rows, THEMES, CFG, universe=rows)
        meta = cols["truncated"]["strong"]
        self.assertEqual(meta["groups_total"], 2)
        self.assertEqual(meta["tickers_total"], 1)

    def test_both_sides_are_reported(self):
        rows = [row("NVDA", 3.0),
                row("MEH", 3.0, sides=(SIDE_WEAK,), industry="Retail")]
        cols = build_columns(rows, THEMES, CFG, universe=rows)
        self.assertIn("strong", cols["truncated"])
        self.assertIn("weak", cols["truncated"])
        self.assertEqual(cols["truncated"]["weak"]["tickers_total"], 1)

    def test_crypto_flat_path_reports_too(self):
        rows = [row(f"C{i}", 3.0) for i in range(80)]
        cols = build_columns(rows, {}, CFG, grouped=False, universe=rows)
        meta = cols["truncated"]["strong"]
        self.assertEqual(meta["tickers_total"], 80)
        self.assertEqual(len(cols["strong"][0]["members"]),
                         CFG.max_rows_per_column)


class TestCryptoPath(unittest.TestCase):
    def test_crypto_path_is_flat(self):
        rows = [row("BTC", 3.0)]
        cols = build_columns(rows, {}, CFG, grouped=False, universe=rows)
        self.assertEqual(len(cols["strong"]), 1)
        self.assertEqual(cols["strong"][0]["name"], "All")
        self.assertEqual(cols["strong"][0]["origin"], "flat")

    def test_the_flat_group_scores_on_the_same_capped_mean(self):
        rows = [row("BTC", 40.0), row("ETH", 1.0), row("SOL", 1.0)]
        cols = build_columns(rows, {}, CFG, grouped=False, universe=rows)
        intensity = (DEFAULT_RVOL_CAP + 1.0 + 1.0) / 3
        self.assertAlmostEqual(cols["strong"][0]["score"],
                               intensity + DEFAULT_BREADTH_COEF, places=4)

    def test_flat_members_run_highest_first(self):
        rows = [row("ETH", 1.0), row("BTC", 4.0), row("SOL", 2.0)]
        cols = build_columns(rows, {}, CFG, grouped=False, universe=rows)
        self.assertEqual([m["symbol"] for m in cols["strong"][0]["members"]],
                         ["BTC", "SOL", "ETH"])


class TestConfigDefaultsMatchTheYaml(unittest.TestCase):
    """`BidAskConfig` does not carry these three knobs yet.

    `grouping` reads them off the config object when present and falls back to
    its own named defaults otherwise. Pin the two together or the shipped YAML
    and the running board drift apart with nothing on screen to show it.
    """

    def test_defaults_equal_the_shipped_yaml(self):
        block = CONFIG.get("bidask") or {}
        self.assertAlmostEqual(float(block["group_rvol_cap"]), DEFAULT_RVOL_CAP)
        self.assertAlmostEqual(float(block["group_breadth_coef"]),
                               DEFAULT_BREADTH_COEF)
        self.assertEqual(int(block["group_breadth_min_members"]),
                         DEFAULT_BREADTH_MIN_MEMBERS)

    def test_the_breadth_term_cannot_out_rank_a_capped_leader(self):
        """The shipped balance: intensity-led, breadth as the tiebreak.

        A fully-qualifying theme gains at most `group_breadth_coef`, and scores
        span roughly the lowest floor to the cap. Raising the coefficient past
        that span restores the breadth-swamps-intensity failure the summed
        margin had.
        """
        self.assertLess(DEFAULT_BREADTH_COEF, DEFAULT_RVOL_CAP)


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

    def test_the_badge_rides_on_the_member_payload(self):
        rows = [row("HIGH", 3.0, industry="Alpha", close=100.0,
                    **{"price_52_week_high": 99.0})]
        cols = build_columns(rows, THEMES, CFG, universe=rows)
        self.assertEqual(cols["strong"][0]["members"][0]["badge"],
                         {"label": "52W", "direction": "high"})


if __name__ == "__main__":
    unittest.main()
