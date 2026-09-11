"""Guards for the SI tab's gate, scoring and L1 clustering.

**The gate fails CLOSED on a missing selloff leg.** That is the opposite of
`filter_metrics`, which fails open. The difference is what each one asserts:
the V/A cutoffs hide a row from a view, so a missing metric should not hide
it, while this gate asserts a selloff happened. A ticker with no price history
has not proved anything, and this tab is entirely about the proof.

**L1 is the ranked unit, not the leaf.** Measured 2026-09-10, the leaf level
gave 67 leaves of which only 24 held 2+ members, so single-name leaves filled
the top and "several shorted names lift the theme" never fired. At L1 level
with a 3-member minimum the same data gives 9 sections covering all 105
tickers.

**Breadth breaks ties; it does not scale the score.** Sum-of-members was
considered and rejected — CLAUDE.md records that exact choice failing on the
tape-pressure board, where 0 of 67 single-name groups ever reached the screen.
"""

import contextlib
import io
import json
import tempfile
import unittest
import unittest.mock
from pathlib import Path

import pandas as pd

from src.reporting import export_dashboard_data as ex

CFG = {
    "min_short_interest": 12.0,
    "max_drawdown_60d": -0.25,
    "max_drop_15d": -0.25,
    "min_tickers_per_l1": 3,
    "top_k_si": 3,
    "hot_radar_rank": 10,
}


def _master(rows):
    """A master frame carrying only the columns the SI builder reads."""
    return pd.DataFrame(rows)


def _bar(ticker, close=10.0, max60=20.0, drop=-0.40, streak=5):
    return {
        "ticker": ticker, "date": "2026-09-10", "close": close, "max60": max60,
        "drop_15d": drop, "max_down_streak": streak,
    }


class SiGateTests(unittest.TestCase):
    def test_passes_on_the_drawdown_leg_alone(self):
        self.assertTrue(ex.si_gate(20.0, -0.30, -0.05, CFG))

    def test_passes_on_the_drop_leg_alone(self):
        self.assertTrue(ex.si_gate(20.0, -0.05, -0.30, CFG))

    def test_rejects_when_short_interest_is_below_the_floor(self):
        self.assertFalse(ex.si_gate(11.9, -0.90, -0.90, CFG))

    def test_accepts_exactly_at_the_selloff_threshold(self):
        self.assertTrue(ex.si_gate(20.0, -0.25, 0.0, CFG))

    def test_rejects_when_neither_selloff_leg_fires(self):
        self.assertFalse(ex.si_gate(40.0, -0.10, -0.10, CFG))

    def test_short_interest_floor_is_exclusive(self):
        """12.0 is 'over 12', matching the Finviz leg's 'Over 10%' wording."""
        self.assertFalse(ex.si_gate(12.0, -0.90, -0.90, CFG))
        self.assertTrue(ex.si_gate(12.01, -0.90, -0.90, CFG))

    def test_nan_selloff_legs_reject(self):
        """Fails closed. A ticker with no price history has not proved a
        selloff, and this tab is about the proof."""
        nan = float("nan")
        self.assertFalse(ex.si_gate(40.0, nan, nan, CFG))

    def test_one_nan_leg_still_passes_on_the_other(self):
        self.assertTrue(ex.si_gate(40.0, float("nan"), -0.40, CFG))

    def test_nan_short_interest_rejects(self):
        self.assertFalse(ex.si_gate(float("nan"), -0.90, -0.90, CFG))


class SiSnapshotTests(unittest.TestCase):
    def _build(self, si_rows, master_rows, themes, radar=None, cfg=None):
        return ex._build_si_snapshot(
            si_rows, _master(master_rows), {}, themes,
            radar or {}, cfg or CFG,
        )

    def test_l1_scores_as_the_mean_of_its_top_three_members(self):
        # The fourth member is ignored by the score but counts toward breadth.
        si_rows = [
            {"ticker": t, "si": v} for t, v in
            [("AA", 40.0), ("BB", 30.0), ("CC", 20.0), ("DD", 13.0)]
        ]
        master = [_bar(t) for t in ("AA", "BB", "CC", "DD")]
        themes = {t: ["AI / Data Center"] for t in ("AA", "BB", "CC", "DD")}

        snap = self._build(si_rows, master, themes)

        self.assertEqual(len(snap["themes"]), 1)
        self.assertAlmostEqual(snap["themes"][0]["score"], 30.0)
        self.assertEqual(snap["themes"][0]["n"], 4)

    def test_a_leaf_with_fewer_than_top_k_members_averages_what_it_has(self):
        """No per-leaf minimum, so a 2-member leaf scores on its two.

        Only leaves can be shorter than top_k here: min_tickers_per_l1 (3)
        equals top_k_si (3), so every surviving L1 has at least k members.
        """
        si_rows = [{"ticker": t, "si": v} for t, v in
                   [("AA", 40.0), ("BB", 20.0), ("CC", 13.0)]]
        master = [_bar(t) for t in ("AA", "BB", "CC")]
        themes = {"AA": ["AI / Data Center"], "BB": ["AI / Data Center"],
                  "CC": ["AI / Semiconductor"]}

        snap = self._build(si_rows, master, themes)
        by_name = {lf["name"]: lf for lf in snap["themes"][0]["leaves"]}

        self.assertAlmostEqual(by_name["AI / Data Center"]["score"], 30.0)
        self.assertAlmostEqual(by_name["AI / Semiconductor"]["score"], 13.0)

    def test_breadth_breaks_a_score_tie(self):
        si_rows = [{"ticker": t, "si": 20.0} for t in ("AA", "BB", "CC", "DD", "EE", "FF")]
        master = [_bar(t) for t in ("AA", "BB", "CC", "DD", "EE", "FF")]
        themes = {
            "AA": ["AI / Data Center"], "BB": ["AI / Data Center"],
            "CC": ["AI / Data Center"], "DD": ["AI / Data Center"],
            "EE": ["Space / Launch"], "FF": ["Space / Launch"],
        }
        themes["GG"] = ["Space / Launch"]
        si_rows.append({"ticker": "GG", "si": 20.0})
        master.append(_bar("GG"))

        snap = self._build(si_rows, master, themes)

        self.assertEqual([t["name"] for t in snap["themes"]], ["AI", "Space"])
        self.assertAlmostEqual(snap["themes"][0]["score"], snap["themes"][1]["score"])

    def test_drops_an_l1_below_the_member_minimum(self):
        si_rows = [{"ticker": t, "si": 50.0} for t in ("AA", "BB")]
        master = [_bar(t) for t in ("AA", "BB")]
        themes = {t: ["Space / Launch"] for t in ("AA", "BB")}

        snap = self._build(si_rows, master, themes)

        self.assertIsNone(snap)

    def test_themes_sort_by_score_descending(self):
        si_rows = [{"ticker": t, "si": v} for t, v in
                   [("AA", 15.0), ("BB", 15.0), ("CC", 15.0),
                    ("XX", 50.0), ("YY", 50.0), ("ZZ", 50.0)]]
        master = [_bar(t) for t in ("AA", "BB", "CC", "XX", "YY", "ZZ")]
        themes = {t: ["AI / Data Center"] for t in ("AA", "BB", "CC")}
        themes.update({t: ["Space / Launch"] for t in ("XX", "YY", "ZZ")})

        snap = self._build(si_rows, master, themes)

        self.assertEqual([t["name"] for t in snap["themes"]], ["Space", "AI"])

    def test_tickers_within_a_leaf_sort_by_short_interest_descending(self):
        si_rows = [{"ticker": t, "si": v} for t, v in
                   [("LOW", 13.0), ("HIGH", 40.0), ("MID", 25.0)]]
        master = [_bar(t) for t in ("LOW", "HIGH", "MID")]
        themes = {t: ["AI / Data Center"] for t in ("LOW", "HIGH", "MID")}

        snap = self._build(si_rows, master, themes)
        leaf = snap["themes"][0]["leaves"][0]

        self.assertEqual([t["ticker"] for t in leaf["tickers"]], ["HIGH", "MID", "LOW"])

    def test_leaves_sort_by_their_own_top_three_mean(self):
        si_rows = [{"ticker": t, "si": v} for t, v in
                   [("AA", 13.0), ("BB", 14.0), ("CC", 60.0), ("DD", 55.0)]]
        master = [_bar(t) for t in ("AA", "BB", "CC", "DD")]
        themes = {"AA": ["AI / Data Center"], "BB": ["AI / Data Center"],
                  "CC": ["AI / Semiconductor"], "DD": ["AI / Semiconductor"]}

        snap = self._build(si_rows, master, themes)
        names = [lf["name"] for lf in snap["themes"][0]["leaves"]]

        self.assertEqual(names, ["AI / Semiconductor", "AI / Data Center"])

    def test_hidden_leaves_are_dropped(self):
        si_rows = [{"ticker": t, "si": 40.0} for t in ("AA", "BB", "CC", "JUNK")]
        master = [_bar(t) for t in ("AA", "BB", "CC", "JUNK")]
        themes = {t: ["AI / Data Center"] for t in ("AA", "BB", "CC")}
        themes["JUNK"] = ["Uncategorized"]

        snap = self._build(si_rows, master, themes)

        self.assertEqual(snap["themes"][0]["n"], 3)
        self.assertNotIn("Uncategorized", [t["name"] for t in snap["themes"]])

    def test_a_ticker_in_two_leaves_of_one_l1_counts_once(self):
        """Otherwise a dual-tagged name inflates its own L1's breadth."""
        si_rows = [{"ticker": t, "si": 40.0} for t in ("AA", "BB", "CC")]
        master = [_bar(t) for t in ("AA", "BB", "CC")]
        themes = {
            "AA": ["AI / Data Center", "AI / Semiconductor"],
            "BB": ["AI / Data Center"], "CC": ["AI / Data Center"],
        }

        snap = self._build(si_rows, master, themes)

        self.assertEqual(snap["themes"][0]["n"], 3)

    def test_a_ticker_failing_the_gate_never_reaches_a_theme(self):
        si_rows = [{"ticker": t, "si": 40.0} for t in ("AA", "BB", "CC", "FLAT")]
        master = [_bar(t) for t in ("AA", "BB", "CC")]
        master.append(_bar("FLAT", close=19.5, max60=20.0, drop=-0.01))
        themes = {t: ["AI / Data Center"] for t in ("AA", "BB", "CC", "FLAT")}

        snap = self._build(si_rows, master, themes)
        rendered = [t["ticker"] for t in snap["themes"][0]["leaves"][0]["tickers"]]

        self.assertNotIn("FLAT", rendered)

    def test_a_ticker_missing_from_the_master_is_dropped(self):
        """Fails closed: no price history, no proof of a selloff."""
        si_rows = [{"ticker": t, "si": 40.0} for t in ("AA", "BB", "CC", "GHOST")]
        master = [_bar(t) for t in ("AA", "BB", "CC")]
        themes = {t: ["AI / Data Center"] for t in ("AA", "BB", "CC", "GHOST")}

        snap = self._build(si_rows, master, themes)

        self.assertEqual(snap["themes"][0]["n"], 3)

    def test_carries_the_columns_the_tab_renders(self):
        si_rows = [{"ticker": t, "si": 40.0, "float_shares": 1.2e7,
                    "inst_trans": 3.4, "price": 10.0}
                   for t in ("AA", "BB", "CC")]
        master = [_bar(t, close=10.0, max60=20.0, drop=-0.40, streak=7)
                  for t in ("AA", "BB", "CC")]
        themes = {t: ["AI / Data Center"] for t in ("AA", "BB", "CC")}

        snap = self._build(si_rows, master, themes)
        row = snap["themes"][0]["leaves"][0]["tickers"][0]

        self.assertAlmostEqual(row["si"], 40.0)
        self.assertAlmostEqual(row["dd60"], -50.0)
        self.assertAlmostEqual(row["drop15"], -40.0)
        self.assertEqual(row["streak"], 7)
        self.assertAlmostEqual(row["price"], 10.0)
        self.assertEqual(row["float"], "12.0")
        self.assertEqual(row["inst"], "+3.4")

    def test_report_date_comes_from_the_master_frame(self):
        si_rows = [{"ticker": t, "si": 40.0} for t in ("AA", "BB", "CC")]
        master = [_bar(t) for t in ("AA", "BB", "CC")]
        themes = {t: ["AI / Data Center"] for t in ("AA", "BB", "CC")}

        snap = self._build(si_rows, master, themes)

        self.assertEqual(snap["report_date"], "2026-09-10")
        self.assertEqual(snap["n_tickers"], 3)


class SiHotBadgeTests(unittest.TestCase):
    def _snap(self, radar):
        si_rows = [{"ticker": t, "si": 40.0} for t in ("AA", "BB", "CC")]
        master = [_bar(t) for t in ("AA", "BB", "CC")]
        themes = {t: ["AI / Data Center"] for t in ("AA", "BB", "CC")}
        return ex._build_si_snapshot(si_rows, _master(master), {}, themes, radar, CFG)

    def test_a_top_ranked_l1_earns_the_badge(self):
        snap = self._snap({"AI": 4})

        self.assertTrue(snap["themes"][0]["hot"])
        self.assertEqual(snap["themes"][0]["radar_rank"], 4)

    def test_a_low_ranked_l1_does_not(self):
        snap = self._snap({"AI": 29})

        self.assertFalse(snap["themes"][0]["hot"])
        self.assertEqual(snap["themes"][0]["radar_rank"], 29)

    def test_the_threshold_rank_is_inclusive(self):
        self.assertTrue(self._snap({"AI": 10})["themes"][0]["hot"])
        self.assertFalse(self._snap({"AI": 11})["themes"][0]["hot"])

    def test_a_missing_radar_drops_the_badge_without_reordering(self):
        """The badge is decoration. Ranking stays on short interest."""
        with_radar = self._snap({"AI": 4})
        without = self._snap({})

        self.assertFalse(without["themes"][0]["hot"])
        self.assertIsNone(without["themes"][0]["radar_rank"])
        self.assertEqual(
            [t["name"] for t in without["themes"]],
            [t["name"] for t in with_radar["themes"]],
        )


class SiStalenessTests(unittest.TestCase):
    """A stale short-interest roster must never pass as this session's.

    Step 7b is deliberately non-critical and `data/short_interest.json` is
    committed, so the file always exists. If the Finviz fetch fails, the
    workflow continues and the previous roster survives — joined to TODAY's
    prices and stamped with today's master date, it would look fresh.

    That is the frozen-NAAIM-tile shape. The repo's settled answer is to keep
    the reading, carry its own date, and make the age visible rather than
    blanking the tab: a one-session-old short-interest roster is still useful,
    an undated one is not.
    """

    def _snap(self, si_date, master_date="2026-09-10"):
        si_rows = [{"ticker": t, "si": 40.0} for t in ("AA", "BB", "CC")]
        master = [dict(_bar(t), date=master_date) for t in ("AA", "BB", "CC")]
        themes = {t: ["AI / Data Center"] for t in ("AA", "BB", "CC")}
        return ex._build_si_snapshot(
            si_rows, _master(master), {}, themes, {}, CFG, si_date=si_date
        )

    def test_a_same_session_roster_is_not_stale(self):
        snap = self._snap("2026-09-10")

        self.assertEqual(snap["si_date"], "2026-09-10")
        self.assertFalse(snap["si_stale"])

    def test_an_older_roster_is_flagged_stale_but_still_publishes(self):
        snap = self._snap("2026-09-09")

        self.assertEqual(snap["si_date"], "2026-09-09")
        self.assertTrue(snap["si_stale"])
        self.assertEqual(snap["report_date"], "2026-09-10")
        self.assertEqual(snap["n_tickers"], 3)

    def test_an_undated_roster_reads_as_stale(self):
        """Unknown age is not proof of freshness."""
        snap = self._snap("")

        self.assertTrue(snap["si_stale"])
        self.assertIsNone(snap["si_date"])

    def test_export_warns_when_the_roster_is_stale(self):
        with tempfile.TemporaryDirectory() as tmp:
            root, out = Path(tmp) / "scratch", Path(tmp) / "docs"
            (root / "master").mkdir(parents=True)
            out.mkdir()
            si_path = Path(tmp) / "short_interest.json"
            si_path.write_text(json.dumps({
                "date": "2026-09-09", "filters": {},
                "rows": [{"ticker": t, "si": 40.0} for t in ("AA", "BB", "CC")],
            }), encoding="utf-8")
            master = _master([dict(_bar(t), date="2026-09-10")
                              for t in ("AA", "BB", "CC")])
            import src.stock_utils as su
            su.save_df_to_parquet(master, root / "master" / "master_2026-09-10.parquet")

            themes = {t: ["AI / Data Center"] for t in ("AA", "BB", "CC")}
            buffer = io.StringIO()
            with unittest.mock.patch(
                "src.themes.theme_registry.load_ticker_themes", return_value=themes
            ):
                with contextlib.redirect_stdout(buffer):
                    snap = ex.export_si({}, root=root, out_dir=out, si_file=si_path)

        self.assertTrue(snap["si_stale"])
        self.assertIn("stale", buffer.getvalue().lower())
        self.assertIn("2026-09-09", buffer.getvalue())


class RadarRankLoadingTests(unittest.TestCase):
    def test_reads_ranks_keyed_by_l1_name(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "radar.json"
            path.write_text(json.dumps(
                {"l1s": [{"name": "AI", "rank": 4}, {"name": "Space", "rank": 29}]}
            ), encoding="utf-8")

            self.assertEqual(ex._load_radar_ranks(path), {"AI": 4, "Space": 29})

    def test_reads_the_legacy_ecosystems_key(self):
        """Entries written before the 2026-07 consolidation."""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "radar.json"
            path.write_text(json.dumps(
                {"ecosystems": [{"name": "AI", "rank": 1}]}
            ), encoding="utf-8")

            self.assertEqual(ex._load_radar_ranks(path), {"AI": 1})

    def test_a_missing_file_yields_no_ranks(self):
        self.assertEqual(ex._load_radar_ranks(Path("does-not-exist.json")), {})

    def test_malformed_json_yields_no_ranks(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "radar.json"
            path.write_text("{not json", encoding="utf-8")

            self.assertEqual(ex._load_radar_ranks(path), {})


if __name__ == "__main__":
    unittest.main()
