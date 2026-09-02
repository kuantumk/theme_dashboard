"""Guards for the AAII Sentiment Survey scrape that replaced NAAIM Exposure.

The AAII page hangs its current-week figures off `ssv2-*` class hooks — a
versioned redesign prefix. A future `ssv3` rename would silently strip every
selector at once, and the same page carries a second set of percentages (the
historical averages) that also sum to 100. So a sum check alone cannot separate
the two; the class hook is what does. These tests pin that, and pin that a
partial or implausible parse yields nothing rather than a half-populated tile.

The fixture reproduces the live markup of 2026-08-27 including the decoys: the
`ssv2-savg` averages beside each figure, and the "1-Year Bullish High" bar that
also renders a bare percentage further down the page.
"""

import json
import unittest
from unittest.mock import patch

import src.reporting.export_dashboard_data as edd


# Faithful to the live gauge region, trimmed to the parse surface plus decoys.
LIVE_MARKUP = """
<div class="ssv2-gauge" role="region" aria-label="Current week sentiment readings">
    <div class="ssv2-gauge-header">
        <span class="ssv2-gauge-title">This week's results</span>
        <span class="ssv2-gauge-week">Week ending August 26, 2026</span>
    </div>
    <div class="ssv2-gauge-bars">
        <div class="ssv2-sbar">
            <div class="ssv2-slabel bull">Bullish</div>
            <div class="ssv2-snum bull">32.9%</div>
            <div class="ssv2-savg">Avg 37.5%</div>
            <div class="ssv2-strack"><div class="ssv2-sfill bull" style="width:32.9%"></div></div>
        </div>
        <div class="ssv2-sbar">
            <div class="ssv2-slabel neut">Neutral</div>
            <div class="ssv2-snum neut">22.6%</div>
            <div class="ssv2-savg">Avg 31.0%</div>
            <div class="ssv2-strack"><div class="ssv2-sfill neut" style="width:22.6%"></div></div>
        </div>
        <div class="ssv2-sbar">
            <div class="ssv2-slabel bear">Bearish</div>
            <div class="ssv2-snum bear">44.4%</div>
            <div class="ssv2-savg">Avg 31.5%</div>
            <div class="ssv2-strack"><div class="ssv2-sfill bear" style="width:44.4%"></div></div>
        </div>
    </div>
</div>
<div class="weekending">
    <div class="datebars">
        <div class="date">1-Year Bullish High</div>
        <div class="bars"><div class="bar bullish" style="width:49.5%">49.5%</div></div>
    </div>
</div>
"""

# Every ssv2- hook renamed, as a v3 redesign would do in one pass.
V3_MARKUP = LIVE_MARKUP.replace("ssv2-", "ssv3-")

# The historical averages survive but the current-week figures are gone. These
# three sum to exactly 100.0, so only the class hook separates them.
AVERAGES_ONLY_MARKUP = """
<div class="ssv2-gauge-bars">
    <div class="ssv2-sbar"><div class="ssv2-savg">Avg 37.5%</div></div>
    <div class="ssv2-sbar"><div class="ssv2-savg">Avg 31.0%</div></div>
    <div class="ssv2-sbar"><div class="ssv2-savg">Avg 31.5%</div></div>
</div>
"""


class ParseAaiiSentimentTests(unittest.TestCase):
    def test_live_markup_yields_all_three_figures_and_the_week(self):
        reading = edd.parse_aaii_sentiment(LIVE_MARKUP)
        self.assertEqual(
            reading,
            {
                "bullish": 32.9,
                "neutral": 22.6,
                "bearish": 44.4,
                "week_ending": "2026-08-26",
            },
        )

    def test_a_missing_figure_yields_nothing(self):
        """Two of three is never emitted: a tile showing bull and neutral with a
        blank bear reads as a real survey result, not as a broken parse."""
        html = LIVE_MARKUP.replace('<div class="ssv2-snum bear">44.4%</div>', "")
        self.assertIsNone(edd.parse_aaii_sentiment(html))

    def test_historical_averages_alone_yield_nothing(self):
        """The averages sum to 100.0, so the sum check passes on them. Only the
        ssv2-snum hook keeps them out of the tile."""
        self.assertIsNone(edd.parse_aaii_sentiment(AVERAGES_ONLY_MARKUP))

    def test_a_redesign_that_renames_every_hook_yields_nothing(self):
        """The whole point of the split warning in fetch_aaii_sentiment: a v3
        rename must fail loudly, not produce a partial reading."""
        self.assertIsNone(edd.parse_aaii_sentiment(V3_MARKUP))

    def test_figures_without_a_week_label_still_parse(self):
        """The date is the staleness signal, but losing it is not a reason to
        drop three good figures. The tile names the gap instead."""
        html = LIVE_MARKUP.replace(
            '<span class="ssv2-gauge-week">Week ending August 26, 2026</span>', ""
        )
        reading = edd.parse_aaii_sentiment(html)
        self.assertIsNotNone(reading)
        self.assertEqual(reading["bullish"], 32.9)
        self.assertIsNone(reading["week_ending"])

    def test_figures_that_do_not_sum_to_a_hundred_yield_nothing(self):
        """A shape change that still matches the hooks but grabs the wrong
        numbers is caught here."""
        html = LIVE_MARKUP.replace(
            '<div class="ssv2-snum bear">44.4%</div>',
            '<div class="ssv2-snum bear">4.4%</div>',
        )
        self.assertIsNone(edd.parse_aaii_sentiment(html))

    def test_a_malformed_figure_yields_nothing_rather_than_raising(self):
        """`[\\d.]+` matches strings float() rejects.

        This must return None, not raise: fetch_aaii_sentiment calls the parse
        outside its request try, and neither update_breadth_history nor
        export_all guards the chain, so an escaping ValueError would abort the
        entire daily export rather than just blanking one tile.
        """
        html = LIVE_MARKUP.replace(
            '<div class="ssv2-snum bull">32.9%</div>',
            '<div class="ssv2-snum bull">3.2.9%</div>',
        )
        self.assertIsNone(edd.parse_aaii_sentiment(html))

    def test_an_agreeing_duplicate_gauge_still_parses(self):
        """A responsive layout can render the gauge twice, desktop and mobile.

        Both copies carry the same reading, so blanking the tile over it would
        be a false negative on a perfectly readable page.
        """
        reading = edd.parse_aaii_sentiment(LIVE_MARKUP + LIVE_MARKUP)
        self.assertIsNotNone(reading)
        self.assertEqual(reading["bullish"], 32.9)
        self.assertEqual(reading["bearish"], 44.4)

    def test_disagreeing_duplicate_gauges_yield_nothing(self):
        """Two different readings on one page: no basis for picking either, and
        the layout has genuinely moved."""
        second = LIVE_MARKUP.replace(
            '<div class="ssv2-snum bull">32.9%</div>',
            '<div class="ssv2-snum bull">41.0%</div>',
        )
        self.assertIsNone(edd.parse_aaii_sentiment(LIVE_MARKUP + second))


class UpdateBreadthHistoryAaiiTests(unittest.TestCase):
    """The caller side: a dead sentiment source does not take the export down
    with it, and the two empty states stay distinguishable.

    `update_breadth_history` loads the published payload, assigns only the keys
    it successfully fetched, and writes the whole object back. That merge is
    load-bearing for NAAIM: a failed fetch leaves the previous reading in place
    *with its own survey date*, which then visibly ages. That is the intended
    behaviour, not the 2026-08 bug -- the bug was a frozen number carrying no
    date at all, so nothing on screen could contradict it.

    Every fetch this function calls must be patched here. An unpatched one
    reaches the network on every suite run and still passes its assertions,
    which is exactly the kind of silence these tests exist to prevent.
    """

    def _run_with(self, aaii_return, existing_payload, tmpdir,
                  naaim_return=None):
        from pathlib import Path

        out = Path(tmpdir)
        history_file = out / "market_breadth.json"
        history_file.write_text(json.dumps(existing_payload), encoding="utf-8")

        with patch.object(edd, "OUTPUT_DIR", out), \
             patch.object(edd, "BREADTH_HISTORY_FILE", history_file), \
             patch.object(edd, "BREADTH_FILE", out / "absent_latest.json"), \
             patch.object(edd, "fetch_barchart_breadth", return_value=None), \
             patch.object(edd, "fetch_cnn_fear_greed", return_value=None), \
             patch.object(edd, "fetch_naaim_exposure", return_value=naaim_return), \
             patch.object(edd, "fetch_aaii_sentiment", return_value=aaii_return):
            edd.update_breadth_history()

        return json.loads(history_file.read_text(encoding="utf-8"))

    AAII_READING = {
        "bullish": 32.9,
        "neutral": 22.6,
        "bearish": 44.4,
        "week_ending": "2026-08-26",
    }

    def test_a_fresh_naaim_reading_publishes(self):
        import tempfile

        naaim = {"value": 102.66, "as_of": "2026-08-26"}
        with tempfile.TemporaryDirectory() as tmp:
            written = self._run_with(
                self.AAII_READING, {"ncfd": {"current": 49.25, "history": [49.25]}},
                tmp, naaim_return=naaim,
            )

        self.assertEqual(written["naaim"], naaim)
        self.assertEqual(written["aaii"], self.AAII_READING)
        self.assertEqual(written["ncfd"]["current"], 49.25)

    def test_a_dead_naaim_fetch_keeps_the_previous_reading_and_its_date(self):
        """The staleness signal is the surviving `as_of`, which ages in place.
        Wiping the key instead would drop the last known reading and make NAAIM
        behave unlike the AAII tile beside it."""
        import tempfile

        stale = {"value": 94.49, "as_of": "2026-08-19"}
        with tempfile.TemporaryDirectory() as tmp:
            written = self._run_with(
                self.AAII_READING, {"naaim": stale}, tmp, naaim_return=None,
            )

        self.assertEqual(written["naaim"], stale)

    def test_a_dead_naaim_fetch_with_no_prior_reading_leaves_the_key_absent(self):
        """The other empty state: first run, or the window after a docs/data
        reset. The tile keeps its markup em dashes and claims nothing."""
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            written = self._run_with(
                self.AAII_READING, {"ncfd": {"current": 49.25, "history": [49.25]}},
                tmp, naaim_return=None,
            )

        self.assertNotIn("naaim", written)

    def test_a_fresh_naaim_reading_replaces_a_stale_one(self):
        import tempfile

        fresh = {"value": 102.66, "as_of": "2026-08-26"}
        with tempfile.TemporaryDirectory() as tmp:
            written = self._run_with(
                self.AAII_READING, {"naaim": {"value": 94.49, "as_of": "2026-08-19"}},
                tmp, naaim_return=fresh,
            )

        self.assertEqual(written["naaim"], fresh)

    def test_every_fetch_the_function_calls_is_patched_here(self):
        """Guards the helper itself. `update_breadth_history` gained a NAAIM
        fetch once; the next source added will slip through the same gap and
        quietly reach the network on every suite run."""
        import inspect

        source = inspect.getsource(edd.update_breadth_history)
        called = {
            name for name in (
                "fetch_barchart_breadth", "fetch_cnn_fear_greed",
                "fetch_aaii_sentiment", "fetch_naaim_exposure",
            ) if f"{name}(" in source
        }
        patched = set(inspect.getsource(self._run_with).split())
        for name in called:
            with self.subTest(fetch=name):
                self.assertTrue(
                    any(name in token for token in patched),
                    f"update_breadth_history calls {name} but _run_with does "
                    "not patch it -- these tests would hit the network",
                )

    def test_a_dead_aaii_source_does_not_fail_the_export(self):
        """This is the path a v3 rename takes. Breadth collection is
        non-critical; the other tiles must still publish."""
        import tempfile

        existing = {"ncfd": {"current": 49.25, "history": [49.25]}}
        with tempfile.TemporaryDirectory() as tmp:
            written = self._run_with(None, existing, tmp)

        self.assertNotIn("aaii", written)
        self.assertEqual(written["ncfd"]["current"], 49.25)


if __name__ == "__main__":
    unittest.main()
