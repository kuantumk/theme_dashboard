"""Relative Volume at Time across the extended session.

The board now runs pre-market and after hours, so the measure re-anchors from
09:30 to **04:00** — the anchor TradingView's own app uses when the chart shows
extended hours. Both legs still cut at the same clock time; the window they cut
from is simply longer.

Every fixture here is synthetic. The bar source is a websocket and a unit test
must never touch it, so the network path gets a separate live smoke check.
"""

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from src.bidask.rvol_at_time import (
    BARS_PER_SESSION,
    BAR_MINUTES,
    CACHE_VERSION,
    SESSION_CLOSE_MIN,
    SESSION_MINUTES,
    SESSION_OPEN_MIN,
    baseline_at,
    build_profiles,
    cache_path,
    extended_volume_is_dead,
    load_profiles,
    minutes_since_open,
    rvol_at_time,
    save_profiles,
)
from src.bidask.tvbars import absorb_bars, bars_to_frame, qualified_symbols

ET = ZoneInfo("America/New_York")

# 04:00-to-09:30 is 330 minutes; 09:30-to-16:00 is 390; 16:00-to-20:00 is 240.
PRE_MINUTES = (9 * 60 + 30) - SESSION_OPEN_MIN
REGULAR_MINUTES = (16 * 60) - (9 * 60 + 30)


def at(hour, minute=0):
    return datetime(2026, 9, 17, hour, minute, tzinfo=ET)


def slot_of(hour, minute=0):
    """Index of the 5-minute slot holding this clock time."""
    return ((hour * 60 + minute) - SESSION_OPEN_MIN) // BAR_MINUTES


def session_frame(day, *, pre_total=0.0, regular_total=0.0, post_total=0.0,
                  drop_empty=True):
    """One extended session's 5-minute bars, volume spread evenly per window.

    `drop_empty` mirrors the real feed, which returns no bar at all for a
    5-minute window in which nothing traded. That absence is the normal shape
    of a quiet pre-market and must not be confused with a bar carrying zero.
    """
    stamps, volumes = [], []
    windows = (
        (slot_of(4, 0), slot_of(9, 30), pre_total),
        (slot_of(9, 30), slot_of(16, 0), regular_total),
        (slot_of(16, 0), BARS_PER_SESSION, post_total),
    )
    base = datetime(2026, 9, day, 4, 0, tzinfo=ET)
    for start, end, total in windows:
        count = end - start
        share = (total / count) if count else 0.0
        for index in range(start, end):
            if drop_empty and share <= 0:
                continue
            stamps.append(base + pd.Timedelta(minutes=BAR_MINUTES * index))
            volumes.append(share)
    return pd.DataFrame({"Volume": volumes}, index=pd.DatetimeIndex(stamps, tz=ET))


def history(days, **kwargs):
    return pd.concat([session_frame(day, **kwargs) for day in days])


class TestAnchor(unittest.TestCase):
    """The clock the whole measure hangs from."""

    def test_the_anchor_is_04_00(self):
        self.assertEqual(SESSION_OPEN_MIN, 4 * 60)
        self.assertEqual(SESSION_CLOSE_MIN, 20 * 60)
        self.assertEqual(SESSION_MINUTES, 960)
        self.assertEqual(BARS_PER_SESSION, 192)

    def test_pre_market_minutes_are_counted_not_clamped_to_zero(self):
        """Under the 09:30 anchor every pre-market moment read 0 elapsed."""
        self.assertEqual(minutes_since_open(at(4, 0)), 0.0)
        self.assertEqual(minutes_since_open(at(7, 0)), 180.0)
        self.assertEqual(minutes_since_open(at(9, 29)), 329.0)

    def test_the_regular_open_is_330_minutes_in(self):
        self.assertEqual(minutes_since_open(at(9, 30)), float(PRE_MINUTES))
        self.assertEqual(minutes_since_open(at(16, 0)),
                         float(PRE_MINUTES + REGULAR_MINUTES))

    def test_after_hours_keeps_accumulating_past_16_00(self):
        """A frozen counter after the close is the failure this replaces."""
        self.assertGreater(minutes_since_open(at(18, 0)), minutes_since_open(at(16, 0)))
        self.assertEqual(minutes_since_open(at(20, 0)), float(SESSION_MINUTES))

    def test_clamped_outside_the_extended_day(self):
        self.assertEqual(minutes_since_open(at(3, 0)), 0.0)
        self.assertEqual(minutes_since_open(at(22, 0)), float(SESSION_MINUTES))


class TestPreMarketReading(unittest.TestCase):
    """A pre-market ratio must use a pre-market baseline, never a full-day one.

    `premarket_volume / average_volume_10d_calc` was proposed and is wrong by a
    per-ticker factor — 374x for GNRC on 2026-09-17. The point of anchoring at
    04:00 is that the denominator is this ticker's own usual pre-market.
    """

    def setUp(self):
        # A quiet history: 2,000 shares pre-market, 1,000,000 in the session.
        self.profiles = build_profiles(
            {"AAA": history(range(3, 13), pre_total=2_000, regular_total=1_000_000)},
            sessions=10)
        self.profile = self.profiles["AAA"]

    def test_the_baseline_before_the_open_is_the_pre_market_mean(self):
        by_open = baseline_at(self.profile, minutes_since_open(at(9, 30)))
        self.assertAlmostEqual(by_open, 2_000.0, delta=1.0)

    def test_unusual_pre_market_volume_scores_far_above_one(self):
        """239,052 shares against a 200-5,724 history is the GNRC case."""
        ratio = rvol_at_time(200_000, self.profile, minutes_since_open(at(9, 0)))
        self.assertGreater(ratio, 50.0)

    def test_a_full_day_divisor_would_have_given_near_zero(self):
        """The error this anchor exists to prevent, stated as a number."""
        full_day_ratio = 200_000 / 1_002_000
        pre_market_ratio = rvol_at_time(200_000, self.profile,
                                        minutes_since_open(at(9, 0)))
        self.assertLess(full_day_ratio, 1.0)
        self.assertGreater(pre_market_ratio / full_day_ratio, 100.0)

    def test_the_reading_falls_when_the_regular_open_enters_the_mean(self):
        """The discontinuity the app shows at 09:30, and its signature.

        The numerator keeps accruing while the denominator jumps, so the plot
        climbs through pre-market and then drops. A flat reading across 09:30
        would mean the denominator is not time-of-day.
        """
        before = rvol_at_time(200_000, self.profile, minutes_since_open(at(9, 25)))
        after = rvol_at_time(300_000, self.profile, minutes_since_open(at(10, 0)))
        self.assertGreater(before, after)
        self.assertGreater(before, 50.0)
        self.assertLess(after, 10.0)


class TestAfterHoursReading(unittest.TestCase):
    def setUp(self):
        self.profile = build_profiles(
            {"AAA": history(range(3, 13), pre_total=2_000,
                            regular_total=1_000_000, post_total=40_000)},
            sessions=10)["AAA"]

    def test_the_baseline_grows_after_the_close(self):
        """A baseline that froze at 16:00 would inflate every evening ratio."""
        self.assertGreater(baseline_at(self.profile, minutes_since_open(at(18, 0))),
                           baseline_at(self.profile, minutes_since_open(at(16, 0))))

    def test_an_earnings_pop_after_the_bell_clears_a_1_5_floor(self):
        """GNRC ran +35.2% after the bell on 2026-09-16 and must be admitted."""
        by_18 = baseline_at(self.profile, minutes_since_open(at(18, 0)))
        ratio = rvol_at_time(by_18 * 2.5, self.profile, minutes_since_open(at(18, 0)))
        self.assertAlmostEqual(ratio, 2.5, places=6)
        self.assertGreater(ratio, 1.5)

    def test_a_quiet_evening_is_rejected_by_the_same_floor(self):
        by_18 = baseline_at(self.profile, minutes_since_open(at(18, 0)))
        ratio = rvol_at_time(by_18 * 1.1, self.profile, minutes_since_open(at(18, 0)))
        self.assertLess(ratio, 1.5)


class TestDeadExtendedVolume(unittest.TestCase):
    """yfinance returns the extended bars with zero volume on every one.

    A zero baseline is not "this ticker is quiet" — it is a zero denominator,
    and every pre-market ratio computed against it is infinite. The response
    must be rejected as unusable instead.
    """

    def _zero_extended(self):
        frame = session_frame(3, pre_total=2_000, regular_total=1_000_000)
        dead = frame.copy()
        pre = dead.index.map(lambda s: (s.hour * 60 + s.minute) < 9 * 60 + 30)
        dead.loc[np.asarray(pre), "Volume"] = 0.0
        return dead

    def test_zero_volume_extended_bars_are_detected(self):
        self.assertTrue(extended_volume_is_dead(self._zero_extended()))

    def test_a_symbol_with_dead_extended_volume_gets_no_profile(self):
        frames = pd.concat([self._zero_extended()] * 1)
        profiles = build_profiles({"AAA": frames}, sessions=10)
        self.assertNotIn("AAA", profiles)

    def test_absent_extended_bars_are_not_dead_volume(self):
        """A quiet name simply has no bar there. That is legitimate history."""
        quiet = session_frame(3, regular_total=1_000_000)
        self.assertFalse(extended_volume_is_dead(quiet))
        self.assertIn("AAA", build_profiles({"AAA": quiet}, sessions=10))

    def test_real_extended_volume_is_not_dead(self):
        self.assertFalse(extended_volume_is_dead(
            session_frame(3, pre_total=2_000, regular_total=1_000_000)))


class TestSessionCompleteness(unittest.TestCase):
    """Completeness is judged on regular-session bars, not on the whole day.

    Most of an extended day has no trade in it, so a raw bar count over the
    04:00-20:00 grid would drop every quiet name's entire history and leave it
    with no baseline at all — scored 0 and excluded, which reads as a dead
    universe rather than a bad completeness rule.
    """

    def test_a_quiet_pre_market_session_still_counts(self):
        frame = session_frame(3, pre_total=300, regular_total=1_000_000)
        self.assertLess(len(frame), BARS_PER_SESSION)
        self.assertIn("AAA", build_profiles({"AAA": frame}, sessions=10))

    def test_a_half_day_is_still_excluded(self):
        short = session_frame(4, regular_total=1_000_000).head(30)
        frame = pd.concat([session_frame(3, regular_total=1_000_000), short])
        profile = build_profiles({"AAA": frame}, sessions=10)["AAA"]
        self.assertAlmostEqual(profile[-1], 1_000_000.0, delta=1.0)

    def test_today_is_excluded_from_its_own_baseline(self):
        """The baseline depends on completed sessions only."""
        frame = history(range(3, 13), pre_total=2_000, regular_total=1_000_000)
        today = session_frame(17, pre_total=500_000, regular_total=1_000_000)
        with_today = build_profiles({"AAA": pd.concat([frame, today])},
                                    sessions=10, exclude_date="2026-09-17")["AAA"]
        self.assertAlmostEqual(baseline_at(with_today, minutes_since_open(at(9, 30))),
                               2_000.0, delta=1.0)


class TestCacheAcrossTheAnchorChange(unittest.TestCase):
    def test_version_bumped_past_the_09_30_anchor(self):
        self.assertGreaterEqual(CACHE_VERSION, 3)

    def test_a_cache_from_the_old_anchor_is_discarded(self):
        """Loading a 09:30-shaped curve would misread every slot by 66 bars."""
        with tempfile.TemporaryDirectory() as tmp:
            stale = {"version": 2, "session_date": "2026-09-17", "bar_minutes": 5,
                     "profiles": {"AAA": [1] * 78}}
            cache_path(Path(tmp), "2026-09-17").write_text(json.dumps(stale),
                                                           encoding="utf-8")
            self.assertEqual(load_profiles(Path(tmp), "2026-09-17"), {})

    def test_a_cache_from_another_session_is_discarded(self):
        curve = np.cumsum(np.full(BARS_PER_SESSION, 100.0))
        with tempfile.TemporaryDirectory() as tmp:
            save_profiles({"AAA": curve}, Path(tmp), "2026-09-16")
            self.assertEqual(load_profiles(Path(tmp), "2026-09-17"), {})

    def test_todays_cache_round_trips(self):
        curve = np.cumsum(np.full(BARS_PER_SESSION, 100.0))
        with tempfile.TemporaryDirectory() as tmp:
            self.assertTrue(save_profiles({"AAA": curve}, Path(tmp), "2026-09-17"))
            loaded = load_profiles(Path(tmp), "2026-09-17")
        np.testing.assert_allclose(loaded["AAA"], curve, rtol=1e-6)


class TestFailClosed(unittest.TestCase):
    def test_no_baseline_scores_zero_never_one(self):
        self.assertEqual(rvol_at_time(5_000_000, None, 400), 0.0)
        self.assertEqual(rvol_at_time(5_000_000, np.array([]), 400), 0.0)

    def test_a_zero_denominator_scores_zero_rather_than_infinity(self):
        curve = np.zeros(BARS_PER_SESSION)
        self.assertEqual(rvol_at_time(5_000_000, curve, 400), 0.0)

    def test_an_unusable_reading_scores_zero(self):
        curve = np.cumsum(np.full(BARS_PER_SESSION, 100.0))
        for bad in (None, float("nan"), float("inf"), 0, -5, "x"):
            self.assertEqual(rvol_at_time(bad, curve, 400), 0.0)


class TestBarDecoding(unittest.TestCase):
    """The chart socket's wire shape, decoded without a socket."""

    UPDATE = {
        "m": "timescale_update",
        "p": ["cs_abc", {"sds_1": {"s": [
            {"i": 0, "v": [1758096000, 1.0, 2.0, 0.5, 1.5, 1_000.0]},
            {"i": 1, "v": [1758096300, 1.5, 2.5, 1.0, 2.0, 2_000.0]},
        ]}}],
    }

    def test_absorbs_rows_keyed_by_timestamp(self):
        collected = {}
        absorb_bars(self.UPDATE, collected)
        self.assertEqual(sorted(collected), [1758096000, 1758096300])

    def test_a_later_update_replaces_the_same_bar(self):
        """The socket resends the forming bar; the newest value wins."""
        collected = {}
        absorb_bars(self.UPDATE, collected)
        absorb_bars({"m": "du", "p": ["cs_abc", {"sds_1": {"s": [
            {"i": 1, "v": [1758096300, 1.5, 2.5, 1.0, 2.0, 9_999.0]}]}}]},
            collected)
        self.assertEqual(collected[1758096300][5], 9_999.0)

    def test_unrelated_messages_are_ignored(self):
        collected = {}
        absorb_bars({"m": "quote_completed", "p": ["cs_abc"]}, collected)
        absorb_bars({"m": "timescale_update", "p": "not-a-list"}, collected)
        self.assertEqual(collected, {})

    def test_frame_is_eastern_sorted_and_carries_volume(self):
        collected = {}
        absorb_bars(self.UPDATE, collected)
        frame = bars_to_frame(collected)
        self.assertEqual(list(frame.columns), ["Open", "High", "Low", "Close", "Volume"])
        self.assertEqual(str(frame.index.tz), "America/New_York")
        self.assertTrue(frame.index.is_monotonic_increasing)
        self.assertEqual(frame["Volume"].tolist(), [1_000.0, 2_000.0])

    def test_an_empty_collection_is_an_empty_frame(self):
        self.assertTrue(bars_to_frame({}).empty)


class TestSymbolQualification(unittest.TestCase):
    """The socket resolves nothing without an exchange prefix."""

    def test_an_exchange_qualified_symbol_is_used_as_given(self):
        self.assertEqual(qualified_symbols("NYSE:GNRC"), ["NYSE:GNRC"])

    def test_a_bare_symbol_gets_candidate_prefixes(self):
        candidates = qualified_symbols("GNRC")
        self.assertTrue(all(":" in c for c in candidates))
        self.assertIn("NASDAQ:GNRC", candidates)
        self.assertIn("NYSE:GNRC", candidates)


if __name__ == "__main__":
    unittest.main()
