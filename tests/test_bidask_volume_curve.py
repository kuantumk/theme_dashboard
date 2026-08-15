"""Intraday volume-curve tests.

The curve exists to convert TradingView's `relative_volume_10d_calc` — a
session-to-date figure divided by a FULL-DAY average — into a pace that means
the same thing at 09:35 as it does at 15:00.
"""

import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from src.bidask.volume_curve import (
    CURVE,
    MIN_FRACTION,
    SESSION_CLOSE_MIN,
    SESSION_OPEN_MIN,
    expected_fraction,
    volume_pace,
)

ET = ZoneInfo("America/New_York")


def at(hour: int, minute: int) -> datetime:
    return datetime(2026, 8, 14, hour, minute, tzinfo=ET)


class TestCurveShape(unittest.TestCase):
    def test_curve_is_monotone_non_decreasing(self):
        fractions = [f for _, f in CURVE]
        self.assertEqual(fractions, sorted(fractions))

    def test_curve_minutes_are_sorted_and_unique(self):
        minutes = [m for m, _ in CURVE]
        self.assertEqual(minutes, sorted(set(minutes)))

    def test_curve_spans_exactly_the_regular_session(self):
        self.assertEqual(CURVE[0][0], SESSION_OPEN_MIN)
        self.assertEqual(CURVE[-1][0], SESSION_CLOSE_MIN)
        self.assertEqual(CURVE[0][1], 0.0)
        self.assertEqual(CURVE[-1][1], 1.0)


class TestExpectedFraction(unittest.TestCase):
    def test_interpolates_between_measured_points(self):
        # 10:00 -> 0.1926 and 10:05 -> 0.2149, so 10:02-ish lands between them.
        value = expected_fraction(at(10, 2))
        self.assertGreater(value, expected_fraction(at(10, 0)))
        self.assertLess(value, expected_fraction(at(10, 5)))

    def test_rises_monotonically_through_the_session(self):
        marks = [at(9, 35), at(10, 0), at(10, 30), at(11, 0), at(13, 0),
                 at(15, 0), at(15, 45)]
        values = [expected_fraction(m) for m in marks]
        self.assertEqual(values, sorted(values))

    def test_never_returns_zero(self):
        """A zero denominator would make pace infinite at the opening bell."""
        for moment in (at(9, 30), at(9, 31), at(4, 0), at(8, 15)):
            self.assertGreaterEqual(expected_fraction(moment), MIN_FRACTION)

    def test_pre_market_uses_the_floor_rather_than_zero(self):
        self.assertEqual(expected_fraction(at(7, 0)), MIN_FRACTION)

    def test_after_the_close_is_a_full_session(self):
        self.assertEqual(expected_fraction(at(16, 30)), 1.0)
        self.assertEqual(expected_fraction(at(19, 59)), 1.0)

    def test_naive_datetime_is_read_as_eastern(self):
        naive = datetime(2026, 8, 14, 10, 0)
        self.assertAlmostEqual(expected_fraction(naive), expected_fraction(at(10, 0)))


class TestVolumePace(unittest.TestCase):
    def test_pace_of_one_is_a_normal_session(self):
        """A stock whose rvol equals the expected fraction is trading normally."""
        for moment in (at(9, 45), at(10, 30), at(13, 0), at(15, 0)):
            frac = expected_fraction(moment)
            self.assertAlmostEqual(volume_pace(frac, frac), 1.0)

    def test_same_pace_reads_the_same_at_every_hour(self):
        """This is the whole point: the raw floor is a different filter each hour.

        A stock trading at twice its normal rate must read 2.0 at 09:35 and at
        15:00 alike. Against the raw figure it reads 0.18 and 1.63.
        """
        paces = []
        for moment in (at(9, 35), at(10, 0), at(10, 30), at(12, 0), at(15, 0)):
            frac = expected_fraction(moment)
            paces.append(volume_pace(2.0 * frac, frac))
        for value in paces:
            self.assertAlmostEqual(value, 2.0)

    def test_missing_rvol_is_no_pace(self):
        self.assertEqual(volume_pace(None, 0.2), 0.0)
        self.assertEqual(volume_pace(float("nan"), 0.2), 0.0)

    def test_regression_two_times_pace_was_unreachable_in_the_morning(self):
        """BE/FCEL, 2026-08-14: the defect this module exists to remove.

        FCEL ran at ~2x normal pace from 10:00 while up double digits. Against
        the raw 1.5 floor that reads 0.39 and is rejected; the volume leg could
        not admit it at any point before roughly 13:00.
        """
        frac_10am = expected_fraction(at(10, 0))
        raw_rvol_at_2x = 2.0 * frac_10am
        self.assertLess(raw_rvol_at_2x, 1.5)             # the old gate rejects
        self.assertGreaterEqual(volume_pace(raw_rvol_at_2x, frac_10am), 1.5)

    def test_the_raw_floor_demanded_absurd_pace_at_the_open(self):
        """At 09:35 a raw 1.5 floor is a demand for >10x normal participation."""
        frac = expected_fraction(at(9, 35))
        self.assertGreater(1.5 / frac, 10.0)


class TestScalarAndVectorPathsAgree(unittest.TestCase):
    """`volume_pace` and the gate's pandas expression must stay identical.

    The gate cannot call `volume_pace` per row without reintroducing a Python
    loop over ~1,900 rows every poll, so the rule is written twice: once as a
    scalar and once as a column expression. They share `pace_divisor`, and this
    pins the rest — the first draft of this change had the gate re-deriving the
    clamp inline, which is how the two would have drifted.
    """

    READINGS = [0.05, 0.5, 1.0, 3.7, 0.0, -1.0, None, float("nan")]
    FRACTIONS = [0.0908, 0.1926, 0.2982, 0.5021, 0.8103, 1.0]

    def test_every_reading_matches_at_every_hour(self):
        import pandas as pd

        from src.bidask.volume_curve import pace_divisor

        for fraction in self.FRACTIONS:
            column = pd.to_numeric(pd.Series(self.READINGS), errors="coerce")
            vectorized = (column.fillna(0.0).clip(lower=0.0)
                          / pace_divisor(fraction)).tolist()
            scalar = [volume_pace(v, fraction) for v in self.READINGS]
            for reading, want, got in zip(self.READINGS, scalar, vectorized):
                self.assertAlmostEqual(
                    want, got, places=12,
                    msg=f"rvol={reading!r} at fraction {fraction} "
                        f"differs between the scalar and column paths")

    def test_both_paths_refuse_to_pass_an_unknown(self):
        """A missing reading must never clear a floor of 1.5."""
        for fraction in self.FRACTIONS:
            for missing in (None, float("nan"), 0.0, -2.0):
                self.assertEqual(volume_pace(missing, fraction), 0.0)


if __name__ == "__main__":
    unittest.main()
