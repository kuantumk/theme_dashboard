"""Tightness and the tight-base flag.

`tightness` is the width of the band the last `window` CLOSES sat in, in ADR
units: `(max - min) / mean / adr_pct`. Lower is tighter. It is a containment
measure, not a per-bar one — a stock swinging a full ADR each way and closing
flat has a narrow band and large daily moves, and the band is what a trader
reads off a chart.

`tight_base` adds exactly one more condition: the close is not more than
`1 - high_frac` off its period high. That gate is a DISQUALIFIER, not a
selector — at 0.70 it passes 82% of rows on its own, so it chooses nothing and
only throws out wreckage. Forward 10-session excess runs 0.66pp BELOW the
universe baseline for bare tightness and above it once the gate applies.

⛔ There is NO moving-average test, and these tests pin its absence. An earlier
version required the close within 0.5 ATR of the EMA10/20. EMA distance is not
tightness — it duplicates a job the closing range already does, since a stock
drifting off its averages has a wide band by construction — and it ejected
names on rounding (PANW by $1.33 on a $330 stock). Swept 0.5-1.5 ATR it moved
ticker excess 0.08pp and theme IC 0.026, both inside noise.

⛔ Descriptive marker, never a ranking input. A period-high test carrying no
tightness at all scores a theme-level IC of +0.169 against ~+0.01 for the flag.

`period_high` is a rolling max of HIGHS (the pipeline passes `max50`), not of
closes. A calibration against close-based highs produced 0.90, which rejected
every name in the motivating case once run against the real column.
"""

import inspect
import unittest

import numpy as np
import pandas as pd

from src.indicators import create_technical_indicators as cti
from src.indicators.create_technical_indicators import (
    TIGHTNESS_FRACTION,
    TIGHTNESS_HIGH_FRAC,
    TIGHTNESS_HIGH_LOOKBACK,
    TIGHTNESS_WINDOW,
    compute_tight_base,
    compute_tightness,
)


def _adr(n, value=0.05):
    return pd.Series([value] * n)


class ComputeTightnessTests(unittest.TestCase):
    def test_flat_closes_score_zero(self):
        t = compute_tightness(pd.Series([50.0] * 6), _adr(6), window=3)
        self.assertAlmostEqual(t.iloc[-1], 0.0)

    def test_band_equal_to_one_adr_scores_one(self):
        # Closes spanning exactly 5% of their mean against a 5% ADR.
        close = pd.Series([97.5, 100.0, 102.5])
        t = compute_tightness(close, _adr(3), window=3)
        self.assertAlmostEqual(t.iloc[-1], 1.0, places=6)

    def test_a_steady_drift_reads_twice_as_loose_as_its_daily_moves(self):
        # THE defining case, and the reason there is no moving-average test.
        # Small daily moves all in one direction are a trend, not a base. The
        # per-bar mean cannot see that — it averages step sizes and is blind to
        # whether the steps accumulate. The band is not: it compounds them.
        # Four closes so the per-bar form has three ratios to average, while
        # the band still reads the last three closes.
        close = pd.Series([98.0, 100.0, 102.0, 104.0])
        adr = _adr(4, 0.02)
        band = compute_tightness(close, adr, window=3).iloc[-1]
        per_bar = (close.pct_change().abs() / adr).rolling(3).mean().iloc[-1]
        self.assertAlmostEqual(per_bar, 1.00, places=2)   # reads "tight"
        self.assertAlmostEqual(band, 1.96, places=2)      # reads "trending"
        self.assertGreater(band, 1.9 * per_bar)

    def test_the_same_extent_reads_the_same_however_it_got_there(self):
        # Path-independence is the flip side: three closes spanning one ADR are
        # one ADR wide whether they walked there or zigzagged.
        adr = _adr(3, 0.05)
        walked = compute_tightness(pd.Series([100.0, 102.5, 105.0]), adr, window=3).iloc[-1]
        zigzag = compute_tightness(pd.Series([100.0, 105.0, 102.5]), adr, window=3).iloc[-1]
        self.assertAlmostEqual(walked, zigzag, places=6)

    def test_partial_window_is_nan(self):
        t = compute_tightness(pd.Series([10.0, 10.1]), _adr(2), window=3)
        self.assertTrue(t.isna().all())

    def test_nan_adr_is_nan(self):
        adr = _adr(5)
        adr.iloc[-1] = np.nan
        t = compute_tightness(pd.Series([10.0] * 5), adr, window=3)
        self.assertTrue(pd.isna(t.iloc[-1]))

    def test_zero_adr_is_nan_not_infinity(self):
        t = compute_tightness(pd.Series([10.0, 11.0, 10.5]), _adr(3, 0.0), window=3)
        self.assertTrue(t.isna().all())
        self.assertFalse(np.isinf(t.to_numpy(dtype=float)).any())

    def test_direction_agnostic(self):
        up = pd.Series([100.0, 101.0, 102.0])
        down = pd.Series([102.0, 101.0, 100.0])
        a = compute_tightness(up, _adr(3), window=3).iloc[-1]
        b = compute_tightness(down, _adr(3), window=3).iloc[-1]
        self.assertAlmostEqual(a, b, places=6)

    def test_widening_the_window_never_tightens(self):
        # A wider window can only add closes, so the band cannot shrink.
        close = pd.Series([100.0, 104.0, 101.0, 103.0, 100.5, 102.0, 99.0])
        adr = _adr(len(close))
        short = compute_tightness(close, adr, window=3)
        long_ = compute_tightness(close, adr, window=5)
        both = short.notna() & long_.notna()
        self.assertTrue(both.any())
        self.assertTrue(bool((long_[both] >= short[both] - 1e-9).all()))

    def test_returns_a_float_series_aligned_to_the_input(self):
        close = pd.Series([10.0, 10.5, 10.2, 10.3])
        t = compute_tightness(close, _adr(4), window=3)
        self.assertEqual(len(t), len(close))
        self.assertTrue(t.index.equals(close.index))
        self.assertEqual(t.dtype, float)


class ComputeTightBaseTests(unittest.TestCase):
    N = 4

    def _frame(self, tightness, close, period_high):
        return dict(
            tightness=pd.Series([tightness] * self.N),
            close=pd.Series([close] * self.N),
            period_high=pd.Series([period_high] * self.N),
        )

    def _verdict(self, **kw):
        return bool(compute_tight_base(**kw).iloc[-1])

    def test_tight_and_holding_qualifies(self):
        self.assertTrue(self._verdict(
            **self._frame(tightness=0.20, close=95.0, period_high=100.0)))

    def test_loose_range_fails(self):
        self.assertFalse(self._verdict(
            **self._frame(tightness=0.60, close=95.0, period_high=100.0)))

    def test_a_broken_chart_fails_however_quiet_it_is(self):
        self.assertFalse(self._verdict(
            **self._frame(tightness=0.05, close=55.0, period_high=100.0)))

    def test_a_shallow_pullback_still_qualifies(self):
        # 75% of the high: down but not broken, admitted by design. The gate
        # disqualifies wreckage; it does not select leaders.
        self.assertTrue(self._verdict(
            **self._frame(tightness=0.20, close=75.0, period_high=100.0)))

    def test_nan_tightness_is_false_not_na(self):
        v = compute_tight_base(
            **self._frame(tightness=np.nan, close=95.0, period_high=100.0))
        self.assertFalse(bool(v.iloc[-1]))
        self.assertEqual(v.dtype, bool)

    def test_nan_period_high_is_false(self):
        self.assertFalse(self._verdict(
            **self._frame(tightness=0.20, close=95.0, period_high=np.nan)))

    def test_boundaries_are_inclusive(self):
        self.assertTrue(self._verdict(**self._frame(
            tightness=TIGHTNESS_FRACTION,
            close=TIGHTNESS_HIGH_FRAC * 100.0,
            period_high=100.0,
        )))

    def test_returns_boolean_dtype_aligned_to_the_input(self):
        v = compute_tight_base(**self._frame(0.2, 95.0, 100.0))
        self.assertEqual(v.dtype, bool)
        self.assertEqual(len(v), self.N)


class NoMovingAverageTestTests(unittest.TestCase):
    """The absence of an EMA conjunct is a decision, so it is pinned."""

    def test_signature_takes_no_moving_average_argument(self):
        params = set(inspect.signature(compute_tight_base).parameters)
        self.assertEqual(
            params,
            {'tightness', 'close', 'period_high', 'fraction', 'high_frac'},
            'compute_tight_base grew a parameter. If an EMA/MA test came back, '
            'read the module docstring first: EMA distance is not tightness, '
            'and it was removed on measurement, not taste.')

    def test_body_references_no_moving_average(self):
        src = inspect.getsource(compute_tight_base)
        body = src.split('"""')[-1]
        for token in ('ema', 'close_to_ma', 'atr'):
            self.assertNotIn(token, body.lower())

    def test_two_conjuncts_decide_everything(self):
        # Exhaustive over the 2x2: only tight AND holding passes.
        cases = {(True, True): True, (True, False): False,
                 (False, True): False, (False, False): False}
        for (tight, holding), expected in cases.items():
            v = compute_tight_base(
                tightness=pd.Series([0.1 if tight else 0.9] * 3),
                close=pd.Series([95.0 if holding else 40.0] * 3),
                period_high=pd.Series([100.0] * 3))
            self.assertEqual(bool(v.iloc[-1]), expected, f'{tight=} {holding=}')


class DefaultsTests(unittest.TestCase):
    def test_defaults_match_the_calibration(self):
        self.assertEqual(TIGHTNESS_WINDOW, 3)
        self.assertEqual(TIGHTNESS_FRACTION, 0.30)
        self.assertEqual(TIGHTNESS_HIGH_LOOKBACK, 50)
        self.assertEqual(TIGHTNESS_HIGH_FRAC, 0.70)

    def test_the_high_gate_admits_far_more_than_it_rejects(self):
        # Pins the disqualifier-not-selector intent against upward re-tuning.
        self.assertLessEqual(TIGHTNESS_HIGH_FRAC, 0.75)

    def test_the_high_lookback_has_a_matching_column(self):
        # The pipeline raises rather than substituting another window.
        self.assertIn(TIGHTNESS_HIGH_LOOKBACK, [30, 50, 60, 90, 120, 150, 252])
        self.assertIn('min_max_lookback',
                      inspect.getsource(cti.calculate_technical_indicators))


if __name__ == '__main__':
    unittest.main()
