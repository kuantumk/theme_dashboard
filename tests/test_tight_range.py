"""Tight range (TR): the tightness ratio and the shape + gate flag.

Source: the canonical "Tight Range (TR)" note, v2.1.

    ratio_N     = ((max - min) / mean of the last N closes) / (ADR% * sqrt(N))
    shape       = ratio_N <= 0.35 OR ratio_N in the stock's own tightest 15%
                  (same N, last 120 sessions), for some N in 3..10
    gate        = trend OR rising-MA hold OR swing-low support
    tight_range = shape AND gate

These tests pin the arithmetic, each gate branch, the fail-closed behaviour,
and one property the note's reference code does not have: no bar reads a bar
after itself. A back-dated session must report what it would have reported
live.
"""

import inspect
import unittest

import numpy as np
import pandas as pd

from src.indicators import create_technical_indicators as cti
from src.indicators.create_technical_indicators import (
    compute_ema_pair,
    compute_sma50_full,
    compute_tight_range,
    compute_tight_ratio,
    tight_range_config,
)


def _adr(n, value=0.05):
    return pd.Series([value] * n, dtype=float)


def _run(close, adr=0.05, low=None, ema10=None, ema20=None, sma50=None, **kw):
    """compute_tight_range on a close series, with real MAs unless supplied."""
    close = pd.Series(close, dtype=float)
    n = len(close)
    e10, e20 = compute_ema_pair(close)
    return compute_tight_range(
        close * 0.99 if low is None else pd.Series(low, dtype=float),
        close,
        _adr(n, adr) if np.isscalar(adr) else pd.Series(adr, dtype=float),
        e10 if ema10 is None else pd.Series(ema10, dtype=float),
        e20 if ema20 is None else pd.Series(ema20, dtype=float),
        compute_sma50_full(close) if sma50 is None else pd.Series(sma50, dtype=float),
        **kw)


def _uptrend_then_flat(flat=5, rise=60, step=0.01):
    """A steady climb, then `flat` closes pinned at the top."""
    up = list(100 * (1 + step) ** np.arange(rise))
    return up + [up[-1]] * flat


class TightRatioTests(unittest.TestCase):
    def test_flat_closes_score_zero(self):
        r = compute_tight_ratio(pd.Series([50.0] * 6), _adr(6), 3)
        self.assertAlmostEqual(r.iloc[-1], 0.0)

    def test_a_span_of_adr_times_root_n_scores_one(self):
        # Closes spanning exactly 5% * sqrt(4) = 10% of their mean, ADR 5%.
        close = pd.Series([95.0, 100.0, 105.0, 100.0])
        r = compute_tight_ratio(close, _adr(4), 4)
        self.assertAlmostEqual(r.iloc[-1], 1.0, places=6)

    def test_the_notes_ntra_example(self):
        # NTRA 9/8-9/11/2026: closes within 1.9%, ADR 3.2% -> 1.9 / (3.2 * 2) = 0.30.
        close = pd.Series([100.0, 101.9, 100.5, 101.2])
        span = (close.max() - close.min()) / close.mean()
        r = compute_tight_ratio(close, _adr(4, 0.032), 4).iloc[-1]
        self.assertAlmostEqual(r, span / (0.032 * 2), places=9)
        self.assertAlmostEqual(r, 0.30, delta=0.01)

    def test_root_n_puts_short_and_long_windows_on_one_scale(self):
        # The same % span reads sqrt(9/3) = 1.73x tighter over 9 closes than 3.
        three = compute_tight_ratio(pd.Series([100.0, 102.0, 101.0]), _adr(3), 3).iloc[-1]
        nine = compute_tight_ratio(
            pd.Series([100.0, 101.0, 102.0, 101.5, 100.5, 101.0, 101.2, 100.8, 101.0]),
            _adr(9), 9).iloc[-1]
        span3 = 2.0 / np.mean([100.0, 102.0, 101.0])
        span9 = 2.0 / np.mean([100.0, 101.0, 102.0, 101.5, 100.5, 101.0, 101.2, 100.8, 101.0])
        self.assertAlmostEqual(three / nine, (span3 / span9) * np.sqrt(3), places=6)

    def test_a_steady_drift_is_not_tight(self):
        # The note's "quiet ascent": small candles, but price is moving.
        close = pd.Series([64.79, 67.12, 69.51])
        r = compute_tight_ratio(close, _adr(3, 0.067), 3).iloc[-1]
        self.assertGreater(r, 0.50)

    def test_path_independent(self):
        adr = _adr(3)
        walked = compute_tight_ratio(pd.Series([100.0, 102.5, 105.0]), adr, 3).iloc[-1]
        zigzag = compute_tight_ratio(pd.Series([100.0, 105.0, 102.5]), adr, 3).iloc[-1]
        self.assertAlmostEqual(walked, zigzag, places=6)

    def test_partial_window_is_nan(self):
        r = compute_tight_ratio(pd.Series([10.0, 10.1]), _adr(2), 3)
        self.assertTrue(r.isna().all())

    def test_nan_or_zero_adr_is_nan_not_infinity(self):
        adr = _adr(5)
        adr.iloc[-1] = np.nan
        self.assertTrue(pd.isna(compute_tight_ratio(pd.Series([10.0] * 5), adr, 3).iloc[-1]))
        r = compute_tight_ratio(pd.Series([10.0, 11.0, 10.5]), _adr(3, 0.0), 3)
        self.assertTrue(r.isna().all())
        self.assertFalse(np.isinf(r.to_numpy(dtype=float)).any())


class ShapeTests(unittest.TestCase):
    def test_flat_closes_after_an_uptrend_are_a_tight_range(self):
        out = _run(_uptrend_then_flat(flat=5)).iloc[-1]
        self.assertTrue(out['tight_range'])
        self.assertEqual(out['tight_gate'], 'trend')
        self.assertGreaterEqual(out['tight_len'], 5)
        self.assertLessEqual(out['tightness'], 0.35)

    def test_the_longest_passing_window_is_reported(self):
        # Five flat closes after a 1%/day climb; the last rising close sits at
        # the same level, so six closes are flat. With the percentile rule off,
        # N=6 passes and N=7 reaches a close 1% lower: 1 / sqrt(7) = 0.38 > 0.35.
        out = _run(_uptrend_then_flat(flat=5), adr=0.01, pctile_max=0).iloc[-1]
        self.assertEqual(out['tight_len'], 6)
        self.assertAlmostEqual(out['tightness'], 0.0)

    def test_the_window_never_exceeds_max_window(self):
        out = _run(_uptrend_then_flat(flat=30)).iloc[-1]
        self.assertEqual(out['tight_len'], 10)

    def test_a_quiet_ascent_fails_the_shape(self):
        # +3% a day against a 3% ADR: small candles, but the closes travel.
        close = list(100 * 1.03 ** np.arange(80))
        out = _run(close, adr=0.03, pctile_max=0).iloc[-1]
        self.assertEqual(out['tight_len'], 0)
        self.assertFalse(out['tight_range'])

    def test_the_own_history_percentile_passes_on_its_own(self):
        # A stock that never gets under 0.35 can still pass by being at its
        # own tightest. Wide alternating closes, then a calmer stretch.
        rng = np.random.default_rng(7)
        wide = list(100 + rng.choice([-6.0, 6.0], size=140))
        calm = [100.0, 102.5, 98.0, 101.5]
        out = _run(wide + calm, ratio_max=0.0).iloc[-1]
        self.assertGreater(out['tight_len'], 0)
        self.assertLessEqual(out['tight_pctile'], 15)
        self.assertGreater(out['tightness'], 0.35)

    def test_no_window_passes_in_the_first_min_history_bars(self):
        out = _run([100.0] * 40)
        self.assertFalse(out['tight_range'].iloc[:19].any())
        self.assertTrue((out['tight_len'].iloc[:19] == 0).all())

    def test_a_missing_adr_fails_closed(self):
        out = _run(_uptrend_then_flat(), adr=np.nan)
        self.assertFalse(out['tight_range'].any())
        self.assertTrue((out['tight_len'] == 0).all())
        self.assertTrue(out['tightness'].isna().all())
        self.assertTrue((out['tight_gate'] == '').all())

    def test_output_columns_and_types(self):
        out = _run(_uptrend_then_flat())
        self.assertEqual(list(out.columns),
                         ['tightness', 'tight_len', 'tight_pctile', 'tight_gate', 'tight_range'])
        self.assertEqual(out['tight_range'].dtype, bool)
        self.assertTrue(np.issubdtype(out['tight_len'].dtype, np.integer))
        self.assertEqual(out['tightness'].dtype, float)

    def test_a_failed_shape_publishes_no_gate_and_no_percentile(self):
        close = list(100 * 1.03 ** np.arange(80))
        out = _run(close, adr=0.03, pctile_max=0).iloc[-1]
        self.assertEqual(out['tight_gate'], '')
        self.assertTrue(np.isnan(out['tight_pctile']))
        # tightness still reports the tightest window, for reference.
        self.assertTrue(np.isfinite(out['tightness']))


class GateTests(unittest.TestCase):
    """Each branch in isolation. The MAs are supplied directly so a test can
    set exactly where they sit; the closes decline into the window so no bar
    before it is a swing low and the support branch stays silent."""

    N = 80

    def _declining_then_flat(self):
        down = list(np.linspace(130, 100.5, self.N - 10))
        return down + [100.0] * 10

    def _gate(self, ema10, ema20, sma50):
        close = self._declining_then_flat()
        out = _run(close, ema10=[ema10] * self.N, ema20=[ema20] * self.N, sma50=sma50)
        return out.iloc[-1]

    def _rising(self, end):
        return list(np.linspace(end - 3, end, self.N))

    def test_trend_branch(self):
        out = self._gate(ema10=99.5, ema20=99.0, sma50=[120.0] * self.N)
        self.assertEqual(out['tight_gate'], 'trend')

    def test_a_rising_ma_underfoot_holds(self):
        # Close 100 is below EMA20 100.2, so trend fails. EMA10 sits 0.02 ADR
        # under EMA20 (flat, not rolled). A rising SMA50 at 99 is underfoot.
        out = self._gate(ema10=100.1, ema20=100.2, sma50=self._rising(99.0))
        self.assertEqual(out['tight_gate'], 'ma_hold')
        self.assertTrue(out['tight_range'])

    def test_a_rising_ma_overhead_does_not_hold(self):
        # The MXL July case: the same shape below a rising 50 SMA broke down.
        out = self._gate(ema10=100.1, ema20=100.2, sma50=self._rising(101.0))
        self.assertEqual(out['tight_gate'], '')
        self.assertFalse(out['tight_range'])
        self.assertGreater(out['tight_len'], 0)

    def test_a_falling_ma_underfoot_does_not_hold(self):
        falling = list(np.linspace(102.0, 99.0, self.N))
        out = self._gate(ema10=100.1, ema20=100.2, sma50=falling)
        self.assertFalse(out['tight_range'])

    def test_a_rolled_over_ema10_does_not_hold(self):
        # EMA10 0.14 ADR under EMA20: clearly rolled over, past the 0.1 limit.
        out = self._gate(ema10=99.5, ema20=100.2, sma50=self._rising(99.0))
        self.assertFalse(out['tight_range'])

    def test_a_ma_too_far_below_the_window_does_not_hold(self):
        # Lowest close 100 sits 4% over the SMA50 at 96: beyond 0.6 x 5% ADR.
        out = self._gate(ema10=100.1, ema20=100.2, sma50=self._rising(96.0))
        self.assertFalse(out['tight_range'])

    def _support_series(self, window_low):
        # Decline to a swing low at 90 thirty bars before the end, rally, fall
        # back, then sit flat at `window_low`. The fall-back stops above the
        # window so a longer window cannot reach a lower close.
        down = list(np.linspace(120, 91, 30)) + [90.0]
        rally = list(np.linspace(92, 104, 15))
        back = list(np.linspace(103, window_low + 1.5, 15))
        flat = [window_low + 0.3, window_low + 0.1, window_low + 0.2, window_low + 0.25]
        close = down + rally + back + flat
        low = [c * 0.995 for c in close]
        low[30] = 89.5
        return close, low

    def test_a_window_holding_a_swing_low_passes_on_support(self):
        close, low = self._support_series(window_low=91.0)
        n = len(close)
        out = _run(close, low=low, ema10=[95.0] * n, ema20=[97.0] * n,
                   sma50=[100.0] * n).iloc[-1]
        self.assertEqual(out['tight_gate'], 'support')

    def test_an_undercut_and_reclaim_passes_on_support(self):
        close, low = self._support_series(window_low=97.0)
        low[-2] = 89.0   # a wick through the old low inside the window
        n = len(close)
        out = _run(close, low=low, ema10=[95.0] * n, ema20=[99.0] * n,
                   sma50=[100.0] * n).iloc[-1]
        self.assertEqual(out['tight_gate'], 'support')

    def test_a_window_far_from_any_swing_low_fails_support(self):
        close, low = self._support_series(window_low=97.0)
        n = len(close)
        out = _run(close, low=low, ema10=[95.0] * n, ema20=[99.0] * n,
                   sma50=[100.0] * n).iloc[-1]
        self.assertEqual(out['tight_gate'], '')


class PointInTimeTests(unittest.TestCase):
    def test_no_bar_reads_a_later_bar(self):
        """Every bar answers the same on the full series as on a series that
        ends at that bar. The note's reference swing test slices ten bars past
        the candidate, which on a short window reaches past the scored bar;
        the percentile and the support search must both stop at it."""
        rng = np.random.default_rng(11)
        n = 260
        close = pd.Series(100 * np.exp(np.cumsum(rng.normal(0, 0.02, n))))
        high = close * (1 + rng.uniform(0.005, 0.04, n))
        low = close * (1 - rng.uniform(0.005, 0.04, n))
        adr = (high / low).rolling(20, min_periods=10).mean() - 1

        def tr(k):
            c = close.iloc[:k]
            e10, e20 = compute_ema_pair(c)
            return compute_tight_range(low.iloc[:k], c, adr.iloc[:k], e10, e20,
                                       compute_sma50_full(c))

        full = tr(n)
        self.assertTrue(full['tight_range'].any(), 'fixture flags nothing; widen it')
        for k in range(30, n, 7):
            cut = tr(k).iloc[-1]
            ref = full.iloc[k - 1]
            for col in ('tight_len', 'tight_gate', 'tight_range'):
                self.assertEqual(cut[col], ref[col], f'{col} at bar {k - 1}')
            for col in ('tightness', 'tight_pctile'):
                a, b = cut[col], ref[col]
                self.assertTrue((np.isnan(a) and np.isnan(b)) or abs(a - b) < 1e-12,
                                f'{col} at bar {k - 1}: {a} vs {b}')


class ConfigTests(unittest.TestCase):
    def test_defaults_match_the_note(self):
        cfg = tight_range_config({})
        self.assertEqual((cfg['min_window'], cfg['max_window']), (3, 10))
        self.assertEqual(cfg['ratio_max'], 0.35)
        self.assertEqual(cfg['pctile_max'], 15.0)
        self.assertEqual(cfg['pctile_lookback'], 120)
        self.assertEqual(cfg['ma_hold_adr'], 0.6)
        self.assertEqual(cfg['ema_rollover_adr'], 0.1)
        self.assertEqual(cfg['support_adr'], 1.0)

    def test_config_overrides_a_default(self):
        self.assertEqual(tight_range_config({'tight_range': {'ratio_max': 0.25}})['ratio_max'], 0.25)

    def test_an_unknown_key_raises(self):
        with self.assertRaises(ValueError):
            tight_range_config({'tight_range': {'ratio_maxx': 0.25}})

    def test_the_pipeline_feeds_the_full_50_bar_mean(self):
        # The 25-bar `sma50` is a partial mean wearing a 50-day name on a young
        # listing; the gate reads `sma50_full` like the highlight ladder does.
        src = inspect.getsource(cti.calculate_technical_indicators)
        self.assertIn("daily['ema20'], daily['sma50_full'], **_tight_cfg", src)


if __name__ == '__main__':
    unittest.main()
