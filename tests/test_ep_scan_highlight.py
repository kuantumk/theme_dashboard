"""The highlight tier on the two EP tables.

The EP scans hold no tightness source, so the coil rung cannot fire here. That
is the ladder working, not a gap — a rung with no source is absent, never
wrong. The scans do hold a live Finviz short float and a year of daily closes,
so the short rung and the two moving-average rungs are reachable.

⛔ `calculate_technicals` substitutes a mean of whatever history exists when a
ticker has fewer than 50 bars, and returns None only below 14. That partial
mean is finite and positive, which is the one shape the ladder's
zero-as-missing rule cannot catch — and a recent listing is exactly the
population an earnings-pivot scan surfaces. So the ladder reads `sma50_full`,
which is None below 50 bars, and never `sma50`, which `atr_multiple` still
needs. These tests pin that split.

⛔ The tier is built in each scan script's row loop, not in
`calculate_technicals`, because the short float never reaches the technicals
helper. A ladder call placed there would ship EP with the moving-average rungs
only, and nothing on screen would say so. The last test runs both loops over
one grid so they cannot drift apart.
"""

import unittest
from unittest import mock

import pandas as pd

from src.reporting import ep_scan_afternoon, ep_scan_common, ep_scan_morning


# ── Fixtures ─────────────────────────────────────────────────────────────────

def _history(bars, start=10.0, step=0.1):
    """A rising daily history of ``bars`` sessions."""
    index = pd.bdate_range('2026-01-02', periods=bars)
    close = pd.Series([start + step * i for i in range(bars)], index=index)
    return pd.DataFrame(
        {'Open': close, 'High': close + 0.5, 'Low': close - 0.5, 'Close': close},
        index=index,
    )


def _technicals(ema10=None, ema20=None, sma50_full=None):
    """The shape `calculate_technicals` returns, with the ladder inputs named."""
    return {
        'dist_52w_high': -5.0,
        'atr_multiple': 1.5,
        'sma50': 10.0,
        'sma50_full': sma50_full,
        'ema10': ema10,
        'ema20': ema20,
        'atr': 0.4,
        'close': 12.0,
    }


# EMA10 > EMA20 > SMA50: the stacked rung.
STACKED = dict(ema10=12.0, ema20=11.0, sma50_full=10.0)
# EMA10 under EMA20: no moving-average rung answers.
BROKEN = dict(ema10=11.0, ema20=12.0, sma50_full=10.0)
# EMA10 > EMA20, EMA20 under SMA50: the split rung.
SPLIT = dict(ema10=12.0, ema20=11.0, sma50_full=13.0)

# Each scan module, the entry point that runs its row loop, and the price
# helper that gates a ticker into that loop.
SCANS = (
    (ep_scan_afternoon, 'run_afternoon_scan', 'get_after_hours_price'),
    (ep_scan_morning, 'run_morning_scan', 'get_premarket_price'),
)


def _run_scan(module, entry_point, price_helper, *, short, technicals):
    """Run one scan's row loop over a single synthetic ticker."""
    with mock.patch.object(module, 'scan_finviz_tickers', return_value=['TEST']), \
            mock.patch.object(module, price_helper, return_value=(12.0, 10.0)), \
            mock.patch.object(
                module, 'get_fundamentals',
                return_value={'float': 8.0, 'short': short, 'avg_volume': 2e6}), \
            mock.patch.object(module, 'calculate_technicals',
                              return_value=dict(technicals)), \
            mock.patch.object(module, 'calculate_rvol_at_time', return_value=2.0), \
            mock.patch.object(module, 'get_ticker_news', return_value=[]), \
            mock.patch.object(module.time, 'sleep'):
        results, _screened = getattr(module, entry_point)()
    return results


# ── calculate_technicals ─────────────────────────────────────────────────────

class TechnicalsLadderInputTests(unittest.TestCase):
    """The helper supplies the two EMAs and a full-window SMA50, or says it
    cannot."""

    def _technicals_for(self, bars):
        history = _history(bars)
        ticker = mock.Mock()
        ticker.history.return_value = history
        with mock.patch.object(ep_scan_common.yf, 'Ticker', return_value=ticker):
            return history, ep_scan_common.calculate_technicals(
                'TEST', float(history['Close'].iloc[-1]))

    def test_the_emas_match_the_indicator_pipeline(self):
        history, technicals = self._technicals_for(60)
        close = history['Close']
        self.assertAlmostEqual(
            technicals['ema10'],
            float(close.ewm(span=10, adjust=False).mean().iloc[-1]), places=9)
        self.assertAlmostEqual(
            technicals['ema20'],
            float(close.ewm(span=20, adjust=False).mean().iloc[-1]), places=9)

    def test_a_full_history_hands_the_ladder_its_fifty_bar_mean(self):
        history, technicals = self._technicals_for(60)
        self.assertAlmostEqual(
            technicals['sma50_full'],
            float(history['Close'].iloc[-50:].mean()), places=9)

    def test_a_short_history_withholds_sma50_from_the_ladder(self):
        # The partial mean stays for atr_multiple. It never reaches the ladder:
        # it is finite and positive, so the zero sentinel cannot catch it.
        _frame, technicals = self._technicals_for(30)
        self.assertIsNone(technicals['sma50_full'])
        self.assertIsNotNone(technicals['sma50'])
        self.assertIsNotNone(technicals['atr_multiple'])

    def test_fifty_bars_is_inside_the_full_window(self):
        self.assertIsNone(self._technicals_for(49)[1]['sma50_full'])
        self.assertIsNotNone(self._technicals_for(50)[1]['sma50_full'])


# ── The row loops ────────────────────────────────────────────────────────────

class ScanRowHighlightTests(unittest.TestCase):
    """Both row loops publish the tier under the shared `highlight` key."""

    def test_a_crowded_short_carries_the_short_tier(self):
        for module, entry_point, price_helper in SCANS:
            with self.subTest(scan=module.__name__):
                rows = _run_scan(module, entry_point, price_helper,
                                 short=28.0, technicals=_technicals(**BROKEN))
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]['highlight'], 'short')

    def test_an_unknown_short_float_falls_through_to_the_averages(self):
        for module, entry_point, price_helper in SCANS:
            with self.subTest(scan=module.__name__):
                rows = _run_scan(module, entry_point, price_helper,
                                 short=None, technicals=_technicals(**STACKED))
                self.assertEqual(rows[0]['highlight'], 'ma_up')

    def test_a_history_too_short_for_sma50_carries_no_tier_and_still_exports(self):
        for module, entry_point, price_helper in SCANS:
            with self.subTest(scan=module.__name__):
                rows = _run_scan(
                    module, entry_point, price_helper, short=4.0,
                    technicals=_technicals(ema10=12.0, ema20=11.0,
                                           sma50_full=None))
                self.assertEqual(len(rows), 1, 'the row must still be exported')
                self.assertIsNone(rows[0]['highlight'])

    def test_the_coil_rung_never_fires_on_an_ep_table(self):
        # R14. EP holds no tightness source, so the rung is absent by
        # construction. Nothing in either loop may invent one.
        for module, entry_point, price_helper in SCANS:
            for short in (None, 4.0, 28.0):
                for averages in (STACKED, BROKEN, SPLIT):
                    with self.subTest(scan=module.__name__, short=short):
                        rows = _run_scan(module, entry_point, price_helper,
                                         short=short,
                                         technicals=_technicals(**averages))
                        self.assertNotEqual(rows[0]['highlight'], 'coil')


class ScanLoopAgreementTests(unittest.TestCase):
    """One grid, two loops, one answer. The loops are near-identical copies, so
    a rung added to one and missed by the other would show only on the tab
    nobody was reading that session."""

    GRID = (
        (28.0, STACKED),
        (28.0, BROKEN),
        (20.0, BROKEN),
        (19.9, BROKEN),
        (19.9, STACKED),
        (None, STACKED),
        (None, BROKEN),
        (None, SPLIT),
        (4.0, dict(ema10=12.0, ema20=11.0, sma50_full=None)),
    )

    EXPECTED = ['short', 'short', 'short', None, 'ma_up', 'ma_up', None,
                'ma_split', None]

    def test_both_scans_answer_the_same_grid_alike(self):
        tiers = {}
        for module, entry_point, price_helper in SCANS:
            tiers[module.__name__] = [
                _run_scan(module, entry_point, price_helper, short=short,
                          technicals=_technicals(**averages))[0]['highlight']
                for short, averages in self.GRID
            ]
        afternoon = tiers[ep_scan_afternoon.__name__]
        morning = tiers[ep_scan_morning.__name__]
        self.assertEqual(afternoon, morning)
        # Guard against both loops agreeing on nothing at all.
        self.assertEqual(afternoon, self.EXPECTED)


if __name__ == '__main__':
    unittest.main()
