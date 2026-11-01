"""EMA20 cross setups and the 20EMA tab's snapshot.

The setup tests hand `setup_state` its EMA columns directly, so each case
pins one rule without depending on how an EMA settles.
"""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

import src.stock_utils as su
from src.reporting import export_dashboard_data
from src.screening import ema20_setups as es

CFG = dict(es._DEFAULTS)


def _bars(close, ema10, ema20, high=None, low=None, volume=3e6, dollar=200e6):
    close = np.asarray(close, float)
    n = len(close)
    return pd.DataFrame({
        'open': close, 'close': close,
        'high': close + 0.5 if high is None else np.asarray(high, float),
        'low': close - 0.5 if low is None else np.asarray(low, float),
        'volume': volume, 'ema10': np.asarray(ema10, float), 'ema20': np.asarray(ema20, float),
        'atr14': 1.0, 'adr_pct': 0.05, 'avg_dollar_vol': dollar, 'sma50_full': 50.0,
    }, index=pd.bdate_range('2026-01-02', periods=n))


def _long_case(n=45, ema10_cross_at=12, ema10=100.5):
    """Ten closes under EMA20 (100), a cross at bar 10, then closes at 101.
    EMA10 sits at 99.5 until `ema10_cross_at`, then at `ema10`."""
    close = [99.0] * 10 + [101.0] * (n - 10)
    e10 = [99.5] * ema10_cross_at + [ema10] * (n - ema10_cross_at)
    return close, e10


class SetupLifecycleTests(unittest.TestCase):
    def test_the_cross_opens_a_setup_that_lives_while_the_emas_stay_crossed(self):
        close, e10 = _long_case()
        st = es.setup_state(_bars(close, e10, 100.0), 'long', CFG)
        self.assertFalse(st['live'].iloc[9])
        self.assertTrue(st['live'].iloc[10])
        self.assertEqual(st['days'].iloc[10], 0)
        self.assertEqual(st['cross_date'].iloc[30], st.index[10])
        # Past the 15-session limit, because EMA10 crossed.
        self.assertTrue(st['live'].iloc[44])

    def test_a_setup_whose_emas_never_cross_ends_after_the_limit(self):
        close, e10 = _long_case(ema10_cross_at=45)
        st = es.setup_state(_bars(close, e10, 100.0), 'long', CFG)
        self.assertTrue(st['live'].iloc[10 + 14])
        self.assertFalse(st['live'].iloc[10 + 15])

    def test_a_close_through_the_overshoot_ends_it_for_good(self):
        close, e10 = _long_case()
        close[30] = 99.4                     # 0.6 ATR under EMA20
        st = es.setup_state(_bars(close, e10, 100.0), 'long', CFG)
        self.assertTrue(st['live'].iloc[29])
        self.assertFalse(st['live'].iloc[30:].any())

    def test_an_overshoot_inside_the_allowance_keeps_it(self):
        close, e10 = _long_case()
        close[30] = 99.6                     # 0.4 ATR under EMA20
        st = es.setup_state(_bars(close, e10, 100.0), 'long', CFG)
        self.assertTrue(st['live'].iloc[30:].all())

    def test_ema10_crossing_back_ends_it(self):
        close, e10 = _long_case()
        e10[30:] = [99.5] * (len(e10) - 30)
        st = es.setup_state(_bars(close, e10, 100.0), 'long', CFG)
        self.assertTrue(st['live'].iloc[29])
        self.assertFalse(st['live'].iloc[30:].any())

    def test_a_cross_needs_most_prior_closes_on_the_other_side(self):
        close, e10 = _long_case()
        close[4:9] = [101.0] * 5             # only 5 of the 10 closes were under
        st = es.setup_state(_bars(close, e10, 100.0), 'long', CFG)
        self.assertFalse(st['live'].iloc[10])

    def test_a_pullback_is_a_low_near_ema10(self):
        close, e10 = _long_case()
        low = np.array(close) - 0.5          # 100.5, on EMA10
        low[20] = 100.8                      # 0.3 over EMA10
        st = es.setup_state(_bars(close, e10, 100.0, low=low), 'long', CFG)
        self.assertTrue(st['pullback'].iloc[19])
        self.assertFalse(st['pullback'].iloc[20])
        self.assertFalse(st['pullback'].iloc[10], 'the cross bar is not a pullback')

    def test_an_illiquid_bar_is_not_live(self):
        close, e10 = _long_case()
        st = es.setup_state(_bars(close, e10, 100.0, volume=1e6, dollar=50e6), 'long', CFG)
        self.assertFalse(st['live'].any())

    def test_either_volume_leg_passes_the_floor(self):
        close, e10 = _long_case()
        st = es.setup_state(_bars(close, e10, 100.0, volume=2.5e6, dollar=50e6), 'long', CFG)
        self.assertTrue(st['live'].iloc[10])

    def test_a_short_mirrors_a_long(self):
        close, e10 = _long_case()
        close = [200.0 - c for c in close]
        e10 = [200.0 - e for e in e10]
        high = np.array(close) + 0.8
        st = es.setup_state(_bars(close, e10, 100.0, high=high), 'short', CFG)
        self.assertTrue(st['live'].iloc[10])
        self.assertTrue(st['live'].iloc[44])
        # High 99.8 against EMA20 100: 0.2 ATR under it.
        self.assertAlmostEqual(st['gap20'].iloc[20], 0.2)


N = 80
SESSIONS = pd.bdate_range('2026-01-02', periods=N)
THEMES = {t: ['AI / Data Center / Chips & Processors'] for t in ('AAA', 'BBB', 'CCC')}


def _universe():
    """Three long setups in one theme, one fallen leader and seven flat names."""
    bars = {}
    t = np.arange(N, dtype=float)
    for name in ('AAA', 'BBB', 'CCC'):
        close = 100 + 0.2 * t
        # Under EMA20 until bar 60, then above; EMA10 crosses one bar later.
        ema20 = np.where(t < 60, close + 1, close - 1)
        ema10 = np.where(t < 60, close + 1.5, np.where(t < 61, close - 1.5, close - 0.9))
        b = _bars(close, ema10, ema20, high=close + 0.1, low=close - 1.0)
        b['sma50_full'] = close - 2
        bars[name] = b
    # A leader: up 1 a session, then down 3 a session from bar 50.
    close = np.where(t < 50, 100 + t, 149 - 3 * (t - 49))
    ema20 = np.where(t < 50, close - 1, close + 1)
    ema10 = np.where(t < 50, close - 1.5, np.where(t < 51, close + 1.5, close + 0.9))
    bars['LEAD'] = _bars(close, ema10, ema20, high=close + 0.9, low=close - 0.5)
    for i in range(7):
        bars[f'F{i}'] = _bars([50.0] * N, [50.0] * N, [50.0] * N)
    return bars


class ScanTests(unittest.TestCase):
    def _scan(self, themes=THEMES, **cfg):
        cfg = {**CFG, 'lead_return_sessions': 5, 'short_min_lead_sessions': 10, **cfg}
        return es.scan(_universe(), SESSIONS[-20:], themes, cfg)

    def test_three_setups_in_one_theme_list_each_long(self):
        out = self._scan()
        longs = out[out['side'] == 'long']
        self.assertEqual(set(longs['ticker']), {'AAA', 'BBB', 'CCC'})
        self.assertTrue((longs['theme_size'] == 3).all())

    def test_two_setups_are_not_a_theme(self):
        out = self._scan(themes={t: p for t, p in THEMES.items() if t != 'CCC'})
        self.assertTrue(out[out['side'] == 'long'].empty)

    def test_a_fallen_leader_is_listed_short(self):
        out = self._scan()
        shorts = out[out['side'] == 'short']
        self.assertEqual(set(shorts['ticker']), {'LEAD'})
        self.assertTrue((shorts['off_high'] <= -0.25).all())
        self.assertEqual(shorts['cross_date'].iloc[0], SESSIONS[50])

    def test_a_short_needs_the_leadership_before_the_cross(self):
        out = self._scan(short_min_lead_sessions=100)
        self.assertTrue(out[out['side'] == 'short'].empty)


class Ema20SnapshotTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        parquet = Path(cls._tmp.name) / 'ema20_2026-10-28.parquet'
        rows = [
            {'ticker': 'VSH', 'side': 'short', 'lead': 71, 'gap': -0.07, 'off_high': -0.57},
            {'ticker': 'ALAB', 'side': 'short', 'lead': 67, 'gap': -0.01, 'off_high': -0.40},
            {'ticker': 'PURR', 'side': 'short', 'lead': 69, 'gap': 0.18, 'off_high': -0.32},
            {'ticker': 'EPAM', 'side': 'long', 'theme_size': 10, 'rs20': 0.96, 'off_high': -0.02},
            {'ticker': 'ACN', 'side': 'long', 'theme_size': 6, 'rs20': 0.98, 'off_high': -0.06},
            {'ticker': 'CTSH', 'side': 'long', 'theme_size': 6, 'rs20': 0.92, 'off_high': -0.10},
        ]
        su.save_df_to_parquet(pd.DataFrame([
            {'date': '2026-10-28', 'days': 12, 'cross_date': '2026-10-09', 'vars': 1.0,
             'vars_20ema': 0.5, 'rs_sts_pct': 50.0, 'close': 30.0, 'avg_dollar_vol': 2e8,
             'adr_pct': 0.05, **r}
            for r in rows
        ]), parquet)
        themes = {
            'VSH': ['AI / Data Center / Components'], 'ALAB': ['AI / Data Center / Components'],
            'PURR': ['Fintech & Crypto / Crypto'], 'EPAM': ['Software & Internet / BPO & IT Services'],
            'ACN': ['Software & Internet / BPO & IT Services'],
            'CTSH': ['Software & Internet / BPO & IT Services'],
        }
        with patch('src.themes.theme_registry.load_ticker_themes', return_value=themes):
            cls.snap = export_dashboard_data._build_ema20_snapshot(
                parquet, day_flags={}, fundamentals={})

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    @staticmethod
    def _ranked(side):
        found = {td['ticker']: td['rank'] for grp in side
                 for leaf in grp['leaves'] for td in leaf['tickers']}
        return sorted(found, key=found.get)

    def test_shorts_rank_by_leadership(self):
        self.assertEqual(self._ranked(self.snap['short']), ['VSH', 'PURR', 'ALAB'])

    def test_longs_rank_by_theme_size_then_strength(self):
        self.assertEqual(self._ranked(self.snap['long']), ['EPAM', 'ACN', 'CTSH'])

    def test_sections_follow_their_best_ranked_member(self):
        self.assertEqual([g['name'] for g in self.snap['short']], ['AI', 'Fintech & Crypto'])

    def test_rows_carry_percent_fields(self):
        rows = {td['ticker']: td for grp in self.snap['long']
                for leaf in grp['leaves'] for td in leaf['tickers']}
        self.assertEqual(rows['EPAM']['rs20'], 96)
        self.assertEqual(rows['EPAM']['off_high'], -2.0)
        self.assertEqual(rows['EPAM']['days'], 12)

    def test_each_side_gets_its_own_zone_map_key(self):
        # No master parquet here, so each map reads "data arrives later".
        self.assertIn('zone_map_short', self.snap)
        self.assertIn('zone_map_long', self.snap)
        self.assertNotIn('zone_map', self.snap)


if __name__ == '__main__':
    unittest.main()
