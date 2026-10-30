"""The trending screener and the Trending tab's snapshot.

Up = six sessions in a row, each low above the session before. Down = six
sessions in a row, each high below the session before. Each side ranks its
tickers by 1-week return: best first going up, worst first going down.
"""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import src.stock_utils as su
from src.indicators.create_technical_indicators import compute_trend_streaks
from src.reporting import export_dashboard_data
from src.screening.screeners import trending


class TrendStreakTests(unittest.TestCase):
    def test_counts_consecutive_higher_lows_and_lower_highs(self):
        low = pd.Series([10, 11, 12, 13, 12, 13])
        high = pd.Series([20, 19, 18, 19, 18, 17])
        up, down = compute_trend_streaks(high, low)
        self.assertEqual(up.tolist(), [0, 1, 2, 3, 0, 1])
        self.assertEqual(down.tolist(), [0, 1, 2, 0, 1, 2])

    def test_a_tie_breaks_the_streak(self):
        low = pd.Series([10, 11, 11, 12])
        high = pd.Series([20, 19, 19, 18])
        up, down = compute_trend_streaks(high, low)
        self.assertEqual(up.tolist(), [0, 1, 0, 1])
        self.assertEqual(down.tolist(), [0, 1, 0, 1])


def _row(**overrides):
    base = {
        'adr_pct_partial': 0.05,
        'avg_dollar_vol_partial': 150e6,
        'vol_sma50_partial': 1e6,
        'higher_low_streak': 6,
        'lower_high_streak': 0,
    }
    base.update(overrides)
    return base


class TrendingScreenerGateTests(unittest.TestCase):
    def _run(self, rows):
        with patch.dict(trending.CONFIG, {'trending_screener': {}}):
            return trending.filter_master_table(pd.DataFrame(rows)).tolist()

    def test_each_gate_rejects_independently(self):
        rows = [
            _row(),                                  # passes every gate
            _row(adr_pct_partial=0.039),             # ADR below 4%
            _row(avg_dollar_vol_partial=99e6),       # dollar volume below $100M
            _row(vol_sma50_partial=799e3),           # share volume below 800K
            _row(higher_low_streak=5),               # five higher lows is not six
        ]
        self.assertEqual(self._run(rows), [True, False, False, False, False])

    def test_either_side_passes(self):
        rows = [
            _row(higher_low_streak=0, lower_high_streak=6),
            _row(higher_low_streak=0, lower_high_streak=5),
        ]
        self.assertEqual(self._run(rows), [True, False])


# (ticker, perf_1w, higher_low_streak, lower_high_streak)
ROWS = [
    ('NVDA', 0.08, 7, 0),
    ('AMD', 0.15, 6, 0),
    ('PLTR', 0.02, 9, 0),
    ('SNOW', -0.12, 0, 6),
    ('MDB', -0.03, 0, 8),
    ('RKLB', -0.20, 0, 6),
    ('ODD', 0.01, 6, 6),     # contracting range: both sides
    ('JUNK', 0.30, 6, 0),    # untagged: lands in Singleton, last on its side
]

THEME_MAP = {
    'NVDA': ['AI / Data Center / Chips & Processors'],
    'AMD': ['AI / Data Center / Chips & Processors'],
    'PLTR': ['Software & Internet / DevOps & Data'],
    'SNOW': ['Software & Internet / DevOps & Data'],
    'MDB': ['Software & Internet / DevOps & Data'],
    'RKLB': ['Space / Launch'],
    'ODD': ['Space / Satellites'],
}


class TrendingSnapshotTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        parquet = Path(cls._tmp.name) / 'trending_2026-10-29.parquet'
        su.save_df_to_parquet(pd.DataFrame([
            {'ticker': t, 'date': '2026-10-29', 'perf_1w': p, 'higher_low_streak': up,
             'lower_high_streak': dn, 'vars': 1.0, 'vars_20ema': 0.5,
             'rs_sts_pct': 50.0, 'close': 100.0}
            for t, p, up, dn in ROWS
        ]), parquet)
        with (
            patch('src.themes.theme_registry.load_ticker_themes', return_value=THEME_MAP),
            patch.dict(trending.CONFIG, {'trending_screener': {}}),
        ):
            cls.snap = export_dashboard_data._build_trending_snapshot(
                parquet, day_flags={}, fundamentals={})

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    @staticmethod
    def _ranked(side):
        found = {td['ticker']: td['rank'] for grp in side
                 for leaf in grp['leaves'] for td in leaf['tickers']}
        return sorted(found, key=found.get)

    def test_up_side_ranks_the_best_week_first(self):
        self.assertEqual(self._ranked(self.snap['up']), ['JUNK', 'AMD', 'NVDA', 'PLTR', 'ODD'])

    def test_down_side_ranks_the_worst_week_first(self):
        self.assertEqual(self._ranked(self.snap['down']), ['RKLB', 'SNOW', 'MDB', 'ODD'])

    def test_sections_follow_their_best_ranked_member(self):
        self.assertEqual([g['name'] for g in self.snap['down']], ['Space', 'Software & Internet'])
        space = self.snap['down'][0]
        self.assertEqual([lf['name'] for lf in space['leaves']],
                         ['Space / Launch', 'Space / Satellites'])

    def test_catchall_sorts_last_even_at_rank_one(self):
        self.assertEqual([g['name'] for g in self.snap['up']],
                         ['AI', 'Software & Internet', 'Space', 'Singleton'])

    def test_rows_carry_the_return_in_percent_and_the_side_streak(self):
        rows = {td['ticker']: td for grp in self.snap['down']
                for leaf in grp['leaves'] for td in leaf['tickers']}
        self.assertEqual(rows['MDB']['perf_1w'], -3.0)
        self.assertEqual(rows['MDB']['streak'], 8)
        up_odd = next(td for grp in self.snap['up'] for leaf in grp['leaves']
                      for td in leaf['tickers'] if td['ticker'] == 'ODD')
        self.assertEqual(up_odd['streak'], 6)


if __name__ == '__main__':
    unittest.main()
