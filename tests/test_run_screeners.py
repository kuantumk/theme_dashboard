"""All screeners run in one process and write what a per-screener run wrote.

Each screener used to run as its own subprocess and reread every master
parquet; the in-process runner reads each master once. The files must not
change: market_history and the exporter read them.
"""

import contextlib
import io
import tempfile
import types
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

import src.stock_utils as su
from config.settings import CONFIG
from src.screening import run_screener as rs


def master(date_str, seed):
    rng = np.random.default_rng(seed)
    n = 60
    df = pd.DataFrame({
        'date': date_str,
        'close': rng.uniform(1, 300, n), 'high': rng.uniform(1, 300, n), 'low': rng.uniform(1, 300, n),
        'volume': rng.uniform(1e5, 5e6, n), 'vol_sma50': rng.uniform(5e5, 5e6, n),
        'avg_dollar_vol': rng.uniform(1e6, 1e8, n), 'adr_pct': rng.uniform(0.01, 0.08, n),
        'rs_sts_pct': rng.uniform(0, 100, n), 'vars': rng.normal(1, 3, n),
        'tight_range': rng.random(n) > 0.5,
    })
    for col in ('sma25', 'sma30', 'sma50', 'sma100', 'sma200', 'ema20', 'max30', 'min30',
                'max150', 'min150', 'max252', 'min252', 'previous_session_high',
                'previous_session_volume', 'atr_multi_50sma', 'price_chg_pct0',
                'perf_1mo', 'perf_3mo', 'perf_6mo', 'up_dollar_vol_max'):
        df[col] = rng.uniform(0, 2, n) * (df['close'] if col[0] in 'smep' else 1)
    for col in ('c0_c30_rank', 'c0_c60_rank', 'c0_c90_rank', 'c0_c120_rank',
                'days_since_highest_volume', 'days_since_up_vol_max', 'days_since_vol_max',
                'higher_low_streak', 'lower_high_streak'):
        df[col] = rng.integers(0, 100, n).astype(float)
    df['up_dollar_vol_max'] *= 1e8
    df.iloc[::7, df.columns.get_loc('sma200')] = np.nan   # zero-filled for the screeners
    for col in ('sma25', 'sma30', 'sma50', 'sma100', 'sma200', 'vol_sma50', 'avg_dollar_vol', 'adr_pct'):
        df[f'{col}_partial'] = df[col]
    df['ticker'] = [f'T{i}' for i in range(n)]
    return df


class RunScreenersTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.masters = self.root / 'master'
        self.dates = ['2026-10-20', '2026-10-21', '2026-10-22', '2026-10-23']
        for i, d in enumerate(self.dates):
            su.save_df_to_parquet(master(d, i), self.masters / f'master_{d}.parquet')
        # Not a session: must not shift which masters count as the newest.
        su.save_df_to_parquet(master('x', 9), self.masters / 'master_backup.parquet')

    def test_outputs_equal_each_screener_on_the_zero_filled_master(self):
        names = list(CONFIG['screeners'])
        with contextlib.redirect_stdout(io.StringIO()):
            done = rs.run_screeners(names, 3, output_root=self.root / 'out',
                                    master_dir=self.masters)
        self.assertEqual(done, self.dates[::-1][:3])
        for d in done:
            filled = su.load_df_from_parquet(self.masters / f'master_{d}.parquet').fillna(0)
            for name, module in rs.load_screeners(names).items():
                expected = filled[module.filter_master_table(filled)]
                got = su.load_df_from_parquet(self.root / 'out' / name / f'{name}_{d}.parquet')
                pd.testing.assert_frame_equal(got, _roundtrip(expected))
        self.assertFalse((self.root / 'out' / names[0] / f'{names[0]}_{self.dates[0]}.parquet').exists())

    def test_a_screener_that_adds_columns_does_not_leak_into_the_next(self):
        def adds_column(frame):
            frame['extra'] = 1.0
            return frame['close'] > 0

        screeners = {
            'first': types.SimpleNamespace(filter_master_table=adds_column),
            'second': types.SimpleNamespace(filter_master_table=lambda f: f['close'] > 0),
        }
        filled = master('2026-10-23', 1).fillna(0)
        out = rs.screen_master(filled, screeners)
        self.assertIn('extra', out['first'].columns)
        self.assertNotIn('extra', out['second'].columns)
        self.assertNotIn('extra', filled.columns)


def _roundtrip(df):
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / 'x.parquet'
        su.save_df_to_parquet(df, path)
        return su.load_df_from_parquet(path)


if __name__ == '__main__':
    unittest.main()
