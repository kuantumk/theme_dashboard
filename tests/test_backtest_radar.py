"""Synthetic-fixture tests for the backtest_radar harness (no live data)."""

import unittest

import numpy as np
import pandas as pd

import backtest_radar as br

CFG = {
    'beta': 0.3,
    'top_k_leaves': 5,
    'top_m_members': 5,
    'min_breadth': 2,
    'min_leaves_for_boost': 2,
    'min_avg_dollar_vol': 10_000_000,
    'min_close': 3.0,
    'composite_weights': {'rs': 0.4, 'vars_pct': 0.4, 'fast': 0.2},
    'fast_leg_column': 'rela_perf_1mo_rank',
    'missing_default': 50.0,
}


def make_close_matrix():
    """30 sessions; A +1%/day, B flat, C -1%/day, SPY flat."""
    idx = pd.bdate_range('2026-01-05', periods=30)
    n = np.arange(30)
    return pd.DataFrame({
        'A': 100.0 * 1.01 ** n,
        'B': np.full(30, 50.0),
        'C': 100.0 * 0.99 ** n,
        'SPY': np.full(30, 400.0),
    }, index=idx)


def make_leaf(theme, l1, composite_avg, tickers):
    return {
        'theme': theme, 'l1': l1,
        'l2': theme.split(' / ')[1] if ' / ' in theme else None, 'l3': None,
        'composite_avg': composite_avg, 'breadth': len(tickers),
        'members': [{'ticker': t, 'composite': composite_avg, 'rs': 50.0,
                     'vars': 0.0, 'price': 10.0} for t in tickers],
    }


class ForwardReturnTests(unittest.TestCase):
    def setUp(self):
        self.mx = make_close_matrix()
        self.dpos = br.date_positions(self.mx)
        self.d0 = self.mx.index[0].strftime('%Y-%m-%d')

    def test_known_geometric_paths(self):
        fwd = br.forward_excess_return(self.mx, self.dpos, self.d0, ['A'], 5)
        self.assertAlmostEqual(fwd, 1.01 ** 5 - 1.0, places=12)  # SPY flat
        fwd_b = br.forward_excess_return(self.mx, self.dpos, self.d0, ['B'], 5)
        self.assertAlmostEqual(fwd_b, 0.0, places=12)
        basket = br.forward_excess_return(self.mx, self.dpos, self.d0, ['A', 'C'], 5)
        expected = ((1.01 ** 5 - 1.0) + (0.99 ** 5 - 1.0)) / 2.0
        self.assertAlmostEqual(basket, expected, places=12)

    def test_skip_day_shifts_entry(self):
        fwd = br.forward_excess_return(self.mx, self.dpos, self.d0, ['A'], 5,
                                       skip_days=1)
        self.assertAlmostEqual(fwd, 1.01 ** 5 - 1.0, places=12)  # geometric: same daily rate

    def test_coverage_gate(self):
        fwd = br.forward_excess_return(self.mx, self.dpos, self.d0,
                                       ['A', 'MISS1', 'MISS2'], 5)
        self.assertIsNone(fwd)  # 1/3 coverage < 0.7

    def test_incomplete_window_returns_none(self):
        late = self.mx.index[-3].strftime('%Y-%m-%d')
        self.assertIsNone(br.forward_excess_return(self.mx, self.dpos, late,
                                                   ['A'], 5))

    def test_unknown_date_returns_none(self):
        self.assertIsNone(br.forward_excess_return(self.mx, self.dpos,
                                                   '1999-01-01', ['A'], 5))


class MetricTests(unittest.TestCase):
    def _snapshot(self, boosted):
        return [{'name': f'L{i}', 'raw': b, 'boosted': b, 'rank': r + 1,
                 'n_leaves': 2}
                for r, (i, b) in enumerate(sorted(enumerate(boosted),
                                                  key=lambda t: -t[1]))]

    def test_rank_ic_perfect_and_inverted(self):
        snap = self._snapshot(list(range(8, 0, -1)))
        aligned = {e['name']: e['boosted'] / 100.0 for e in snap}
        self.assertAlmostEqual(br.session_rank_ic(snap, aligned), 1.0)
        inverted = {e['name']: -e['boosted'] / 100.0 for e in snap}
        self.assertAlmostEqual(br.session_rank_ic(snap, inverted), -1.0)

    def test_rank_ic_needs_min_l1s(self):
        snap = self._snapshot([3, 2, 1])
        fwd = {e['name']: 0.01 for e in snap}
        self.assertIsNone(br.session_rank_ic(snap, fwd))

    def test_topk_hit(self):
        snap = self._snapshot(list(range(8, 0, -1)))
        fwd = {e['name']: (0.05 if e['rank'] <= 3 else -0.01) for e in snap}
        hit, spread = br.topk_hit(snap, fwd, k=3)
        self.assertTrue(hit)
        self.assertAlmostEqual(spread, 0.06)

    def test_rank_autocorr_identical_snapshots(self):
        snap = self._snapshot(list(range(8, 0, -1)))
        self.assertAlmostEqual(br.rank_autocorr(snap, snap), 1.0)
        self.assertIsNone(br.rank_autocorr(None, snap))

    def test_top5_jaccard(self):
        a = self._snapshot(list(range(8, 0, -1)))
        self.assertAlmostEqual(br.top5_jaccard(a, a), 1.0)

    def test_bootstrap_ci_constant_series(self):
        lo, mean, hi = br.moving_block_bootstrap_ci([0.2] * 40, block_len=10)
        self.assertAlmostEqual(lo, 0.2)
        self.assertAlmostEqual(mean, 0.2)
        self.assertAlmostEqual(hi, 0.2)

    def test_bootstrap_ci_short_series_degenerates_to_mean(self):
        lo, mean, hi = br.moving_block_bootstrap_ci([0.1, 0.3], block_len=10)
        self.assertAlmostEqual(mean, 0.2)
        self.assertEqual(lo, hi)


class ScoringLayerTests(unittest.TestCase):
    def test_apply_weights_corners(self):
        uni = pd.DataFrame({
            'ticker': ['A', 'B'],
            'rs_leg': [90.0, 10.0],
            'vars_leg': [20.0, 80.0],
            'fast_leg': [50.0, 50.0],
        })
        rs_only = br.apply_weights(uni, (1.0, 0.0, 0.0))
        self.assertListEqual(list(rs_only['composite']), [90.0, 10.0])
        blended = br.apply_weights(uni, (0.4, 0.4, 0.2))
        self.assertAlmostEqual(blended['composite'].iloc[0],
                               0.4 * 90 + 0.4 * 20 + 0.2 * 50)

    def test_multi_beta_snapshots_share_leaves_and_scale_boost(self):
        leaves = (
            [make_leaf(f'Cybersecurity / L{i}', 'Cybersecurity', 80.0 + i,
                       [f'CY{i}A', f'CY{i}B']) for i in range(3)]
            + [make_leaf(f'Biotech / F{i}', 'Biotech', 40.0 + i,
                         [f'BI{i}A', f'BI{i}B']) for i in range(5)]
        )
        snaps = br.l1_snapshots_multi_beta(leaves, CFG, [0.0, 0.3])
        zero = {e['name']: e for e in snaps[0.0]}
        boosted = {e['name']: e for e in snaps[0.3]}
        for name, e in zero.items():
            self.assertAlmostEqual(e['boosted'], e['raw'])   # beta=0 baseline
            self.assertAlmostEqual(boosted[name]['raw'], e['raw'])  # raw beta-free
        cyber = boosted['Cybersecurity']
        self.assertAlmostEqual(cyber['boosted'], cyber['raw'] * 1.3)

    def test_l1_baskets_distinct_union_and_top_mode(self):
        leaves = [
            make_leaf('AI / X', 'AI', 60.0, ['AAA', 'BBB']),
            make_leaf('AI / Y', 'AI', 55.0, ['BBB', 'CCC']),
        ]
        allb = br.l1_baskets(leaves, mode='all')
        self.assertEqual(allb['AI'], ['AAA', 'BBB', 'CCC'])
        top = br.l1_baskets(leaves, mode='top', top_chips=1)
        self.assertEqual(top['AI'], ['AAA', 'BBB'])


if __name__ == '__main__':
    unittest.main()
