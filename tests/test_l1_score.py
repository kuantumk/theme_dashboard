import unittest

import numpy as np
import pandas as pd

from src.themes.l1_score import (
    build_radar_universe,
    compute_leaf_scores,
    compute_radar,
    rollup_l1s,
)

# Explicit config for every test so results don't drift with workflow_config.yaml.
CFG = {
    'beta': 0.3,
    'top_k_leaves': 5,
    'top_m_members': 5,
    'min_breadth': 2,
    'min_leaves_for_boost': 2,
    'min_avg_dollar_vol': 10_000_000,
    'min_close': 3.0,
    'min_avg_volume': 750_000,
    'min_avg_volume_dollar_exempt': 40_000_000,
    'composite_weights': {'rs': 0.4, 'vars_pct': 0.4, 'fast': 0.2},
    'fast_leg_column': 'rela_perf_1mo_rank',
    'missing_default': 50.0,
}


def make_master(rows):
    defaults = {
        'date': '2026-07-13',
        'close': 50.0,
        'avg_dollar_vol': 50_000_000.0,
        'rs_sts_pct': 50.0,
        'vars': 0.0,
        'rela_perf_1mo_rank': 50,
    }
    return pd.DataFrame([{**defaults, **r} for r in rows])


def make_leaf(theme, l1, composite_avg, n_members=3):
    return {
        'theme': theme,
        'l1': l1,
        'l2': theme.split(' / ')[1] if ' / ' in theme else None,
        'l3': None,
        'composite_avg': composite_avg,
        'breadth': n_members,
        'members': [
            {'ticker': f'{theme[:2].upper()}{i}', 'composite': composite_avg,
             'rs': 50.0, 'vars': 0.0, 'price': 10.0}
            for i in range(n_members)
        ],
    }


class BuildRadarUniverseTests(unittest.TestCase):
    def test_filters_spx_untagged_price_and_liquidity(self):
        master = make_master([
            {'ticker': 'SPX'},                                  # index row dropped
            {'ticker': 'AAA'},                                  # kept
            {'ticker': 'bbb'},                                  # kept, uppercased
            {'ticker': 'CCC', 'close': 2.0},                    # below min_close
            {'ticker': 'DDD', 'avg_dollar_vol': 1_000_000.0},   # below liquidity floor
            {'ticker': 'EEE'},                                  # untagged
            {'ticker': 'AAA', 'close': 99.0},                   # duplicate dropped
        ])
        uni = build_radar_universe(master, {'AAA', 'BBB', 'CCC', 'DDD', 'SPX'}, CFG)
        self.assertEqual(sorted(uni['ticker']), ['AAA', 'BBB'])
        self.assertEqual(float(uni[uni['ticker'] == 'AAA']['close'].iloc[0]), 50.0)

    def test_share_volume_floor_with_dollar_exemption(self):
        master = make_master([
            {'ticker': 'AAA', 'vol_sma50': 800_000.0,
             'avg_dollar_vol': 20_000_000.0},                   # above share floor
            {'ticker': 'BBB', 'vol_sma50': 500_000.0,
             'avg_dollar_vol': 20_000_000.0},                   # thin, not exempt -> dropped
            {'ticker': 'CCC', 'vol_sma50': 300_000.0,
             'avg_dollar_vol': 60_000_000.0},                   # thin but $40M+ exempt
            {'ticker': 'DDD', 'vol_sma50': np.nan,
             'avg_dollar_vol': 20_000_000.0},                   # young IPO: NaN passes
        ])
        uni = build_radar_universe(master, {'AAA', 'BBB', 'CCC', 'DDD'}, CFG)
        self.assertEqual(sorted(uni['ticker']), ['AAA', 'CCC', 'DDD'])

    def test_composite_legs_and_weights(self):
        master = make_master([
            {'ticker': 'AAA', 'rs_sts_pct': 90.0, 'vars': 10.0, 'rela_perf_1mo_rank': 80},
            {'ticker': 'BBB', 'rs_sts_pct': 30.0, 'vars': -5.0, 'rela_perf_1mo_rank': 20},
            {'ticker': 'CCC', 'rs_sts_pct': 60.0, 'vars': 2.0, 'rela_perf_1mo_rank': 50},
        ])
        uni = build_radar_universe(master, {'AAA', 'BBB', 'CCC'}, CFG)
        a = uni[uni['ticker'] == 'AAA'].iloc[0]
        # vars percentiles for (10, -5, 2): AAA is highest -> 100
        self.assertAlmostEqual(a['vars_leg'], 100.0)
        self.assertAlmostEqual(a['composite'], 0.4 * 90 + 0.4 * 100 + 0.2 * 80)
        b = uni[uni['ticker'] == 'BBB'].iloc[0]
        self.assertAlmostEqual(b['vars_leg'], 100.0 / 3)

    def test_vars_percentile_anchored_to_all_tagged_pool(self):
        # ILL has the highest VARS but fails the liquidity floor: it must
        # anchor the percentile pool without ever becoming a member.
        master = make_master([
            {'ticker': 'AAA', 'vars': 10.0},
            {'ticker': 'BBB', 'vars': 5.0},
            {'ticker': 'ILL', 'vars': 20.0, 'avg_dollar_vol': 1_000_000.0},
        ])
        uni = build_radar_universe(master, {'AAA', 'BBB', 'ILL'}, CFG)
        self.assertEqual(sorted(uni['ticker']), ['AAA', 'BBB'])
        a = uni[uni['ticker'] == 'AAA'].iloc[0]
        b = uni[uni['ticker'] == 'BBB'].iloc[0]
        # Pool percentiles over (10, 5, 20): AAA 2/3, BBB 1/3 — NOT re-ranked
        # to 100/50 within the two floor survivors.
        self.assertAlmostEqual(a['vars_leg'], 200.0 / 3)
        self.assertAlmostEqual(b['vars_leg'], 100.0 / 3)

    def test_missing_columns_fall_back_to_neutral(self):
        master = pd.DataFrame([
            {'ticker': 'AAA', 'close': 50.0},
            {'ticker': 'BBB', 'close': 50.0, 'rs_sts_pct': np.nan},
        ])
        uni = build_radar_universe(master, {'AAA', 'BBB'}, CFG)
        self.assertEqual(len(uni), 2)  # no avg_dollar_vol column -> floor skipped
        for _, row in uni.iterrows():
            self.assertAlmostEqual(row['rs_leg'], 50.0)
            self.assertAlmostEqual(row['vars_leg'], 50.0)
            self.assertAlmostEqual(row['fast_leg'], 50.0)
            self.assertAlmostEqual(row['composite'], 50.0)


class ComputeLeafScoresTests(unittest.TestCase):
    def test_top_m_mean_breadth_gate_and_hidden_l1(self):
        rows = [{'ticker': f'T{i:02d}', 'rs_sts_pct': v, 'vars': float(v),
                 'rela_perf_1mo_rank': int(v)} for i, v in enumerate(range(30, 96, 5))]
        master = make_master(rows + [{'ticker': 'LON', 'rs_sts_pct': 99.0}])
        tickers = [r['ticker'] for r in rows]
        theme_map = {
            'Cybersecurity / Network': tickers[:8],
            'Cybersecurity / Identity': ['LON'],           # breadth 1 -> unscored
            'Singleton': tickers[8:11],                    # hidden L1 -> skipped
        }
        uni = build_radar_universe(master, set(tickers) | {'LON'}, CFG)
        leaves = compute_leaf_scores(uni, theme_map, CFG)
        self.assertEqual([lf['theme'] for lf in leaves], ['Cybersecurity / Network'])
        leaf = leaves[0]
        self.assertEqual(leaf['breadth'], 8)
        self.assertEqual(leaf['l1'], 'Cybersecurity')
        self.assertEqual(leaf['l2'], 'Network')
        # top-5 of the 8 members by composite
        expected = np.mean(sorted((m['composite'] for m in leaf['members']), reverse=True)[:5])
        self.assertAlmostEqual(leaf['composite_avg'], float(expected))
        composites = [m['composite'] for m in leaf['members']]
        self.assertEqual(composites, sorted(composites, reverse=True))


class RollupTests(unittest.TestCase):
    def test_zscores_have_zero_mean_unit_std(self):
        leaves = [make_leaf(f'AI / L{i}', 'AI', 40.0 + i * 5) for i in range(6)]
        rollup_l1s(leaves, CFG)
        zs = np.array([lf['raw'] for lf in leaves])
        self.assertAlmostEqual(float(zs.mean()), 0.0, places=9)
        self.assertAlmostEqual(float(zs.std()), 1.0, places=9)

    def test_degenerate_std_yields_zero_z(self):
        leaves = [make_leaf(f'AI / L{i}', 'AI', 55.0) for i in range(4)]
        rollup_l1s(leaves, CFG)
        for lf in leaves:
            self.assertEqual(lf['raw'], 0.0)

    def test_single_leaf_l1_gets_no_boost(self):
        leaves = [
            make_leaf('AI / Lonely', 'AI', 90.0),
            make_leaf('Biotech / A', 'Biotech', 40.0),
            make_leaf('Biotech / B', 'Biotech', 45.0),
        ]
        out = rollup_l1s(leaves, CFG)
        ai = next(e for e in out['l1s'] if e['name'] == 'AI')
        self.assertEqual(ai['delta'], 0.0)
        self.assertEqual(ai['boosted'], ai['raw'])
        lonely = ai['leaves'][0]
        self.assertEqual(lonely['boosted'], lonely['raw'])

    def test_boost_matches_decoded_screenshot_relationships(self):
        """Locks the boost mechanism decoded from the competitor screenshot:
        l1_raw = mean(top-K leaf raws); every leaf under the L1 shares the
        same boosted - raw = beta * l1_raw; L1 boosted = l1_raw * (1 + beta)."""
        # 6 cyber leaves (like the screenshot) + filler L1s so cyber z > 0
        cyber = [make_leaf(f'Cybersecurity / L{i}', 'Cybersecurity', v)
                 for i, v in enumerate([88.0, 87.5, 80.0, 76.0, 71.0, 62.0])]
        filler = [make_leaf(f'Biotech / F{i}', 'Biotech', 40.0 + i) for i in range(10)]
        out = rollup_l1s(cyber + filler, CFG)
        cyber_l1 = next(e for e in out['l1s'] if e['name'] == 'Cybersecurity')

        top5 = sorted((lf['raw'] for lf in cyber_l1['leaves']), reverse=True)[:5]
        self.assertAlmostEqual(cyber_l1['raw'], float(np.mean(top5)))
        self.assertGreater(cyber_l1['raw'], 0)
        expected_boost = 0.3 * cyber_l1['raw']
        self.assertAlmostEqual(cyber_l1['delta'], expected_boost)
        self.assertAlmostEqual(cyber_l1['boosted'], cyber_l1['raw'] * 1.3)
        for lf in cyber_l1['leaves']:
            self.assertAlmostEqual(lf['boosted'] - lf['raw'], expected_boost)

    def test_confirmed_l1_outranks_equal_isolated_leaf(self):
        cyber_leaves = [make_leaf(f'Cybersecurity / L{i}', 'Cybersecurity', 80.0 + i * 0.1)
                        for i in range(3)]
        isolated = make_leaf('Space / Launch', 'Space', 80.2)
        filler = [make_leaf(f'Biotech / F{i}', 'Biotech', 40.0 + i) for i in range(8)]
        out = rollup_l1s(cyber_leaves + [isolated] + filler, CFG)

        cyber_ranks = [lf['global_rank'] for lf in cyber_leaves]
        self.assertGreater(isolated['global_rank'], max(cyber_ranks))

        ranks = sorted(lf['global_rank'] for lf in cyber_leaves + [isolated] + filler)
        self.assertEqual(ranks, list(range(1, len(ranks) + 1)))

        l1_ranks = [e['rank'] for e in out['l1s']]
        self.assertEqual(l1_ranks, list(range(1, len(l1_ranks) + 1)))
        boosted = [e['boosted'] for e in out['l1s']]
        self.assertEqual(boosted, sorted(boosted, reverse=True))


class ComputeRadarTests(unittest.TestCase):
    def test_end_to_end_schema_and_is_screened(self):
        master = make_master([
            {'ticker': 'AAA', 'rs_sts_pct': 95.0, 'vars': 9.0, 'rela_perf_1mo_rank': 90},
            {'ticker': 'BBB', 'rs_sts_pct': 90.0, 'vars': 8.0, 'rela_perf_1mo_rank': 85},
            {'ticker': 'CCC', 'rs_sts_pct': 85.0, 'vars': 7.0, 'rela_perf_1mo_rank': 80},
            {'ticker': 'DDD', 'rs_sts_pct': 40.0, 'vars': -2.0, 'rela_perf_1mo_rank': 30},
            {'ticker': 'EEE', 'rs_sts_pct': 35.0, 'vars': -3.0, 'rela_perf_1mo_rank': 25},
            {'ticker': 'FFF', 'rs_sts_pct': 30.0, 'vars': -4.0, 'rela_perf_1mo_rank': 20},
        ])
        themes = {
            'AAA': ['Cybersecurity / Network'],
            'BBB': ['Cybersecurity / Network'],
            'CCC': ['Cybersecurity / Identity', 'AI / Software & Analytics'],
            'DDD': ['Cybersecurity / Identity'],
            'EEE': ['Biotech / Immunology'],
            'FFF': ['Biotech / Immunology'],
        }
        snap = compute_radar(master, ticker_themes=themes,
                             screened_tickers={'AAA', 'CCC'}, cfg=CFG)
        self.assertIsNotNone(snap)
        self.assertEqual(snap['universe_size'], 6)
        # AI leaf has breadth 1 -> unscored; 3 scored leaves remain
        self.assertEqual(snap['n_leaves_scored'], 3)
        self.assertEqual(snap['params']['beta'], 0.3)

        l1s = snap['l1s']
        self.assertEqual([e['rank'] for e in l1s], [1, 2])
        self.assertEqual(l1s[0]['name'], 'Cybersecurity')
        self.assertEqual(l1s[0]['n_leaves'], 2)
        self.assertEqual(l1s[0]['n_members'], 4)
        self.assertEqual(l1s[0]['n_screened'], 2)
        # Two-leaf L1 gets boosted above its raw (positive l1_raw)
        self.assertGreater(l1s[0]['boosted'], l1s[0]['raw'])

        for l1_entry in l1s:
            for leaf in l1_entry['leaves']:
                self.assertIn('global_rank', leaf)
                for m in leaf['members']:
                    self.assertEqual(m['is_screened'], m['ticker'] in {'AAA', 'CCC'})

    def test_returns_none_when_nothing_scores(self):
        master = make_master([{'ticker': 'AAA', 'close': 1.0}])  # below price floor
        self.assertIsNone(compute_radar(master, ticker_themes={'AAA': ['AI / Robotics']},
                                        cfg=CFG))


if __name__ == '__main__':
    unittest.main()
