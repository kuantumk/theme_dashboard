"""The zone-map helper: floors, distances, zones and states.

The helper reads an UNFILLED master frame. A zero-filled weekly EMA would
put a name far up the extended zone with nothing on screen to show it, so
every missing input must leave the name off and be counted.
"""

import unittest

import pandas as pd

from src.reporting import export_dashboard_data as ex

CFG = dict(ex.ZONE_MAP_DEFAULTS)
NAN = float('nan')


def _master(rows):
    """rows: (ticker, close, ema20, ema20_w, atr14, tight_range)."""
    return pd.DataFrame([{
        'ticker': t, 'close': c, 'ema20': e, 'ema20_w': w, 'atr14': a,
        'tight_range': tr, 'tight_len': 5,
    } for t, c, e, w, a, tr in rows])


#: One clean bar: d_daily +0.5, d_weekly +2.5, the buy area.
ONE = _master([('A', 100, 99, 95, 2, False)])


def _row(ticker, dollar_vol=150e6, adr=0.05, vars_=5.0, rs=80.0, highlight=None):
    return {'ticker': ticker, 'dollar_vol': dollar_vol, 'adr_pct': adr,
            'vars': vars_, 'rs': rs, 'highlight': highlight}


class ClassifyZoneTests(unittest.TestCase):
    CASES = [
        (0.0, 2.0, 'band', 'normal', 'buy_area'),
        (-0.5, 0.0, 'band', 'normal', 'buy_area'),      # both lower limits are inclusive
        (2.0, 4.0, 'band', 'normal', 'buy_area'),       # both upper limits are inclusive
        (0.0, 4.01, 'band', 'extended', 'buy_stretched'),
        (2.01, 2.0, 'above', 'normal', 'above'),
        (3.0, 5.0, 'above', 'extended', 'extended'),
        (-0.51, 2.0, 'under', 'normal', 'under_daily'),
        (-1.0, 6.0, 'under', 'extended', 'under_daily'),
        (0.0, -0.01, 'band', 'below', 'weekly_broken'),
        (3.0, -1.0, 'above', 'below', 'weekly_broken'),
        (-1.0, -1.0, 'under', 'below', 'under_both'),
    ]

    def test_each_cell_and_each_limit(self):
        for d, w, zd, zw, state in self.CASES:
            self.assertEqual((zd, zw, state), ex.classify_zone(d, w, CFG), f'({d}, {w})')

    def test_all_nine_cells_have_a_state_and_all_states_are_reachable(self):
        self.assertEqual(9, len(ex._ZONE_STATE_BY_CELL))
        self.assertEqual(set(ex.ZONE_STATES), set(ex._ZONE_STATE_BY_CELL.values()))
        self.assertEqual(7, len(ex.ZONE_STATES))


class ZoneDistanceTests(unittest.TestCase):
    def test_a_clean_bar(self):
        self.assertEqual(1.0, ex.zone_distance(12.0, 10.0, 2.0))

    def test_missing_or_non_positive_inputs_are_none(self):
        for args in [(None, 10, 1), (NAN, 10, 1), (10, NAN, 1), (10, 9, NAN),
                     (10, 0, 1), (10, 9, 0), (0, 9, 1), (-5, 9, 1)]:
            self.assertIsNone(ex.zone_distance(*args), args)


class BuildZoneMapTests(unittest.TestCase):
    def test_floors_drop_and_count_a_missing_metric_fails_closed(self):
        master = _master([(t, 100, 99, 95, 2, False) for t in 'ABCD'])
        rows = [_row('A'), _row('B', dollar_vol=99e6), _row('C', adr=0.039),
                _row('D', dollar_vol=None)]
        zm = ex.build_zone_map(rows, master, {}, CFG)
        self.assertEqual(['A'], [t['ticker'] for t in zm['tickers']])
        self.assertEqual(4, zm['n_candidates'])
        self.assertEqual(3, zm['n_floor_dropped'])

    def test_a_missing_weekly_ema_is_left_off_never_read_as_zero(self):
        master = _master([('A', 100, 99, 95, 2, False), ('B', 100, 99, NAN, 2, False)])
        zm = ex.build_zone_map([_row('A'), _row('B')], master, {}, CFG)
        self.assertEqual(['A'], [t['ticker'] for t in zm['tickers']])
        self.assertEqual(1, zm['n_no_weekly'])

    def test_a_bad_bar_counts_as_no_daily_reading(self):
        master = _master([('A', 100, 99, 95, 0.0, False)])
        zm = ex.build_zone_map([_row('A')], master, {}, CFG)
        self.assertEqual([], zm['tickers'])
        self.assertEqual(1, zm['n_no_daily'])

    def test_a_ticker_with_no_master_row_counts_as_no_weekly(self):
        master = ONE
        zm = ex.build_zone_map([_row('ZZZ')], master, {}, CFG)
        self.assertEqual(1, zm['n_no_weekly'])

    def test_a_master_without_the_column_publishes_none(self):
        master = ONE.drop(columns=['ema20_w'])
        self.assertIsNone(ex.build_zone_map([_row('A')], master, {}, CFG))
        self.assertIsNone(ex.build_zone_map([_row('A')], None, {}, CFG))

    def test_a_duplicate_row_makes_one_dot(self):
        master = ONE
        zm = ex.build_zone_map([_row('A'), _row('a')], master, {}, CFG)
        self.assertEqual(1, zm['n_candidates'])
        self.assertEqual(1, len(zm['tickers']))

    def test_leaves_keep_every_real_leaf_and_drop_catch_alls(self):
        master = ONE
        leaves = {'A': ['AI / Data Center / Optics', 'Cybersecurity / Network', 'Singleton']}
        zm = ex.build_zone_map([_row('A')], master, leaves, CFG)
        self.assertEqual(['AI / Data Center / Optics', 'Cybersecurity / Network'],
                         zm['tickers'][0]['leaves'])

    def test_a_nan_tight_flag_is_not_tight(self):
        master = _master([('A', 100, 99, 95, 2, NAN), ('B', 100, 99, 95, 2, True)])
        zm = ex.build_zone_map([_row('A'), _row('B')], master, {}, CFG)
        by = {t['ticker']: t for t in zm['tickers']}
        self.assertFalse(by['A']['tight'])
        self.assertIsNone(by['A']['tight_len'])
        self.assertTrue(by['B']['tight'])
        self.assertEqual(5, by['B']['tight_len'])

    def test_the_payload_shape(self):
        master = ONE
        row = dict(_row('A', highlight='ma_up'), ticker_color='green')
        zm = ex.build_zone_map([row], master, {}, CFG)
        self.assertEqual({'params', 'n_candidates', 'n_floor_dropped', 'n_no_weekly',
                          'n_no_daily', 'tickers'}, set(zm))
        t = zm['tickers'][0]
        self.assertEqual({'ticker', 'd_daily', 'd_weekly', 'zone_daily', 'zone_weekly',
                          'state', 'vars', 'rs', 'tight', 'tight_len', 'highlight',
                          'ticker_color', 'leaves'}, set(t))
        self.assertEqual((0.5, 2.5, 'buy_area'), (t['d_daily'], t['d_weekly'], t['state']))
        self.assertEqual(('ma_up', 'green'), (t['highlight'], t['ticker_color']))
        self.assertEqual(CFG['top_leaves'], zm['params']['top_leaves'])


if __name__ == '__main__':
    unittest.main()
