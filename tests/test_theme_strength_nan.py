"""rs_sts_pct is NaN for tickers without the full RS lookback (young IPOs).

Guards the fillna(0) removal in create_master_table: theme metrics must
aggregate over known RS values only, fall back to the neutral default when
none are known, and score NaN-RS members with the neutral default instead
of dragging them to 0.
"""

import unittest

import numpy as np
import pandas as pd

from src.themes.analyze_theme_strength import (
    MISSING_DEFAULT,
    MOMENTUM_THRESHOLD,
    calculate_theme_metrics,
)


def make_master(rows):
    defaults = {'rs_sts_pct': 50.0, 'vars': 0.0}
    return pd.DataFrame([{**defaults, **r} for r in rows])


class NaNRSTest(unittest.TestCase):
    def test_nan_member_aggregates_over_known_values(self):
        master = make_master([
            {'ticker': 'AAA', 'rs_sts_pct': 90.0, 'vars': 10.0},
            {'ticker': 'BBB', 'rs_sts_pct': 80.0, 'vars': 5.0},
            {'ticker': 'CCC', 'rs_sts_pct': np.nan, 'vars': 20.0},
        ])
        m = calculate_theme_metrics('AI / Test', ['AAA', 'BBB', 'CCC'], master, fundamentals={})
        self.assertIsNotNone(m)
        self.assertAlmostEqual(m['avg_rs_sts'], 85.0)
        self.assertAlmostEqual(m['median_rs_sts'], 85.0)
        expected_count = int(90.0 > MOMENTUM_THRESHOLD) + int(80.0 > MOMENTUM_THRESHOLD)
        self.assertEqual(m['high_momentum_count'], expected_count)
        self.assertAlmostEqual(m['high_momentum_pct'], expected_count / 2 * 100)
        # NaN-RS member composites with the neutral default, not 0
        self.assertAlmostEqual(m['ticker_scores']['CCC'], (MISSING_DEFAULT + 20.0) / 2)

    def test_all_nan_rs_falls_back_to_neutral(self):
        master = make_master([
            {'ticker': 'AAA', 'rs_sts_pct': np.nan, 'vars': 10.0},
            {'ticker': 'BBB', 'rs_sts_pct': np.nan, 'vars': 5.0},
        ])
        m = calculate_theme_metrics('AI / Test', ['AAA', 'BBB'], master, fundamentals={})
        self.assertIsNotNone(m)
        self.assertAlmostEqual(m['avg_rs_sts'], MISSING_DEFAULT)
        self.assertAlmostEqual(m['median_rs_sts'], MISSING_DEFAULT)
        self.assertEqual(m['high_momentum_count'], 0)
        self.assertEqual(m['high_momentum_pct'], 0.0)
        self.assertFalse(np.isnan(m['avg_rs_sts']))


if __name__ == '__main__':
    unittest.main()
