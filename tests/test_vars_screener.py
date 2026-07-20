import unittest
from unittest.mock import patch

import pandas as pd

from src.screening.screeners import vars as vars_screener


def _row(**overrides):
    base = {
        'avg_dollar_vol': 50e6,
        'vol_sma50': 2e6,
        'close': 100.0,
        'adr_pct': 0.04,
        'vars': 5.0,
    }
    base.update(overrides)
    return base


class VarsScreenerGateTests(unittest.TestCase):
    def _run(self, rows, cfg=None):
        df = pd.DataFrame(rows)
        with patch.dict(vars_screener.CONFIG, {'vars_screener': cfg or {}}):
            return vars_screener.filter_master_table(df).tolist()

    def test_each_gate_rejects_independently(self):
        rows = [
            _row(),                          # passes every gate
            _row(avg_dollar_vol=30e6),       # dollar volume below $40M
            _row(vol_sma50=0.5e6),           # share volume below 1M
            _row(close=1.5),                 # price below $2
            _row(adr_pct=0.015),             # ADR below the 2% floor
            _row(vars=1.0),                  # VARS below 2
        ]
        self.assertEqual(self._run(rows), [True, False, False, False, False, False])

    def test_consolidating_large_cap_passes_at_2pct_not_at_legacy_3_3pct(self):
        # PANW-style: huge liquidity, ADR compressed to 2.5% post-run while
        # VARS keeps rising — the exact profile the old hardcoded 3.3% ejected.
        leader = _row(avg_dollar_vol=800e6, vol_sma50=5e6, close=350.0,
                      adr_pct=0.025, vars=14.0)
        self.assertEqual(self._run([leader]), [True])
        self.assertEqual(self._run([leader], cfg={'min_adr_pct': 0.033}), [False])

    def test_gates_read_config_overrides(self):
        self.assertEqual(self._run([_row(vars=2.5)], cfg={'min_vars': 3.0}), [False])
        self.assertEqual(self._run([_row(vars=2.5)], cfg={'min_vars': 2.0}), [True])


if __name__ == '__main__':
    unittest.main()
