import unittest

import pandas as pd

from src.reporting.export_dashboard_data import filter_parabolic_candidates
from src.screening.screeners.parabolic import filter_master_table


class ParabolicTests(unittest.TestCase):
    def test_export_filter_uses_project608_atr_multiple_and_previous_master(self) -> None:
        master_df = pd.DataFrame([
            {
                "date": "2026-05-01",
                "ticker": "AAA",
                "close": 20.0,
                "high": 22.0,
                "low": 18.0,
                "volume": 200_000,
                "sma50": 10.0,
                "atr14": 1.0,
                "avg_dollar_vol": 20_000_000,
                "adr_pct": 0.05,
            },
            {
                "date": "2026-05-01",
                "ticker": "BBB",
                "close": 20.0,
                "high": 21.0,
                "low": 16.0,
                "volume": 200_000,
                "sma50": 10.0,
                "atr14": 1.0,
                "avg_dollar_vol": 20_000_000,
                "adr_pct": 0.05,
            },
        ])
        previous_df = pd.DataFrame([
            {"ticker": "AAA", "high": 17.0, "low": 15.0, "volume": 100_000},
            {"ticker": "BBB", "high": 17.0, "low": 15.0, "volume": 100_000},
        ])

        result = filter_parabolic_candidates(master_df, previous_df)

        self.assertEqual(result["ticker"].tolist(), ["AAA"])
        self.assertAlmostEqual(result["atr_multi_50sma"].iloc[0], 20.0)

    def test_screener_filter_requires_no_overlap_and_volume_expansion(self) -> None:
        master_df = pd.DataFrame([
            {
                "ticker": "PASS",
                "close": 30.0,
                "high": 35.0,
                "low": 31.0,
                "volume": 500_000,
                "previous_session_high": 30.5,
                "previous_session_volume": 400_000,
                "atr_multi_50sma": 12.0,
                "avg_dollar_vol": 15_000_000,
                "adr_pct": 0.06,
            },
            {
                "ticker": "FAIL_VOL",
                "close": 30.0,
                "high": 35.0,
                "low": 31.0,
                "volume": 300_000,
                "previous_session_high": 30.5,
                "previous_session_volume": 400_000,
                "atr_multi_50sma": 12.0,
                "avg_dollar_vol": 15_000_000,
                "adr_pct": 0.06,
            },
        ])

        mask = filter_master_table(master_df)

        self.assertEqual(mask.tolist(), [True, False])

    def test_screener_and_export_filters_share_candidate_thresholds(self) -> None:
        master_df = pd.DataFrame([
            {
                "date": "2026-05-01",
                "ticker": "PASS",
                "close": 30.0,
                "high": 35.0,
                "low": 31.0,
                "volume": 500_000,
                "previous_session_high": 30.5,
                "previous_session_volume": 400_000,
                "atr_multi_50sma": 10.0,
                "avg_dollar_vol": 10_000_000,
                "adr_pct": 0.04,
            },
            {
                "date": "2026-05-01",
                "ticker": "FAIL_ATR",
                "close": 30.0,
                "high": 35.0,
                "low": 31.0,
                "volume": 500_000,
                "previous_session_high": 30.5,
                "previous_session_volume": 400_000,
                "atr_multi_50sma": 9.9,
                "avg_dollar_vol": 10_000_000,
                "adr_pct": 0.04,
            },
            {
                "date": "2026-05-01",
                "ticker": "FAIL_DV",
                "close": 30.0,
                "high": 35.0,
                "low": 31.0,
                "volume": 500_000,
                "previous_session_high": 30.5,
                "previous_session_volume": 400_000,
                "atr_multi_50sma": 10.0,
                "avg_dollar_vol": 9_999_999,
                "adr_pct": 0.04,
            },
        ])

        screener_tickers = master_df[filter_master_table(master_df)]["ticker"].tolist()
        export_tickers = filter_parabolic_candidates(master_df)["ticker"].tolist()

        self.assertEqual(screener_tickers, ["PASS"])
        self.assertEqual(export_tickers, ["PASS"])


if __name__ == "__main__":
    unittest.main()
