# screener name: momentum_136
# description: 1/3/6-month leading momentum (25%/50%/100%) with strong liquidity and ADR


def filter_master_table(master_df):
    filter_conditions = (
        # liquidity (shares)
        (master_df['vol_sma50'] >= 750e3) &

        # liquidity (dollars, 20-day rolling)
        (master_df['avg_dollar_vol'] >= 15.0e6) &

        # ADR %
        (master_df['adr_pct'] >= 0.04) &

        # any-of momentum thresholds
        (
            (master_df['perf_1mo'] >= 0.25) |
            (master_df['perf_3mo'] >= 0.50) |
            (master_df['perf_6mo'] >= 1.00)
        )
    )
    return filter_conditions
