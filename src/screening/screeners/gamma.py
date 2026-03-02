# screener name: gamma
# description: top performer >= 20% gain in last 30 sessions


def filter_master_table(master_df):
    filter_conditions = (
        # liquidity
        (master_df['avg_dollar_vol'] >= 10.0e6) &

        # volume and average volume
        (
            (master_df['volume'] >= 500e3) &
            (master_df['vol_sma50'] >= 1.0e6)
        ) &

        # ADR %
        (master_df['adr_pct'] >= 0.04) &

        # price
        (
            (master_df['close'] >= 1.0) &
            (master_df['high'] > master_df['sma30'])
        ) &

        # performance
        (master_df['max30'] / master_df['min30'] >= 1.2)
    )

    return filter_conditions
