# screener name: darvas
# description: Darvas stock selection


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
            (master_df['close'] > master_df['sma50'])
        ) &
        # performance
        (
            (master_df['max252'] / master_df['min252'] >= 2) &
            (master_df['close'] >= 0.7 * master_df['max252'])
        )
    )

    return filter_conditions
