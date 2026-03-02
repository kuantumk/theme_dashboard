# screener name: topdog
# description: top performancer with high ADR%


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
        (master_df['adr_pct'] > 0.04) &

        # price
        (
            (master_df['close'] >= 1.0)
        ) &

        # current price vs past low rank
        (
            (master_df['c0_c30_rank'] >= 96) |
            (master_df['c0_c60_rank'] >= 96) |
            (master_df['c0_c90_rank'] >= 96) |
            (master_df['c0_c120_rank'] >= 96)
        )
    )

    return filter_conditions
