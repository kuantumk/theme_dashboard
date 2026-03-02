# screener name: htf
# description: high tight flag


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
            (master_df['close'] >= 1.0) &
            (master_df['close'] > master_df['sma25']) &
            (master_df['ema20'] > master_df['sma50']) &
            (master_df['sma50'] > master_df['sma100'])
        ) &

        # performance
        (master_df['max150'] / master_df['min150'] >= 2) &

        # tight range before B/O
        (
            (master_df['price_chg_pct0'].apply(lambda x: abs(x)) <= 0.03)
        )
    )

    return filter_conditions
