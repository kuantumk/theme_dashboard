# screener name: denvol
# description: dense up-volume spike that is also the highest volume of the trailing 365 days
# Ported from project608; up_dollar_vol_max threshold 10M -> 40M and price 1.0 -> 2.0.


def filter_master_table(master_df):
    filter_conditions = (
        # liquidity
        (
            (master_df['up_dollar_vol_max'] >= 40e6) &
            (master_df['volume'] >= 300e3)
        ) &

        # ADR %
        (master_df['adr_pct'] >= 0.04) &

        # price
        (
            (master_df['close'] >= 2.0) &
            (master_df['close'] >= master_df['sma200'])
        ) &

        # calendar days since max up volume happened
        (master_df['days_since_up_vol_max'] <= 30) &
        # need to check if max volume and up max volume happened on the same day
        (master_df['days_since_up_vol_max'] == master_df['days_since_vol_max'])
    )

    return filter_conditions
