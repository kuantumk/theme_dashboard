# screener name: volspike
# description: highest trailing-365d volume within last 30 days + liquidity/trend filters


def filter_master_table(master_df):
    return (
        # highest-volume bar (vs trailing 365 calendar days) within the last 30 calendar days
        (master_df['days_since_highest_volume'] <= 30) &

        # max up-day dollar volume >= $40M
        (master_df['up_dollar_vol_max'] >= 40e6) &

        # average volume >= 1M shares (50-day)
        (master_df['vol_sma50'] >= 1e6) &

        # ADR% >= 4%
        (master_df['adr_pct'] >= 0.04) &

        # price >= $2 and above the 200-day SMA
        (master_df['close'] >= 2.0) &
        (master_df['close'] >= master_df['sma200'])
    )
