# screener name: vars
# description: Volatility-Adjusted Relative Strength leaders, both raw and EMA(20) > 2


def filter_master_table(master_df):
    return (
        # dollar volume >= $40M
        (master_df['avg_dollar_vol'] >= 40e6) &

        # average volume > 1M shares
        (master_df['vol_sma50'] >= 1e6) &

        # price > $2
        (master_df['close'] > 2) &

        # ADR% >= 3.3%
        (master_df['adr_pct'] >= 0.033) &

        # VARS > 2 and VARS 20EMA > 2
        (master_df['vars'] > 2) &
        (master_df['vars_20ema'] > 2)
    )
