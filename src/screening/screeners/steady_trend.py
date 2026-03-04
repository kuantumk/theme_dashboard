# screener name: steady_trend
# description: Captures slow-moving (low ADR), highly liquid stocks in strong structural uptrends with extreme relative strength.

def filter_master_table(master_df):
    filter_conditions = (
        # Enhanced liquidity for low-ADR names (avg dollar vol >= $20M)
        (master_df['avg_dollar_vol'] >= 20.0e6) &
        
        # Volume minimums
        (
            (master_df['volume'] >= 500e3) &
            (master_df['vol_sma50'] >= 1.0e6)
        ) &
        
        # ADR % specifically targeting the slow-mover pocket (2% to 4%)
        (master_df['adr_pct'] >= 0.02) &
        (master_df['adr_pct'] < 0.04) &

        # Price minimum
        (master_df['close'] >= 5.0) &

        # Extreme Leadership Required (RS_STS >= 90)
        (master_df['rs_sts_pct'] >= 90.0) &

        # Structural Uptrend Alignment
        (
            (master_df['close'] > master_df['sma50']) &
            (master_df['sma50'] > master_df['sma200']) 
        )
    )

    return filter_conditions
