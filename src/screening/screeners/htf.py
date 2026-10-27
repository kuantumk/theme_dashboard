# screener name: htf
# description: high tight flag

from src.screening.screeners._liquidity import base_liquidity


def filter_master_table(master_df):
    return (
        base_liquidity(master_df) &
        (master_df['adr_pct'] > 0.04) &
        (master_df['close'] > master_df['sma25']) &
        (master_df['ema20'] > master_df['sma50']) &
        (master_df['sma50'] > master_df['sma100']) &
        # performance
        (master_df['max150'] / master_df['min150'] >= 2) &
        # tight range before B/O
        (master_df['price_chg_pct0'].abs() <= 0.03)
    )
