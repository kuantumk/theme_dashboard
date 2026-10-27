# screener name: darvas
# description: Darvas stock selection

from src.screening.screeners._liquidity import base_liquidity


def filter_master_table(master_df):
    return (
        base_liquidity(master_df) &
        (master_df['adr_pct'] >= 0.04) &
        (master_df['close'] > master_df['sma50']) &
        # performance: doubled off the 252-day low, within 30% of the high
        (master_df['max252'] / master_df['min252'] >= 2) &
        (master_df['close'] >= 0.7 * master_df['max252'])
    )
