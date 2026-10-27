# screener name: gamma
# description: top performer >= 20% high-to-low range in last 30 sessions

from src.screening.screeners._liquidity import base_liquidity


def filter_master_table(master_df):
    return (
        base_liquidity(master_df) &
        (master_df['adr_pct_partial'] >= 0.04) &
        (master_df['high'] > master_df['sma30_partial']) &
        # performance
        (master_df['max30'] / master_df['min30'] >= 1.2)
    )
