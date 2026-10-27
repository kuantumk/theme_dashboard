# screener name: topdog
# description: top performancer with high ADR%

from src.screening.screeners._liquidity import base_liquidity


def filter_master_table(master_df):
    return (
        base_liquidity(master_df) &
        (master_df['adr_pct'] > 0.04) &
        # current price vs past low rank
        (
            (master_df['c0_c30_rank'] >= 96) |
            (master_df['c0_c60_rank'] >= 96) |
            (master_df['c0_c90_rank'] >= 96) |
            (master_df['c0_c120_rank'] >= 96)
        )
    )
