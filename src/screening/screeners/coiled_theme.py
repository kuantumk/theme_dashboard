# screener name: coiled_theme
# description: former leaders compressing near moving averages before RS/VARS re-accelerate

from src.screening.coiled_theme import add_coiled_theme_metrics, copy_coiled_columns


def filter_master_table(master_df):
    enriched = add_coiled_theme_metrics(master_df)
    copy_coiled_columns(enriched, master_df)
    return enriched["coiled_is_candidate"].fillna(False)
