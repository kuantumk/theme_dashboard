---
name: backtest-tba
description: Play back one or more stocks' spots on the Viz zone map (daily EMA20 vs weekly EMA20, in ATR14) on any past session, from live Yahoo Finance bars. Use when the user gives tickers and a date and wants to see where the stocks sat on the zone map, or invokes /backtest-tba (for example `/backtest-tba PLTR SNOW MSFT 2026-10-10`).
---

# backtest-tba

Plot one or more tickers on one date on the zone map that the five Viz tabs draw. The chart holds the given names and nothing else from the dashboard.

## Input

- One or more tickers, for example `PLTR SNOW MSFT`.
- One date as `YYYY-MM-DD`, after the tickers. Correct an obvious typo (`2026-010-29` is `2026-10-29`) and say so.

## Steps

1. Run the script. Put the page in this session's scratchpad directory, not in the repo:

   ```bash
   uv run python tools/backtest_tba.py <TICKER> [<TICKER> ...] <YYYY-MM-DD> --out "<scratchpad dir>"
   ```

2. Read the JSON that the script prints:
   - `readings` holds one entry per plotted name.
   - `skipped` lists the names that did not plot, each with a `reason`.
   - `page` is the path of the HTML chart.
3. Show the page to the user with `SendUserFile` and `display: "render"`.
4. Tell the user these values for each plotted name. Use a table when there is more than one name.
   - The session.
   - The state label.
   - The daily and weekly distances.
   - The tight-range length, if the stock is in a tight range.
5. Name each skipped ticker and give its reason.

## What the script does

- It downloads 2 years of daily bars up to the date from Yahoo Finance, one ticker at a time. A non-trading date reads the session before it. Each reading's `session` shows which session it used.
- It uses the pipeline's own functions: `compute_ema_pair`, `compute_weekly_ema`, `compute_atr14` and `compute_tight_range`. It uses the export's own `zone_distance`, `classify_zone` and `zone_map_config`. A rule change in the repo moves this chart too.
- It places the labels with a port of the dashboard's label placer, so no label covers another. The dashboard places the remaining names by the tab's row number (VARS or RS). This script does not compute that number, so it places them in the order given.
- It does not apply the $100M dollar-volume and 4% ADR floors. Those floors choose a tab's universe, and here the user names the stocks.

## Errors

A ticker that fails is skipped, and the other tickers still plot. The script exits with an error only when no ticker plots.

- **Fewer than 20 completed weeks before the date.** The stock has no weekly EMA20, so there is no spot to plot.
- **No bars from Yahoo.** The ticker is delisted, renamed or wrong.

## Agreement with the dashboard

On 2026-10-29 the script matched the published maps for PLTR, CRL, TEAM, NVAX and GMAB, and for CRWD and SNOW on back-dated sessions, to within 0.01 ATR. A 0.01 gap on the weekly distance is expected. The pipeline seeds the weekly EMA20 on about 500 days of bars, and this script seeds it on 730.
