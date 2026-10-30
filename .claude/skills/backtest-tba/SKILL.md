---
name: backtest-tba
description: Play back one stock's spot on the Viz zone map (daily EMA20 vs weekly EMA20, in ATR14) on any past session, from live Yahoo Finance bars. Use when the user gives a ticker and a date and wants to see where the stock sat on the zone map, or invokes /backtest-tba.
---

# backtest-tba

Plot one ticker on one date on the zone map that the five Viz tabs draw. The chart holds one dot and nothing else from the dashboard.

## Input

- A ticker, for example `PLTR`.
- A date as `YYYY-MM-DD`. Correct an obvious typo (`2026-010-29` is `2026-10-29`) and say so.

## Steps

1. Run the script. Put the page in this session's scratchpad directory, not in the repo:

   ```bash
   uv run python tools/backtest_tba.py <TICKER> <YYYY-MM-DD> --out "<scratchpad dir>"
   ```

2. Read the JSON that the script prints. `page` is the path of the HTML chart.
3. Show the page to the user with `SendUserFile` and `display: "render"`.
4. Tell the user these values:
   - The session.
   - The state label.
   - The daily and weekly distances.
   - The tight-range length, if the stock is in a tight range.

## What the script does

- It downloads 2 years of daily bars up to the date from Yahoo Finance. A non-trading date reads the session before it. The JSON `session` shows which session it used.
- It uses the pipeline's own functions: `compute_ema_pair`, `compute_weekly_ema`, `compute_atr14` and `compute_tight_range`. It uses the export's own `zone_distance`, `classify_zone` and `zone_map_config`. A rule change in the repo moves this chart too.
- It does not apply the $100M dollar-volume and 4% ADR floors. Those floors choose a tab's universe, and here the user names the stock.

## Errors

- **Fewer than 20 completed weeks before the date.** The stock has no weekly EMA20, so the script plots nothing. Tell the user this.
- **No bars from Yahoo.** The ticker is delisted, renamed or wrong. Tell the user this.

## Agreement with the dashboard

On 2026-10-29 the script matched the published maps for PLTR, CRL, TEAM, NVAX and GMAB, and for CRWD and SNOW on back-dated sessions, to within 0.01 ATR. A 0.01 gap on the weekly distance is expected. The pipeline seeds the weekly EMA20 on about 500 days of bars, and this script seeds it on 730.
