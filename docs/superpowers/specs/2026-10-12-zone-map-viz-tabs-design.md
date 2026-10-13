# Zone Map — replace the four Viz networks

Date: 2026-10-12. Status: design approved in chat, spec awaiting review.

## Goal

The four Viz tabs (Theme Viz, VARS Viz, Momentum Viz, Volume Viz) draw every
survivor as a network. The user does not use them, because the network shows
too much and gives no place to focus.

Replace each network with a **zone map**. The map answers one question per
name: is the trend intact, and is there an entry now? It plots each ticker by
two distances, groups the names into seven states, and lists the leaf themes
that hold them.

The design comes from a reference chart the user supplied ("Where is each
leader?"). This spec keeps its structure and changes the moving averages, the
universe, the grouping and the colours to fit this dashboard.

## Decisions taken in chat

| Question | Decision | Reason |
|---|---|---|
| Moving averages | Daily EMA20 (x), weekly EMA20 (y) | `ema20` exists. Weekly EMA20 is one new column. |
| Unit | Daily ATR14 on both axes | One unit, so a limit means the same on each axis. `atr14` exists. |
| Liquidity floor | `avg_dollar_vol` >= $100M | User choice. |
| Volatility floor | `adr_pct` >= 4% | User choice. |
| Theme grouping | Leaf (full path, down to L3) | L2 is too coarse: `AI / Data Center` held 64 of 247 names. |
| Strength gate | Each tab's own screener. Theme Viz: the top 20 radar leaves. | See Universe. RS_STS% was rejected as a gate. |
| Scope | All four Viz tabs | Theme Viz included at the user's request. |
| Row number | RS_STS% on Momentum Viz, VARS on the other three | User choice. |
| Layout | The existing Viz split | The Viz tabs already pair a picture pane with the TradingView chart. |
| Colours | Existing tokens only | No new hue. See Colours. |

### Why RS_STS% is not a gate

`rs_sts_pct` (`src/indicators/calculate_rs_score.py`) is the percentile of
today's ticker/SPY ratio inside its own last 27 sessions. A leader that pulls
back into the daily band lowers its own ratio, so its RS_STS% falls. As a gate
it removes the names the map exists to show. Measured on the 2026-10-08
snapshot: RS_STS% >= 70 left 0 names under the daily band, while VARS > 2 left
11 names there with the weekly trend intact. RS_STS% is still fine as a
display number, which is why Momentum Viz prints it.

## Universe

Every tab uses the same two floors, then its own strength rule:

| Tab | Source | Strength rule | Names on 2026-10-08 |
|---|---|---|---|
| Theme Viz | radar members (all tagged tickers) | member of one of the top `top_leaves` (20) leaves by global radar rank | 82 |
| VARS Viz | VARS tab survivors | the `vars` screener (VARS > 2) | 70 |
| Momentum Viz | Momentum tab survivors | the `momentum_136` screener | 58 |
| Volume Viz | Volume tab survivors | the `volspike` / `denvol` union | 26 |

Floors: `avg_dollar_vol` >= `min_avg_dollar_vol` ($100M) and `adr_pct` >=
`min_adr_pct` (4%).

Theme Viz reads the **uncapped** members of each leaf. `radar_history.json`
caps chips at `tickers_per_leaf` (10), so a browser-side rule would see fewer
members on a back-dated session than on the newest one. The server therefore
picks the universe for every tab (see Payload).

Measured overlaps on 2026-10-08: Momentum and VARS share 43 names, Volume and
VARS share 15, Theme Viz and VARS share 47. Each map shows names the others do
not.

## Data

### New indicator: `ema20_w`

Add `compute_weekly_ema(close, span=20, min_weeks=20)` to
`src/indicators/create_technical_indicators.py`, and write its result to the
`ema20_w` column.

- Group the daily bars by week (`W-FRI` period of the trading date).
- `wema` = the EMA (span 20, `adjust=False`) of the last close of each
  completed week.
- For a bar in week `k`: `ema20_w = a * close + (1 - a) * wema[k-1]`, with
  `a = 2 / 21`. The day's close stands in as the live weekly bar, the way a
  TradingView weekly chart reads mid-week.
- Before `min_weeks` completed weeks exist, the value is NaN. An IPO younger
  than 20 weeks has no weekly EMA20, and the map leaves it off.
- No look-ahead: each bar reads only completed weeks and its own close.

The master table copies indicator columns as of the run date, so `ema20_w`
reaches `master_<date>.parquet` with no change to `create_master_table.py`.
The daily workflow rebuilds 130 sessions, so the full time-travel window
carries the column after the first run.

### Distances

For each ticker on a session:

- `d_daily  = (close - ema20)   / atr14`
- `d_weekly = (close - ema20_w) / atr14`

Round to 2 decimals. A NaN or a zero input publishes as null, and a null
distance leaves the ticker off the map. The payload counts such tickers in
`n_no_weekly` (missing `ema20_w`) and `n_no_daily` (anything else).

**⛔ The distances must never pass through `.fillna(0)`.** The Momentum, VARS
and Volume builders load their frames with `.fillna(0)`. A zero-filled
`ema20_w` gives `d_weekly = close / atr14`, a large positive number, and the
name plots far up the extended zone with nothing on screen to show it. The
zone-map helper reads the session's master parquet itself, without a fill.

### Zones and states

The server assigns the zones. The browser never re-decides them; it reads
`zone_daily`, `zone_weekly` and `state` off the payload, like `highlight`.

| Axis | Zone | Rule |
|---|---|---|
| daily | `under` | `d_daily < band_low` (-0.5) |
| daily | `band` | `band_low <= d_daily <= band_high` (+1.0) |
| daily | `above` | `d_daily > band_high` |
| weekly | `below` | `d_weekly < weekly_low` (0) |
| weekly | `normal` | `weekly_low <= d_weekly <= weekly_high` (+4.0) |
| weekly | `extended` | `d_weekly > weekly_high` |

Seven states, best first. Every one of the nine cells maps to exactly one state:

| # | `state` | Label | Cells |
|---|---|---|---|
| 1 | `buy_area` | In the buy area | band x normal |
| 2 | `buy_stretched` | Buy area, weekly stretched | band x extended |
| 3 | `above` | Above the buy area | above x normal |
| 4 | `extended` | Extended on both | above x extended |
| 5 | `under_daily` | Under the daily band, weekly intact | under x normal, under x extended |
| 6 | `weekly_broken` | Weekly broken | band x below, above x below |
| 7 | `under_both` | Under both | under x below |

The buy area is state 1. The limits come from the reference chart. On the
2026-10-08 VARS universe they put 25 names in the buy area and 23 above +4.
**Nothing here measures whether the buy area predicts forward returns.** The
limits are a display choice.

### Config

A new `zone_map:` block in `config/workflow_config.yaml`:

```yaml
zone_map:
  min_avg_dollar_vol: 100000000
  min_adr_pct: 4.0
  top_leaves: 20          # Theme Viz universe: members of the top N radar leaves
  weekly_ema_span: 20
  weekly_min_weeks: 20
  band_low: -0.5          # daily, in ATR14
  band_high: 1.0
  weekly_low: 0.0         # weekly, in ATR14
  weekly_high: 4.0
  x_range: [-2.0, 5.0]    # fixed plot window, in ATR14
  y_range: [-4.0, 8.0]
```

The plot window is fixed, not fitted to the data, so the zones keep their
place on screen when the user moves between sessions. A name outside the
window is pinned to the edge (see Picture). On 2026-10-08 the VARS universe
spans x -0.15 to +4.8 and y -3.0 to +7.4.

## Payload

One shared helper, `build_zone_map(master_df, tickers, leaves_by_ticker, cfg)`
in `src/reporting/export_dashboard_data.py`, returns one block:

```json
"zone_map": {
  "params": {"min_avg_dollar_vol": 1e8, "min_adr_pct": 4.0, "band_low": -0.5,
             "band_high": 1.0, "weekly_low": 0.0, "weekly_high": 4.0,
             "x_range": [-2, 5], "y_range": [-4, 8]},
  "n_candidates": 214, "n_floor_dropped": 144, "n_no_weekly": 1, "n_no_daily": 0,
  "tickers": [
    {"ticker": "CRDO", "d_daily": 0.42, "d_weekly": 2.10,
     "zone_daily": "band", "zone_weekly": "normal", "state": "buy_area",
     "vars": 7.81, "rs": 88.0, "tight": true, "tight_len": 5,
     "highlight": "tight", "ticker_color": "green",
     "leaves": ["AI / Data Center / Optics"]}
  ]
}
```

- The four snapshot builders call the helper with their own ticker list:
  `_build_radar_snapshot` (members of the top 20 leaves, uncapped),
  `_build_vars_snapshot`, `_build_momentum_136_snapshot`,
  `_build_volume_snapshot`. Each writes the block into its current file and
  into every entry of its history file.
- `leaves` comes from `data/ticker_themes.json`, the same source every other
  tab uses. A ticker with two leaves lists both.
- Both `vars` and `rs` are published. The browser picks the row number per tab.
- `highlight` and `ticker_color` come from the same per-session sources the
  builders already use, so the colour rules below apply unchanged. The short
  rung still reaches the newest session only.
- The `network` key and `_build_network` are removed from all four builders.
  The plan must first confirm nothing else calls `_build_network`.

Size: about 80 names x ~200 bytes x ~124 sessions is about 2 MB added to
`radar_history.json`, and less for the other three. Removing `network` offsets
part of it. The plan measures both numbers.

## Picture

One shared renderer, `renderZoneMap(snap, mode, date)` in `docs/app.js`,
replaces `renderNetwork`, `renderThemeNetwork`, `radarVizSnap` and
`varsVizSnap`. It reads only `snap.zone_map` and draws plain SVG. No chart
library.

### Layout

Keep the existing Viz split. The picture pane (`.viz-left`) holds, top to
bottom:

1. The time-travel bar (unchanged).
2. The scatter card.
3. The states card.
4. The clusters card.

The pane scrolls. The TradingView chart stays in the other pane, and a click
on any ticker opens it, as today. Raise the default `.viz-left` width from 50%
to 60%. The scatter needs width more than the chart does. The resize handle
still works.

### Scatter

- x = `d_daily`, y = `d_weekly`, inside the fixed `x_range` / `y_range`.
- Zone backgrounds in three vertical bands (under, band, above). The weekly
  zones are read from position against the rails.
- Rails: vertical at `band_low` and `band_high`, horizontal at `weekly_low`
  (labelled "weekly EMA20") and `weekly_high` (dashed, labelled "weekly
  extended > +4 ATR").
- The buy-area box: an outlined rectangle over band x normal, with a count tag
  ("Buy area · 25").
- Column heads over the three bands with their counts ("In the daily band · 32
  of 74").
- One dot per ticker. Fill = daily zone. A tight ticker gets a ring.
- A ticker outside the window draws as a triangle on the nearest edge, at its
  clamped position, pointing outward. Its tooltip prints the real distances.
- Labels: buy-area names first, then tight names, then the rest by row number.
  A label is skipped if it would overlap a label already drawn. Every dot
  shows its ticker, both distances, the state and the row number on hover.
- Click selects the ticker (yellow ring and label) and opens the chart.

### States card

Seven rows in state order. Each row shows the label, a one-line meaning, the
count, and the tickers. The tickers sort by the tab's row number, descending.
An empty state prints "—", not a blank row.

### Clusters card

- Group the map's tickers by leaf. A ticker in two leaves counts once in each.
- Sort leaves by buy-area count, then by total count, then by name.
- Show the top 8. Then print one line: "N other themes hold the remaining M".
- Each row has:
  - the leaf name
  - a stacked bar of daily zones (band / above / under) scaled to the row's
    total
  - a "buy-area / total" count
  - the tickers with their row number, sorted by the row number
- Names in the buy area are bold. Bold is a font weight, so this card must not
  sit inside a radar chip row that `syncRadarClamps` measures.

### Row number per tab

| Tab | Number | Colour band |
|---|---|---|
| Theme Viz, VARS Viz, Volume Viz | `vars` | `COLOR_BANDS.vars` |
| Momentum Viz | `rs` (RS_STS%) | `COLOR_BANDS.rs` |

The card caption names the number, so a reader never has to guess which one
a tab prints.

## Colours

No new hue. The dashboard is dark only (`style.css` has no light-mode block),
so one palette serves.

Retiring the networks frees their node hues. `vizRs` and `vizVars` painted
green / cyan / amber / red for strength on these same tabs; the zone map gives
the same tokens a new meaning there.

| Element | Token |
|---|---|
| Dot, daily band | `--green` |
| Dot, above the band | `--amber` |
| Dot, under the band | `--red` |
| Zone backgrounds | `--gdim`, `--adim2`, `--rdim` |
| Buy-area box outline and tag | `--green` |
| Tight ring | `--tight` |
| Selected ring and label | `--yellow` (`VIZ_COLORS.selected`) |
| Rails | `--border2`; labels `--text3` |
| Axes, captions | `--text2`, `--text3`, `--mono` |
| Dot labels | `VIZ_COLORS.tickerLabel`, `--mono` |
| Cluster bar segments | `--green` / `--amber` / `--red` |

- **A rail never takes a marker's colour.** This is the NASI rule: a red rail
  under red markers reads as one thick line.
- **Ticker names in the states and clusters cards are real `.tn-link`
  spans.** They get the highlight-tier tint, the day-pattern green text and
  the yellow `active-ticker` selection with no new rule.
- **Red for "under the band"** follows the dashboard's `dn` rule: the
  distance is negative. With a strength gate in place, those names are
  usually leaders in a pullback, not broken charts. This was a judgment call,
  taken knowingly.

### Registry

- `ZONE_RULES` in the colour registry section of `docs/app.js` mirrors the
  zone limits and the plot window. It is a mirror for drawing and for the
  Legends tab, like `PATTERN_RULES`. A test pins it to the config block.
- `ZONE_COLORS` names the CSS variables the renderer paints with. The SVG
  reads colours through `getComputedStyle`, not through hex literals.
- `window.MM_COLORS` exposes both to `docs/legends.js`.
- Remove `vizRs`, `vizVars`, `vizThemeStrength`, `VIZ_HOT` and the network
  entries of `VIZ_COLORS`. Keep `selected` and `tickerLabel`, which the map
  still uses.

## Legends

- Add one card for the zones (the three dot hues and the six limits), one for
  the buy-area box, and one for the tight ring.
- Remove the network cards and the `data-viz-legend` keys under the networks.
  Each map gets a short key drawn by `legends.js` from `ZONE_RULES` and
  `ZONE_COLORS`.
- `tests/test_dashboard_legends.py` must pass in both directions: every new
  colour class has a card, and no card names a removed class.

## Removed

- `_build_network` and every `network` payload key.
- `renderNetwork`, `renderThemeNetwork`, `renderVARSNetwork`, `radarVizSnap`,
  `varsVizSnap` and the per-mode cytoscape state.
- The cytoscape `<script>` tag in `docs/index.html`, once no tab uses it.
- The network CSS (`.theme-network*`, `.themeviz-overlay`, `.themeviz-tooltip`,
  and the matching rules for the other three tabs).
- The CLAUDE.md text that describes the networks: the Theme Viz paragraph in
  L1 Radar, the `varsVizSnap` lines in VARS, and the Viz notes in Volume and
  Highlight Tiers.

## Testing

Python (`uv run python -m unittest discover -s tests`):

- `tests/test_weekly_ema.py`
  - Point in time: each bar's `ema20_w` is the same on the full series and on
    the series cut at that bar.
  - Mid-week value equals `a * close + (1 - a) * wema[previous week]`.
  - Fewer than 20 completed weeks gives NaN.
  - A holiday-shortened week groups correctly.
- `tests/test_zone_map.py`
  - Floors: a ticker below either floor is dropped and counted.
  - The fillna trap: a frame with NaN `ema20_w` yields null, never a number.
    The test feeds the helper through a builder that fills, to prove the
    helper reads its own unfilled frame.
  - Boundaries: each of the nine cells, plus values exactly on each of the
    four limits, map to the state in the table.
  - Theme Viz: membership reads uncapped leaves, so a member ranked 11th
    inside a top-20 leaf is on the map.
  - Shape: all four builders emit the same `zone_map` keys.
  - No builder emits `network`.

Browser markup (the existing `tests/test_dashboard_*_markup.py` pattern):

- `tests/test_dashboard_zone_map_markup.py`
  - `ZONE_RULES` equals the config block.
  - The renderer has no hex literal and reads colours through `ZONE_COLORS`.
  - Tickers in the states and clusters cards render as `.tn-link`.
  - The row number on Momentum reads `rs` with `COLOR_BANDS.rs`. The other
    three read `vars` with `COLOR_BANDS.vars`.
  - No reference to cytoscape remains.
- Update `tests/test_dashboard_panel_layout.py` if it pins `.viz-left`.

Live check: build a throwaway master parquet from yfinance (the repo's usual
method when pipeline data is absent), run the export, open the dashboard in
the browser pane, and confirm each of the four tabs. Record the four
universe counts and the payload sizes in the PR.

## Build order

One spec, one plan, built in three stages:

1. `ema20_w`, `build_zone_map`, and the payload in all four builders, with
   the Python tests.
2. The shared renderer, VARS Viz, the registry and the Legends cards.
3. Theme Viz, Momentum Viz and Volume Viz on the shared renderer, then the
   removals and the CLAUDE.md updates.

## Deployment lag

A code PR resets `docs/data/`, so the four maps are empty until the next daily
workflow run republishes the files with `zone_map`. The renderer must say so
when `snap.zone_map` is absent: "Zone map data arrives with the next daily
run". It must not show an empty chart that reads as a quiet market.

## Out of scope

- Arrow-key navigation on the map.
- V / A cutoff dropdowns on the Viz tabs. The floors are fixed in config.
- A forward-return test of the buy area. It belongs in a separate study.
- Renaming the tabs. They keep their current labels.
