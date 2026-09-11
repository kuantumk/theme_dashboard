# SI Tab — Highly Shorted Stocks in a Drawdown

**Date:** 2026-09-10
**Status:** approved, ready to implement

## Goal

Add a dashboard tab named **SI**. It shows stocks with high short interest that
have already sold off, grouped under the theme taxonomy. A theme that holds
several heavily-shorted names ranks above a theme that holds one.

The trade this serves: a crowded short in a broken chart inside a strong sector
is a squeeze candidate. The sector strength must come from the sector, not from
the broken name.

## Data source

Finviz screener, Ownership view (`v_page=131`). Five filters:

| Finviz URL token | finvizfinance filter |
|---|---|
| `sh_avgvol_o1000` | `Average Volume` = `Over 1M` |
| `sh_curvol_o750` | `Current Volume` = `Over 750K` |
| `sh_price_o10` | `Price` = `Over $10` |
| `sh_short_o10` | `Float Short` = `Over 10%` |
| `ta_volatility_mo4` | `Volatility` = `Month - Over 4%` |

The Ownership view returns `Short Float`, `Float`, `Market Cap`, `Inst Trans`,
`Price` and `Volume` in the screener table itself. No per-ticker quote page is
needed. One screener call replaces 231 rate-limited page loads.

Measured 2026-09-10: 231 rows returned, 184 above 12% short interest.

### The ticker-avatar repair is mandatory here

Finviz's 2026-07-15 redesign doubles the first character of every ticker
(`OKLO` arrives as `OOKLO`). Measured on this screener: **231 of 231 rows were
corrupted**. `ep_scan_common.repair_ticker_column` already fixes this, but the
subclass wrapping it is bound to `Overview`. Extract a
`make_ticker_repair_view(base_view_cls)` factory in `ep_scan_common.py` and
build both the EP scans' `Overview` and this module's `Ownership` from it.

Do not un-double the first character. See CLAUDE.md.

### Why `Current Volume` stays in the filter set

It makes the result time-of-day dependent: before the open, no ticker has
750K of current volume. The daily workflow runs at 1:30 PM Pacific, after the
close, so current volume is the full session. Keep the filter to match the
source URL, and keep the collection step inside the daily workflow only.

## New indicator columns

Added to `create_technical_indicators.py`. `create_master_table.py` copies the
last row of each ticker frame wholesale, so both columns reach every master
parquet with no further work, and stay point-in-time safe for back-dated
sessions.

- **`drop_15d`** — `(close / close.rolling(16).max() - 1).rolling(45).min()`.
  The worst close-to-close drawdown over any window of 15 sessions or fewer,
  inside the last 45 sessions.
- **`down_streak`** — consecutive down closes ending on the current bar.

`drawdown_60` needs no column. `max60` already exists, so the builder computes
`close / max60 - 1`.

### The rolling minimum is load-bearing

The obvious form — today's bar against its own trailing 15-session high — is
wrong. Measured over 183 names it correlates only **0.37** with a true window
search, and it reads AEHR at **−22.5%**, which fails AEHR's own gate. The
rolling minimum reads AEHR at **−47.6%**, matches the window search exactly on
that name, and correlates **0.914** across the set.

A name that fell 50% three weeks ago and then went flat is still a broken
chart. Today's bar cannot see that. The 45-session rolling minimum can.

## Gate

```
short_interest > 12%
AND (drawdown_60 <= -25% OR drop_15d <= -25%)
```

Measured 2026-09-10: 184 names above 12% SI, **105 survive** the selloff gate.
AEHR passes on both legs (`drawdown_60` −35.2%, `drop_15d` −47.6%).

`down_streak` is displayed, never gated. AEHR's streak of 11 is useful colour,
but a name that fell 40% in three gap-downs has a streak of 1 and belongs on
this tab.

Both thresholds are config knobs. Tighten `drop_15d` toward −30% to cut the
survivor count if the tab reads too loose.

## Scoring and grouping

**L1 is the ranked unit.** Leaves render as sub-tables inside their L1 section,
matching the VARS tab.

- **L1 score** = mean of the top 3 member short-interest values.
- **L1 order** = score descending, then member count descending.
- **L1 minimum** = 3 members. No per-leaf minimum.
- **Leaf order inside an L1** = leaf's own top-3 mean, descending.
- **Ticker order inside a leaf** = short interest descending.

### Why L1 and not leaf

At leaf level the measured data gives 67 leaves, of which only 24 hold 2 or
more members. Single-name leaves would fill the top of the tab and the
"multiple names boost the theme" requirement would never fire. Breadth as a
tiebreak does nothing against float scores — exact ties do not occur.

At L1 level with a 3-member minimum the data resolves to 9 sections covering
all 105 tickers, and breadth and intensity both show.

Measured preview:

```
AI                    54.80   n=29   8 leaves   HOT
Fintech & Crypto      34.93   n=15   5 leaves   HOT
Biotech               32.47   n=19  15 leaves   HOT
Space                 25.04   n=5    2 leaves
Software & Internet   24.73   n=5    3 leaves   HOT
Consumer              23.58   n=6    5 leaves
Nuclear               22.59   n=3    2 leaves
Clean Energy          22.49   n=4    3 leaves
MedTech               20.70   n=3    3 leaves
```

## Hot sector badge

An L1 earns **HOT** when its rank in `docs/data/radar.json` is 10 or better.

The L1 Radar is the correct source. It scores every tagged ticker with no
screener gate, so it reports that semiconductors are strong while AEHR is
broken. The broken name's own RS cannot report that — it is low by
construction, which is why the name is on this tab.

The badge never changes the order. Ranking stays on short interest, as
specified. A missing or unreadable `radar.json` drops every badge and changes
no ordering.

## Outputs

- `data/short_interest.json` — committed. `{date, filters, rows: [...]}`.
  One row per Finviz result, before the selloff gate.
- `docs/data/si.json` — current snapshot.
- `docs/data/si_history.json` — forward-accumulating session history.

### History only grows forward

Finviz publishes current short interest, not a per-session series. Rebuilding
history from the master parquet would pin today's short interest onto a
three-month-old price bar and publish the result as that session's reading.
That is a fabricated number, not a stale one.

So this tab accumulates one entry per workflow run through
`_update_history_file`, exactly like the ETF and EP tabs, pruned to the same
`THEMES_HISTORY_DAYS` window. The time-travel dropdown holds one date on the
first run and grows from there. That is correct behaviour, not a defect to fix.

## Dashboard

- Tab button **SI**, last in the bar, after Parabolic.
- Two-pane split: list left, chart right, matching every other table tab.
  (`.left-panel` is the list pane — see CLAUDE.md on the `order` swap.)
- Columns: Ticker, SI%, DD60%, Drop15%, Streak, Price, Float(M), Inst%.
- Time-travel bar with the standard date button and `+ more` dropdown.

### No V/A cutoff dropdowns

The Finviz screen already gates price above $10, average volume above 1M and
monthly volatility above 4%. CLAUDE.md's rule for exactly this case is the EP
tab: no cutoffs when the screen happens upstream.

Two consequences. The payload carries no `dollar_vol` / `adr_pct`, and the
panel keeps the shared 400px default width rather than needing the 132px the
cutoff pair costs. A test pins the absence so nobody harmonises it later.

## Config

```yaml
si_tab:
  min_short_interest: 12.0   # percent of float
  max_drawdown_60d: -0.25    # close vs 60-day high
  max_drop_15d: -0.25        # worst <=15-session drop inside the last 45
  min_tickers_per_l1: 3
  top_k_si: 3                # members averaged into an L1 score
  hot_radar_rank: 10         # radar L1 rank at or better than this earns HOT
```

## Tests

- `tests/test_si_gate.py` — gate arithmetic, both legs, boundary values, NaN
  handling.
- `tests/test_si_grouping.py` — top-3 mean, breadth tiebreak, 3-member minimum,
  leaf and ticker ordering, HOT from radar rank, missing radar file.
- `tests/test_si_fetch.py` — ticker repair through the `Ownership` view, short
  interest percent parsing, 200-but-empty warning distinct from fetch failure.
- `tests/test_drop_15d.py` — the rolling-minimum definition, including the
  property that it never reads shallower than the per-bar form.
- Extend `tests/test_dashboard_panel_layout.py` with the SI panel.
- Markup test pinning that the SI tab carries no V/A cutoff selects.

## Out of scope

- No taxonomy work. Tag coverage of the measured list is 184 of 184.
- No EPS or Sales columns. Those need per-ticker quote pages.
- No change to any existing tab's data or layout.
- No network viz. This tab is a table.
