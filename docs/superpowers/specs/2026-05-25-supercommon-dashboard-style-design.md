# Supercommon-inspired dashboard style refresh

**Date:** 2026-05-25
**Branch:** `claude/tender-dijkstra-e42f69`
**Reference:** [supercommon.systems](https://supercommon.systems/) — brutalist-minimalist aesthetic.
**Approach:** Brutalist chassis, functional core. Adopt the mood without breaking trading-dashboard readability.

## Goal

Reskin the Theme Dashboard so it feels like supercommon.systems while preserving the dashboard's information density and functional color semantics (red/green up/down, RS tier colors, status badges).

## Decisions (locked)

- **Font:** Inter (weights 100/200/300/400) for display chrome via Google Fonts. IBM Plex Mono kept for tabular numerics + tickers. DM Sans removed.
- **Lowercase scope:** chrome only — logo, tabs, card labels, section tags, badges. Tickers and table data stay as-is.
- **Functional colors:** red/green up/down, RS tier colors, market-status badges all preserved.
- **Background:** pure black `#000`.
- **Link blue:** `#5566ff` (a brighter cousin of supercommon's `#0000EE` that passes WCAG AA on black at 4.86:1).

## Color tokens

| Token | Old | New | Use |
|---|---|---|---|
| `--bg` | `#07090d` | `#000000` | page |
| `--bg2` | `#0c0f15` | `#050505` | header / card subtle layer |
| `--bg3` | `#111520` | `#0a0a0a` | table head, hover deeper |
| `--bg4` | `#161b26` | `#111111` | hover/active |
| `--border` | `#1c2535` | `#1a1a1a` | hairline borders |
| `--border2` | `#243044` | `#262626` | hover/secondary borders |
| `--accent` | `#00c8ff` | `#5566ff` | links, active tab, accent |
| `--accent2` | `#0099cc` | `#4455ee` | hover blue |
| `--adim` | `rgba(0,200,255,.07)` | `rgba(85,102,255,.10)` | accent dim fill |
| `--text` | `#c8d8ea` | `#eeeeee` | body |
| `--text2` | `#7292b0` | `#888888` | muted |
| `--text3` | `#3d5a78` | `#555555` | tertiary |
| `--white` | n/a | `#ffffff` | hero numbers, white CTAs |
| `--green` `#00e676` | unchanged | unchanged | up |
| `--red` `#ff3355` | unchanged | unchanged | down |
| `--amber` `#ffb300` | unchanged | unchanged | warn / premarket |

## Typography

```css
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@300;400;500;600&family=Inter:wght@100;200;300;400&display=swap');

--mono: 'IBM Plex Mono', monospace;
--display: 'Inter', sans-serif;
/* DM Sans removed */
```

| Element | Old | New |
|---|---|---|
| `body` | mono 14 | mono 14 (kept — data legibility) |
| `.logo` | mono 600 17 +3px UPPER | Inter 200 22 −0.5px lowercase |
| `.tab-btn` | mono 500 12 +2px UPPER | Inter 300 13 0 lowercase |
| `.card-lbl` | mono 500 11 +2px UPPER | Inter 300 12 0 lowercase |
| `thead th` | mono 500 10 +1.5px UPPER | Inter 300 10 0 lowercase |
| `.section-tag` | mono UPPER | Inter 300 lowercase |
| `.breadth-value` | mono 600 22 | Inter 200 28 −1.2px |
| `.theme-name` | mono 600 13 | Inter 400 14 lowercase |
| `.tn` ticker | mono 500 13 | unchanged (uppercase functional) |
| `td` | mono 13 | unchanged |
| Links | colored hover | `text-decoration: underline 1px` always, color `--accent` |

## Structural changes

- `border-radius` → `0` everywhere (cards, badges, buttons, dropdowns, network containers).
- Cards lose `background-color`, keep `1px solid var(--border)` for structure.
- Card label loses colored bg strip — plain lowercase label + bottom hairline.
- Resize handle uses `#111` background, drops `⋮` glyph.

## Decorative removals

- `body::before` dot-grid overlay — removed.
- `.hdr::after` cyan→green gradient sweep — removed.
- `▸` prefix in 13 tab buttons + ~19 card labels (32 occurrences total in `docs/index.html`) — removed.
- `📅` emoji from macro events button — removed.
- `theme-network` radial-gradient — replaced with solid `#000`.

## CTAs

- `.events-button` → white background `#ffffff`, black text, no radius, lowercase.
- `.chart-alert-btn` (TV alert) → keep amber color but drop radius, drop gradient, lowercase.
- Market status badges → keep colored, lowercase (`open`/`closed`/`premarket`).

## Files

- `docs/style.css` — bulk of changes.
- `docs/index.html` — strip 32 decoration glyphs.
- `docs/app.js` — change `.toUpperCase()` calls on lines 110, 535 to `.toLowerCase()`.
- No data/JSON files modified.

## Out of scope

- No layout/structural changes (tabs, panels, columns unchanged).
- No JS interaction logic changes (resize, time-travel, sorts unchanged).
- Cytoscape node fill colors stay (functional — RS / VARS tiers, leader/bridge rings).

## Verification

- Open `docs/index.html` locally; walk every tab.
- Confirm link contrast (`#5566ff` on `#000` = 4.86:1 — passes AA 4.5).
- Confirm tickers stay uppercase, table numbers legible.
- Confirm red/green up/down still pop against new black background.
- Confirm no regressions in time-travel, theme viz, EP scan tables.
