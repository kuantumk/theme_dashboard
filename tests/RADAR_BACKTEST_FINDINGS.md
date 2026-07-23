# L1 Radar Backtest: Composite Weights & Boost β

**Status**: results filled 2026-07-23 (protocol §1 was pre-registered before any sweep ran); user approved same day — §8 "Adopt" items shipped (weights 0.5/0.5/0 in config + DEFAULTS, episodes regression in `tools/radar_episodes.yaml`).
**Data**: 130 point-in-time master tables, 2026-01-21 → 2026-07-22 (fresh 500-day yfinance download, NaN-honest rs_sts_pct, tagged-pool VARS anchoring); forward windows truncate the tail → 125/120/110 scored sessions at H=5/10/20.
**Harness**: `tests/backtest_radar.py` (manual research tool, not in the daily workflow).
**Adoption gate**: no `workflow_config.yaml` change ships with this document — user reviews first.

---

## 0. Why this backtest exists

The radar's per-ticker composite weights (0.4 rs / 0.4 vars_pct / 0.2 fast) and the
sibling-confirmation boost β=0.3 were **reverse-engineered from a single competitor
screenshot** (2026-07-13 Cybersecurity case, PR #52) and had never been validated
against forward returns. `tools/validate_radar.py --sweep` prints ranks for one date
and optimizes nothing. This harness adds the missing objective function.

## 1. Pre-registered protocol (written BEFORE any sweep numbers were produced)

- **Primary metric**: mean per-session rank IC — Spearman(L1 `boosted` score,
  forward excess return of the L1's **all-members** basket), horizon **H=10**
  sessions, equal-weight close-to-close, excess vs SPY, ≥ 70% member coverage,
  ≥ 8 scored L1s per session.
- **Win rule** (for any config challenging the current one): primary IC improves,
  AND mean day-over-day rank autocorrelation degrades by less than 10% relative,
  AND the IC sign holds at H=5 and H=20.
- **Secondary metrics**: top-3 hit rate vs median L1, top-3 spread (bps), top-5
  day-over-day Jaccard, per-session IC>0 share, `--pit-tags` and `--skip-day`
  variants.
- **Statistical care**: overlapping H-day windows ≈ N/H independent observations
  (≈ 6 at H=20). Moving-block bootstrap CI (block = max(2H, 10), 2000 resamples,
  90%). H=20 numbers are direction, not significance.
- **Grids**: weights {current, equal, rs_only, vars_only, fast_only, no_fast,
  fast_heavy, rs_heavy} × β {0, 0.15, 0.3, 0.5} × H {5, 10, 20}.
- **Out of scope by user decision (2026-07-23)**: liquidity-floor sweep ($10M
  stays); fast-leg re-anchoring variant (superseded by the vars-leg re-anchoring
  that shipped as a production change, measured descriptively in §6).

## 2. Known biases and caveats

1. **Tag lookahead**: live tags are point-in-time-today and the weekday audit
   routine densifies the *top radar L1s'* baskets. Mitigation: `--pit-tags`
   replays git-history tags (available from 2026-03-01 → 90 scored sessions).
   Config **ordering is preserved** under PIT tags (§4), so the weight
   conclusion is not a lookahead artifact. PIT absolute ICs are *higher* —
   window composition differs (drops Jan-Feb), so compare orderings, not levels.
2. **Survivorship**: fresh yfinance download lacks delisted tickers.
3. **Single regime**: Jan–Jul 2026. "Supported", never "proven".
4. **Warmup**: radar legs (27/100/21 sessions) fully warmed across all 130
   sessions; only non-radar 252-day columns are partial in the oldest ~25.

## 3. Leg redundancy (Q1) — results

Pairwise Spearman across the radar universe, mean over 130 sessions (p10/p90):

| pair | mean ρ | p10 / p90 |
|------|--------|-----------|
| rs_leg ~ fast_leg | **+0.812** | +0.78 / +0.84 |
| vars_leg ~ fast_leg | +0.413 | +0.26 / +0.61 |
| rs_leg ~ vars_leg | +0.363 | +0.21 / +0.54 |

Solo-corner ICs at H=10 (live tags / PIT tags): rs_only 0.088 / 0.099,
vars_only 0.085 / 0.145, fast_only **0.065 / 0.090 — worst in both**, CI
straddling 0 at every horizon.

**Verdict**: the "duplication" critique is confirmed for the rs–fast pair —
ρ≈0.81, nearly one signal measured twice (27d time-series percentile vs 21d
cross-sectional rank of the same close/index ratio). VARS is the genuinely
distinct leg (vol-adjusted, 100-session). The fast leg adds no incremental
predictive value anywhere in the grid.

## 4. Weight sweep — results

H=10 primary (basket=all, β=0.3, 120 sessions), sorted by mean IC:

| weights | mean IC | 90% CI | IC>0 share | top-3 hit | top-3 spread | autocorr | top-5 Jaccard |
|---------|---------|--------|-----------|-----------|--------------|----------|---------------|
| **no_fast (.5/.5/0)** | **0.1014** | **+0.012 .. +0.156** | 0.708 | 0.725 | 184 bps | 0.953 | 0.765 |
| current (.4/.4/.2) | 0.0944 | +0.001 .. +0.152 | 0.675 | 0.733 | 198 bps | 0.956 | 0.770 |
| equal | 0.0911 | −0.005 .. +0.152 | 0.692 | 0.725 | 198 bps | 0.959 | 0.764 |
| rs_heavy (.6/.2/.2) | 0.0906 | −0.006 .. +0.145 | 0.675 | 0.742 | 185 bps | 0.946 | 0.749 |
| fast_heavy (.2/.4/.4) | 0.0886 | −0.004 .. +0.151 | 0.700 | 0.725 | 214 bps | 0.965 | 0.817 |
| rs_only | 0.0883 | −0.011 .. +0.139 | 0.675 | 0.725 | 182 bps | 0.933 | 0.714 |
| vars_only | 0.0845 | +0.014 .. +0.144 | 0.625 | 0.675 | 111 bps | **0.989** | **0.910** |
| fast_only | 0.0653 | −0.033 .. +0.130 | 0.633 | 0.700 | 143 bps | 0.955 | 0.789 |

**no_fast wins at every horizon** (H=5: 0.1044 vs 0.0998; H=20: 0.0666 vs
0.0611) and is the only config whose 90% CI excludes 0 at the primary. Win
rule vs current: IC improves ✓; autocorr 0.953 vs 0.956 (−0.3% relative,
inside the 10% guardrail) ✓; sign holds at H=5 and H=20 ✓. **Cleared.**

Robustness: PIT tags (90 sessions) — no_fast again best (0.1463, CI
+0.077..+0.197). Skip-day entry — no_fast again best (0.1002, only CI
excluding 0). vars_only is remarkable for stability (autocorr 0.989) but
weak at the top end (spread 37–111 bps, hit rate ≤ 0.675) — level without
timing; the rs leg earns its 0.5 by adding the freshness dimension.

## 5. β sweep incl. β=0 baseline (Q2) — results

Mean IC at current weights, live tags:

| β | H=5 | H=10 | H=20 | top-3 hit H=10 |
|---|-----|------|------|----------------|
| 0.00 | 0.0997 | 0.0944 | 0.0620 | 0.750 |
| 0.15 | 0.0998 | 0.0952 | 0.0620 | 0.733 |
| 0.30 | 0.0998 | 0.0944 | 0.0611 | 0.733 |
| 0.50 | 0.0991 | 0.0945 | 0.0610 | 0.733 |

**Predictively flat** — differences are within a couple of sessions of noise;
stability identical to 3 decimals. Rank sensitivity (`--mode history`, 124
exported sessions, top-20-truncated): β=0 changes the visible top-3 on 20/124
sessions, β=0.15 on 13/124, β=0.5 on 7/124 — the boost shapes *which* L1s you
look at ~1 session in 6, but neither ordering is measurably better.

**Answer to "how was β=0.3 chosen"**: it reproduces one competitor screenshot's
arithmetic (boosted = raw × 1.3) and was never tuned — and notably, the
motivating acceptance case itself does not hold on our data: Cybersecurity
ranks **6**, not ≤3, on 2026-07-13 — in this harness, with PIT 7/13 tags, AND
in production's own committed radar_history.json (rebuilt 7/22). The deferred
post-merge replay in PR #52 evidently never ran.

## 6. VARS re-anchoring diff (descriptive)

Tagged-pool vs old floor-survivor anchoring, production config, 130 sessions:
mean L1 rank correlation **+0.9987**, identical top-3 sets **125/130**. The
consistency change shipped per user directive; its behavioral impact is
negligible (floors currently exclude few tagged names, so the pools nearly
coincide).

## 7. Episode candidates for validate_radar regression

`--mode episodes-scan` (first top-3 entry in ≥10 sessions AND H=10 all-basket
excess > +3%; live tags — re-verify with `--pit-tags` before adopting):

| date | L1 | fwd-10 excess |
|------|----|---------------|
| 2026-01-26 | Oil & Gas | +7.84% |
| 2026-04-08 | Fintech & Crypto | +3.11% |
| 2026-04-16 | Semiconductors | +6.15% |
| 2026-06-08 | Corrections & Detention | +16.79% |

These are the radar's actual early calls on our own data — better regression
material than the aspirational 2026-07-13 Cybersecurity case (which fails,
§5). Human verification required before landing an episodes file.

## 8. Recommendations (user-approved 2026-07-23; "Adopt" items shipped)

**Adopt (config-only, pending review)** — *approved and applied*. Episode
verification during adoption rejected Fintech & Crypto 2026-04-08 (rank 9-10
under point-in-time 4/08 tags — early only retroactively; tag lookahead), so
`tools/radar_episodes.yaml` carries three episodes, not four.
- `radar.composite_weights` → `rs: 0.5, vars_pct: 0.5, fast: 0.0` (the
  `no_fast` winner; clears the pre-registered win rule, robust to PIT tags
  and skip-day). Mirror into `DEFAULTS` in `src/themes/l1_score.py`. Two legs
  also read cleaner: level (VARS, 100d vol-adjusted) + freshness (RS-line
  27d percentile); the redundant 21d rank goes.
- Replace the stale single-case acceptance in `tools/validate_radar.py` /
  CLAUDE.md with a small episodes file seeded from §7 after human
  verification (`--episodes` mode, PR-E scope).

**Keep**
- β = 0.3: no measurable predictive effect either way (§5); changing it would
  churn the visible top-3 on ~10-16% of sessions for no demonstrated gain.
  Revisit only with a longer, multi-regime window.
- $10M liquidity floor (user decision 2026-07-23, out of scope).

**Rejected for now**
- fast_heavy / fast-leg retention arguments: the leg is ρ≈0.81 redundant with
  rs and never adds IC. Revisit if a sharp-rotation regime (where 21d rank
  might lead 27d percentile) shows up in a future window.

**Needs more data**
- Everything at H=20 (≈ 6 independent windows), and any conclusion's
  generalization beyond the Jan–Jul 2026 regime. Re-run the harness after
  another quarter of history.
