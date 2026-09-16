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

---

# Addendum: the coil leg (2026-09-15)

**Status**: exploratory screen, **NOT pre-registered**, and **§9.2-§9.4 are
SUPERSEDED**. Read §9.0 and §9.1 before quoting any number here.

## 9.0 What §9.2-§9.4 measured, and what actually shipped

⛔ **Every row below labelled "shipped" describes a definition and a weight that
were both retired after this section was written.** The feature moved twice
afterwards. Do not quote §9.2-§9.4 as a description of current behaviour; they
are retained because the *comparisons* inside them are still informative about
the constructs they name.

| §9.2-§9.4 measured | What ships now |
|---|---|
| per-bar mean of \|close-to-close change\| / ADR%, 4 sessions | **range of the last 3 CLOSES** / mean / ADR% |
| three conjuncts, incl. close within 0.5 ATR of EMA10/20 | **two conjuncts** — no moving-average test at all |
| graded coil leg (percentile of inverted tightness) | **binary** leg, 100 or 0 |
| `coil: 0.05`, composite 0.5 rs / 0.5 vars | **`coil: 0.2`**, composite **0.4 / 0.4 / 0.2**, plus **`coil_gamma: 0.5`** |
| "shipped flag" theme IC **+0.011**, ticker **+0.40pp** | **+0.030** and **+0.37pp** (§9.6) |

§9.6 carries the measurements for what actually ships.
**Data**: ~2,220 tagged tickers priced from yfinance (Sep 2025 → Sep 2026),
165–170 scored sessions from 2026-01-02. Theme baskets are equal-weight over all
L1 members with ≥ 6 priced members above $3; returns are excess vs SPY.
**Harness**: ad hoc scripts, *not* `backtest_radar.py`. The harness gained the
coil leg (`apply_weights` widened to four weights, `WEIGHT_GRID` plus a
`coil_10`/`coil_20`/`coil_30` ladder) but has not been run on real master
parquets for this question.

## 9.1 Why this is not pre-registered, and what that costs

§1 of this document records its protocol as written before any sweep, and that
ordering is what makes its conclusions credible. **This addendum cannot make the
same claim.** The parameters were chosen and the sweeps run during
implementation, in response to an acceptance case that failed: the original
threshold had been calibrated against a period high derived from *closes*, while
the repo's `max50`/`max60` columns are rolling maxima of *highs*, which are
strictly larger. Against the real column that threshold rejected every name in
the motivating case, which forced a recalibration mid-flight.

So treat every figure below as a **directional screen that shaped a display
decision**, never as confirmation. Specifically, it does not establish:

- Any interval. No bootstrap CI was computed. At H=10, 165 overlapping windows
  give ≈ 16 independent observations, and §4's comparable measurement carried a
  90% CI half-width of ≈ 0.072 — wider than most point estimates here.
- Anything out-of-sample. The window, fraction and threshold were selected on
  the same span that scored them, after comparing three windows, several
  ticker-level definitions and three theme-level constructs. No
  multiple-comparisons correction was applied.
- Anything about the radar's own scoring path. Baskets are equal-weight over all
  members, not the mean-of-top-M composite, and strength is a 3-month
  relative-performance proxy rather than the shipped composite.
- Anything about the population the gates apply to. These are ~2,220
  yfinance-priced tagged tickers; the radar universe after its floors was 1,874.

A pre-registered run against `backtest_radar.py` on real master parquets is the
outstanding work. Write its protocol first.

## 9.2 Theme-level breadth IC (H=10) — the result that decided the design

| construct | mean IC | median | IC>0 |
|---|---|---|---|
| bare tightness breadth | **−0.077** | −0.048 | 43% |
| RETIRED per-bar flag (tight + on MA + ≥ 0.70 × max50) | **+0.011** | +0.003 | 50% |
| stricter variant (tight + on MA + ≥ 0.85 × max60) | +0.074 | +0.096 | 65% |
| **control: period-high test, no tightness** | **+0.169** | +0.216 | 72% |
| 3-month strength (reference) | +0.109 | +0.134 | 63% |

**The control is the finding.** A near-period-high test carrying no tightness at
all beats every tightness-bearing construct and beats the strength reference.
The location half carries the edge; the tightness half subtracts from it. This
control had not been run at theme level when the feature was designed, and
running it is what stopped a ranking claim from shipping.

That near-high result is itself a lead worth a real pre-registered run — it may
simply be collinear with the existing `rs` and `vars` legs, which is exactly how
the `fast` leg died in §3.

## 9.3 Ticker-level forward 10-session excess (baseline −0.28%)

| definition | share of rows | mean excess | vs baseline |
|---|---|---|---|
| bare tight (≤ 0.30) | 16.8% | −0.94% | −0.66pp |
| tight + on EMA10/20 | 10.4% | −0.82% | −0.55pp |
| RETIRED per-bar flag (+ ≥ 0.70 × max50) | **7.9%** | +0.11% | **+0.40pp** |
| stricter (+ ≥ 0.85 × max60) | 5.0% | +0.21% | +0.49pp |
| control: ≥ 0.85 × max60 alone | 53.5% | +0.24% | +0.53pp |

The gate earns its place here even though it does not at theme level: it removes
26% of otherwise-tight rows and moves the cohort from 0.66pp below baseline to
0.40pp above it. That is the disqualifier doing its job — at 0.70 it passes 82%
of all rows on its own, so it selects nothing.

## 9.4 Coil weight sweep — RETIRED per-bar definition, RETIRED 0.05 weight

Strength z-score plus λ × coil-breadth z-score, L1 level:

| λ | H=5 | H=10 |
|---|---|---|
| 0.00 | +0.0941 | +0.1089 |
| **0.05** | **+0.0942** | **+0.1094** |
| 0.10 | +0.0932 | +0.1087 |
| 0.20 | +0.0910 | +0.1069 |
| 0.30 | +0.0827 | +0.1003 |
| 0.50 | +0.0743 | +0.0914 |

0.05 was the argmax at both horizons and the gain was noise (+0.0001 / +0.0005).
⛔ **This table is superseded and its conclusion no longer holds.** It measured
the retired per-bar definition, whose theme IC was negative; the shipped closing
range measures positive, and the weight moved to 0.2 on the §9.6 sweep. Do not
cite "everything above 0.05 costs real IC" as an argument about the current
flag — it was never measured on it.

## 9.5 What would change the verdict

- A pre-registered run of the `coil_*` ladder through `backtest_radar.py` on
  real master parquets, with bootstrap intervals and the near-high control arm
  scored over the same sessions.
- A collinearity check of the near-high control against `rs_leg` and `vars_leg`.
  If it is redundant, §9.2's headline is a restatement of momentum.
- A conditional-entry study. Every figure here measures *holding* a coiled
  stock; the entry this marker serves is a breakout the following session, which
  nothing here models. That gap is why the flag ships as a marker and why its
  weak unconditional numbers are not treated as disqualifying.

## 9.6 What ships — closing-range flag, binary leg, 0.4/0.4/0.2, γ=0.5

**Status**: exploratory, **NOT pre-registered**, **in-sample**, and it does not
clear the §1 win rule. Details below; read them before citing any figure.

**Definition.** `tightness` = `(max − min of the last 3 CLOSES) / mean / adr_pct`.
`tight_base` = that ≤ 0.30 **and** close ≥ 0.70 × the 50-day high (a rolling max
of highs). Two conjuncts; no moving-average test.

**Flag, at ticker and theme level** (165 sessions of 2026, ~2,220 tickers, H=10):

| construct | ticker fwd excess vs baseline | theme IC |
|---|---|---|
| bare tightness, no high gate | −0.66pp | −0.077 |
| **shipped flag** | **+0.37pp** on 8.9% | **+0.030** |
| retired per-bar flag + EMA conjunct | +0.35pp on 7.9% | −0.003 |
| **period-high test, NO tightness** | +0.49pp on 45.6% | **+0.169** |

The last row is the one that matters and it has not changed: a location test
carrying no tightness still beats every tightness-bearing construct. The flag
ships as a marker for a conditional breakout entry, not because it out-predicts.

**Composite and γ** (28 sampled sessions, real point-in-time legs, L1 rank IC at
H=10): 0.5/0.5 no coil **+0.0750** (68% of sessions IC>0); 0.4/0.4/0.2 γ=0
**+0.0785** (61%); γ=0.25 +0.0806; **γ=0.5 +0.0807** (61%, the argmax); γ=1.0
+0.0792; γ=2.0 +0.0778.

Mean improves, **median and hit rate both fall**. The leg helps a minority of
sessions a lot and hurts more sessions a little.

## 9.7 Why this does not clear the §1 win rule, and what is still missing

⛔ §8 adopted 0.5/0.5 under the §1 protocol: *primary IC improves, AND
day-over-day rank autocorrelation degrades by less than 10% relative, AND the IC
sign holds at H=5 and H=20.* **This change supplies leg one only.**

- **No autocorrelation figure** was computed for the coil configs.
- **No H=5 or H=20 sign check** was run.
- **28 overlapping H=10 windows is ~3 independent observations.** §4's
  comparable measurement carried a 90% CI half-width of ≈0.072 — an order of
  magnitude wider than the +0.0057 this change moves the mean.
- **Everything is in-sample.** Window, fraction, high threshold, lookback,
  weight and γ were all chosen on the 2026 span that scored them, across **30+
  configurations** once the definition change, the EMA sweep, the `high_frac`
  walk and both ladders are counted. No multiple-comparisons correction.
- **⛔ The static-bias axis was never run.** `docs/solutions/conventions/self-referential-signal-validation.md`
  requires two axes — direction *and* correlation with a per-ticker attribute
  carrying no direction — and the plan specified the attribute (each stock's own
  ADR% percentile) in R12. It was not implemented. `tightness` divides by ADR%,
  so a low reading may track low volatility rather than a base. **Nobody has
  checked.** This is the largest open gap in the evidence.
- **`coil_gamma` is not sweepable by this harness.** `rollup_l1s` reads it from
  cfg and there is no `GAMMAS` grid, so every `WEIGHT_GRID` row runs at the
  config value. The γ figures above came from an ad hoc script.

**So the weights ship as a user-directed display choice, not as a measured
winner.** Recorded here rather than in §8 because §8 is the pre-registered
adoption log and this change did not go through it.

**Outstanding, in priority order:** the static-bias axis; a `GAMMAS` grid; an
autocorrelation and H=5/H=20 check against the §1 rule; a held-out window; and
re-baselining `tools/radar_episodes.yaml`, whose three episodes were verified
under 0.4/0.4/0.2-fast and 0.5/0.5, never under this third weighting.
