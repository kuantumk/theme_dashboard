---
name: audit-theme-tags
description: Audit and maintain data/ticker_themes.json — mechanical defects (bare-L1, invalid paths, duplicates), AI-judgment passes for business pivots and L2 selection, first-time classification of untagged screened tickers, evidence-based Singleton rescue, and capped basket densification (cross-listing dual-role names + filling pure-play roster gaps in the top radar ecosystems). Use for the weekday tag-audit routine, periodic tag reviews, before merging a taxonomy change, or when investigating viz/scoring/radar oddities.
---

# Audit theme tags

Quality review and upkeep of `data/ticker_themes.json`. This skill is the complete tagging playbook: the daily GitHub Actions pipeline only *surfaces* untagged tickers (it does no LLM classification), and this skill — run by the weekday Claude Code routine (`.claude/routines/theme_tag_audit.md`) or interactively — does all the judgment work.

Triggers:

- The weekday tag-audit routine invokes this skill on schedule (its git PR/merge tail lives in the routine prompt, not here)
- It's been a while since the last audit
- The daily report shows "Untagged tickers awaiting audit" > 0
- A taxonomy edit in `config/theme_taxonomy.yaml` removed or renamed an L2 — downstream paths may now be invalid
- The network viz shows orphan or duplicate nodes
- Theme scoring produces unexpected leaders
- You're reviewing narrative-shift catalysts (earnings, M&A, pivots)
- A radar ecosystem looks thinner than reality — obvious pure-plays or dual-role members missing from its baskets (run Phase 5)

## L1/L2/L3 rules being audited

| # | Rule | Enforcement |
|---|---|---|
| 1 | L1 is the **trading narrative**, L2 is the application, L3 is the specialty. Sibling L1s are first-class (`Clean Energy` and `Oil & Gas` are separate; never `Energy / Clean` and `Energy / Fossil`). | `theme_taxonomy.yaml` |
| 2 | **Bare-L1 paths** are valid ONLY for L1s with empty `children:` (currently `Quantum Computing` and `Singleton`). Every other L1 requires at least L2. | `tools/audit_theme_tags.py` — `validate_path` does NOT catch this |
| 3 | **1–3 paths per ticker.** Use multiple paths for genuinely diversified businesses (AMZN = `Software & Internet / E-commerce` + `AI / Data Center / Cloud & Hyperscalers`). | `tools/audit_theme_tags.py` flags >3 |
| 4 | **`Singleton` is the escape hatch** for tickers with no peer group. Don't shoehorn a Singleton into a generic L1 just to avoid the label. | Human/AI judgment |
| 5 | **Existing tags are git-locked.** Sheet sync and any other automated path WILL NOT overwrite a canonical tag. The `retag` CLI is the only sanctioned write path — for corrections AND for first-time classification. | `tag_new_tickers.py:apply_google_sheet_ground_truth` |

## Workflow

> **Hard precondition — always run Phase 1 first.** Do NOT skip ahead to AI-judgment phases, even when the request sounds narrow ("just check ASTS for a pivot"). Mechanical defects corrupt the inputs that AI judgments depend on: a bare-`Cybersecurity` tag will mislead "is this still a cybersecurity company?" because the LLM sees a generic tag and infers generic relevance. If Phase 1 reports any `[BUG]`, you MUST fix every BUG via the printed retag commands before moving to Phase 3. WARN/INFO findings can be triaged in parallel with later phases.

### Phase 1 — Mechanical checks (deterministic, MANDATORY FIRST)

Run the audit script:

```bash
uv run python tools/audit_theme_tags.py
```

The script is the canonical source of mechanical-check definitions. Treat its `[BUG]` exit (code 1) as a hard gate. Re-run after each retag batch until exit code is 0 before proceeding to Phase 3.

Severity levels:

| Tag | Meaning | Action |
|---|---|---|
| `[BUG]` | Defect that produces a viz bug or breaks downstream tooling. | Fix before doing anything else — auto-applicable via retag CLI. |
| `[WARN]` | Suspicious tagging (too many paths, duplicates within ticker, empty list). | Triage manually. |
| `[UNTAGGED]` | Screened tickers awaiting first-time classification (no entry / empty / `Uncategorized`-only; `Singleton`-only excluded). | This is Phase 4's worklist, not a defect. Exit code unaffected. |
| `[INFO]` | Counts of generic tags. | Tracking signal, not a defect. |

Exit code: `1` if any `[BUG]`, else `0`.

### Phase 2 — Apply mechanical fixes

For each `[BUG]` finding, the script prints a ready-to-run `retag` command template. For bare-L1-with-children, you need to pick the right L2 — the script lists valid L2s per L1.

**Picking the L2 matters.** PR #14 (the audit that spawned this skill) found 11 tickers tagged with bare `"Space"` that actually spanned 4 different L2s — Launch (RKLB), Satellites & Communication (ASTS, SATS, SIDU, VSAT), Imaging & Earth Observation (BKSY, PL, SATL), Infrastructure (LUNR, RDW, VOYG). Don't lump them under one bucket. When unsure, fetch the company's current business description before deciding.

Apply each fix with the `retag` CLI — every retag is logged to `data/theme_review_state.json` under `manual_retags`:

```bash
uv run python -m src.themes.retag --ticker RKLB \
  --reason "Bare-L1 audit: RKLB is launch services (Electron rocket)" \
  --paths "Space / Launch"
```

### Phase 3 — Narrative-shift detection (AI judgment)

The script can't tell whether a ticker's current tag still reflects reality. For each ticker in recent screener output (`screening_output/consolidated/`), spot-check:

1. Is the company still in the business its tag describes?
2. Did a recent earnings call announce a pivot? (Examples caught by the May 22 audit: AKAN → LatAm telecom, VISN → ex-CommScope, VOR → immunology, ALKS → spun off oncology.)
3. Has a divestiture or acquisition changed the primary narrative?

Use the WebFetch / WebSearch tools to verify. Don't try to audit every ticker — focus on:

- **High attention**: tickers in recent EP scan results
- **Concentration risk**: tickers leading a hot theme (a mis-tag distorts the whole hub)
- **Stale validation**: tickers untouched in `data/theme_review_state.json` for >90 days

Apply pivots with explicit reasons:

```bash
uv run python -m src.themes.retag --ticker AKAN \
  --reason "Pivoted to LatAm telecom after divesting US assets" \
  --paths "Telecom / Latin America"
```

### Phase 4 — Classify untagged tickers + Singleton rescue (AI judgment)

Work through the `[UNTAGGED]` list from Phase 1. For each ticker:

1. **Get company context.** Check the committed profile cache `data/ticker_company_metadata.json` first (the daily pipeline warms it for exactly these tickers). On a cache miss or a thin `business_summary`, use WebSearch/WebFetch for the company's current business description.
2. **Pick 1–3 taxonomy paths** using the classification rules below.
3. **Write via the retag CLI** (never hand-edit the JSON):

```bash
uv run python -m src.themes.retag --ticker XYZ \
  --reason "New ticker classification: <one-line business summary>" \
  --paths "<L1 / L2 [/ L3]>"
```

**Classification rules** (these carried over from the retired Gemini prompt — they are the discipline, not suggestions):

1. **Only taxonomy paths.** Every path must exist in `config/theme_taxonomy.yaml`. Pick the MOST specific level that fits (prefer L3 over L2 when applicable). Never invent a path — if a genuinely new L2 is needed, add it to `theme_taxonomy.yaml` first (that file then goes in the same commit), or fall back to `Singleton`.
2. **L1 = narrative.** Tickers sharing an L1 share a trading thesis. `Clean Energy` and `Oil & Gas` are SEPARATE L1s — a fuel-cell company (BE) must NEVER share L1 with an oilfield-services company (PUMP).
3. **Sector consistency.** The chosen L1 must align with the company's sector/industry. An Energy (oil/gas) sector company belongs under `Oil & Gas`, not `Clean Energy`; a Healthcare company never lands under `Fintech & Crypto` or `Oil & Gas`; an Internet-Retail company is `Software & Internet / E-commerce`, not `Logistics`.
4. **Core business only.** Classify by primary revenue source. NOT by headquarters location (use `Geographic / ...` only when geography IS the thesis — YPF is `Oil & Gas / E&P`, not `Geographic / Argentina`), and NOT by customer segment (a grocery-delivery app is `Gig Economy / Delivery`, not `Logistics`).
5. **Multi-theme (max 3) only for distinct material revenue lines.** AMZN earns both `Software & Internet / E-commerce` and `AI / Data Center / Cloud & Hyperscalers`; a second path is never justified by country or customer type.
6. **Mandatory L2 for L1s with children.** A bare-L1 path for `Space`, `AI`, `Cybersecurity`, etc. is the exact bug Phase 1 exists to catch — don't create new ones.
7. **`Singleton` escape hatch.** If the business genuinely has no peer group in the taxonomy, tag `Singleton` with a reason — don't shoehorn.

**Singleton rescue (same phase, capped).** Cross-reference `Singleton`-only tickers against the current screened pool (the union file Phase 1 used): those are liquid, in-play names whose "no peer group" call may have gone stale. Re-evaluate at most ~10 per run. Rescue a Singleton into a real theme ONLY on clear evidence (sector + industry + business summary all point at an existing theme); when in doubt, leave it. Never force a theme to avoid the label, and never downgrade a themed ticker to `Singleton` without a pivot-grade reason.

### Phase 5 — Basket densification: cross-listings + roster gaps (AI judgment, capped)

The Ecosystem Radar scores fixed theme baskets over **all** tagged tickers, so its output quality is bounded by basket *membership*, not just per-ticker correctness. Tags historically enter via screener discovery, which leaves two systematic holes — both exposed by the 2026-07-13 cybersecurity case, where a competitor's ecosystem table (overlapping baskets, large caps included) had the family at #2 one session pre-breakout while our fragmented baskets buried it:

- **Missing cross-listings** — dual-role companies carry only their discovery-era path. DDOG sat only under `Software & Internet / DevOps & Data` although Cloud SIEM / App Security is a material security line (`Cybersecurity / Data Security` appended 2026-07-15); FSLY likewise gained `Cybersecurity / Network` (Signal Sciences WAF/DDoS).
- **Missing pure-plays** — leaders that never passed a momentum screener are absent entirely. CYBR (CyberArk, the PAM/identity leader) was untagged until 2026-07-15.

Per run, keep it bounded:

1. **Pick 2–3 focus families**: the top ecosystems by boosted score in `docs/data/radar.json` (committed by the daily workflow; if the file doesn't exist yet — first run after the radar merge — skip this phase for the run). Rotate — skip a family already densified within ~2 weeks (`data/theme_review_state.json` entries with a `Basket densification:` reason prefix are the trail).
2. **Cross-listing sweep** (existing tickers): for each focus family, shortlist dual-role candidates among already-tagged tickers of *other* L1s (for Cybersecurity: observability, CDN/edge, identity-adjacent names). Verify with WebSearch that the family-relevant product line is a **distinct material revenue line** — a real product suite, not a marketing page — then append the second/third path.
3. **Roster-gap sweep** (missing tickers): list the recognized liquid pure-plays of each focus family and diff against `data/ticker_themes.json`. Classify the genuinely missing ones with the full Phase 4 rules (profile/web context, most-specific path). Skip names that would fail the radar's liquidity floor anyway (close < $3 or < ~$10M/day dollar volume) and non-US listings.
4. **Cap: ~10 writes per run across both sweeps.** This is slow-drip curation, not a one-shot basket rebuild.

Mechanics — the retag CLI **sets the complete path list**, so a cross-listing must repeat the existing paths:

```bash
uv run python -m src.themes.retag --ticker DDOG \
  --reason "Basket densification: Cloud SIEM / App Security is a material security revenue line" \
  --paths "Software & Internet / DevOps & Data" "Cybersecurity / Data Security"
```

Guardrails:

- Classification rule 5 still governs: cross-list on a **distinct material revenue line**, never on thematic vibes. When a competitor's basket includes a name we'd have to stretch for (e.g. semis AVGO/NXPI/LSCC/SKYT under "hardware security"), skip it — L1 = dominant narrative is this taxonomy's core invariant, and the radar's boost math rewards genuine co-movement, not padded rosters.
- Never drop an existing path while appending — repeat every current path in `--paths`.
- The 3-path cap is hard. If a ticker seems to need a 4th path, its primary tag is probably wrong — re-evaluate the whole list instead.
- A new L2 is justified only when ≥ 2 real members need it; edit `theme_taxonomy.yaml` in the same commit (Phase 4 rule 1).

### Phase 6 — Verify

```bash
uv run python tools/audit_theme_tags.py   # BUG count 0; [UNTAGGED] count 0 (or explained)
```

Optionally regenerate the viz JSON to spot-check (`uv run python -m src.reporting.export_dashboard_data`, then eyeball `docs/index.html` Theme Viz — one hexagon per L1, no duplicate-label nodes). This step needs `GOOGLE_SHEET_ID` and network access for the ETF tabs — **skip it when running unattended** (the daily workflow regenerates all of `docs/data/` anyway, and those files are never committed from an audit).

### Phase 7 — Commit and PR

One commit per logical batch (e.g. "Retag bare-L1 Space cluster", "Classify 2026-07-02 untagged tickers", "Densify Cybersecurity basket: 2 cross-lists + 1 roster add"). Reference findings in the commit body. **Do NOT include regenerated `docs/data/*.json`** — the daily workflow rewrites them; reset with `git checkout -- docs/data/` before committing. Commit only tag files: `data/ticker_themes.json`, `data/theme_review_state.json`, and `config/theme_taxonomy.yaml` when a new L2 was added.

When running interactively, stop at the PR. When running as the weekday routine, the routine prompt (`.claude/routines/theme_tag_audit.md`) owns the PR → squash-merge → branch-cleanup tail.

## Anti-patterns

- **Don't run between 1:30 PM and ~5:00 PM PT on weekdays** — the daily-screening workflow starts at 1:30 PM PT and its results commit lands 4:01–4:57 PM PT (observed range); racing it means rebasing against a moving main and auditing a half-updated worklist. The routine is scheduled at 5:30 PM PT for this reason.
- **Don't bulk-rewrite after a taxonomy edit** — that's `tools/migrate_themes.py`. This skill is for ongoing quality, not migration.
- **Don't bypass the retag CLI** by hand-editing `data/ticker_themes.json`. The CLI validates against the taxonomy and appends an audit trail; hand-edits do neither.
- **Don't `--paths` your way around a bare-L1 finding** without picking a real L2. If no existing L2 fits, add one to `theme_taxonomy.yaml` first, then retag.
- **Don't churn Singletons.** `Singleton`-only tickers are excluded from `[UNTAGGED]` on purpose — they were deliberately classified. Only the capped, evidence-based rescue pass in Phase 4 revisits them.
