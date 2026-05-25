---
name: audit-theme-tags
description: Audit data/ticker_themes.json for tagging quality — mechanical defects (bare-L1, invalid paths, duplicates) plus AI-judgment passes for business pivots and L2 selection. Use when running a periodic tag review, before merging a taxonomy change, or when investigating viz/scoring oddities.
---

# Audit theme tags

Periodic quality review of `data/ticker_themes.json`. Triggers:

- It's been a while since the last audit (weekly–monthly cadence)
- A taxonomy edit in `config/theme_taxonomy.yaml` removed or renamed an L2 — downstream paths may now be invalid
- The network viz shows orphan or duplicate nodes
- Theme scoring produces unexpected leaders
- You're reviewing narrative-shift catalysts (earnings, M&A, pivots)

## L1/L2/L3 rules being audited

| # | Rule | Enforcement |
|---|---|---|
| 1 | L1 is the **trading narrative**, L2 is the application, L3 is the specialty. Sibling L1s are first-class (`Clean Energy` and `Oil & Gas` are separate; never `Energy / Clean` and `Energy / Fossil`). | `theme_taxonomy.yaml` |
| 2 | **Bare-L1 paths** are valid ONLY for L1s with empty `children:` (currently `Quantum Computing` and `Singleton`). Every other L1 requires at least L2. | `tools/audit_theme_tags.py` — `validate_path` does NOT catch this |
| 3 | **1–3 paths per ticker.** Use multiple paths for genuinely diversified businesses (AMZN = `Software & Internet / E-commerce` + `AI / Data Center / Cloud & Hyperscalers`). | `tools/audit_theme_tags.py` flags >3 |
| 4 | **`Singleton` is the escape hatch** for tickers with no peer group. Don't shoehorn a Singleton into a generic L1 just to avoid the label. | Human judgment |
| 5 | **Existing tags are git-locked.** Sheet sync, Gemini revalidation, and any other automated path WILL NOT overwrite a canonical tag. The `retag` CLI is the only sanctioned path. | `tag_new_tickers.py:apply_google_sheet_ground_truth` |

## Workflow

> **Hard precondition — always run Phase 1 first.** Do NOT skip ahead to AI-judgment phases, even when the user's request sounds narrow ("just check ASTS for a pivot"). Mechanical defects corrupt the inputs that AI judgments depend on: a bare-`Cybersecurity` tag will mislead "is this still a cybersecurity company?" because the LLM sees a generic tag and infers generic relevance. If Phase 1 reports any `[BUG]`, you MUST fix every BUG via the printed retag commands before moving to Phase 3. WARN/INFO findings can be triaged in parallel with later phases.

### Phase 1 — Mechanical checks (deterministic, MANDATORY FIRST)

Run the audit script:

```bash
python tools/audit_theme_tags.py
```

The script is the canonical source of mechanical-check definitions. Treat its `[BUG]` exit (code 1) as a hard gate. Re-run after each retag batch until exit code is 0 before proceeding to Phase 3.

Severity levels:

| Tag | Meaning | Action |
|---|---|---|
| `[BUG]` | Defect that produces a viz bug or breaks downstream tooling. | Fix before doing anything else — auto-applicable via retag CLI. |
| `[WARN]` | Suspicious tagging (too many paths, duplicates within ticker, empty list). | Triage manually. |
| `[INFO]` | Counts of generic tags. | Tracking signal, not a defect. |

Exit code: `1` if any `[BUG]`, else `0`. Suitable for CI.

### Phase 2 — Apply mechanical fixes

For each `[BUG]` finding, the script prints a ready-to-run `retag` command template. For bare-L1-with-children, you need to pick the right L2 — the script lists valid L2s per L1.

**Picking the L2 matters.** PR #14 (the audit that spawned this skill) found 11 tickers tagged with bare `"Space"` that actually spanned 4 different L2s — Launch (RKLB), Satellites & Communication (ASTS, SATS, SIDU, VSAT), Imaging & Earth Observation (BKSY, PL, SATL), Infrastructure (LUNR, RDW, VOYG). Don't lump them under one bucket. When unsure, fetch the company's current business description before deciding.

Apply each fix with the `retag` CLI — every retag is logged to `data/theme_review_state.json` under `manual_retags`:

```bash
python -m src.themes.retag --ticker RKLB \
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
python -m src.themes.retag --ticker AKAN \
  --reason "Pivoted to LatAm telecom after divesting US assets" \
  --paths "Telecom / Latin America"
```

### Phase 4 — Verify

```bash
python tools/audit_theme_tags.py     # confirm BUG count is 0
python -m src.reporting.export_dashboard_data  # regenerate viz JSON
```

Spot-check `docs/index.html` (Theme Viz + Momentum Viz tabs) — one hexagon per L1, proper L2 circles, no duplicate-label nodes.

### Phase 5 — Commit and PR

One commit per logical batch (e.g. "Retag bare-L1 Space cluster", "Audit narrative shifts post-Q1 earnings"). Reference findings in the commit body. **Do NOT include regenerated `docs/data/*.json`** — the daily workflow rewrites them; reset with `git checkout -- docs/data/` before committing.

## Anti-patterns

- **Don't run during the 1:30 PM PT daily-screening window** — would race with `tag_new_tickers.py` writing to `ticker_themes.json`.
- **Don't bulk-rewrite after a taxonomy edit** — that's `tools/migrate_themes.py`. This skill is for ongoing quality, not migration.
- **Don't bypass the retag CLI** by hand-editing `data/ticker_themes.json`. The CLI validates against the taxonomy and appends an audit trail; hand-edits do neither.
- **Don't `--paths` your way around a bare-L1 finding** without picking a real L2. If no existing L2 fits, add one to `theme_taxonomy.yaml` first, then retag.
