# Periodic Theme Audit Routine

You are running as a scheduled routine to audit stale theme tags in `data/ticker_themes.json` for the theme_dashboard project. You have repo access via the standard Claude Code tools.

## Goal

Re-evaluate the **stale subset** of ticker→theme mappings (tickers that have not been validated by the daily Gemini pipeline in the last 30 days, and are not in today's screened pool). For each stale ticker, decide whether its current theme assignment is still correct given its company profile. Apply high-confidence corrections, defer borderline cases for human review, and open a PR with the diff.

## Procedure

Execute these steps in order. Report progress in one short sentence per step.

### 1. Sync repo state

```bash
git fetch origin
git checkout main
git pull --ff-only
git checkout -b theme-audit/$(date -u +%Y-%m-%d)
```

If the branch already exists (a prior run failed midway), check it out instead and continue from where it left off.

### 2. Generate batches

```bash
PYTHONPATH=. python src/themes/prepare_theme_audit.py
```

This writes `data/audit/<today>/batch_NNN.json` files. The output prints the list of batch paths. Note the count.

If the count is 0, there's nothing stale to audit — exit cleanly with a one-line message and skip the rest.

### 3. Process each batch

For each `batch_NNN.json` in `data/audit/<today>/`:

a. Read the batch file with the Read tool.

b. For each ticker in `tickers`, decide one of:
   - `"action": "keep"` — current themes are still correct
   - `"action": "change"` — propose new themes from `valid_themes` (the list at the top of the batch)
   - `"action": "uncertain"` — profile is too thin or ambiguous to judge

c. For each decision, include `confidence` (0.0–1.0) and a one-sentence `rationale`.

d. Write decisions to `data/audit/<today>/decisions_NNN.json` with this exact shape:

```json
{
  "batch_id": "NNN",
  "decisions": {
    "TICKER1": {
      "action": "change",
      "themes": ["Some Existing Theme From valid_themes"],
      "confidence": 0.92,
      "rationale": "One sentence explaining the decision."
    },
    "TICKER2": {
      "action": "keep",
      "confidence": 0.95,
      "rationale": "..."
    },
    "TICKER3": {
      "action": "uncertain",
      "themes": [],
      "confidence": 0.40,
      "rationale": "..."
    }
  }
}
```

### 4. Apply decisions

```bash
PYTHONPATH=. python src/themes/apply_theme_audit.py
```

This script enforces safety gates (taxonomy, confidence, generic-target) before mutating `data/ticker_themes.json`, and writes `audit_summary.md` + `deferred.json` + updates `data/theme_audit_state.json`.

### 5. Commit and open PR

```bash
git add data/ticker_themes.json data/theme_audit_state.json data/audit/<today>/
git commit -m "Theme audit YYYY-MM-DD: applied N changes, deferred M for review"
git push -u origin theme-audit/<today>
gh pr create --title "Theme audit YYYY-MM-DD" --body-file data/audit/<today>/audit_summary.md
```

Replace `YYYY-MM-DD` with today's actual date and `N` / `M` with the actual counts from `apply_theme_audit.py`'s output.

## Decision rules (read carefully)

These are the rules you must follow when deciding `keep` / `change` / `uncertain`:

### Prefer existing themes from `valid_themes`

The batch file includes `valid_themes` — a curated list of every theme currently in use across the project. **Only propose themes from this list.** If you propose a novel theme, the apply step rejects it as "not in taxonomy" and your work is wasted.

### Rescue singletons aggressively

A ticker currently tagged `["Individual Episodic Pivots / Singletons"]` is the primary target of this audit. If the company profile clearly fits an existing theme (sector + industry + business_summary point at it), propose the change with high confidence (≥0.9). This is the SNAP-style fix the audit exists to deliver.

Examples:
- SNAP (Communication Services / Internet Content / "Snapchat ad platform") → propose `["Ads Tech & Marketing"]` at 0.92.
- A biotech tagged singleton whose summary describes oncology drug pipelines → propose `["Biotech / Oncology"]` at 0.9+.

### Don't downgrade

**Never** propose `Singleton` or `Uncategorized` as a target theme. If the profile is too thin or the company doesn't fit any existing theme, return `"action": "uncertain"` instead. The apply step rejects singleton/uncategorized targets and routes them to deferred anyway, so this is wasted output.

### Confidence calibration

- `≥0.90`: company profile clearly supports the proposed theme; sector + industry + business_summary all align.
- `0.85–0.89`: solid match, minor ambiguity (e.g., company has multiple business lines and you picked the dominant one).
- `0.70–0.84`: plausible but not certain — return as `"action": "change"` so the apply step routes to deferred, OR mark `"action": "uncertain"` if you're really unsure.
- `<0.70`: use `"action": "uncertain"`.

### Multi-theme assignments

Tickers can have up to 2 themes. If a ticker spans two themes (e.g., TSLA = AI Robotics + EV), keep both. Don't trim to one theme just because.

### Empty/poor profile

If `business_summary` is empty or extremely sparse and the current theme is non-singleton, prefer `"action": "keep"` with low-medium confidence rather than guessing. The daily Gemini pipeline will refresh profiles when this ticker re-enters the screened pool.

## Examples

### Rescue example

Input:
```json
{"ticker": "SNAP", "current_themes": ["Individual Episodic Pivots / Singletons"],
 "profile": {"company_name": "Snap Inc.", "sector": "Communication Services",
             "industry": "Internet Content & Information",
             "business_summary": "Snap Inc. operates as a technology company... Snapchat... advertising..."}}
```

Decision:
```json
"SNAP": {"action": "change", "themes": ["Ads Tech & Marketing"], "confidence": 0.92,
         "rationale": "Snap's Snapchat platform is ad-revenue dominant; singleton tag is a stale fallback."}
```

### Keep example

Input:
```json
{"ticker": "TSLA", "current_themes": ["AI - Robotics", "EV & AV"],
 "profile": {"sector": "Consumer Cyclical", "industry": "Auto Manufacturers",
             "business_summary": "Tesla designs, develops, manufactures, leases, and sells electric vehicles..."}}
```

Decision:
```json
"TSLA": {"action": "keep", "confidence": 0.95,
         "rationale": "Both themes accurately reflect Tesla's main business lines."}
```

### Uncertain example

Input:
```json
{"ticker": "ABCD", "current_themes": ["Some Theme"],
 "profile": {"sector": "", "industry": "", "business_summary": ""}}
```

Decision:
```json
"ABCD": {"action": "uncertain", "themes": [], "confidence": 0.20,
         "rationale": "Empty profile; cannot validate."}
```

## Failure modes to avoid

- **Don't invent themes.** Only use entries from `valid_themes`. The taxonomy whitelist will reject novel themes anyway.
- **Don't downgrade to Singleton/Uncategorized.** Use `"action": "uncertain"` instead.
- **Don't skip the safety gates by editing `ticker_themes.json` directly.** Always go through `apply_theme_audit.py` so confidence/taxonomy/state-tracking are enforced.
- **Don't process batches in parallel.** Read one, decide, write decisions, then move to the next. The decisions JSON files are independent so order doesn't matter, but processing serially keeps your reasoning auditable.
- **Don't commit `data/audit/<today>/` if the apply step failed.** If `apply_theme_audit.py` exits non-zero, investigate before pushing — a half-applied state is worse than no state.

## Reporting back

When done, report a one-paragraph summary:
- Total stale tickers audited
- Counts: applied, kept, deferred, invalid, skipped
- PR URL
- Anything notable in the deferred bucket (e.g., systematic patterns Claude struggled with)

If anything failed mid-run, leave the branch in place (don't force-push or delete) so a human can pick up from where you stopped. Report the failure and the last successful step.
