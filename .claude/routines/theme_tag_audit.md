# Weekday Theme Tag Audit Routine

You are a scheduled Claude Code routine maintaining `data/ticker_themes.json` for the theme_dashboard repo. You run weekdays at 5:30 PM Pacific — after the daily screening workflow's results commit (which lands 4:01–4:57 PM PT) — so today's consolidated screener output and warmed company profiles are already on main.

Your job: run the `audit-theme-tags` skill in full (fix tag bugs, correct stale tags, classify untagged tickers, rescue stale Singletons), then ship any tag changes to main via PR + squash-merge, and report.

Report progress in one short sentence per step. Never force-push. Never commit `docs/data/*.json`.

## Step 1 — Environment bootstrap

Verify the essentials; if any is missing and cannot be set up, report which one and stop cleanly (a skipped day is fine, a half-run is not):

```bash
git --version && gh --version        # gh must be authenticated with repo write access
uv --version || pip install uv      # uv provisions Python 3.11 + deps from the lockfile
uv sync --locked                     # no-op when .venv is already current
```

## Step 2 — Sync main

```bash
git fetch origin
git checkout main
git pull --ff-only
```

If a branch `theme-tags/<today>` already exists (a prior run failed midway), check it out, rebase it on main, and continue from where it stopped instead of starting over.

## Step 3 — Run the audit skill

Read `.claude/skills/audit-theme-tags/SKILL.md` and execute its workflow completely, in phase order:

1. Phase 1 mechanical checks (`uv run python tools/audit_theme_tags.py`)
2. Phase 2: fix every `[BUG]` via the retag CLI until the script exits 0
3. Phase 3: narrative-shift spot-checks (web-verified, focused — not exhaustive)
4. Phase 4: classify every `[UNTAGGED]` ticker; Singleton rescue capped at ~10
5. Phase 5 verify: re-run the script — `[BUG]` count must be 0 and `[UNTAGGED]` count 0 before shipping. Skip the dashboard-export spot-check (unattended run; no sheet credentials needed or wanted here)

Worklist notes:

- If the newest `_union_*.txt` is not from today (holiday, CI failure), proceed anyway and say so in your report — the audit of existing tags is still worth doing; the stale worklist date goes in the PR body.
- If Phase 4 needs an L2 that doesn't exist, add it to `config/theme_taxonomy.yaml` in the same working tree; it ships in the same PR.
- All writes go through `uv run python -m src.themes.retag ... --paths ...` — never hand-edit `data/ticker_themes.json`.

## Step 4 — Diff gate

```bash
git checkout -- docs/data/ 2>/dev/null || true
git status --porcelain data/ticker_themes.json data/theme_review_state.json config/theme_taxonomy.yaml
```

**If the three tag files are all clean:** report "Theme tag audit <today>: no changes" with the counts you observed (bugs fixed: 0, retags: 0, new classifications: 0) and stop. Do not create a branch or PR.

**If anything changed**, continue to Step 5.

## Step 5 — Ship: branch → commit → PR → merge → cleanup

```bash
git checkout -b theme-tags/$(date +%Y-%m-%d)
git add data/ticker_themes.json data/theme_review_state.json config/theme_taxonomy.yaml
git commit -m "Theme tag audit $(date +%Y-%m-%d): <N> bug fixes, <M> retags, <K> new classifications"
git push -u origin theme-tags/$(date +%Y-%m-%d)
gh pr create --title "Theme tag audit $(date +%Y-%m-%d)" --body "<summary>"
gh pr merge --squash --delete-branch
```

Replace the placeholders with real counts. The PR body must summarize, in short bullet lists:

- `[BUG]` fixes applied (ticker: old → new)
- Narrative-shift retags with their one-line reasons
- New classifications (ticker → paths)
- Singleton rescues, if any
- Worklist session date (and a note if it was stale)

If any command after branching fails (push rejected, merge blocked, checks failing), do NOT retry destructively and do NOT delete anything: leave the branch and PR standing for a human, and report the failure plus the last successful step.

## Step 6 — Report

One short paragraph: bugs fixed / retags / new classifications / singletons rescued counts, the PR URL (or "no changes"), and anything odd you noticed (e.g., a taxonomy gap you worked around, a ticker you deliberately left uncertain).
