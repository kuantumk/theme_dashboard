"""Acceptance check for the Ecosystem Radar against a known session.

The motivating case: on 2026-07-13 (one session before the cybersecurity
breakout) the radar should rank the Cybersecurity ecosystem near the top,
while the screened Themes lens buried it at leaf ranks 14/17/20.

Modes (picked automatically):
  A (parquet)  — `screening_output/master/master_<date>.parquet` exists
                 locally: score it directly with compute_radar. Supports
                 `--sweep` to grid-search beta/top-K/top-M/fast-weight and
                 print the expected L1's ecosystem rank per combination.
  B (exported) — no local parquet: read the date's entry from
                 `docs/data/radar_history.json` (available after the first
                 daily workflow run that follows the radar merge; masters
                 regenerate ~130 sessions back, so 2026-07-13 stays coverable
                 until roughly mid-January 2027).

Usage:
  uv run python tools/validate_radar.py --date 2026-07-13 \
      --expect-l1 Cybersecurity --max-rank 3 [--sweep]

Exit code 1 when --expect-l1 is given and its rank is worse than --max-rank.
"""

import argparse
import itertools
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import src.stock_utils as su  # noqa: E402
from config.settings import CONFIG, DOCS_DATA_DIR, SCREENING_OUTPUT_DIR  # noqa: E402
from src.themes.ecosystem_score import compute_radar, radar_config  # noqa: E402


def print_ecosystems(ecosystems, limit=15):
    print(f"{'#':>3}  {'ECOSYSTEM':<28} {'BOOSTED':>8} {'RAW':>8} {'Δ':>7}  LEAVES(top)")
    for eco in ecosystems[:limit]:
        leaves = eco.get('leaves', [])
        top = leaves[0] if leaves else {}
        top_name = top.get('l2') or top.get('theme') or top.get('name') or '-'
        top_rank = top.get('global_rank', '?')
        print(
            f"{eco['rank']:>3}  {eco['name']:<28} {eco['boosted']:>8.3f} "
            f"{eco['raw']:>8.3f} {eco['delta']:>+7.3f}  "
            f"{eco['n_leaves']}({top_name} #{top_rank})"
        )


def eco_rank(ecosystems, l1_name):
    for eco in ecosystems:
        if eco['name'] == l1_name:
            return eco['rank']
    return None


def run_mode_a(master_file, date_str, args):
    master_df = su.load_df_from_parquet(master_file)
    screened = su.union_tickers_for_date(date_str, CONFIG['screeners'])

    snap = compute_radar(master_df, screened_tickers=screened)
    if snap is None:
        print("Radar produced no snapshot for this master file")
        return None

    print(f"\nRadar for {date_str} (mode A: local parquet, "
          f"universe {snap['universe_size']}, {snap['n_leaves_scored']} leaves)\n")
    print_ecosystems(snap['ecosystems'])

    if args.sweep:
        print("\nParameter sweep — ecosystem rank of "
              f"{args.expect_l1 or 'n/a (pass --expect-l1)'} per combo:")
        base = radar_config()
        grid = itertools.product([0.2, 0.3, 0.4], [3, 5], [3, 5, 10], [0.2, 0.4])
        print(f"{'beta':>5} {'topK':>5} {'topM':>5} {'w_fast':>7} {'rank':>5}")
        for beta, k, m, w_fast in grid:
            w_rest = (1.0 - w_fast) / 2
            cfg = dict(base, beta=beta, top_k_leaves=k, top_m_members=m,
                       composite_weights={'rs': w_rest, 'vars_pct': w_rest, 'fast': w_fast})
            s = compute_radar(master_df, screened_tickers=screened, cfg=cfg)
            rank = eco_rank(s['ecosystems'], args.expect_l1) if s and args.expect_l1 else None
            print(f"{beta:>5.1f} {k:>5} {m:>5} {w_fast:>7.1f} {str(rank or '-'):>5}")

    return snap['ecosystems']


def run_mode_b(date_str):
    history_file = DOCS_DATA_DIR / 'radar_history.json'
    if not history_file.exists():
        print(f"Neither local master parquet for {date_str} nor {history_file} found.\n"
              "Run the daily workflow (or wait for the first post-merge CI run).")
        return None
    with open(history_file, encoding='utf-8') as fh:
        history = json.load(fh)
    snap = next((h for h in history if h.get('report_date') == date_str), None)
    if snap is None:
        dates = [h.get('report_date') for h in history]
        print(f"No radar_history entry for {date_str}. "
              f"Available: {dates[-1] if dates else '-'} .. {dates[0] if dates else '-'}")
        return None
    print(f"\nRadar for {date_str} (mode B: exported radar_history.json, "
          f"universe {snap.get('universe_size')}, {snap.get('n_leaves_scored')} leaves)\n")
    print_ecosystems(snap['ecosystems'])
    return snap['ecosystems']


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--date', required=True, help='Session date YYYY-MM-DD')
    ap.add_argument('--expect-l1', help='Ecosystem (taxonomy L1) expected near the top')
    ap.add_argument('--max-rank', type=int, default=3,
                    help='Expected L1 must rank <= this (default 3)')
    ap.add_argument('--sweep', action='store_true',
                    help='Mode A only: grid-search beta/top-K/top-M/fast-weight')
    args = ap.parse_args()

    master_file = SCREENING_OUTPUT_DIR / 'master' / f'master_{args.date}.parquet'
    if master_file.exists():
        ecosystems = run_mode_a(master_file, args.date, args)
    else:
        if args.sweep:
            print("(--sweep needs a local master parquet; falling back to mode B without sweep)")
        ecosystems = run_mode_b(args.date)

    if ecosystems is None:
        sys.exit(1)

    if args.expect_l1:
        rank = eco_rank(ecosystems, args.expect_l1)
        if rank is None:
            print(f"\nFAIL: {args.expect_l1} not among scored ecosystems")
            sys.exit(1)
        verdict = 'PASS' if rank <= args.max_rank else 'FAIL'
        print(f"\n{verdict}: {args.expect_l1} ecosystem rank {rank} "
              f"(required <= {args.max_rank}) on {args.date}")
        sys.exit(0 if rank <= args.max_rank else 1)


if __name__ == '__main__':
    main()
