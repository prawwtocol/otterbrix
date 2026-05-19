#!/usr/bin/env python3
"""Cross-process pushdown comparison harness.

Filter pushdown is a process-wide switch controlled by OTTERBRIX_DISABLE_PUSHDOWN
(read once by the C++ dispatcher). To honestly compare with/without pushdown
we need two independent processes — one per state.

This script:
  1. Runs bench_optimizer.py twice via subprocess:
       run #1 with OTTERBRIX_DISABLE_PUSHDOWN=1  →  pushdown_off raw runs
       run #2 with that env unset                →  pushdown_on  raw runs
  2. Loads per-run timings from both --raw-output JSON files.
  3. For each (scenario, rows) pair, runs Welch's t-test + 10k bootstrap on
     speedup = mean_off / mean_on (higher = pushdown helps more) via the same
     compare_significance() that the inner bench uses.
  4. Prints a per-scenario table and writes results_compare.csv.
"""

import argparse
import json
import os
import subprocess
import sys
from typing import Any, Dict, List, Tuple


BENCH_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_BENCH = os.path.join(BENCH_DIR, "bench_optimizer.py")
DEFAULT_RUN_DIR = os.path.join(BENCH_DIR, "compare_runs")
DEFAULT_COMPARE_CSV = os.path.join(BENCH_DIR, "results_compare.csv")


def _run_bench(python_exec: str, bench_script: str, env_disable: bool,
               raw_path: str, csv_path: str, sizes: List[int]) -> None:
    """Spawn bench_optimizer.py in a clean process state with the chosen env."""
    env = os.environ.copy()
    if env_disable:
        env["OTTERBRIX_DISABLE_PUSHDOWN"] = "1"
    else:
        env.pop("OTTERBRIX_DISABLE_PUSHDOWN", None)
    cmd = [python_exec, bench_script,
           "--output", csv_path,
           "--raw-output", raw_path]
    if sizes:
        cmd += ["--sizes", *map(str, sizes)]
    label = "pushdown_off" if env_disable else "pushdown_on"
    print(f"\n>>> running bench ({label}): {' '.join(cmd)}", flush=True)
    res = subprocess.run(cmd, env=env)
    if res.returncode != 0:
        raise SystemExit(f"bench_optimizer.py failed for {label} (rc={res.returncode})")


def _load_raw(path: str) -> Dict[Tuple[str, int], List[float]]:
    """Read raw timings JSON and reindex by (scenario, rows) — mode is ignored
    here because each compare-run process has exactly one mode tag."""
    with open(path) as f:
        data = json.load(f)
    out: Dict[Tuple[str, int], List[float]] = {}
    for key, runs in data.items():
        scenario, _mode, rows = key.split("||")
        out[(scenario, int(rows))] = runs
    return out


def _format_p(p: float) -> str:
    if p != p:  # NaN
        return "nan"
    if p < 1e-6:
        return f"{p:.1e}"
    return f"{p:.4f}"


def compare(raw_off: Dict[Tuple[str, int], List[float]],
            raw_on: Dict[Tuple[str, int], List[float]]) -> List[Dict[str, Any]]:
    """Run compare_significance(off, on) for every shared (scenario, rows)."""
    # Import lazily so this script can run standalone if bench_optimizer is missing.
    sys.path.insert(0, BENCH_DIR)
    from bench_optimizer import compare_significance
    import numpy as np
    from scipy.stats import ttest_ind

    rows = []
    shared_keys = sorted(set(raw_off) & set(raw_on))
    for key in shared_keys:
        scenario, n_rows = key
        off_runs = raw_off[key]
        on_runs = raw_on[key]
        if not off_runs or not on_runs:
            continue
        cmp = compare_significance(off_runs, on_runs)
        # compare_significance returns mean_a/mean_b → off/on, i.e. speedup
        # of "on" relative to "off". We also expose t-value (compare_significance
        # only returns p-value, so recompute t).
        _, _p = ttest_ind(off_runs, on_runs, equal_var=False)
        # Welch's t statistic
        a = np.array(off_runs); b = np.array(on_runs)
        mean_a, mean_b = a.mean(), b.mean()
        var_a, var_b = a.var(ddof=1), b.var(ddof=1)
        na, nb = len(a), len(b)
        denom = (var_a / na + var_b / nb) ** 0.5
        t_val = float((mean_a - mean_b) / denom) if denom > 0 else float("nan")

        rows.append({
            "scenario": scenario,
            "rows": n_rows,
            "off_mean_s": float(mean_a),
            "on_mean_s": float(mean_b),
            "off_n": na,
            "on_n": nb,
            "speedup_off_over_on": cmp["speedup"],
            "bootstrap_l": cmp["ci_95"][0],
            "bootstrap_r": cmp["ci_95"][1],
            "t_val": t_val,
            "p_value": cmp["p_value"],
            "significant": cmp["significant"],
        })
    return rows


def print_table(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        print("No shared (scenario, rows) keys between off and on raw files.")
        return
    print(f"\n{'='*128}")
    print("CROSS-PROCESS PUSHDOWN COMPARISON  (speedup = off / on; >1.0x = pushdown helps)")
    print(f"{'='*128}")
    print(f"{'Scenario':27s} {'Rows':>8s} {'Off(s)':>10s} {'On(s)':>10s} "
          f"{'Speedup':>9s} {'Boot CI95':>20s} {'t':>8s} {'p':>10s} {'Sig?':>5s}")
    print(f"{'-'*128}")
    for r in rows:
        ci = f"[{r['bootstrap_l']:.2f},{r['bootstrap_r']:.2f}]"
        sig = "*" if r["significant"] else "ns"
        print(f"{r['scenario']:27s} {r['rows']:8,d} "
              f"{r['off_mean_s']:10.5f} {r['on_mean_s']:10.5f} "
              f"{r['speedup_off_over_on']:8.2f}x {ci:>20s} "
              f"{r['t_val']:8.2f} {_format_p(r['p_value']):>10s} {sig:>5s}")


def save_compare_csv(rows: List[Dict[str, Any]], path: str) -> None:
    import csv
    if not rows:
        print(f"\nNo rows to save to {path}")
        return
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nCompare results saved to {path}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sizes", type=int, nargs="+", default=None,
                        help="Forwarded to bench_optimizer.py --sizes")
    parser.add_argument("--bench", type=str, default=DEFAULT_BENCH,
                        help="Path to bench_optimizer.py")
    parser.add_argument("--python", type=str, default=sys.executable,
                        help="Python executable for the bench subprocesses")
    parser.add_argument("--run-dir", type=str, default=DEFAULT_RUN_DIR,
                        help="Where to write per-run CSV/JSON artifacts")
    parser.add_argument("--output", type=str, default=DEFAULT_COMPARE_CSV,
                        help="Where to write the comparison CSV")
    parser.add_argument("--skip-runs", action="store_true",
                        help="Reuse existing raw JSON files in --run-dir (debug aid)")
    args = parser.parse_args()

    os.makedirs(args.run_dir, exist_ok=True)
    off_raw = os.path.join(args.run_dir, "pushdown_off_raw.json")
    on_raw = os.path.join(args.run_dir, "pushdown_on_raw.json")
    off_csv = os.path.join(args.run_dir, "pushdown_off.csv")
    on_csv = os.path.join(args.run_dir, "pushdown_on.csv")

    if not args.skip_runs:
        _run_bench(args.python, args.bench, env_disable=True,
                   raw_path=off_raw, csv_path=off_csv, sizes=args.sizes or [])
        _run_bench(args.python, args.bench, env_disable=False,
                   raw_path=on_raw, csv_path=on_csv, sizes=args.sizes or [])

    raw_off = _load_raw(off_raw)
    raw_on = _load_raw(on_raw)

    rows = compare(raw_off, raw_on)
    print_table(rows)
    save_compare_csv(rows, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
