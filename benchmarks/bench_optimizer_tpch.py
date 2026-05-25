"""
Benchmark (TPC-H real data): optimized vs non-optimized DataFrame execution
in the OtterBrix Spark API.

Companion to bench_optimizer.py — identical opt/no_opt methodology and the same
measurement harness (imported from bench_optimizer), but data comes from real
TPC-H tables instead of synthetic generators. Closes the representativeness gaps
of the synthetic bench: a wide real table (lineitem, 16 columns) for projection
pushdown, and a real FK->PK fan-out join (lineitem x orders) instead of a
perfect 1:1 join.

Each (scenario, size) pair measures no_opt and opt interleaved with randomized
within-pair order (measure_paired), so environmental drift cancels instead of
aliasing onto the optimize flag.

Correctness gate: for every pair the no_opt and opt result row counts must
match. A mismatch means optimize=True changed the result (an engine bug), so the
pair's timings are NOT comparable — flagged in the `correctness` CSV column.

Data prep (one-time, NOT part of the timed run):
    otterbrix/benchmark/download_data.sh --suite=tpch --scale=0.01
produces pipe-delimited .tbl files in otterbrix/benchmark/data/tpch/.

Scenarios (full parity with bench_optimizer.py, on TPC-H lineitem / orders):
  filter                  — lineitem.filter(l_quantity > 30)                        O(n)
  project_filter          — lineitem.select(3 of 16 cols).filter(...)               O(n)        [projection pushdown, wide table]
  chained_filters         — lineitem.filter(a).filter(b).filter(c)                  O(n)
  groupby_agg             — lineitem.groupBy(l_returnflag).agg(sum(...))             O(n*k)
  filter_over_groupby_key — groupBy(l_returnflag).agg(...).filter(l_returnflag==A)   O(n)
  filter_over_sort_sel_*  — lineitem.sort(l_shipdate).filter(l_quantity > t)         O(n log n)  sweep x4 selectivity
  join_filter_sel_*       — lineitem.join(orders).filter(l_quantity > t)             O(n^2)      sweep x4 selectivity [pushdown below a fan-out join]

Results: results_tpch.csv (+ results_tpch.env.json) — separate from results.csv.
"""

import os
import sys
from typing import Callable, List, Tuple

# (column name, caster) — TPC-H schemas as emitted by dbgen / download_data.sh.
LINEITEM_COLUMNS: List[Tuple[str, Callable]] = [
    ("l_orderkey", int), ("l_partkey", int), ("l_suppkey", int),
    ("l_linenumber", int), ("l_quantity", float), ("l_extendedprice", float),
    ("l_discount", float), ("l_tax", float), ("l_returnflag", str),
    ("l_linestatus", str), ("l_shipdate", str), ("l_commitdate", str),
    ("l_receiptdate", str), ("l_shipinstruct", str), ("l_shipmode", str),
    ("l_comment", str),
]

ORDERS_COLUMNS: List[Tuple[str, Callable]] = [
    ("o_orderkey", int), ("o_custkey", int), ("o_orderstatus", str),
    ("o_totalprice", float), ("o_orderdate", str), ("o_orderpriority", str),
    ("o_clerk", str), ("o_shippriority", int), ("o_comment", str),
]


def load_tpch_table(path: str, columns: List[Tuple[str, Callable]],
                    limit: int = None) -> List[Tuple]:
    """Parse a pipe-delimited TPC-H .tbl file into a list of typed tuples.

    Skips the header line prepended by download_data.sh and strips the single
    trailing '|' that dbgen emits on every data row.
    """
    casters = [c for _, c in columns]
    ncols = len(columns)
    rows: List[Tuple] = []
    with open(path, encoding="utf-8") as f:
        next(f, None)  # header line
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            if line.endswith("|"):
                line = line[:-1]
            fields = line.split("|")
            if len(fields) != ncols:
                raise ValueError(
                    f"{path}: expected {ncols} columns, got {len(fields)}")
            rows.append(tuple(cast(v) for cast, v in zip(casters, fields)))
            if limit is not None and len(rows) >= limit:
                break
    return rows


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_TPCH_DIR = os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "benchmark", "data", "tpch"))

# lineitem O(n)/O(n*k) scenarios run at two sizes; the join is O(n^2) (nested-loop
# join in operator_join.cpp), so it runs on small subsamples only.
LINEITEM_SIZES = [10_000, 50_000]
JOIN_SIZES = [5_000, 10_000]

# Selectivity sweep — (l_quantity threshold, approx % of rows passing).
# l_quantity is ~ uniform integer 1..50, so P(l_quantity > t) = (50 - t) / 50.
SELECTIVITY_POINTS = [(5, 90), (25, 50), (45, 10), (49, 2)]

SPARK_MASTER = "local[2]"


def _names(columns, rename=None) -> List[str]:
    """Column-name list for createDataFrame, with optional renames."""
    rename = rename or {}
    return [rename.get(n, n) for n, _ in columns]


# ---------------------------------------------------------------------------
# Scenario factories
#
# col / spark_sum are passed in (not imported at module level) so that
# load_tpch_table stays unit-testable without the otterbrix engine.
# run() returns the collected result so the runner can compare no_opt vs opt.
# ---------------------------------------------------------------------------

def scenario_filter(spark, col, _sum, lineitem, optimize) -> Callable:
    """lineitem.filter(l_quantity > 30)"""
    df = spark.createDataFrame(lineitem, schema=_names(LINEITEM_COLUMNS),
                               optimize=optimize)
    def run():
        return df.filter(col("l_quantity") > 30).collect()
    return run


def scenario_project_filter(spark, col, _sum, lineitem, optimize) -> Callable:
    """lineitem.select(3 of 16 cols).filter(l_quantity > 30) — projection pushdown."""
    df = spark.createDataFrame(lineitem, schema=_names(LINEITEM_COLUMNS),
                               optimize=optimize)
    def run():
        return df.select("l_orderkey", "l_quantity", "l_extendedprice") \
                 .filter(col("l_quantity") > 30).collect()
    return run


def scenario_chained_filters(spark, col, _sum, lineitem, optimize) -> Callable:
    """lineitem.filter(a).filter(b).filter(c) — filter merge / reorder."""
    df = spark.createDataFrame(lineitem, schema=_names(LINEITEM_COLUMNS),
                               optimize=optimize)
    def run():
        return df.filter(col("l_quantity") > 10) \
                 .filter(col("l_quantity") < 45) \
                 .filter(col("l_discount") > 0.02).collect()
    return run


def scenario_groupby(spark, col, _sum, lineitem, optimize) -> Callable:
    """lineitem.groupBy(l_returnflag).agg(sum(l_extendedprice)) — real low-card key."""
    df = spark.createDataFrame(lineitem, schema=_names(LINEITEM_COLUMNS),
                               optimize=optimize)
    def run():
        return df.groupBy("l_returnflag").agg(_sum("l_extendedprice")).collect()
    return run


def scenario_filter_over_groupby_key(spark, col, _sum, lineitem, optimize) -> Callable:
    """groupBy(l_returnflag).agg(sum(...)).filter(l_returnflag == 'A') — post-agg filter."""
    df = spark.createDataFrame(lineitem, schema=_names(LINEITEM_COLUMNS),
                               optimize=optimize)
    def run():
        return df.groupBy("l_returnflag").agg(_sum("l_extendedprice")) \
                 .filter(col("l_returnflag") == "A").collect()
    return run


def scenario_filter_over_sort_sel(spark, col, _sum, lineitem,
                                  threshold, optimize) -> Callable:
    """lineitem.sort(l_shipdate).filter(l_quantity > threshold) — pushdown below sort."""
    df = spark.createDataFrame(lineitem, schema=_names(LINEITEM_COLUMNS),
                               optimize=optimize)
    def run():
        return df.sort("l_shipdate").filter(col("l_quantity") > threshold).collect()
    return run


def scenario_join_filter_sel(spark, col, _sum, lineitem, orders,
                             threshold, optimize) -> Callable:
    """lineitem.join(orders, orderkey).filter(l_quantity > threshold) — pushdown below a join.

    The join key is renamed to a shared name on both sides (join(other, str)
    requires the column to exist under the same name on both DataFrames).
    """
    li_names = _names(LINEITEM_COLUMNS, {"l_orderkey": "orderkey"})
    or_names = _names(ORDERS_COLUMNS, {"o_orderkey": "orderkey"})
    df_l = spark.createDataFrame(lineitem, schema=li_names, optimize=optimize)
    df_o = spark.createDataFrame(orders, schema=or_names, optimize=optimize)
    def run():
        return df_l.join(df_o, "orderkey") \
                   .filter(col("l_quantity") > threshold).collect()
    return run


# Regular scenarios — factory(spark, col, spark_sum, lineitem, optimize).
REGULAR_SCENARIOS = {
    "filter": scenario_filter,
    "project_filter": scenario_project_filter,
    "chained_filters": scenario_chained_filters,
    "groupby_agg": scenario_groupby,
    "filter_over_groupby_key": scenario_filter_over_groupby_key,
}


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_benchmarks_tpch(tpch_dir: str = None) -> list:
    """Run the TPC-H opt/no_opt benchmark, return a list of result dicts."""
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from bench_optimizer import (measure_paired, measure_memory,
                                 compare_significance, runs_for_size,
                                 time_budget, _make_result)
    from otterbrix.experimental.spark.sql import SparkSession
    from otterbrix.experimental.spark.sql.functions import col, sum as spark_sum
    from tqdm import tqdm

    tpch_dir = tpch_dir or DEFAULT_TPCH_DIR
    li_path = os.path.join(tpch_dir, "lineitem.tbl")
    or_path = os.path.join(tpch_dir, "orders.tbl")
    if not (os.path.exists(li_path) and os.path.exists(or_path)):
        raise SystemExit(
            f"TPC-H data not found in {tpch_dir}\n"
            f"Generate it first (one-time):\n"
            f"  otterbrix/benchmark/download_data.sh --suite=tpch --scale=0.01")

    # Load once outside the timed region; slice per size.
    lineitem_full = load_tpch_table(li_path, LINEITEM_COLUMNS,
                                    limit=max(LINEITEM_SIZES))
    join_lineitem_full = load_tpch_table(li_path, LINEITEM_COLUMNS,
                                         limit=max(JOIN_SIZES))
    join_orders_full = load_tpch_table(or_path, ORDERS_COLUMNS,
                                       limit=max(JOIN_SIZES))

    spark = SparkSession.builder.master(SPARK_MASTER) \
        .appName("benchmark-tpch").getOrCreate()

    # One task per (scenario, size) pair — both optimize modes are measured
    # together, interleaved with randomized within-pair order by measure_paired
    # (same methodology as bench_optimizer.run_benchmarks).
    tasks = []
    for s in REGULAR_SCENARIOS:
        for sz in LINEITEM_SIZES:
            tasks.append(("regular", s, sz, None))
    for sz in LINEITEM_SIZES:
        for thr, pct in SELECTIVITY_POINTS:
            tasks.append(("sort_sel", f"filter_over_sort_sel_{pct}", sz, thr))
    for sz in JOIN_SIZES:
        for thr, pct in SELECTIVITY_POINTS:
            tasks.append(("join_sel", f"join_filter_sel_{pct}", sz, thr))

    results = []
    pbar = tqdm(total=len(tasks), desc="TPC-H benchmarks", unit="pair")
    for kind, scenario, size, threshold in tasks:
        if kind == "regular":
            data = lineitem_full[:size]
            factory = REGULAR_SCENARIOS[scenario]
            fn_no = factory(spark, col, spark_sum, data, False)
            fn_opt = factory(spark, col, spark_sum, data, True)
            runs_key = "groupby_agg" if scenario == "groupby_agg" else "filter"
            actual = len(data)
        elif kind == "sort_sel":
            data = lineitem_full[:size]
            fn_no = scenario_filter_over_sort_sel(spark, col, spark_sum,
                                                  data, threshold, False)
            fn_opt = scenario_filter_over_sort_sel(spark, col, spark_sum,
                                                   data, threshold, True)
            runs_key = "filter_over_sort_sel"
            actual = len(data)
        else:  # join_sel
            li = join_lineitem_full[:size]
            orders = join_orders_full[:size]
            fn_no = scenario_join_filter_sel(spark, col, spark_sum,
                                             li, orders, threshold, False)
            fn_opt = scenario_join_filter_sel(spark, col, spark_sum,
                                              li, orders, threshold, True)
            runs_key = "selectivity"
            actual = len(li)

        pbar.set_postfix_str(f"{scenario} n={actual:,}")

        # Correctness gate: optimize=True must not change the result. A row-count
        # mismatch means the no_opt and opt plans disagree — their timings are
        # then not comparable (one of them computed the wrong answer).
        rows_no = len(fn_no())
        rows_opt = len(fn_opt())
        correctness = "ok" if rows_no == rows_opt else "MISMATCH"
        if correctness != "ok":
            pbar.write(f"  !! {scenario} n={actual:,}: no_opt={rows_no} rows vs "
                       f"opt={rows_opt} rows — RESULT MISMATCH (timings not comparable)")

        n_runs = runs_for_size(actual, scenario=runs_key)
        # paired budget covers BOTH modes (interleaved), so it is 2x per-mode
        stats_no, stats_opt = measure_paired(fn_no, fn_opt, runs=n_runs,
                                             budget=2 * time_budget(runs_key))
        res_no = _make_result(scenario, "no_opt", actual, stats_no,
                              measure_memory(fn_no), None)
        res_opt = _make_result(scenario, "opt", actual, stats_opt,
                               measure_memory(fn_opt), None)
        res_no["result_rows"], res_opt["result_rows"] = rows_no, rows_opt
        res_no["correctness"] = res_opt["correctness"] = correctness
        results.append(res_no)
        results.append(res_opt)
        pbar.update(1)
    pbar.close()

    # Pairwise opt/no_opt significance per (scenario, rows).
    runs_lookup = {(r["scenario"], r["mode"], r["rows"]): r.get("raw_runs", [])
                   for r in results}
    for r in results:
        runs_no = runs_lookup.get((r["scenario"], "no_opt", r["rows"]), [])
        runs_opt = runs_lookup.get((r["scenario"], "opt", r["rows"]), [])
        if runs_no and runs_opt:
            cmp = compare_significance(runs_no, runs_opt)
            r["sig"] = "*" if cmp["significant"] else "ns"
            r["p_value"] = cmp["p_value"]
            if r["mode"] == "no_opt":
                r["speedup"] = r["bootstrap_l"] = r["bootstrap_r"] = 1.0
            else:
                r["speedup"] = cmp["speedup"]
                r["bootstrap_l"], r["bootstrap_r"] = cmp["ci_95"]
        else:
            r["sig"] = ""
            r["p_value"] = r["speedup"] = r["bootstrap_l"] = r["bootstrap_r"] = None

    return results


# ---------------------------------------------------------------------------
# Results & environment
# ---------------------------------------------------------------------------

def collect_environment(tpch_dir: str) -> dict:
    """Hardware/software stand + TPC-H run config, for results_tpch.env.json.

    Reuses bench_optimizer.collect_environment for the hardware/library capture,
    then replaces benchmark_config with TPC-H specifics — the base function
    records the synthetic bench's scenario sizes, which do not apply here.
    """
    from bench_optimizer import collect_environment as _base_env, WARMUP_RUNS
    env = _base_env()
    env["benchmark_config"] = {
        "spark_master": SPARK_MASTER,
        "warmup_runs": WARMUP_RUNS,
        "data_source": "TPC-H (dbgen via download_data.sh)",
        "tpch_dir": tpch_dir,
        "lineitem_sizes": LINEITEM_SIZES,
        "join_sizes": JOIN_SIZES,
        "selectivity_points": SELECTIVITY_POINTS,
    }
    return env


def save_results(results: list, path: str, tpch_dir: str):
    """Write the results CSV and a TPC-H environment sidecar (.env.json)."""
    import json
    import pandas as pd
    from bench_optimizer import print_environment

    df = pd.DataFrame(results)
    if "raw_runs" in df.columns:  # list column — not suitable for CSV
        df = df.drop(columns=["raw_runs"])
    df.to_csv(path, index=False)
    print(f"\nResults saved to {path}")

    env = collect_environment(tpch_dir)
    env_path = os.path.splitext(path)[0] + ".env.json"
    with open(env_path, "w") as f:
        json.dump(env, f, indent=2, ensure_ascii=False)
    print(f"Environment metadata saved to {env_path}")
    print_environment(env)
    return df


def print_correctness_gate(results: list) -> None:
    """Report scenarios where optimize=True changed the result row count."""
    bad = sorted({(r["scenario"], r["rows"]) for r in results
                  if r.get("correctness") == "MISMATCH"})
    print(f"\n{'='*60}")
    print("CORRECTNESS GATE: opt vs no_opt result row count")
    print(f"{'='*60}")
    if not bad:
        print("  OK — every scenario returns the same row count in both modes.")
        return
    print(f"  {len(bad)} scenario(s) where optimize=True changes the result.")
    print("  opt/no_opt timings for these are NOT comparable (one is wrong):")
    for scenario, rows in bad:
        print(f"    {scenario:28s} n={rows:,}")


def main():
    import argparse
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from bench_optimizer import print_summary_table, print_selectivity_summary

    parser = argparse.ArgumentParser(
        description="TPC-H opt/no_opt DataFrame benchmark (companion to bench_optimizer.py)")
    parser.add_argument("--tpch-dir", default=None,
                        help=f"TPC-H .tbl directory (default: {DEFAULT_TPCH_DIR})")
    args = parser.parse_args()

    tpch_dir = args.tpch_dir or DEFAULT_TPCH_DIR
    results = run_benchmarks_tpch(tpch_dir=tpch_dir)
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "results_tpch.csv")
    df = save_results(results, path, tpch_dir)
    print_summary_table(df, raw_results=results)
    print_selectivity_summary(df, raw_results=results)
    print_correctness_gate(results)


if __name__ == "__main__":
    main()
