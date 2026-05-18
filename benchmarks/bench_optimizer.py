"""
Benchmark: Optimized vs non-optimized DataFrame execution in OtterBrix Spark API.

Scenarios:
  1. filter                   — df.filter(col("age") > threshold)              O(n)
  2. project+filter           — df.select("a","b").filter(col("a") > t)        O(n)        [pushdown]
  3. chained_filters          — df.filter(a).filter(b).filter(c)               O(n)
  4. groupby_agg              — df.groupBy("key").agg(sum("value"))            O(n*k)
  5. filter_over_sort         — df.sort("name").filter(col("age") > t)         O(n log n)  [pushdown]
  6. filter_over_groupby_key  — df.groupBy("key").agg(...).filter(key == c)    O(n)        [pushdown]
  7. join+filter              — df1.join(df2, "id").filter(col("v") > t)       O(n²)       [pushdown]
  8. filter_over_sort_sel_*   — variant of (5) at multiple filter selectivities (1/10/50/90%)
  9. join_filter_sel_*        — variant of (7) at multiple filter selectivities (1/10/50/90%)

Modes: no_opt (optimize=False), opt (optimize=True).

Data sizes are per-scenario — fast O(n) scenarios run up to 1M rows,
while O(n²) join uses smaller sizes to keep total runtime ~5-10 min.

Data types covered: int (id, age), float (value), string (name, group_key).
"""

# todo 1 Интегрируй в директорию benchmark/

import gc
import os
import sys
import time
import statistics
import random
from typing import List, Tuple, Callable, Dict, Any

_repo_integration_python = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "integration", "python")
)
sys.path.append(_repo_integration_python)

from otterbrix.experimental.spark.sql import SparkSession
from otterbrix.experimental.spark.sql.functions import col, sum as spark_sum


# ---------------------------------------------------------------------------
# Data generation
# ---------------------------------------------------------------------------

def generate_main_data(n: int) -> List[Tuple]:
    """Generate n rows of (id, name, age, value, group_key).

    Column types: int, str, int, float, str — covers all required types.
    """
    names = ["Alice", "Bob", "Charlie", "Diana", "Eve",
             "Frank", "Grace", "Hank", "Ivy", "Jack"]
    random.seed(42)
    return [
        (i, names[i % len(names)], random.randint(1, 100),
         round(random.uniform(0.0, 1000.0), 2), f"g{i % 50}")
        for i in range(n)
    ]

MAIN_SCHEMA = ["id", "name", "age", "value", "group_key"]


def generate_join_data(n: int) -> List[Tuple]:
    """Generate n rows of (id, extra_value) for join benchmarks."""
    random.seed(99)
    return [(i, round(random.uniform(0, 500), 2)) for i in range(n)]

JOIN_SCHEMA = ["id", "extra_value"]


# ---------------------------------------------------------------------------
# Per-scenario data sizes & time budgets
# ---------------------------------------------------------------------------
# O(n) filter scenarios are cheap — test up to 1M rows.
# O(n*k) groupby is moderate — cap at 100k (k=50 groups).
# O(n²) join is expensive  — cap at 10k; add 5k for finer granularity.

SIZES_FAST = [1_000, 10_000, 100_000]               # filter, project, chained
SIZES_GROUPBY = [1_000, 10_000]                     # groupby_agg  (O(n*k) + transpose is very slow)
SIZES_JOIN = [1_000, 3_000]                         # join+filter, selectivity (O(n²), capped early)

SCENARIO_SIZES = {
    "filter":                  SIZES_FAST,
    "project_filter":          SIZES_FAST,
    "chained_filters":         SIZES_FAST,
    "filter_over_sort":        SIZES_FAST,
    "filter_over_groupby_key": SIZES_FAST,
    "filter_over_sort_sel":    SIZES_FAST,
    "groupby_agg":             SIZES_GROUPBY,
    "join_filter":             SIZES_JOIN,
    "selectivity":             SIZES_JOIN,
}

# Fallback for --sizes CLI override (applies to all scenarios uniformly)
DATA_SIZES = [1_000, 10_000, 100_000, 1_000_000]


# ---------------------------------------------------------------------------
# Timing & memory helpers
# ---------------------------------------------------------------------------

WARMUP_RUNS = 2


def runs_for_size(n: int, scenario: str = "filter") -> int:
    """Adaptive run count based on scenario complexity and data size.

    Fast O(n) scenarios get many runs for stable statistics.
    Slow O(n²) join scenarios get fewer runs but still enough for t-test (>=10).
    """
    is_join = scenario in ("join_filter", "selectivity")
    is_groupby = scenario == "groupby_agg"

    if is_join:
        if n <= 1_000:
            return 30
        if n <= 5_000:
            return 15
        return 10                   # 10k: ~1s/run → 10 runs ≈ 10s
    if is_groupby:
        if n <= 1_000:
            return 50
        if n <= 10_000:
            return 30
        return 15                   # 100k: ~1s/run → 15 runs ≈ 15s
    # fast O(n) scenarios
    if n <= 1_000:
        return 100
    if n <= 10_000:
        return 50
    if n <= 100_000:
        return 30
    return 20


def time_budget(scenario: str) -> float:
    """Per-scenario wall-clock budget in seconds (excludes warmup)."""
    if scenario in ("join_filter", "selectivity"):
        return 120          # O(n²) needs more headroom per size
    if scenario == "groupby_agg":
        return 60
    return 30               # O(n) filter scenarios


def measure(fn: Callable[[], Any], warmup: int = WARMUP_RUNS,
            runs: int = 20, budget: float = 30.0) -> Dict[str, Any]:
    """Run fn() warmup+runs times, return timing stats in seconds.

    Respects *budget* (wall-clock seconds, excludes warmup).
    Stops early once the budget is exceeded but always does at least
    MIN_RUNS iterations so that t-test / bootstrap CI stay meaningful.
    """
    from math import sqrt

    MIN_RUNS = 8  # minimum for Welch's t-test + bootstrap

    for _ in range(warmup):
        fn()

    gc.disable()
    times = []
    wall_start = time.perf_counter()
    for i in range(runs):
        t0 = time.perf_counter_ns()
        fn()
        t1 = time.perf_counter_ns()
        times.append((t1 - t0) / 1e9)
        if i >= MIN_RUNS - 1 and (time.perf_counter() - wall_start) > budget:
            break
    gc.enable()

    sorted_times = sorted(times)
    n = len(sorted_times)
    mean = statistics.mean(times)
    stdev = statistics.stdev(times) if n > 1 else 0.0

    # 95% CI via t-distribution
    try:
        from scipy.stats import t as t_dist
        t_val = t_dist.ppf(0.975, df=n - 1)
    except ImportError:
        # Fallback: use z=1.96 for large n
        t_val = 1.96
    sem = stdev / sqrt(n) if n > 1 else 0.0
    ci_half = t_val * sem

    cv = stdev / mean if mean > 0 else 0.0

    return {
        "mean": mean,
        "median": statistics.median(times),
        "stdev": stdev,
        "min": min(times),
        "max": max(times),
        "p95": sorted_times[int(n * 0.95)] if n >= 20 else sorted_times[-1],
        "p99": sorted_times[int(n * 0.99)] if n >= 100 else sorted_times[-1],
        "sem": sem,
        "t_val": t_val,
        "ci_95_low": mean - ci_half,
        "ci_95_high": mean + ci_half,
        "cv": cv,
        "actual_runs": n,
        "runs": times,
    }


def measure_memory(fn: Callable[[], Any]) -> float:
    """Measure peak Python memory of fn() in MB using tracemalloc."""
    import tracemalloc
    gc.collect()
    tracemalloc.start()
    fn()
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return peak / (1024 * 1024)  # MB


def compare_significance(runs_a: List[float], runs_b: List[float]) -> Dict[str, Any]:
    """Compare two sets of timing runs via Welch's t-test and bootstrap CI.

    Returns speedup (mean_a / mean_b), 95% bootstrap CI for speedup,
    p-value, and whether the difference is significant at alpha=0.05.
    """
    import numpy as np

    mean_a = np.mean(runs_a)
    mean_b = np.mean(runs_b)
    speedup = mean_a / mean_b if mean_b > 0 else float("inf")

    # Welch's t-test
    try:
        from scipy.stats import ttest_ind
        _, p_value = ttest_ind(runs_a, runs_b, equal_var=False)
    except ImportError:
        p_value = float("nan")

    # Bootstrap 95% CI for speedup
    rng = np.random.default_rng(42)
    n_boot = 10_000
    boot_speedups = []
    arr_a = np.array(runs_a)
    arr_b = np.array(runs_b)
    for _ in range(n_boot):
        sa = rng.choice(arr_a, size=len(arr_a), replace=True)
        sb = rng.choice(arr_b, size=len(arr_b), replace=True)
        mb = np.mean(sb)
        if mb > 0:
            boot_speedups.append(np.mean(sa) / mb)
    boot_speedups = np.array(boot_speedups)
    ci_low = float(np.percentile(boot_speedups, 2.5))
    ci_high = float(np.percentile(boot_speedups, 97.5))

    return {
        "speedup": float(speedup),
        "ci_95": (ci_low, ci_high),
        "p_value": float(p_value),
        "significant": bool(p_value < 0.05) if not np.isnan(p_value) else False,
    }


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------

def scenario_filter(spark: SparkSession, data: List[Tuple], optimize: bool) -> Callable:
    """filter(age > 50)"""
    df = spark.createDataFrame(data, schema=MAIN_SCHEMA, optimize=optimize)
    def run():
        df.filter(col("age") > 50).collect()
    return run


def scenario_project_filter(spark: SparkSession, data: List[Tuple], optimize: bool) -> Callable:
    """select('id','name','age').filter(age > 50)"""
    df = spark.createDataFrame(data, schema=MAIN_SCHEMA, optimize=optimize)
    def run():
        df.select("id", "name", "age").filter(col("age") > 50).collect()
    return run


def scenario_chained_filters(spark: SparkSession, data: List[Tuple], optimize: bool) -> Callable:
    """filter(age>20).filter(age<80).filter(value>100)"""
    df = spark.createDataFrame(data, schema=MAIN_SCHEMA, optimize=optimize)
    def run():
        df.filter(col("age") > 20).filter(col("age") < 80).filter(col("value") > 100).collect()
    return run


def scenario_groupby(spark: SparkSession, data: List[Tuple], optimize: bool) -> Callable:
    """groupBy('group_key').agg(sum('value'))"""
    df = spark.createDataFrame(data, schema=MAIN_SCHEMA, optimize=optimize)
    def run():
        df.groupBy("group_key").agg(spark_sum("value")).collect()
    return run


def scenario_filter_over_sort(spark: SparkSession, data: List[Tuple], optimize: bool) -> Callable:
    """sort('name').filter(col('age') > 50)

    Pushdown: filter moves below sort, reducing the number of rows sorted.
    Effect grows with size (sort is O(n log n)) and with filter selectivity.
    """
    df = spark.createDataFrame(data, schema=MAIN_SCHEMA, optimize=optimize)
    def run():
        df.sort("name").filter(col("age") > 50).collect()
    return run


def scenario_filter_over_groupby_key(spark: SparkSession, data: List[Tuple], optimize: bool) -> Callable:
    """groupBy('group_key').agg(sum('value')).filter(col('group_key') == 'g0')

    Pushdown: filter on a group key moves below the groupBy, so fewer rows
    participate in the aggregation. Effect is largest when the predicate is
    selective (here 1/50 = 2% of rows match a single key).
    """
    df = spark.createDataFrame(data, schema=MAIN_SCHEMA, optimize=optimize)
    def run():
        df.groupBy("group_key").agg(spark_sum("value")).filter(col("group_key") == "g0").collect()
    return run


def scenario_join_filter(spark: SparkSession, data: List[Tuple],
                         join_data: List[Tuple], optimize: bool) -> Callable:
    """join(df2, 'id').filter(value > 500)"""
    df1 = spark.createDataFrame(data, schema=MAIN_SCHEMA, optimize=optimize)
    df2 = spark.createDataFrame(join_data, schema=JOIN_SCHEMA, optimize=optimize)
    def run():
        df1.join(df2, "id").filter(col("value") > 500).collect()
    return run


# ---------------------------------------------------------------------------
# Selectivity sweep: join + filter at varying selectivity
# ---------------------------------------------------------------------------

# threshold -> approximate % of rows passing (uniform 0..1000)
SELECTIVITY_POINTS = [
    (100, 90),   # ~90% pass
    (500, 50),   # ~50% pass
    (900, 10),   # ~10% pass
    (990, 1),    # ~1% pass
]


def scenario_join_filter_selectivity(spark: SparkSession, data: List[Tuple],
                                     join_data: List[Tuple], mode: str,
                                     threshold: int) -> Callable:
    """join(df2, 'id').filter(value > threshold) with configurable optimizer.

    mode: 'no_opt' or 'opt'
    """
    optimize = mode == "opt"
    df1 = spark.createDataFrame(data, schema=MAIN_SCHEMA, optimize=optimize)
    df2 = spark.createDataFrame(join_data, schema=JOIN_SCHEMA, optimize=optimize)

    def run():
        df1.join(df2, "id").filter(col("value") > threshold).collect()
    return run


SCENARIOS = {
    "filter": scenario_filter,
    "project_filter": scenario_project_filter,
    "chained_filters": scenario_chained_filters,
    "groupby_agg": scenario_groupby,
    "filter_over_sort": scenario_filter_over_sort,
    "filter_over_groupby_key": scenario_filter_over_groupby_key,
    # join_filter handled separately because it needs extra join_data arg
}


def scenario_filter_over_sort_selectivity(spark: SparkSession, data: List[Tuple],
                                          mode: str, threshold: int) -> Callable:
    """sort('name').filter(col('value') > threshold) with configurable optimizer.

    Non-quadratic counterpart of the join+filter selectivity sweep — lets us
    show pushdown impact on large data (up to 100k rows) without paying O(n^2).

    mode: 'no_opt' or 'opt'
    """
    optimize = mode == "opt"
    df = spark.createDataFrame(data, schema=MAIN_SCHEMA, optimize=optimize)

    def run():
        df.sort("name").filter(col("value") > threshold).collect()
    return run


# ---------------------------------------------------------------------------
# Plan node counter
# ---------------------------------------------------------------------------

def count_plan_nodes(node) -> int:
    """Recursively count nodes in a logical plan tree."""
    if node is None:
        return 0
    count = 1
    if hasattr(node, "children"):
        for child in node.children:
            count += count_plan_nodes(child)
    return count


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def _build_task_list(sizes_override=None):
    """Pre-compute the list of (label, ...) tasks for progress tracking.

    When *sizes_override* is given (from --sizes CLI), it applies to ALL
    scenarios uniformly.  Otherwise each scenario uses its own size list
    from SCENARIO_SIZES.
    """
    tasks = []

    def _sizes_for(scenario_key):
        if sizes_override is not None:
            return sizes_override
        return SCENARIO_SIZES.get(scenario_key, SIZES_FAST)

    # Regular scenarios
    for name in SCENARIOS:
        for size in _sizes_for(name):
            for optimize in [False, True]:
                mode = "no_opt" if not optimize else "opt"
                tasks.append(("scenario", size, name, mode, optimize))

    # Join scenario
    for size in _sizes_for("join_filter"):
        for optimize in [False, True]:
            mode = "no_opt" if not optimize else "opt"
            tasks.append(("join", size, "join_filter", mode, optimize))

    # Selectivity sweep
    for size in _sizes_for("selectivity"):
        for threshold, pct in SELECTIVITY_POINTS:
            for mode in ["no_opt", "opt"]:
                tasks.append(("selectivity", size, f"join_filter_sel_{pct}", mode, threshold))

    # Filter-over-sort selectivity sweep (O(n log n), runs on fast sizes)
    for size in _sizes_for("filter_over_sort_sel"):
        for threshold, pct in SELECTIVITY_POINTS:
            for mode in ["no_opt", "opt"]:
                tasks.append(("filter_over_sort_sel", size, f"filter_over_sort_sel_{pct}", mode, threshold))

    return tasks


def _make_result(scenario, mode, size, stats, mem_mb, plan_nodes):
    """Build a result dict from stats."""
    return {
        "scenario": scenario,
        "mode": mode,
        "rows": size,
        "mean_s": stats["mean"],
        "median_s": stats["median"],
        "p95_s": stats["p95"],
        "p99_s": stats["p99"],
        "stdev_s": stats["stdev"],
        "min_s": stats["min"],
        "max_s": stats["max"],
        "actual_runs": stats["actual_runs"],
        "sem": stats["sem"],
        "t_val": stats["t_val"],
        "ci_95_low": stats["ci_95_low"],
        "ci_95_high": stats["ci_95_high"],
        "cv": stats["cv"],
        "throughput_rows_per_s": size / stats["mean"] if stats["mean"] > 0 else 0,
        "peak_memory_mb": mem_mb,
        "plan_nodes": plan_nodes,
        "raw_runs": stats["runs"],
    }


def run_benchmarks(sizes=None):
    from tqdm import tqdm

    spark = SparkSession.builder.master("local[2]").appName("benchmark").getOrCreate()
    results = []

    sizes_override = sizes  # None means use per-scenario defaults
    tasks = _build_task_list(sizes_override)
    pbar = tqdm(total=len(tasks), desc="Benchmarks", unit="bench",
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]")

    # Cache generated data per size to avoid regenerating
    data_cache: Dict[int, List[Tuple]] = {}
    join_cache: Dict[int, List[Tuple]] = {}

    def _get_data(size):
        if size not in data_cache:
            data_cache[size] = generate_main_data(size)
        return data_cache[size]

    def _get_join_data(size):
        if size not in join_cache:
            join_cache[size] = generate_join_data(size)
        return join_cache[size]

    for task in tasks:
        kind = task[0]
        size = task[1]
        scenario_name = task[2]
        mode = task[3]

        data = _get_data(size)
        join_data = _get_join_data(size)

        # Determine scenario key for runs/budget lookup
        scenario_key = scenario_name
        if kind == "join":
            scenario_key = "join_filter"
        elif kind == "selectivity":
            scenario_key = "selectivity"
        elif kind == "filter_over_sort_sel":
            scenario_key = "filter_over_sort_sel"

        n_runs = runs_for_size(size, scenario=scenario_key)
        budget = time_budget(scenario_key)
        pbar.set_postfix_str(f"{scenario_name} [{mode}] n={size:,} runs={n_runs}")

        if kind == "scenario":
            optimize = task[4]
            factory = SCENARIOS[scenario_name]
            fn = factory(spark, data, optimize)
            stats = measure(fn, runs=n_runs, budget=budget)
            mem_mb = measure_memory(fn)
            plan_nodes = None
            if optimize:
                df_tmp = spark.createDataFrame(data, schema=MAIN_SCHEMA, optimize=True)
                if scenario_name == "filter":
                    df_tmp = df_tmp.filter(col("age") > 50)
                elif scenario_name == "project_filter":
                    df_tmp = df_tmp.select("id", "name", "age").filter(col("age") > 50)
                elif scenario_name == "chained_filters":
                    df_tmp = df_tmp.filter(col("age") > 20).filter(col("age") < 80).filter(col("value") > 100)
                elif scenario_name == "groupby_agg":
                    df_tmp = df_tmp.groupBy("group_key").agg(spark_sum("value"))
                elif scenario_name == "filter_over_sort":
                    df_tmp = df_tmp.sort("name").filter(col("age") > 50)
                elif scenario_name == "filter_over_groupby_key":
                    df_tmp = (df_tmp.groupBy("group_key")
                                    .agg(spark_sum("value"))
                                    .filter(col("group_key") == "g0"))
                plan_nodes = count_plan_nodes(df_tmp._plan)
            results.append(_make_result(scenario_name, mode, size, stats, mem_mb, plan_nodes))

        elif kind == "join":
            optimize = task[4]
            fn = scenario_join_filter(spark, data, join_data, optimize)
            stats = measure(fn, runs=n_runs, budget=budget)
            mem_mb = measure_memory(fn)
            plan_nodes = None
            if optimize:
                df1 = spark.createDataFrame(data, schema=MAIN_SCHEMA, optimize=True)
                df2 = spark.createDataFrame(join_data, schema=JOIN_SCHEMA, optimize=True)
                df_tmp = df1.join(df2, "id").filter(col("value") > 500)
                plan_nodes = count_plan_nodes(df_tmp._plan)
            results.append(_make_result("join_filter", mode, size, stats, mem_mb, plan_nodes))

        elif kind == "selectivity":
            threshold = task[4]
            fn = scenario_join_filter_selectivity(spark, data, join_data, mode, threshold)
            stats = measure(fn, runs=n_runs, budget=budget)
            mem_mb = measure_memory(fn)
            plan_nodes = None
            if mode == "opt":
                df1 = spark.createDataFrame(data, schema=MAIN_SCHEMA, optimize=True)
                df2 = spark.createDataFrame(join_data, schema=JOIN_SCHEMA, optimize=True)
                df_tmp = df1.join(df2, "id").filter(col("value") > threshold)
                plan_nodes = count_plan_nodes(df_tmp._plan)
            results.append(_make_result(scenario_name, mode, size, stats, mem_mb, plan_nodes))

        elif kind == "filter_over_sort_sel":
            threshold = task[4]
            fn = scenario_filter_over_sort_selectivity(spark, data, mode, threshold)
            stats = measure(fn, runs=n_runs, budget=budget)
            mem_mb = measure_memory(fn)
            plan_nodes = None
            if mode == "opt":
                df_tmp = spark.createDataFrame(data, schema=MAIN_SCHEMA, optimize=True)
                df_tmp = df_tmp.sort("name").filter(col("value") > threshold)
                plan_nodes = count_plan_nodes(df_tmp._plan)
            results.append(_make_result(scenario_name, mode, size, stats, mem_mb, plan_nodes))

        pbar.update(1)

    pbar.close()

    # Post-process: compute pairwise significance for each (scenario, rows) pair
    runs_lookup = {}
    for r in results:
        key = (r["scenario"], r["mode"], r["rows"])
        runs_lookup[key] = r.get("raw_runs", [])

    for r in results:
        scenario, mode, rows = r["scenario"], r["mode"], r["rows"]
        other_mode = "opt" if mode == "no_opt" else "no_opt"
        runs_self = runs_lookup.get((scenario, mode, rows), [])
        runs_other = runs_lookup.get((scenario, other_mode, rows), [])
        if runs_self and runs_other:
            cmp = compare_significance(runs_self, runs_other)
            r["sig"] = "*" if cmp["significant"] else "ns"
            r["p_value"] = cmp["p_value"]
            r["speedup"] = cmp["speedup"]
            r["bootstrap_l"] = cmp["ci_95"][0]
            r["bootstrap_r"] = cmp["ci_95"][1]
        else:
            r["sig"] = ""
            r["p_value"] = None
            r["speedup"] = None
            r["bootstrap_l"] = None
            r["bootstrap_r"] = None

    return results


def save_csv(results: list, path: str = None):
    import pandas as pd
    if path is None:
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results.csv")
    df = pd.DataFrame(results)
    # Drop raw_runs column (list) — not suitable for CSV
    if "raw_runs" in df.columns:
        df = df.drop(columns=["raw_runs"])
    df.to_csv(path, index=False)
    print(f"\nResults saved to {path}")
    return df


def print_summary_table(df, raw_results=None):
    """Print a formatted comparison table with CI and significance."""
    print(f"\n{'='*120}")
    print("SUMMARY: No Optimization vs Optimization")
    print(f"{'='*120}")
    print(f"{'Scenario':25s} {'Rows':>8s} {'NoOpt(s)':>10s} {'NO CI95':>16s} {'Opt(s)':>10s} "
          f"{'O CI95':>16s} {'Speedup':>8s} {'Sig?':>5s} {'NO CV':>6s} {'O CV':>6s}")
    print(f"{'-'*120}")

    # Build lookup for raw_runs
    runs_lookup = {}
    if raw_results:
        for r in raw_results:
            key = (r["scenario"], r["mode"], r["rows"])
            runs_lookup[key] = r.get("raw_runs", [])

    for scenario in df["scenario"].unique():
        for size in sorted(df["rows"].unique()):
            row_no = df[(df["scenario"] == scenario) & (df["mode"] == "no_opt") & (df["rows"] == size)]
            row_opt = df[(df["scenario"] == scenario) & (df["mode"] == "opt") & (df["rows"] == size)]
            if row_no.empty or row_opt.empty:
                continue
            no = row_no.iloc[0]
            o = row_opt.iloc[0]
            speedup = no["mean_s"] / o["mean_s"] if o["mean_s"] > 0 else float("inf")

            # Significance test
            sig = ""
            runs_no = runs_lookup.get((scenario, "no_opt", size), [])
            runs_opt = runs_lookup.get((scenario, "opt", size), [])
            if runs_no and runs_opt:
                cmp = compare_significance(runs_no, runs_opt)
                sig = "*" if cmp["significant"] else "ns"

            no_ci = f"[{no['ci_95_low']:.4f},{no['ci_95_high']:.4f}]"
            o_ci = f"[{o['ci_95_low']:.4f},{o['ci_95_high']:.4f}]"
            print(f"{scenario:25s} {size:8,d} {no['mean_s']:10.4f} {no_ci:>16s} {o['mean_s']:10.4f} "
                  f"{o_ci:>16s} {speedup:7.2f}x {sig:>5s} {no['cv']:5.1%} {o['cv']:5.1%}")
        print()

    # Plan node summary for opt mode
    opt_with_plans = df[(df["mode"] == "opt") & (df["plan_nodes"].notna())]
    if not opt_with_plans.empty:
        print(f"\n{'='*60}")
        print("Plan Nodes (opt)")
        print(f"{'='*60}")
        for _, row in opt_with_plans.drop_duplicates(["scenario"]).iterrows():
            print(f"  {row['scenario']:25s}: {int(row['plan_nodes'])} nodes")


def print_selectivity_summary(df, raw_results=None):
    """Print selectivity sweep results with pushdown speedup (no_opt vs opt)."""
    sel_scenarios = [s for s in df["scenario"].unique() if s.startswith("join_filter_sel_")]
    if not sel_scenarios:
        return

    # Build lookup for raw_runs
    runs_lookup = {}
    if raw_results:
        for r in raw_results:
            key = (r["scenario"], r["mode"], r["rows"])
            runs_lookup[key] = r.get("raw_runs", [])

    print(f"\n{'='*100}")
    print("SELECTIVITY SWEEP: Predicate Pushdown Impact")
    print(f"  Pushdown speedup = no_opt / opt")
    print(f"{'='*100}")
    print(f"{'Scenario':25s} {'Rows':>8s} {'NoOpt(s)':>10s} {'Opt(s)':>10s} "
          f"{'Pushdown':>10s} {'Sig?':>5s} {'Opt CV':>7s}")
    print(f"{'-'*100}")

    for scenario in sorted(sel_scenarios):
        for size in sorted(df["rows"].unique()):
            row_no = df[(df["scenario"] == scenario) & (df["mode"] == "no_opt") & (df["rows"] == size)]
            row_opt = df[(df["scenario"] == scenario) & (df["mode"] == "opt") & (df["rows"] == size)]
            if row_no.empty or row_opt.empty:
                continue
            no = row_no.iloc[0]["mean_s"]
            opt = row_opt.iloc[0]["mean_s"]
            opt_cv = row_opt.iloc[0].get("cv", 0.0)
            pushdown_speedup = no / opt if opt > 0 else float("inf")

            # Significance of pushdown (no_opt vs opt)
            sig = ""
            runs_no = runs_lookup.get((scenario, "no_opt", size), [])
            runs_opt = runs_lookup.get((scenario, "opt", size), [])
            if runs_no and runs_opt:
                cmp = compare_significance(runs_no, runs_opt)
                sig = "*" if cmp["significant"] else "ns"

            print(f"{scenario:25s} {size:8,d} {no:10.4f} {opt:10.4f} "
                  f"{pushdown_speedup:9.2f}x {sig:>5s} {opt_cv:6.1%}")


def plot_results(df):
    """Generate bar charts: one per scenario, x=data_size, bars no_opt/opt."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed, skipping charts")
        return

    out_dir = os.path.dirname(os.path.abspath(__file__))
    scenarios = df["scenario"].unique()
    for scenario in scenarios:
        sub = df[df["scenario"] == scenario]
        modes = sorted(sub["mode"].unique())
        is_selectivity = (
            scenario.startswith("join_filter_sel_")
            or scenario.startswith("filter_over_sort_sel_")
        )
        fig, ax = plt.subplots(figsize=(10, 6))

        sizes = sorted(sub["rows"].unique())
        x_labels = [f"{s:,}" for s in sizes]
        x_pos = list(range(len(sizes)))

        width = 0.35
        colors = {"no_opt": "#C44E52", "opt": "#55A868"}
        no_opt_data = sub[sub["mode"] == "no_opt"].set_index("rows").reindex(sizes)
        opt_data = sub[sub["mode"] == "opt"].set_index("rows").reindex(sizes)
        bars1 = ax.bar([p - width/2 for p in x_pos], no_opt_data["mean_s"].values,
                       width, label="no_opt", color=colors["no_opt"], alpha=0.85)
        bars2 = ax.bar([p + width/2 for p in x_pos], opt_data["mean_s"].values,
                       width, label="opt", color=colors["opt"], alpha=0.85)
        all_bars = list(bars1) + list(bars2)

        ax.set_xlabel("Rows")
        ax.set_ylabel("Mean time (seconds)")
        ax.set_title(f"Benchmark: {scenario}")
        ax.set_xticks(x_pos)
        ax.set_xticklabels(x_labels)
        ax.legend()
        ax.grid(axis="y", alpha=0.3)

        for bar in all_bars:
            h = bar.get_height()
            if h > 0:
                ax.annotate(f"{h:.3f}",
                            xy=(bar.get_x() + bar.get_width() / 2, h),
                            xytext=(0, 3), textcoords="offset points",
                            ha="center", va="bottom", fontsize=8)

        fig.tight_layout()
        chart_path = os.path.join(out_dir, f"chart_{scenario}.png")
        fig.savefig(chart_path, dpi=150)
        plt.close(fig)
        print(f"Chart saved: {chart_path}")

    # Speedup summary chart at largest data size
    max_size = max(df["rows"])
    summary = df[df["rows"] == max_size].pivot(
        index="scenario", columns="mode", values="mean_s"
    )
    if "no_opt" in summary.columns and "opt" in summary.columns:
        summary["speedup"] = summary["no_opt"] / summary["opt"]

        fig, ax = plt.subplots(figsize=(10, 6))
        summary["speedup"].plot(kind="bar", ax=ax, color="#55A868", alpha=0.85)
        ax.axhline(y=1.0, color="red", linestyle="--", alpha=0.5, label="break-even")
        ax.set_ylabel("Speedup (no_opt / opt)")
        ax.set_title(f"Optimizer Speedup at {max_size:,} rows")
        ax.set_xticklabels(ax.get_xticklabels(), rotation=30, ha="right")
        ax.legend()
        ax.grid(axis="y", alpha=0.3)
        fig.tight_layout()
        chart_path = os.path.join(out_dir, "chart_speedup_summary.png")
        fig.savefig(chart_path, dpi=150)
        plt.close(fig)
        print(f"Chart saved: {chart_path}")

    # Throughput chart
    fig, ax = plt.subplots(figsize=(12, 6))
    for mode in ["no_opt", "opt"]:
        sub = df[(df["mode"] == mode) & (df["rows"] == max_size)]
        ax.bar([f"{s}\n({mode})" for s in sub["scenario"]],
               sub["throughput_rows_per_s"] / 1000,
               alpha=0.85, label=mode)
    ax.set_ylabel("Throughput (krows/s)")
    ax.set_title(f"Throughput at {max_size:,} rows")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    chart_path = os.path.join(out_dir, "chart_throughput.png")
    fig.savefig(chart_path, dpi=150)
    plt.close(fig)
    print(f"Chart saved: {chart_path}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Benchmark optimized vs non-optimized DataFrame execution",
        epilog=("Default per-scenario sizes: "
                f"filter/project/chained={SIZES_FAST}, "
                f"groupby={SIZES_GROUPBY}, "
                f"join/selectivity={SIZES_JOIN}. "
                "Use --sizes to override ALL scenarios with a uniform list."),
    )
    parser.add_argument(
        "--sizes", type=int, nargs="+", default=None,
        help="Override data sizes for ALL scenarios (e.g. --sizes 1000 5000)",
    )
    args = parser.parse_args()
    sizes = args.sizes if args.sizes else None  # None → per-scenario defaults

    results = run_benchmarks(sizes=sizes)
    df = save_csv(results)
    print_summary_table(df, raw_results=results)
    print_selectivity_summary(df, raw_results=results)
    plot_results(df)
