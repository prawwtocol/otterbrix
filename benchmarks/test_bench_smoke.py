"""Smoke test: verify each benchmark scenario runs without errors at small scale."""
import sys
import os

# Ensure imports work from project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
sys.path.insert(0, "otterbrix/integration/python")

from otterbrix.experimental.spark.sql import SparkSession
from otterbrix.experimental.spark.sql.functions import col

from bench_lazy_vs_eager import (
    generate_main_data, generate_join_data,
    MAIN_SCHEMA, JOIN_SCHEMA, SCENARIOS,
    scenario_join_filter, scenario_join_filter_selectivity,
    measure, measure_memory, runs_for_size,
)


def test_all_scenarios_eager():
    spark = SparkSession.builder.master("local[1]").appName("smoke").getOrCreate()
    data = generate_main_data(100)
    for name, factory in SCENARIOS.items():
        fn = factory(spark, data, lazy=False)
        stats = measure(fn, warmup=0, runs=1)
        assert stats["mean"] >= 0, f"{name} eager failed"


def test_all_scenarios_lazy():
    spark = SparkSession.builder.master("local[1]").appName("smoke").getOrCreate()
    data = generate_main_data(100)
    for name, factory in SCENARIOS.items():
        fn = factory(spark, data, lazy=True)
        stats = measure(fn, warmup=0, runs=1)
        assert stats["mean"] >= 0, f"{name} lazy failed"


def test_join_scenario():
    spark = SparkSession.builder.master("local[1]").appName("smoke").getOrCreate()
    data = generate_main_data(100)
    join_data = generate_join_data(100)
    for lazy in [False, True]:
        fn = scenario_join_filter(spark, data, join_data, lazy)
        stats = measure(fn, warmup=0, runs=1)
        assert stats["mean"] >= 0, f"join_filter lazy={lazy} failed"


def test_data_generators():
    data = generate_main_data(50)
    assert len(data) == 50
    assert len(data[0]) == 5  # id, name, age, value, group_key
    assert isinstance(data[0][0], int)    # id
    assert isinstance(data[0][1], str)    # name
    assert isinstance(data[0][2], int)    # age
    assert isinstance(data[0][3], float)  # value
    assert isinstance(data[0][4], str)    # group_key

    join_data = generate_join_data(50)
    assert len(join_data) == 50
    assert len(join_data[0]) == 2  # id, extra_value


def test_selectivity_scenarios():
    """Smoke test: all 3 modes × a single threshold run without errors."""
    spark = SparkSession.builder.master("local[1]").appName("smoke").getOrCreate()
    data = generate_main_data(100)
    join_data = generate_join_data(100)
    for mode in ["eager", "lazy_no_opt", "lazy_opt"]:
        fn = scenario_join_filter_selectivity(spark, data, join_data, mode, threshold=500)
        stats = measure(fn, warmup=0, runs=1)
        assert stats["mean"] >= 0, f"selectivity mode={mode} failed"


def test_measure_returns_stats():
    """Verify measure() returns CI and CV fields."""
    spark = SparkSession.builder.master("local[1]").appName("smoke").getOrCreate()
    data = generate_main_data(50)
    fn = SCENARIOS["filter"](spark, data, lazy=False)
    stats = measure(fn, warmup=0, runs=3)
    assert "ci_95_low" in stats
    assert "ci_95_high" in stats
    assert "cv" in stats
    assert "actual_runs" in stats
    assert stats["actual_runs"] == 3
    assert stats["ci_95_low"] <= stats["mean"] <= stats["ci_95_high"]


def test_measure_memory():
    """Verify measure_memory returns a non-negative float."""
    spark = SparkSession.builder.master("local[1]").appName("smoke").getOrCreate()
    data = generate_main_data(50)
    fn = SCENARIOS["filter"](spark, data, lazy=False)
    mem = measure_memory(fn)
    assert isinstance(mem, float)
    assert mem >= 0


def test_runs_for_size():
    """Verify adaptive run counts per scenario type."""
    # Fast O(n) filter scenarios
    assert runs_for_size(500, scenario="filter") == 100
    assert runs_for_size(1_000, scenario="filter") == 100
    assert runs_for_size(5_000, scenario="filter") == 50
    assert runs_for_size(10_000, scenario="filter") == 50
    assert runs_for_size(50_000, scenario="filter") == 30
    assert runs_for_size(100_000, scenario="filter") == 30
    assert runs_for_size(500_000, scenario="filter") == 20
    assert runs_for_size(1_000_000, scenario="filter") == 20
    # O(n*k) groupby
    assert runs_for_size(1_000, scenario="groupby_agg") == 50
    assert runs_for_size(10_000, scenario="groupby_agg") == 30
    assert runs_for_size(100_000, scenario="groupby_agg") == 15
    # O(n²) join scenarios get fewer runs
    assert runs_for_size(1_000, scenario="join_filter") == 30
    assert runs_for_size(5_000, scenario="join_filter") == 15
    assert runs_for_size(10_000, scenario="join_filter") == 10
