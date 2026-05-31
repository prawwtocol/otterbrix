"""Functional equivalence of the query-plan optimizer (predicate pushdown).

For every query shape on which the pushdown rule actually fires
(filter over sort / join / group-by key / projection, plus conjunction
splitting), the result of ``collect(optimize=True)`` must equal the result of
``collect(optimize=False)`` *by value*, and the result must be non-empty so the
check is meaningful (a no-op optimizer would pass a value comparison trivially,
so each shape here is one the C++ trace tests in
``integration/cpp/test/test_pushdown_filter.cpp`` confirm is transformed).

This is the automated backing for the criterion of task 4 in the thesis:
"results with optimization on and off coincide on all functional tests".
"""

import pytest

pytest.importorskip("otterbrix.experimental.spark")

from otterbrix.experimental.spark.sql.functions import col, sum as Fsum


def _equiv(make_df, *, expect_rows=None):
    """Build the query fresh for each mode, compare results by value.

    Returns the (sorted) optimized result. Asserts:
      * optimize=True result == optimize=False result (the task-4 claim);
      * result is non-empty (the shape actually returns rows);
      * optional exact row count, to pin the ground-truth answer.
    """
    no_opt = sorted(tuple(r) for r in make_df().collect(optimize=False))
    opt = sorted(tuple(r) for r in make_df().collect(optimize=True))
    assert opt == no_opt, f"optimize changed the result:\n no_opt={no_opt}\n opt   ={opt}"
    assert len(opt) > 0, "empty result: the test does not exercise anything"
    if expect_rows is not None:
        assert len(opt) == expect_rows, f"expected {expect_rows} rows, got {len(opt)}"
    return opt


class TestOptimizerEquivalence:
    # filter over sort -> filter pushed below the sort
    def test_filter_over_sort(self, spark):
        data = [(i, float(i)) for i in range(1, 11)]
        _equiv(
            lambda: spark.createDataFrame(data, ["id", "val"])
            .sort("val")
            .filter(col("val") > 5),
            expect_rows=5,  # val in {6,7,8,9,10}
        )

    # filter over inner join, predicate on one side -> pushed into that branch
    def test_filter_over_inner_join(self, spark):
        left = [(i, float(i)) for i in range(1, 11)]
        right = [(i, "x%d" % i) for i in range(1, 11)]
        _equiv(
            lambda: spark.createDataFrame(left, ["id", "val"])
            .join(spark.createDataFrame(right, ["id", "name"]), "id")
            .filter(col("val") > 6),
            expect_rows=4,  # val in {7,8,9,10}
        )

    # filter over group-by on the grouping key -> pushed below the group
    def test_filter_over_groupby_key(self, spark):
        data = [("A" if i % 2 else "B", float(i)) for i in range(1, 11)]
        _equiv(
            lambda: spark.createDataFrame(data, ["grp", "val"])
            .groupBy("grp")
            .agg(Fsum("val").alias("s"))
            .filter(col("grp") == "A"),
            expect_rows=1,
        )

    # filter over identity projection -> pushed below the projection
    def test_filter_over_identity_projection(self, spark):
        data = [(i, float(i)) for i in range(1, 11)]
        _equiv(
            lambda: spark.createDataFrame(data, ["id", "val"])
            .select("id", "val")
            .filter(col("id") > 5),
            expect_rows=5,
        )

    # filter over narrowing projection -> cost guard vetoes the push (still equal)
    def test_filter_over_narrowing_projection(self, spark):
        data = [(i, float(i), "p%d" % i) for i in range(1, 11)]
        _equiv(
            lambda: spark.createDataFrame(data, ["id", "val", "pad"])
            .select("id")
            .filter(col("id") > 5),
            expect_rows=5,
        )

    # conjunction split across an inner join: a-> left, b-> right
    def test_conjunction_split_over_join(self, spark):
        left = [(i, float(i)) for i in range(1, 11)]
        right = [(i, float(i * 10)) for i in range(1, 11)]
        _equiv(
            lambda: spark.createDataFrame(left, ["id", "a"])
            .join(spark.createDataFrame(right, ["id", "b"]), "id")
            .filter((col("a") > 3) & (col("b") < 80)),
            expect_rows=4,  # a>3 (id 4..10) AND b=id*10<80 (id 1..7) -> id 4,5,6,7
        )

    # SAFETY TRAP: filter on the null-producing side of a LEFT join.
    # Pushing this predicate into the right branch would change the result;
    # optimize=True must still equal optimize=False.
    def test_left_join_filter_on_null_side(self, spark):
        left = [(1, 10), (2, 99), (3, 10)]  # id, k
        right = [(10, "x")]  # k, v
        _equiv(
            lambda: spark.createDataFrame(left, ["id", "k"])
            .join(spark.createDataFrame(right, ["k", "v"]), "k", "left")
            .filter(col("v") == "x"),
            expect_rows=2,  # k=10 matches: id 1 and 3
        )
