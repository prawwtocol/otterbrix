import pytest

_ = pytest.importorskip("otterbrix.experimental.spark")

from otterbrix.experimental.spark.sql.types import Row
from otterbrix.experimental.spark.sql.functions import col


class TestDataFrameSelectProjection(object):
    # Duplicate (id, name) rows: a pure projection must preserve them.
    # Pre-fix: Project delegated to Group → operator_group_t deduplicated by keys
    # → result would lose duplicates. This regression catches that.
    data = [
        (1, "Alice", 30),
        (2, "Bob", 25),
        (1, "Alice", 40),
        (3, "Carol", 30),
        (2, "Bob", 50),
    ]

    def test_select_preserves_duplicates(self, spark):
        df = spark.createDataFrame(self.data, ["id", "name", "age"])
        projected = df.select("id", "name")
        rows = projected.collect()

        assert len(rows) == 5, (
            f"Pure projection must preserve every input row, got {len(rows)}"
        )
        expected = sorted([(1, "Alice"), (2, "Bob"), (1, "Alice"), (3, "Carol"), (2, "Bob")])
        actual = sorted([(r.id, r.name) for r in rows])
        assert actual == expected

    def test_select_column_subset(self, spark):
        df = spark.createDataFrame(self.data, ["id", "name", "age"])
        projected = df.select("name")
        rows = projected.collect()
        assert len(rows) == 5
        assert sorted([r.name for r in rows]) == sorted(["Alice", "Bob", "Alice", "Carol", "Bob"])

    def test_select_via_col_expression(self, spark):
        df = spark.createDataFrame(self.data, ["id", "name", "age"])
        projected = df.select(col("id"), col("age"))
        rows = projected.collect()
        assert len(rows) == 5
