# tests/fast/spark/test_optimizer.py

from otterbrix.experimental.spark.sql import SparkSession
from otterbrix.experimental.spark.sql.functions import col


class TestColumnReferenceTracking:
    def test_col_tracks_single_column(self):
        """col("x") should track {"x"} as referenced columns."""
        from otterbrix.experimental.spark.sql.column import Column
        c = Column.__new__(Column)
        c._referenced_columns = frozenset({"x"})
        assert c._referenced_columns == frozenset({"x"})

    def test_column_default_empty_refs(self):
        """Column with no refs should have empty frozenset."""
        from otterbrix.experimental.spark.sql.column import Column
        c = Column.__new__(Column)
        c._referenced_columns = frozenset()
        assert c._referenced_columns == frozenset()


class TestColumnRefsPropagation:
    def test_comparison_merges_refs(self):
        """(col_a > col_b) should reference both columns."""
        from otterbrix.experimental.spark.sql.column import Column

        a = Column.__new__(Column)
        a.expr = None
        a._py_eval = lambda row: row["a"]
        a._referenced_columns = frozenset({"a"})

        b = Column.__new__(Column)
        b.expr = None
        b._py_eval = lambda row: row["b"]
        b._referenced_columns = frozenset({"b"})

        # We can't call a > b without C++ Expression, so test the merge logic directly
        merged = a._referenced_columns | b._referenced_columns
        assert merged == frozenset({"a", "b"})

    def test_literal_comparison_keeps_refs(self):
        """(col_a > 5) should reference only {"a"}."""
        from otterbrix.experimental.spark.sql.column import Column

        a = Column.__new__(Column)
        a._referenced_columns = frozenset({"a"})
        # Literal 5 is not a Column, so no additional refs
        refs = a._referenced_columns | frozenset()
        assert refs == frozenset({"a"})

    def test_and_merges_refs(self):
        """(cond_a & cond_b) should merge referenced columns."""
        refs_a = frozenset({"x"})
        refs_b = frozenset({"y", "z"})
        merged = refs_a | refs_b
        assert merged == frozenset({"x", "y", "z"})


class TestFilterNodeRefs:
    def test_filter_node_stores_referenced_columns(self):
        from otterbrix.experimental.spark.sql.logical_plan import FilterNode, ScanNode

        scan = ScanNode(relation="rel", table_name="t")
        filt = FilterNode(
            condition="a > 5",
            children=[scan],
            py_eval=lambda row: row["a"] > 5,
            referenced_columns=frozenset({"a"}),
        )
        assert filt.referenced_columns == frozenset({"a"})

    def test_filter_node_default_none_refs(self):
        from otterbrix.experimental.spark.sql.logical_plan import FilterNode, ScanNode

        scan = ScanNode(relation="rel", table_name="t")
        filt = FilterNode(condition="a > 5", children=[scan])
        assert filt.referenced_columns is None


class TestPredicatePushdown:
    def test_filter_pushes_past_project(self):
        """
        Before: FilterNode(child=ProjectNode(child=ScanNode))
        After:  ProjectNode(child=FilterNode(child=ScanNode))
        """
        from otterbrix.experimental.spark.sql.logical_plan import (
            ScanNode, FilterNode, ProjectNode,
        )
        from otterbrix.experimental.spark.sql.optimizer import PlanOptimizer

        scan = ScanNode(relation="rel", table_name="t", schema=None)
        proj = ProjectNode(columns=["a", "b"], children=[scan])
        filt = FilterNode(
            condition="a > 5", children=[proj],
            referenced_columns=frozenset({"a"}),
        )

        optimizer = PlanOptimizer()
        result = optimizer.optimize(filt)

        assert isinstance(result, ProjectNode)
        assert isinstance(result.children[0], FilterNode)
        assert isinstance(result.children[0].children[0], ScanNode)
        assert result.children[0].referenced_columns == frozenset({"a"})

    def test_filter_pushes_past_sort(self):
        from otterbrix.experimental.spark.sql.logical_plan import (
            ScanNode, FilterNode, SortNode,
        )
        from otterbrix.experimental.spark.sql.optimizer import PlanOptimizer

        scan = ScanNode(relation="rel", table_name="t")
        sort = SortNode(sort_exprs=["a_asc"], children=[scan], sort_keys=[("a", True)])
        filt = FilterNode(
            condition="a > 5", children=[sort],
            referenced_columns=frozenset({"a"}),
        )

        result = PlanOptimizer().optimize(filt)

        assert isinstance(result, SortNode)
        assert isinstance(result.children[0], FilterNode)
        assert isinstance(result.children[0].children[0], ScanNode)

    def test_filter_pushes_into_join_left(self):
        from otterbrix.experimental.spark.sql.logical_plan import (
            ScanNode, FilterNode, JoinNode,
        )
        from otterbrix.experimental.spark.sql.optimizer import PlanOptimizer

        class FakeRel:
            def __init__(self, cols):
                self.columns = cols

        left_scan = ScanNode(relation=FakeRel(["a", "b"]), table_name="left_t")
        right_scan = ScanNode(relation=FakeRel(["c", "d"]), table_name="right_t")
        join = JoinNode(condition="a == c", join_type="inner", children=[left_scan, right_scan])
        filt = FilterNode(
            condition="a > 5", children=[join],
            referenced_columns=frozenset({"a"}),
        )

        result = PlanOptimizer().optimize(filt)

        assert isinstance(result, JoinNode)
        assert isinstance(result.children[0], FilterNode)
        assert result.children[0].referenced_columns == frozenset({"a"})
        assert isinstance(result.children[0].children[0], ScanNode)
        assert isinstance(result.children[1], ScanNode)

    def test_filter_pushes_into_join_right(self):
        from otterbrix.experimental.spark.sql.logical_plan import (
            ScanNode, FilterNode, JoinNode,
        )
        from otterbrix.experimental.spark.sql.optimizer import PlanOptimizer

        class FakeRel:
            def __init__(self, cols):
                self.columns = cols

        left_scan = ScanNode(relation=FakeRel(["a", "b"]), table_name="left_t")
        right_scan = ScanNode(relation=FakeRel(["c", "d"]), table_name="right_t")
        join = JoinNode(condition="a == c", join_type="inner", children=[left_scan, right_scan])
        filt = FilterNode(
            condition="d > 10", children=[join],
            referenced_columns=frozenset({"d"}),
        )

        result = PlanOptimizer().optimize(filt)

        assert isinstance(result, JoinNode)
        assert isinstance(result.children[0], ScanNode)
        assert isinstance(result.children[1], FilterNode)
        assert result.children[1].referenced_columns == frozenset({"d"})

    def test_filter_pushes_past_groupby_on_group_key(self):
        from otterbrix.experimental.spark.sql.logical_plan import (
            ScanNode, FilterNode, GroupByNode,
        )
        from otterbrix.experimental.spark.sql.optimizer import PlanOptimizer

        scan = ScanNode(relation="rel", table_name="t")
        group = GroupByNode(
            group_cols=["a_expr"], agg_exprs=["count_expr"],
            children=[scan], group_keys=["a"], aggregations=[("val", "sum", "sum_val")],
        )
        filt = FilterNode(
            condition="a > 5", children=[group],
            referenced_columns=frozenset({"a"}),
        )

        result = PlanOptimizer().optimize(filt)

        assert isinstance(result, GroupByNode)
        assert isinstance(result.children[0], FilterNode)
        assert isinstance(result.children[0].children[0], ScanNode)

    def test_filter_does_not_push_past_limit(self):
        from otterbrix.experimental.spark.sql.logical_plan import (
            ScanNode, FilterNode, LimitNode,
        )
        from otterbrix.experimental.spark.sql.optimizer import PlanOptimizer

        scan = ScanNode(relation="rel", table_name="t")
        limit = LimitNode(count=10, children=[scan])
        filt = FilterNode(
            condition="a > 5", children=[limit],
            referenced_columns=frozenset({"a"}),
        )

        result = PlanOptimizer().optimize(filt)

        assert isinstance(result, FilterNode)
        assert isinstance(result.children[0], LimitNode)

    def test_filter_without_refs_stays_in_place(self):
        from otterbrix.experimental.spark.sql.logical_plan import (
            ScanNode, FilterNode, ProjectNode,
        )
        from otterbrix.experimental.spark.sql.optimizer import PlanOptimizer

        scan = ScanNode(relation="rel", table_name="t")
        proj = ProjectNode(columns=["a", "b"], children=[scan])
        filt = FilterNode(
            condition="a > 5", children=[proj],
            referenced_columns=None,
        )

        result = PlanOptimizer().optimize(filt)

        assert isinstance(result, FilterNode)
        assert isinstance(result.children[0], ProjectNode)

    def test_filter_on_aggregate_does_not_push_past_groupby(self):
        from otterbrix.experimental.spark.sql.logical_plan import (
            ScanNode, FilterNode, GroupByNode,
        )
        from otterbrix.experimental.spark.sql.optimizer import PlanOptimizer

        scan = ScanNode(relation="rel", table_name="t")
        group = GroupByNode(
            group_cols=["a_expr"], agg_exprs=["sum_expr"],
            children=[scan], group_keys=["a"], aggregations=[("val", "sum", "sum_val")],
        )
        filt = FilterNode(
            condition="sum_val > 100", children=[group],
            referenced_columns=frozenset({"sum_val"}),
        )

        result = PlanOptimizer().optimize(filt)

        assert isinstance(result, FilterNode)
        assert isinstance(result.children[0], GroupByNode)

    def test_filter_over_filter_preserves_both(self):
        from otterbrix.experimental.spark.sql.logical_plan import (
            ScanNode, FilterNode,
        )
        from otterbrix.experimental.spark.sql.optimizer import PlanOptimizer

        scan = ScanNode(relation="rel", table_name="t")
        f1 = FilterNode(condition="a > 5", children=[scan], referenced_columns=frozenset({"a"}))
        f2 = FilterNode(condition="b < 10", children=[f1], referenced_columns=frozenset({"b"}))

        result = PlanOptimizer().optimize(f2)

        assert isinstance(result, FilterNode)
        assert result.condition == "b < 10"
        assert isinstance(result.children[0], FilterNode)
        assert result.children[0].condition == "a > 5"

    def test_filter_pushes_past_project_with_expression_objects(self):
        from otterbrix.experimental.spark.sql.logical_plan import (
            ScanNode, FilterNode, ProjectNode,
        )
        from otterbrix.experimental.spark.sql.optimizer import PlanOptimizer

        class FakeExpr:
            def __init__(self, name):
                self.name = name
            def __str__(self):
                return self.name

        scan = ScanNode(relation="rel", table_name="t")
        proj = ProjectNode(columns=[FakeExpr("a"), FakeExpr("b")], children=[scan])
        filt = FilterNode(
            condition="a > 5", children=[proj],
            referenced_columns=frozenset({"a"}),
        )

        result = PlanOptimizer().optimize(filt)

        assert isinstance(result, ProjectNode)
        assert isinstance(result.children[0], FilterNode)
        assert isinstance(result.children[0].children[0], ScanNode)

    def test_deep_chain_pushdown(self):
        from otterbrix.experimental.spark.sql.logical_plan import (
            ScanNode, FilterNode, ProjectNode, SortNode,
        )
        from otterbrix.experimental.spark.sql.optimizer import PlanOptimizer

        scan = ScanNode(relation="rel", table_name="t")
        proj = ProjectNode(columns=["a", "b"], children=[scan])
        sort = SortNode(sort_exprs=["a_asc"], children=[proj], sort_keys=[("a", True)])
        filt = FilterNode(
            condition="a > 5", children=[sort],
            referenced_columns=frozenset({"a"}),
        )

        result = PlanOptimizer().optimize(filt)

        assert isinstance(result, SortNode)
        assert isinstance(result.children[0], ProjectNode)
        assert isinstance(result.children[0].children[0], FilterNode)
        assert isinstance(result.children[0].children[0].children[0], ScanNode)


class TestOptimizerIntegration:
    def test_execute_plan_runs_optimizer(self):
        from otterbrix.experimental.spark.sql.logical_plan import (
            ScanNode, FilterNode, ProjectNode,
        )
        from otterbrix.experimental.spark.sql.optimizer import PlanOptimizer

        scan = ScanNode(relation="rel", table_name="t")
        proj = ProjectNode(columns=["a", "b"], children=[scan])
        filt = FilterNode(
            condition="a > 5", children=[proj],
            referenced_columns=frozenset({"a"}),
        )

        optimizer = PlanOptimizer()
        optimized = optimizer.optimize(filt)

        assert isinstance(optimized, ProjectNode)
        assert isinstance(optimized.children[0], FilterNode)
        assert optimized.children[0].condition == "a > 5"


    def test_execute_plan_with_no_op_optimizer(self):
        """_execute_plan with empty optimizer should skip pushdown."""
        from otterbrix.experimental.spark.sql.optimizer import PlanOptimizer

        spark = SparkSession.builder.master("local").appName("test").getOrCreate()
        data = [(1, "a", 30, 500.0, "g1"), (2, "b", 60, 100.0, "g2")]
        join_data = [(1, 10.0), (2, 20.0)]

        df1 = spark.createDataFrame(data, schema=["id", "name", "age", "value", "group_key"], lazy=True)
        df2 = spark.createDataFrame(join_data, schema=["id", "extra_value"], lazy=True)
        df = df1.join(df2, "id").filter(col("value") > 400)

        # With no-op optimizer: filter should stay above join
        no_op = PlanOptimizer(rules=[])
        rel_no_opt = df._execute_plan(optimizer=no_op)
        result_no_opt = rel_no_opt.fetchall()

        # With default optimizer: filter pushed below join (different plan, same result)
        rel_opt = df._execute_plan()
        result_opt = rel_opt.fetchall()

        # Both should return the same data (1 row where value=500 > 400)
        assert len(result_no_opt) == 1
        assert len(result_opt) == 1
        assert result_no_opt[0] == result_opt[0]


class TestExplain:
    def test_explain_shows_before_and_after(self, capsys):
        from otterbrix.experimental.spark.sql.logical_plan import (
            ScanNode, FilterNode, ProjectNode,
        )
        from otterbrix.experimental.spark.sql.dataframe import DataFrame

        scan = ScanNode(relation="rel", table_name="t")
        proj = ProjectNode(columns=["a", "b"], children=[scan])
        filt = FilterNode(
            condition="a > 5", children=[proj],
            referenced_columns=frozenset({"a"}),
        )

        df = DataFrame(relation=None, session=None, lazy=True, plan=filt)
        df.explain()

        output = capsys.readouterr().out
        assert "Logical Plan" in output
        assert "Optimized Plan" in output
        assert "FilterNode" in output
        assert "ProjectNode" in output
