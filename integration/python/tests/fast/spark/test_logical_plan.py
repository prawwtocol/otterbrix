class TestLogicalNodes:
    def test_logical_node_base(self):
        from otterbrix.experimental.spark.sql.logical_plan import LogicalNode

        node = LogicalNode()
        assert node.children == ()
        assert repr(node) == "LogicalNode"

    def test_scan_node(self):
        from otterbrix.experimental.spark.sql.logical_plan import ScanNode

        node = ScanNode(relation="fake_rel", table_name="users", schema=None)
        assert node.relation == "fake_rel"
        assert node.table_name == "users"
        assert node.schema is None
        assert node.children == ()
        assert repr(node) == "ScanNode(table=users)"

    def test_scan_node_no_table_name(self):
        from otterbrix.experimental.spark.sql.logical_plan import ScanNode

        node = ScanNode(relation="fake_rel")
        assert node.table_name is None

    def test_node_equality(self):
        from otterbrix.experimental.spark.sql.logical_plan import ScanNode

        a = ScanNode(relation="rel", table_name="t")
        b = ScanNode(relation="rel", table_name="t")
        assert a == b

    def test_node_inequality(self):
        from otterbrix.experimental.spark.sql.logical_plan import ScanNode

        a = ScanNode(relation="rel1", table_name="t")
        b = ScanNode(relation="rel2", table_name="t")
        assert a != b

    def test_filter_node(self):
        from otterbrix.experimental.spark.sql.logical_plan import FilterNode, ScanNode

        scan = ScanNode(relation="rel", table_name="users")
        filt = FilterNode(condition="age > 18", children=[scan])
        assert filt.condition == "age > 18"
        assert filt.children == (scan,)
        assert repr(filt) == "FilterNode(condition=age > 18)"

    def test_project_node(self):
        from otterbrix.experimental.spark.sql.logical_plan import ProjectNode, ScanNode

        scan = ScanNode(relation="rel", table_name="users")
        proj = ProjectNode(columns=["name", "age"], children=[scan])
        assert proj.columns == ["name", "age"]
        assert proj.children == (scan,)
        assert repr(proj) == "ProjectNode(columns=['name', 'age'])"

    def test_filter_project_chain(self):
        from otterbrix.experimental.spark.sql.logical_plan import (
            FilterNode, ProjectNode, ScanNode
        )

        scan = ScanNode(relation="rel", table_name="users")
        filt = FilterNode(condition="age > 18", children=[scan])
        proj = ProjectNode(columns=["name"], children=[filt])
        assert proj.children[0] is filt
        assert proj.children[0].children[0] is scan

    def test_join_node(self):
        from otterbrix.experimental.spark.sql.logical_plan import JoinNode, ScanNode

        left = ScanNode(relation="rel1", table_name="users")
        right = ScanNode(relation="rel2", table_name="orders")
        join = JoinNode(condition="id_expr", join_type="inner", children=[left, right])
        assert join.condition == "id_expr"
        assert join.join_type == "inner"
        assert len(join.children) == 2
        assert repr(join) == "JoinNode(type=inner)"

    def test_group_by_node(self):
        from otterbrix.experimental.spark.sql.logical_plan import GroupByNode, ScanNode

        scan = ScanNode(relation="rel", table_name="users")
        group = GroupByNode(
            group_cols=["name_expr"], agg_exprs=["count_expr"], children=[scan]
        )
        assert group.group_cols == ["name_expr"]
        assert group.agg_exprs == ["count_expr"]
        assert repr(group) == "GroupByNode(groups=['name_expr'], aggs=['count_expr'])"

    def test_sort_node(self):
        from otterbrix.experimental.spark.sql.logical_plan import SortNode, ScanNode

        scan = ScanNode(relation="rel", table_name="users")
        sort = SortNode(sort_exprs=["age_desc"], children=[scan])
        assert sort.sort_exprs == ["age_desc"]
        assert repr(sort) == "SortNode(exprs=['age_desc'])"

    def test_limit_node(self):
        from otterbrix.experimental.spark.sql.logical_plan import LimitNode, ScanNode

        scan = ScanNode(relation="rel", table_name="users")
        limit = LimitNode(count=10, children=[scan])
        assert limit.count == 10
        assert repr(limit) == "LimitNode(count=10)"

    def test_complex_tree(self):
        from otterbrix.experimental.spark.sql.logical_plan import (
            ScanNode, FilterNode, ProjectNode, SortNode, LimitNode
        )

        # df.filter(cond).select(cols).sort(exprs).limit(10)
        scan = ScanNode(relation="rel", table_name="t")
        filt = FilterNode(condition="cond", children=[scan])
        proj = ProjectNode(columns=["a", "b"], children=[filt])
        sort = SortNode(sort_exprs=["a_asc"], children=[proj])
        limit = LimitNode(count=10, children=[sort])

        # Verify full tree traversal
        assert isinstance(limit.children[0], SortNode)
        assert isinstance(limit.children[0].children[0], ProjectNode)
        assert isinstance(limit.children[0].children[0].children[0], FilterNode)
        assert isinstance(limit.children[0].children[0].children[0].children[0], ScanNode)


class TestLimitedRelation:
    def test_fetchall_truncates(self):
        from otterbrix.experimental.spark.sql.logical_plan import _LimitedRelation

        class FakeRelation:
            columns = ["a", "b"]
            types = ["int", "str"]

            def fetchall(self):
                return [(1, "x"), (2, "y"), (3, "z"), (4, "w")]

        rel = _LimitedRelation(FakeRelation(), 2)
        result = rel.fetchall()
        assert result == [(1, "x"), (2, "y")]

    def test_columns_and_types_delegated(self):
        from otterbrix.experimental.spark.sql.logical_plan import _LimitedRelation

        class FakeRelation:
            columns = ["id", "name"]
            types = ["int", "str"]

            def fetchall(self):
                return []

        rel = _LimitedRelation(FakeRelation(), 5)
        assert rel.columns == ["id", "name"]
        assert rel.types == ["int", "str"]

    def test_limit_zero(self):
        from otterbrix.experimental.spark.sql.logical_plan import _LimitedRelation

        class FakeRelation:
            columns = ["a"]
            types = ["int"]

            def fetchall(self):
                return [(1,), (2,)]

        rel = _LimitedRelation(FakeRelation(), 0)
        assert rel.fetchall() == []