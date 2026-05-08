class LogicalPlan:
    pass

class LogicalNode(LogicalPlan):
    """
    This is a base logical plan node.
    """

    def __init__(self, children: list["LogicalNode"]|None = None):
        self.children = tuple(children) if children else ()

    def __eq__(self, other: object) -> bool:
        if type(self) != type(other):
            return False
        # compare complete attributes -- field names and values
        return self.__dict__ == other.__dict__

    def __repr__(self) -> str:
        # "LogicalNode"
        return self.__class__.__name__


class ScanNode(LogicalNode):
    """
    Leaf node.

    Represents a scan of a data source
    """
    def __init__(self, relation: str, table_name: str=None, schema=None):
        super().__init__()
        self.relation = relation
        self.table_name = table_name
        self.schema = schema

    def __repr__(self) -> str:
        return f"ScanNode(table={self.table_name})"


class FilterNode(LogicalNode):
    """Node representing a filter/where operation."""

    def __init__(self, condition, children=None, py_eval=None, referenced_columns=None):
        super().__init__(children)
        self.condition = condition
        self.py_eval = py_eval
        self.referenced_columns = referenced_columns

    def __repr__(self):
        return f"FilterNode(condition={self.condition})"


class ProjectNode(LogicalNode):
    """Node representing a select/projection operation."""

    def __init__(self, columns, children=None):
        super().__init__(children)
        self.columns = columns

    def __repr__(self):
        return f"ProjectNode(columns={self.columns})"


class JoinNode(LogicalNode):
    """Node representing a join operation. Children: [left, right]."""

    def __init__(self, condition, join_type="inner", children=None):
        super().__init__(children)
        self.condition = condition
        self.join_type = join_type

    def __repr__(self):
        return f"JoinNode(type={self.join_type})"


class GroupByNode(LogicalNode):
    """Node representing a groupBy + aggregation operation."""

    def __init__(self, group_cols, agg_exprs, children=None,
                 group_keys=None, aggregations=None):
        super().__init__(children)
        self.group_cols = group_cols
        self.agg_exprs = agg_exprs
        self.group_keys = group_keys or []
        self.aggregations = aggregations or []

    def __repr__(self):
        return f"GroupByNode(groups={self.group_cols}, aggs={self.agg_exprs})"


class SortNode(LogicalNode):
    """Node representing a sort/orderBy operation."""

    def __init__(self, sort_exprs, children=None, sort_keys=None):
        super().__init__(children)
        self.sort_exprs = sort_exprs
        self.sort_keys = sort_keys

    def __repr__(self):
        return f"SortNode(exprs={self.sort_exprs})"


class LimitNode(LogicalNode):
    """Node representing a limit operation."""

    def __init__(self, count, children=None):
        super().__init__(children)
        self.count = count

    def __repr__(self):
        return f"LimitNode(count={self.count})"


class _MockType:
    """Lightweight stand-in for OtterBrixPyType used by Python-side relations."""
    def __init__(self, type_id):
        self.id = type_id


# ---------------------------------------------------------------------------
# DEPRECATED wrapper classes.
# All DataFrame operations now route through the C++ engine (PyRelation API).
# These classes are kept with RuntimeError to catch any stale references.
# ---------------------------------------------------------------------------

class _DEPRECATED_GroupedRelation:
    """DEPRECATED: Use PyRelation.group() C++ path instead."""
    def __init__(self, *args, **kwargs):
        raise RuntimeError("_GroupedRelation is deprecated: use PyRelation.group() C++ path")

class _DEPRECATED_SortedRelation:
    """DEPRECATED: Use PyRelation.sort() C++ path instead."""
    def __init__(self, *args, **kwargs):
        raise RuntimeError("_SortedRelation is deprecated: use PyRelation.sort() C++ path")

class _DEPRECATED_FilteredRelation:
    """DEPRECATED: Use PyRelation.filter() C++ path instead."""
    def __init__(self, *args, **kwargs):
        raise RuntimeError("_FilteredRelation is deprecated: use PyRelation.filter() C++ path")

class _DEPRECATED_ProjectedRelation:
    """DEPRECATED: Use PyRelation.select() C++ path instead."""
    def __init__(self, *args, **kwargs):
        raise RuntimeError("_ProjectedRelation is deprecated: use PyRelation.select() C++ path")

class _DEPRECATED_JoinedRelation:
    """DEPRECATED: Use PyRelation.join() C++ path instead."""
    def __init__(self, *args, **kwargs):
        raise RuntimeError("_JoinedRelation is deprecated: use PyRelation.join() C++ path")

class _LimitedRelation:
    """Wraps a relation and truncates fetchall() results to at most `count` rows."""
    def __init__(self, relation, count):
        self._relation = relation
        self._count = count

    @property
    def columns(self):
        return self._relation.columns

    @property
    def types(self):
        return self._relation.types

    def fetchall(self):
        return self._relation.fetchall()[:self._count]
