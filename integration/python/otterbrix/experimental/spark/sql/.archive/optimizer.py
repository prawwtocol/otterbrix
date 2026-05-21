# otterbrix/integration/python/otterbrix/experimental/spark/sql/optimizer.py

from .logical_plan import (
    LogicalNode, ScanNode, FilterNode, ProjectNode,
    JoinNode, GroupByNode, SortNode, LimitNode,
)


def _replace_children(node, new_children):
    """Create a shallow copy of node with different children."""
    if isinstance(node, ScanNode):
        return node  # leaf, no children

    if isinstance(node, FilterNode):
        return FilterNode(
            condition=node.condition,
            children=new_children,
            py_eval=node.py_eval,
            referenced_columns=node.referenced_columns,
        )

    if isinstance(node, ProjectNode):
        return ProjectNode(columns=node.columns, children=new_children)

    if isinstance(node, JoinNode):
        return JoinNode(
            condition=node.condition,
            join_type=node.join_type,
            children=new_children,
        )

    if isinstance(node, GroupByNode):
        return GroupByNode(
            group_cols=node.group_cols,
            agg_exprs=node.agg_exprs,
            children=new_children,
            group_keys=node.group_keys,
            aggregations=node.aggregations,
        )

    if isinstance(node, SortNode):
        return SortNode(
            sort_exprs=node.sort_exprs,
            children=new_children,
            sort_keys=node.sort_keys,
        )

    if isinstance(node, LimitNode):
        return LimitNode(count=node.count, children=new_children)

    return node


def _column_name(col_expr):
    """Extract a plain column name from a string or Expression object.

    C++ ColumnExpression objects use str() which may return the plain name.
    We also check for a .name attribute as a fallback.
    """
    if isinstance(col_expr, str):
        return col_expr
    if hasattr(col_expr, 'name'):
        return col_expr.name
    return str(col_expr)


def _get_output_columns(node):
    """Return the set of column names a node outputs, or None if unknown."""
    if isinstance(node, ProjectNode):
        return {_column_name(c) for c in node.columns}
    if isinstance(node, ScanNode):
        rel = node.relation
        if hasattr(rel, 'columns'):
            return set(rel.columns)
    # For other nodes we don't restrict — assume all columns pass through
    return None


def _pushdown_filter(node):
    """Recursively push FilterNodes closer to data sources."""
    if isinstance(node, ScanNode):
        return node

    # First, recurse into children
    new_children = [_pushdown_filter(c) for c in node.children]
    node = _replace_children(node, new_children)

    if not isinstance(node, FilterNode):
        return node

    child = node.children[0]
    refs = node.referenced_columns

    # Rule 1: Filter over Project -> push filter below project
    if isinstance(child, ProjectNode) and refs is not None:
        proj_cols = {_column_name(c) for c in child.columns}
        if refs <= proj_cols:
            # Safe to push: filter only uses columns that project outputs
            new_filter = _replace_children(node, list(child.children))
            new_proj = _replace_children(child, [new_filter])
            return _pushdown_filter(new_proj)

    # Rule 2: Filter over Sort -> push filter below sort (always safe semantically)
    if isinstance(child, SortNode) and refs is not None:
        new_filter = _replace_children(node, list(child.children))
        new_sort = _replace_children(child, [new_filter])
        return _pushdown_filter(new_sort)

    # Rule 3: Filter over Join -> push into appropriate side
    if isinstance(child, JoinNode) and refs is not None and len(child.children) == 2:
        left_child = child.children[0]
        right_child = child.children[1]
        left_cols = _get_output_columns(left_child)
        right_cols = _get_output_columns(right_child)

        if left_cols is not None and refs <= left_cols:
            new_filter = _replace_children(node, [left_child])
            new_join = _replace_children(child, [new_filter, right_child])
            return _pushdown_filter(new_join)

        if right_cols is not None and refs <= right_cols:
            new_filter = _replace_children(node, [right_child])
            new_join = _replace_children(child, [left_child, new_filter])
            return _pushdown_filter(new_join)

    # Rule 4: Filter over GroupBy -> push below if refs are all group keys
    if isinstance(child, GroupByNode) and refs is not None:
        group_key_names = set(child.group_keys)
        if refs <= group_key_names:
            new_filter = _replace_children(node, list(child.children))
            new_group = _replace_children(child, [new_filter])
            return _pushdown_filter(new_group)

    # No rule applies — return as is
    return node


class PlanOptimizer:
    """Logical plan optimizer with a pipeline of transformation rules."""

    def __init__(self, rules=None):
        if rules is None:
            rules = [_pushdown_filter]
        self._rules = rules

    def optimize(self, plan):
        """Apply all optimization rules to the plan tree."""
        for rule in self._rules:
            plan = rule(plan)
        return plan
