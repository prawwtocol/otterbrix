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


class _GroupedRelation:
    """Wrapper that performs group-by aggregation in Python.
    Used because PyRelation group() crashes in the C++ engine."""

    def __init__(self, relation, group_keys, aggregations):
        """
        Parameters
        ----------
        relation : OtterBrixPyRelation or compatible wrapper
        group_keys : list of column name strings to group by
        aggregations : list of (input_col, agg_func, output_name) tuples
            agg_func is one of: 'sum', 'count', 'min', 'max', 'avg', 'count_all'
            input_col can be None for 'count_all'
        """
        self._relation = relation
        self._group_keys = group_keys
        self._aggregations = aggregations
        src_columns = relation.columns
        src_types = relation.types
        self._col_type_map = dict(zip(src_columns, src_types))

    def fetchall(self):
        data = self._relation.fetchall()
        if not data:
            return data
        columns = self._relation.columns
        col_idx = {col: i for i, col in enumerate(columns)}

        groups = {}
        order = []
        for row in data:
            key = tuple(row[col_idx[k]] for k in self._group_keys) if self._group_keys else ()
            if key not in groups:
                groups[key] = []
                order.append(key)
            groups[key].append(row)

        result = []
        for key in order:
            rows = groups[key]
            output_row = list(key)
            for input_col, agg_func, _ in self._aggregations:
                if agg_func == 'count_all':
                    output_row.append(len(rows))
                else:
                    values = [row[col_idx[input_col]] for row in rows]
                    if agg_func == 'sum':
                        output_row.append(sum(values))
                    elif agg_func == 'count':
                        output_row.append(len(values))
                    elif agg_func == 'min':
                        output_row.append(min(values))
                    elif agg_func == 'max':
                        output_row.append(max(values))
                    elif agg_func == 'avg':
                        output_row.append(float(sum(values)) / len(values))
            result.append(tuple(output_row))
        return result

    def fetchone(self):
        data = self.fetchall()
        return data[0] if data else None

    @property
    def columns(self):
        cols = list(self._group_keys)
        for _, _, output_name in self._aggregations:
            cols.append(output_name)
        return cols

    @property
    def types(self):
        result_types = []
        for key in self._group_keys:
            result_types.append(self._col_type_map[key])
        for input_col, agg_func, _ in self._aggregations:
            if agg_func in ('sum', 'min', 'max'):
                result_types.append(self._col_type_map[input_col])
            elif agg_func in ('count', 'count_all'):
                result_types.append(_MockType('bigint'))
            elif agg_func == 'avg':
                result_types.append(_MockType('float'))
        return result_types


class _SortedRelation:
    """Wrapper that sorts rows returned from a relation in Python.
    Used because PyRelation sort() binding does not apply sorting."""

    def __init__(self, relation, sort_keys):
        """
        Parameters
        ----------
        relation : OtterBrixPyRelation or compatible wrapper
        sort_keys : list of (column_name: str, ascending: bool)
        """
        self._relation = relation
        self._sort_keys = sort_keys

    def fetchall(self):
        data = self._relation.fetchall()
        if not data or not self._sort_keys:
            return data
        columns = self._relation.columns
        col_indices = []
        for col_name, _ in self._sort_keys:
            try:
                col_indices.append(columns.index(col_name))
            except ValueError:
                raise ValueError(f"Sort column '{col_name}' not found in {columns}")

        # Build a sort key: iterate sort keys in reverse priority
        # (Python's sort is stable, so we sort by least significant key first)
        result = list(data)
        for (col_name, ascending), idx in reversed(list(zip(self._sort_keys, col_indices))):
            # Partition into non-null and null rows so we never compare None with real values
            non_null = [(r, j) for j, r in enumerate(result) if r[idx] is not None]
            null_rows = [r for r in result if r[idx] is None]
            non_null.sort(key=lambda pair, i=idx: pair[0][i], reverse=not ascending)
            result = [r for r, _ in non_null] + null_rows
        return result

    def fetchone(self):
        return self._relation.fetchone()

    @property
    def columns(self):
        return self._relation.columns

    @property
    def types(self):
        return self._relation.types


class _FilteredRelation:
    """Wrapper that filters rows in Python using a predicate.
    Used because PyRelation filter() crashes in the C++ engine."""

    def __init__(self, relation, py_predicate):
        """
        Parameters
        ----------
        relation : OtterBrixPyRelation or compatible wrapper
        py_predicate : callable(row_dict) -> bool
        """
        self._relation = relation
        self._predicate = py_predicate

    def fetchall(self):
        data = self._relation.fetchall()
        if not data:
            return data
        columns = self._relation.columns
        result = []
        for row in data:
            row_dict = {col: row[i] for i, col in enumerate(columns)}
            if self._predicate(row_dict):
                result.append(row)
        return result

    def fetchone(self):
        return self._relation.fetchone()

    @property
    def columns(self):
        return self._relation.columns

    @property
    def types(self):
        return self._relation.types


class _ProjectedRelation:
    """Wrapper that projects (selects) specific columns from a relation in Python."""

    def __init__(self, relation, columns):
        self._relation = relation
        # columns may be C++ Expression objects or strings; normalize to strings
        self._columns = [str(c) for c in columns]

    def fetchall(self):
        data = self._relation.fetchall()
        if not data:
            return data
        src_columns = self._relation.columns
        indices = [src_columns.index(c) for c in self._columns]
        return [tuple(row[i] for i in indices) for row in data]

    def fetchone(self):
        row = self._relation.fetchone()
        if row is None:
            return None
        src_columns = self._relation.columns
        indices = [src_columns.index(c) for c in self._columns]
        return tuple(row[i] for i in indices)

    @property
    def columns(self):
        return list(self._columns)

    @property
    def types(self):
        src_columns = self._relation.columns
        src_types = self._relation.types
        col_type_map = dict(zip(src_columns, src_types))
        return [col_type_map[c] for c in self._columns]


class _JoinedRelation:
    """Wrapper that performs join operations in Python.
    Used because PyRelation join() crashes in the C++ engine."""

    def __init__(self, left_relation, right_relation, condition, join_type="inner"):
        """
        Parameters
        ----------
        left_relation : OtterBrixPyRelation or compatible wrapper
        right_relation : OtterBrixPyRelation or compatible wrapper
        condition : callable(row_dict) -> bool, or None for cross join
        join_type : str, one of 'inner', 'full', 'left', 'right', 'semi', 'anti', 'cross'
        """
        self._left = left_relation
        self._right = right_relation
        self._condition = condition
        self._join_type = join_type

    def _equi_join_keys(self):
        """Return list of column name strings if this is an equi-join, else None."""
        cond = self._condition
        if isinstance(cond, list) and all(isinstance(c, str) for c in cond):
            return cond
        return None

    def _match(self, lrow, rrow, left_cols, right_cols):
        """Check if a (left_row, right_row) pair satisfies the join condition."""
        cond = self._condition
        if cond is None:
            return True
        if isinstance(cond, list):
            # Equi-join on column names: compare values by index before merging
            for c in cond:
                if isinstance(c, str):
                    lv = lrow[left_cols.index(c)]
                    rv = rrow[right_cols.index(c)]
                    if lv != rv:
                        return False
                else:
                    # Column expression with _py_eval
                    merged = {col: lrow[i] for i, col in enumerate(left_cols)}
                    merged.update({col: rrow[i] for i, col in enumerate(right_cols)})
                    if not c._py_eval(merged):
                        return False
            return True
        # callable condition
        merged = {col: lrow[i] for i, col in enumerate(left_cols)}
        merged.update({col: rrow[i] for i, col in enumerate(right_cols)})
        return cond(merged)

    def _hash_join(self, left_data, right_data, left_cols, right_cols, keys):
        """O(n+m) hash join for equi-join on column name keys.

        Build phase: hash the right relation.
        Probe phase: scan the left relation.
        NULL keys never match (SQL semantics).
        """
        jtype = self._join_type

        def null_row(cols):
            return tuple(None for _ in cols)

        left_key_idx = [left_cols.index(k) for k in keys]
        right_key_idx = [right_cols.index(k) for k in keys]

        def make_key(row, idx):
            return tuple(row[i] for i in idx)

        def has_null(key):
            return any(v is None for v in key)

        # Build hash table from right relation: key -> list of (index, row)
        right_map = {}
        for ri, rrow in enumerate(right_data):
            k = make_key(rrow, right_key_idx)
            if has_null(k):
                continue  # NULL never matches
            right_map.setdefault(k, []).append((ri, rrow))

        result = []

        if jtype in ("inner", "left", "right", "full"):
            right_matched = set()

            for lrow in left_data:
                k = make_key(lrow, left_key_idx)
                if has_null(k):
                    if jtype in ("left", "full"):
                        result.append(lrow + null_row(right_cols))
                    continue
                matches = right_map.get(k, [])
                if matches:
                    for ri, rrow in matches:
                        result.append(lrow + rrow)
                        right_matched.add(ri)
                elif jtype in ("left", "full"):
                    result.append(lrow + null_row(right_cols))

            if jtype in ("right", "full"):
                lnull = null_row(left_cols)
                for ri, rrow in enumerate(right_data):
                    if ri not in right_matched:
                        result.append(lnull + rrow)

            return result

        if jtype == "semi":
            for lrow in left_data:
                k = make_key(lrow, left_key_idx)
                if not has_null(k) and k in right_map:
                    result.append(lrow)
            return result

        if jtype == "anti":
            for lrow in left_data:
                k = make_key(lrow, left_key_idx)
                if has_null(k) or k not in right_map:
                    result.append(lrow)
            return result

        return result

    def _nested_loop_join(self, left_data, right_data, left_cols, right_cols):
        """O(n*m) nested loop join for complex conditions (Column expressions, callables)."""
        def null_row(cols):
            return tuple(None for _ in cols)

        result = []
        jtype = self._join_type

        if jtype == 'cross':
            for lrow in left_data:
                for rrow in right_data:
                    result.append(lrow + rrow)
            return result

        if jtype in ('inner', 'full', 'left', 'right'):
            left_matched = set()
            right_matched = set()

            for li, lrow in enumerate(left_data):
                for ri, rrow in enumerate(right_data):
                    if self._match(lrow, rrow, left_cols, right_cols):
                        result.append(lrow + rrow)
                        left_matched.add(li)
                        right_matched.add(ri)
            if jtype in ('full', 'left'):
                rnull = null_row(right_cols)
                for li, lrow in enumerate(left_data):
                    if li not in left_matched:
                        result.append(lrow + rnull)
            if jtype in ('full', 'right'):
                lnull = null_row(left_cols)
                for ri, rrow in enumerate(right_data):
                    if ri not in right_matched:
                        result.append(lnull + rrow)
            return result

        if jtype == 'semi':
            for lrow in left_data:
                for rrow in right_data:
                    if self._match(lrow, rrow, left_cols, right_cols):
                        result.append(lrow)
                        break
            return result

        if jtype == 'anti':
            for lrow in left_data:
                matched = False
                for rrow in right_data:
                    if self._match(lrow, rrow, left_cols, right_cols):
                        matched = True
                        break
                if not matched:
                    result.append(lrow)
            return result

        raise ValueError(f"Unsupported join type: {jtype}")

    def fetchall(self):
        left_data = self._left.fetchall()
        right_data = self._right.fetchall()
        left_cols = self._left.columns
        right_cols = self._right.columns

        # Use O(n+m) hash join for equi-joins on column names
        keys = self._equi_join_keys()
        if keys is not None and self._join_type != "cross":
            return self._hash_join(left_data, right_data, left_cols, right_cols, keys)

        # Fallback: O(n*m) nested loop join for complex conditions
        return self._nested_loop_join(left_data, right_data, left_cols, right_cols)

    def fetchone(self):
        data = self.fetchall()
        return data[0] if data else None

    @property
    def columns(self):
        if self._join_type in ('semi', 'anti'):
            return self._left.columns
        return self._left.columns + self._right.columns

    @property
    def types(self):
        if self._join_type in ('semi', 'anti'):
            return self._left.types
        return self._left.types + self._right.types


class _LimitedRelation:
    """Wrapper that limits rows returned from a relation.
    Used because PyRelation has no C++ limit() binding."""

    def __init__(self, relation, count):
        self._relation = relation
        self._count = count

    def fetchall(self):
        return self._relation.fetchall()[:self._count]

    def fetchone(self):
        return self._relation.fetchone()

    @property
    def columns(self):
        return self._relation.columns

    @property
    def types(self):
        return self._relation.types