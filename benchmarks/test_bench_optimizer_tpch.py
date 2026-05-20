import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(__file__))
from bench_optimizer_tpch import load_tpch_table


def test_skips_header_and_casts_types(tmp_path):
    tbl = tmp_path / "t.tbl"
    tbl.write_text("colA|colB|colC\n1|2.5|hello|\n3|4.0|world|\n")
    columns = [("a", int), ("b", float), ("c", str)]
    rows = load_tpch_table(str(tbl), columns)
    assert rows == [(1, 2.5, "hello"), (3, 4.0, "world")]


def test_respects_limit(tmp_path):
    tbl = tmp_path / "t.tbl"
    tbl.write_text("h1|h2\n10|x|\n20|y|\n30|z|\n")
    columns = [("n", int), ("s", str)]
    rows = load_tpch_table(str(tbl), columns, limit=2)
    assert rows == [(10, "x"), (20, "y")]


def test_raises_on_column_count_mismatch(tmp_path):
    tbl = tmp_path / "t.tbl"
    tbl.write_text("h1|h2\n1|2|3|\n")
    columns = [("n", int), ("s", str)]
    with pytest.raises(ValueError):
        load_tpch_table(str(tbl), columns)
