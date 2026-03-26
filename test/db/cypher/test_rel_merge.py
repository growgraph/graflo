"""Tests for graflo.db.cypher rel-merge and escape helpers."""

from __future__ import annotations

import pytest

from graflo.db.cypher import (
    cypher_map_key,
    cypher_string_literal,
    rel_merge_props_map_from_row_index,
    rel_merge_props_map_from_row_props,
)


def test_cypher_string_literal_escapes() -> None:
    assert cypher_string_literal("a") == "'a'"
    assert cypher_string_literal("a'b") == "'a\\'b'"
    assert cypher_string_literal(r"a\b") == r"'a\\b'"


def test_cypher_map_key_strips_backticks() -> None:
    assert cypher_map_key("date") == "`date`"
    assert cypher_map_key("weird`x") == "`weirdx`"


def test_cypher_map_key_empty_raises() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        cypher_map_key("")
    with pytest.raises(ValueError, match="non-empty"):
        cypher_map_key("   ")


def test_rel_merge_from_row_index() -> None:
    s = rel_merge_props_map_from_row_index(("date", "relation"))
    assert "`date`: row[2]['date']" in s
    assert "`relation`: row[2]['relation']" in s


def test_rel_merge_from_row_index_custom_row() -> None:
    s = rel_merge_props_map_from_row_index(("k",), row_index=0)
    assert s == "`k`: row[0]['k']"


def test_rel_merge_from_row_index_dedupes() -> None:
    s = rel_merge_props_map_from_row_index(("a", "a", "b"))
    assert s == "`a`: row[2]['a'], `b`: row[2]['b']"


def test_rel_merge_from_row_props() -> None:
    s = rel_merge_props_map_from_row_props(("kind",))
    assert s == "`kind`: row.props['kind']"


def test_rel_merge_from_row_props_custom_expr() -> None:
    s = rel_merge_props_map_from_row_props(("x",), props_expr="edge")
    assert s == "`x`: edge['x']"
