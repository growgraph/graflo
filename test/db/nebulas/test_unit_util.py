"""Unit tests for NebulaGraph utility functions (no Docker required)."""

from graflo.architecture.vertex import FieldType
from graflo.db.nebula.util import (
    escape_nebula_string,
    make_vid,
    nebula_type,
    render_filters_cypher,
    render_filters_ngql,
    serialize_nebula_value,
)
from graflo.filter.onto import FilterExpression
from graflo.onto import ExpressionFlavor


# ── Type mapping ─────────────────────────────────────────────────────────


def test_nebula_type_int():
    assert nebula_type(FieldType.INT) == "int64"


def test_nebula_type_string():
    assert nebula_type(FieldType.STRING) == "string"


def test_nebula_type_bool():
    assert nebula_type(FieldType.BOOL) == "bool"


def test_nebula_type_double():
    assert nebula_type(FieldType.DOUBLE) == "double"


def test_nebula_type_none_defaults_to_string():
    assert nebula_type(None) == "string"


def test_nebula_type_float():
    assert nebula_type(FieldType.FLOAT) == "float"


def test_nebula_type_datetime():
    assert nebula_type(FieldType.DATETIME) == "string"


# ── Value serialisation ──────────────────────────────────────────────────


def test_serialize_int():
    assert serialize_nebula_value(42) == "42"


def test_serialize_string():
    assert serialize_nebula_value("hello") == '"hello"'


def test_serialize_bool_true():
    assert serialize_nebula_value(True) == "true"


def test_serialize_bool_false():
    assert serialize_nebula_value(False) == "false"


def test_serialize_none():
    assert serialize_nebula_value(None) == "NULL"


def test_serialize_float():
    assert serialize_nebula_value(3.14) == "3.14"


def test_serialize_list():
    result = serialize_nebula_value([1, 2, 3])
    assert '"[1, 2, 3]"' == result


def test_serialize_dict():
    result = serialize_nebula_value({"a": 1})
    assert result.startswith('"')
    assert result.endswith('"')


# ── String escaping ──────────────────────────────────────────────────────


def test_escape_plain_string():
    assert escape_nebula_string("hello") == "hello"


def test_escape_quotes():
    assert escape_nebula_string('say "hi"') == 'say \\"hi\\"'


def test_escape_backslash():
    assert escape_nebula_string("path\\to") == "path\\\\to"


def test_escape_combined():
    assert escape_nebula_string('a\\b"c') == 'a\\\\b\\"c'


# ── VID helpers ──────────────────────────────────────────────────────────


def test_make_vid_single_key():
    assert make_vid({"name": "Alice"}, ["name"]) == "Alice"


def test_make_vid_composite():
    vid = make_vid({"a": "x", "b": "y"}, ["a", "b"])
    assert vid == "x::y"


def test_make_vid_missing_key():
    vid = make_vid({"a": "x"}, ["a", "missing"])
    assert vid == "x::"


# ── Filter rendering (nGQL) ──────────────────────────────────────────────


def test_render_filters_ngql_none():
    assert render_filters_ngql(None, "v") == ""


def test_render_filters_ngql_dict():
    result = render_filters_ngql(
        {"field": "age", "cmp_operator": ">=", "value": 18}, "v.Person"
    )
    assert "v.Person.age >= 18" in result


def test_render_filters_ngql_filter_expression():
    fe = FilterExpression.from_dict(
        {"field": "name", "cmp_operator": "==", "value": "Alice"}
    )
    result = render_filters_ngql(fe, "v.Tag")
    assert 'v.Tag.name == "Alice"' in result


# ── Filter rendering (Cypher) ────────────────────────────────────────────


def test_render_filters_cypher_none():
    assert render_filters_cypher(None, "v") == ""


def test_render_filters_cypher_dict():
    result = render_filters_cypher(
        {"field": "age", "cmp_operator": ">=", "value": 18}, "v"
    )
    assert "age" in result
    assert "18" in result


# ── FilterExpression._cast_ngql ──────────────────────────────────────────


def test_filter_ngql_equality():
    fe = FilterExpression.from_dict({"field": "age", "cmp_operator": "==", "value": 30})
    result = fe(doc_name="v.Person", kind=ExpressionFlavor.NGQL)
    assert result == "v.Person.age == 30"


def test_filter_ngql_string_value():
    fe = FilterExpression.from_dict(
        {"field": "name", "cmp_operator": "==", "value": "Alice"}
    )
    result = fe(doc_name="v.Tag", kind=ExpressionFlavor.NGQL)
    assert result == 'v.Tag.name == "Alice"'


def test_filter_ngql_is_empty():
    fe = FilterExpression.from_dict({"field": "name", "cmp_operator": "IS_NULL"})
    result = fe(doc_name="v.Tag", kind=ExpressionFlavor.NGQL)
    assert isinstance(result, str)
    assert "IS EMPTY" in result


def test_filter_ngql_composite_and():
    fe = FilterExpression.from_dict(
        {
            "AND": [
                {"field": "age", "cmp_operator": ">", "value": 20},
                {"field": "age", "cmp_operator": "<", "value": 40},
            ]
        }
    )
    result = fe(doc_name="v.P", kind=ExpressionFlavor.NGQL)
    assert isinstance(result, str)
    assert "v.P.age > 20" in result
    assert "v.P.age < 40" in result
    assert "AND" in result
