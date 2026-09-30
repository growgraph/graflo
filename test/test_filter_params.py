"""Filter values travel as bound parameters where the driver takes them."""

from __future__ import annotations

from datetime import datetime

import pytest

from graflo.filter.onto import BoundParams, FilterExpression, render_conjunct
from graflo.onto import ExpressionFlavor

HOSTILE = 'x" || true || "'


def _expr(raw) -> FilterExpression:
    return FilterExpression.from_dict(raw)


@pytest.mark.parametrize(
    ("kind", "doc_name", "expected"),
    [
        (ExpressionFlavor.AQL, "d", 'd["name"] == @f0'),
        (ExpressionFlavor.CYPHER, "n", "n.name = $f0"),
        (ExpressionFlavor.SQL, "", '"name" = %(f0)s'),
    ],
)
def test_a_value_is_bound_not_written(kind, doc_name, expected) -> None:
    params = BoundParams(kind)

    rendered = _expr(["==", HOSTILE, "name"])(
        doc_name=doc_name, kind=kind, params=params
    )

    assert rendered == expected
    assert params.values == {"f0": HOSTILE}


def test_in_binds_its_members() -> None:
    cypher = BoundParams(ExpressionFlavor.CYPHER)
    sql = BoundParams(ExpressionFlavor.SQL)
    expr = _expr(["IN", ["a", "b"], "status"])

    assert expr(doc_name="n", kind=ExpressionFlavor.CYPHER, params=cypher) == (
        "n.status IN $f0"
    )
    assert cypher.values == {"f0": ["a", "b"]}
    assert expr(kind=ExpressionFlavor.SQL, params=sql) == (
        '"status" IN (%(f0)s, %(f1)s)'
    )
    assert sql.values == {"f0": "a", "f1": "b"}


def test_names_run_on_across_a_composite_and_a_second_filter() -> None:
    params = BoundParams(ExpressionFlavor.AQL)
    expr = _expr({"OR": [["==", 1, "a"], {"AND": [["==", 2, "b"], [">", 3, "c"]]}]})

    first = render_conjunct(
        expr, kind=ExpressionFlavor.AQL, doc_name="d", params=params
    )
    second = _expr(["==", 4, "e"])(
        doc_name="d", kind=ExpressionFlavor.AQL, params=params
    )

    assert first == '(d["a"] == @f0 OR (d["b"] == @f1 AND d["c"] > @f2))'
    assert second == 'd["e"] == @f3'
    assert params.values == {"f0": 1, "f1": 2, "f2": 3, "f3": 4}


@pytest.mark.parametrize("kind", [ExpressionFlavor.NGQL, ExpressionFlavor.GSQL])
def test_a_flavor_without_parameters_refuses_them(kind) -> None:
    with pytest.raises(ValueError, match="bound parameters"):
        BoundParams(kind)


def test_a_python_style_operator_is_not_written_into_a_query() -> None:
    expr = _expr({"field": "x", "operator": "__eq__", "value": 1})

    assert expr(doc_name="d", kind=ExpressionFlavor.AQL) == 'd["x"] == 1'
    assert expr(doc_name="n", kind=ExpressionFlavor.CYPHER) == "n.x = 1"
    assert expr.matches({"x": 1})


def test_a_query_fragment_operator_still_is() -> None:
    expr = FilterExpression.from_list(["==", 2, "y", "% 2"])

    assert expr(doc_name="d", kind=ExpressionFlavor.AQL) == 'd["y"] % 2 == 2'


@pytest.mark.parametrize(
    ("value", "literal"),
    [
        (True, "true"),
        (None, "null"),
        (2.5, "2.5"),
        ('a\nb\t\\"c', '"a\\nb\\t\\\\\\"c"'),
        (datetime(2024, 1, 2, 3, 4, 5), '"2024-01-02T03:04:05"'),
    ],
)
def test_literals_are_well_formed(value, literal) -> None:
    rendered = _expr(["==", value, "x"])(doc_name="v", kind=ExpressionFlavor.NGQL)

    assert rendered == f"v.x == {literal}"


@pytest.mark.parametrize("value", ["a\x00b", "a\x1bb", float("nan"), object()])
def test_a_value_with_no_safe_literal_is_refused(value) -> None:
    with pytest.raises(ValueError, match="literal"):
        _expr(["==", value, "x"])(doc_name="v", kind=ExpressionFlavor.NGQL)


def test_a_rest_filter_value_that_would_split_the_filter_is_refused() -> None:
    expr = _expr(["==", 'a",b', "name"])

    with pytest.raises(ValueError, match="REST"):
        expr(doc_name="", kind=ExpressionFlavor.GSQL)
