"""``ANY_IN``: a list field that shares a member with a value list, and ``conjoin``."""

from __future__ import annotations

import pytest

from graflo.filter.onto import (
    BoundParams,
    ComparisonOperator,
    FilterExpression,
    LogicalOperator,
    conjoin,
)
from graflo.onto import ExpressionFlavor


def _zones(*ids: int) -> FilterExpression:
    return FilterExpression(
        kind="leaf",
        field="zones",
        cmp_operator=ComparisonOperator.ANY_IN,
        value=[list(ids)],
    )


def test_any_in_renders_a_null_safe_list_predicate_for_cypher() -> None:
    params = BoundParams(ExpressionFlavor.CYPHER)

    rendered = _zones(1, 2)(doc_name="n", kind=ExpressionFlavor.CYPHER, params=params)

    assert rendered == "any(_any_in IN coalesce(n.zones, []) WHERE _any_in IN $f0)"
    assert params.values == {"f0": [1, 2]}


def test_any_in_without_params_writes_a_list_literal() -> None:
    rendered = _zones(7)(doc_name="r", kind=ExpressionFlavor.CYPHER)

    assert rendered == "any(_any_in IN coalesce(r.zones, []) WHERE _any_in IN [7])"


@pytest.mark.parametrize(
    ("doc", "expected"),
    [
        ({"zones": [3, 1]}, True),
        ({"zones": [3]}, False),
        ({"zones": []}, False),
        ({}, False),
        ({"zones": 1}, False),
    ],
)
def test_any_in_evaluates_in_python(doc: dict, expected: bool) -> None:
    assert _zones(1, 2).matches(doc) is expected


@pytest.mark.parametrize(
    "kind",
    [
        ExpressionFlavor.AQL,
        ExpressionFlavor.NGQL,
        ExpressionFlavor.GSQL,
        ExpressionFlavor.SQL,
    ],
)
def test_any_in_is_refused_where_it_is_not_rendered(kind: ExpressionFlavor) -> None:
    with pytest.raises(ValueError, match="ANY_IN"):
        _zones(1)(doc_name="d", kind=kind)


def test_any_in_inside_a_composite_is_refused_too() -> None:
    expr = conjoin({"field": "name", "cmp_operator": "==", "value": "a"}, _zones(1))
    assert expr is not None

    with pytest.raises(ValueError, match="ANY_IN"):
        expr(doc_name="d", kind=ExpressionFlavor.AQL)


def test_conjoin_skips_none_and_keeps_a_single_operand() -> None:
    guard = _zones(1)

    assert conjoin(None, None) is None
    assert conjoin(None, guard) is guard


def test_conjoin_ands_every_operand() -> None:
    expr = conjoin(["==", "a", "name"], _zones(1, 2))

    assert expr is not None
    assert expr.operator == LogicalOperator.AND
    assert expr.matches({"name": "a", "zones": [2]})
    assert not expr.matches({"name": "a", "zones": [5]})
    assert not expr.matches({"name": "b", "zones": [2]})


HOSTILE_FIELD = "w = 0) OR true OR all(_gr IN [] WHERE _gr.w"


def test_a_field_that_is_not_an_identifier_is_quoted_in_cypher() -> None:
    expr = FilterExpression.from_dict(["==", 0, HOSTILE_FIELD])

    rendered = expr(doc_name="r", kind=ExpressionFlavor.CYPHER)

    assert rendered == f"r.`{HOSTILE_FIELD}` = 0"
    assert (
        FilterExpression.from_dict(["==", 0, "a`b"])(
            doc_name="n", kind=ExpressionFlavor.CYPHER
        )
        == "n.`a``b` = 0"
    )


def test_an_identifier_field_renders_as_before() -> None:
    expr = FilterExpression.from_dict(["==", 0, "weight"])

    assert expr(doc_name="r", kind=ExpressionFlavor.CYPHER) == "r.weight = 0"
    assert expr(doc_name="d", kind=ExpressionFlavor.AQL) == 'd["weight"] == 0'


def test_a_field_cannot_close_an_aql_attribute_access() -> None:
    expr = FilterExpression.from_dict(["==", 0, 'x"] || true || d["y'])

    assert expr(doc_name="d", kind=ExpressionFlavor.AQL) == (
        'd["x\\"] || true || d[\\"y"] == 0'
    )


def test_gsql_refuses_a_field_it_cannot_quote() -> None:
    expr = FilterExpression.from_dict(["==", 0, HOSTILE_FIELD])

    with pytest.raises(ValueError, match="cannot be written in GSQL"):
        expr(doc_name="v", kind=ExpressionFlavor.GSQL)
