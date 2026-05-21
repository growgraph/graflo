"""Bindings YAML filter parsing and SQL pushdown for logical operators."""

from __future__ import annotations

import pytest
import yaml

from graflo.architecture.contract.bindings import BindingsConfig, TableConnector
from graflo.filter.onto import (
    ComparisonOperator,
    FilterExpression,
    LogicalOperator,
    parse_filter_expression,
)
from graflo.filter.select import SelectSpec
from graflo.onto import ExpressionFlavor


def _bindings_yaml(connector_body: str) -> BindingsConfig:
    data = yaml.safe_load(
        f"""
connectors:
  - name: t1
    {connector_body}
resource_connector:
  - resource: r1
    connector: t1
"""
    )
    return BindingsConfig.model_validate(data)


def _table_connector(bindings: BindingsConfig) -> TableConnector:
    connector = bindings.connectors[0]
    assert isinstance(connector, TableConnector)
    return connector


class TestParseFilterExpression:
    def test_shorthand_or_dict(self) -> None:
        expr = parse_filter_expression(
            {
                "OR": [
                    {"field": "a", "cmp_operator": "==", "value": [1]},
                    {"field": "b", "cmp_operator": "==", "value": [2]},
                ]
            }
        )
        assert expr.kind == "composite"
        assert expr.operator == LogicalOperator.OR

    def test_operator_deps_and(self) -> None:
        inner = {"field": "a", "cmp_operator": ComparisonOperator.EQ, "value": [1]}
        expr = parse_filter_expression(
            {"operator": "AND", "deps": [inner, dict(inner)]}
        )
        assert expr.kind == "composite"
        assert expr.operator == LogicalOperator.AND


class TestBindingsYamlFilters:
    def test_shorthand_or_loads_and_builds_sql(self) -> None:
        bindings = _bindings_yaml(
            """
    table_name: events
    filters:
      - OR:
          - field: a
            cmp_operator: "=="
            value: [1]
          - field: b
            cmp_operator: "=="
            value: [2]
"""
        )
        q = _table_connector(bindings).build_query("public")
        assert " OR " in q
        assert '"a" = 1' in q
        assert '"b" = 2' in q

    def test_operator_deps_or_regression(self) -> None:
        bindings = _bindings_yaml(
            """
    table_name: events
    filters:
      - operator: OR
        deps:
          - field: a
            cmp_operator: "=="
            value: [1]
          - field: b
            cmp_operator: "=="
            value: [2]
"""
        )
        q = _table_connector(bindings).build_query("public")
        assert " OR " in q

    def test_invalid_shorthand_fails_at_bindings_load(self) -> None:
        with pytest.raises(ValueError, match="filters"):
            _bindings_yaml(
                """
    table_name: events
    filters:
      - OR: not-a-list
"""
            )

    def test_if_then_sql_implication(self) -> None:
        bindings = _bindings_yaml(
            """
    table_name: events
    filters:
      - IF_THEN:
          - field: name
            cmp_operator: "=="
            value: [Open]
          - field: value
            cmp_operator: ">"
            value: [0]
"""
        )
        q = _table_connector(bindings).build_query("public")
        assert "IF_THEN" not in q
        assert "(NOT (" in q
        assert " OR (" in q

    def test_not_sql(self) -> None:
        bindings = _bindings_yaml(
            """
    table_name: events
    filters:
      - NOT:
          - field: x
            cmp_operator: IS_NULL
"""
        )
        q = _table_connector(bindings).build_query("public")
        assert "NOT " in q
        assert '"x" IS NULL' in q

    def test_multiple_filters_implicit_and(self) -> None:
        bindings = _bindings_yaml(
            """
    table_name: events
    filters:
      - field: a
        cmp_operator: "=="
        value: [1]
      - field: b
        cmp_operator: "=="
        value: [2]
"""
        )
        q = _table_connector(bindings).build_query("public")
        assert '"a" = 1' in q
        assert '"b" = 2' in q
        assert " AND " in q


class TestFilterExpressionSqlRendering:
    def test_nested_or_and_parentheses(self) -> None:
        a = FilterExpression(
            kind="leaf", field="a", cmp_operator=ComparisonOperator.EQ, value=[1]
        )
        b = FilterExpression(
            kind="leaf", field="b", cmp_operator=ComparisonOperator.EQ, value=[2]
        )
        c = FilterExpression(
            kind="leaf", field="c", cmp_operator=ComparisonOperator.EQ, value=[3]
        )
        or_ab = FilterExpression(
            kind="composite", operator=LogicalOperator.OR, deps=[a, b]
        )
        expr = FilterExpression(
            kind="composite", operator=LogicalOperator.AND, deps=[or_ab, c]
        )
        sql = expr(kind=ExpressionFlavor.SQL)
        assert sql == '("a" = 1 OR "b" = 2) AND "c" = 3'

    def test_if_then_requires_two_deps(self) -> None:
        expr = FilterExpression(
            kind="composite",
            operator=LogicalOperator.IMPLICATION,
            deps=[
                FilterExpression(
                    kind="leaf",
                    field="a",
                    cmp_operator=ComparisonOperator.EQ,
                    value=[1],
                )
            ],
        )
        with pytest.raises(ValueError, match="exactly 2 deps"):
            expr(kind=ExpressionFlavor.SQL)


class TestSelectSpecWhereAfterParserUnify:
    def test_view_where_shorthand_or(self) -> None:
        view = SelectSpec.from_dict(
            {
                "kind": "select",
                "where": {
                    "OR": [
                        {"field": "a", "cmp_operator": "==", "value": [1]},
                        {"field": "b", "cmp_operator": "==", "value": [2]},
                    ]
                },
            }
        )
        tp = TableConnector(table_name="t", view=view)
        q = tp.build_query("public")
        assert " OR " in q
