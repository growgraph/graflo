"""Conditions joined into one WHERE keep their meaning; IN renders every member.

Every target language binds AND tighter than OR, so an OR condition placed
beside another condition has to be parenthesised. These tests assert whole
clauses: a substring such as ``"status" = 'active'`` is also contained in the
broken ``base."status" = 'active'``.
"""

from __future__ import annotations

import pytest

from graflo.architecture.contract.bindings import (
    BindingsConfig,
    ColumnTimeFilter,
    TableConnector,
)
from graflo.filter import JoinClause
from graflo.filter.onto import (
    ComparisonOperator,
    FilterExpression,
    LogicalOperator,
    parse_filter_expression,
    render_conjunct,
)
from graflo.filter.select import SelectSpec
from graflo.onto import ExpressionFlavor


def _eq(field: str, value: object) -> FilterExpression:
    return FilterExpression(
        kind="leaf", field=field, cmp_operator=ComparisonOperator.EQ, value=[value]
    )


def _or(*deps: FilterExpression) -> FilterExpression:
    return FilterExpression(
        kind="composite", operator=LogicalOperator.OR, deps=list(deps)
    )


def _in(field: str, value: object) -> FilterExpression:
    return parse_filter_expression(
        {"field": field, "cmp_operator": "IN", "value": value}
    )


def _where(query: str) -> str:
    return query.split(" WHERE ", 1)[1]


class TestTopLevelOr:
    def test_or_beside_a_second_filter_is_parenthesised(self) -> None:
        connector = TableConnector(
            table_name="events", filters=[_or(_eq("a", 1), _eq("b", 2)), _eq("c", 3)]
        )

        assert connector.build_query("public") == (
            'SELECT * FROM "public"."events" WHERE ("a" = 1 OR "b" = 2) AND "c" = 3'
        )

    def test_or_beside_a_time_filter_is_parenthesised(self) -> None:
        connector = TableConnector(
            table_name="events",
            time_filter=ColumnTimeFilter(column="dt", start="2024-01-01"),
            filters=[_or(_eq("a", 1), _eq("b", 2))],
        )

        assert _where(connector.build_query("public")) == (
            '"dt" >= \'2024-01-01\' AND ("a" = 1 OR "b" = 2)'
        )

    def test_or_in_a_view_where_beside_a_connector_filter(self) -> None:
        view = SelectSpec(
            kind="select",
            select=[{"base": "id"}],
            where=_or(_eq("a", 1), _eq("b", 2)),
        )
        connector = TableConnector(
            table_name="users", view=view, filters=[_eq("status", "active")]
        )

        assert _where(connector.build_query("public")) == (
            '("a" = 1 OR "b" = 2) AND "status" = \'active\''
        )

    def test_and_and_leaf_conditions_stay_bare(self) -> None:
        both = FilterExpression(
            kind="composite",
            operator=LogicalOperator.AND,
            deps=[_eq("a", 1), _eq("b", 2)],
        )

        assert render_conjunct(both, kind=ExpressionFlavor.SQL) == '"a" = 1 AND "b" = 2'
        assert render_conjunct(_eq("a", 1), kind=ExpressionFlavor.SQL) == '"a" = 1'

    @pytest.mark.parametrize(
        ("kind", "doc_name", "expected"),
        [
            (ExpressionFlavor.AQL, "d", '(d["a"] == 1 OR d["b"] == 2)'),
            (ExpressionFlavor.CYPHER, "n", "(n.a = 1 OR n.b = 2)"),
            (ExpressionFlavor.NGQL, "v.T", "(v.T.a == 1 OR v.T.b == 2)"),
            (ExpressionFlavor.GSQL, "s", "(s.a == 1 OR s.b == 2)"),
        ],
    )
    def test_or_is_parenthesised_in_every_graph_flavor(
        self, kind: ExpressionFlavor, doc_name: str, expected: str
    ) -> None:
        expr = _or(_eq("a", 1), _eq("b", 2))

        assert render_conjunct(expr, kind=kind, doc_name=doc_name) == expected


class TestInRendering:
    def test_sql_lists_every_member(self) -> None:
        expr = _in("status", ["open", "in_progress"])

        assert expr(kind=ExpressionFlavor.SQL) == (
            "\"status\" IN ('open', 'in_progress')"
        )

    def test_sql_one_member_is_still_a_list(self) -> None:
        assert _in("status", ["open"])(kind=ExpressionFlavor.SQL) == (
            "\"status\" IN ('open')"
        )

    def test_a_scalar_value_is_a_one_member_list(self) -> None:
        assert _in("status", "open")(kind=ExpressionFlavor.SQL) == (
            "\"status\" IN ('open')"
        )

    def test_sql_numbers_are_not_quoted(self) -> None:
        assert _in("n", [1, 2.5])(kind=ExpressionFlavor.SQL) == '"n" IN (1, 2.5)'

    def test_sql_escapes_a_quote_in_a_member(self) -> None:
        assert _in("name", ["o'brien"])(kind=ExpressionFlavor.SQL) == (
            "\"name\" IN ('o''brien')"
        )

    def test_sql_refuses_an_empty_list(self) -> None:
        with pytest.raises(ValueError, match="IN"):
            _in("status", [])(kind=ExpressionFlavor.SQL)

    @pytest.mark.parametrize(
        ("kind", "doc_name", "expected"),
        [
            (ExpressionFlavor.AQL, "d", 'd["status"] IN ["open", "done"]'),
            (ExpressionFlavor.CYPHER, "n", 'n.status IN ["open", "done"]'),
            (ExpressionFlavor.NGQL, "v.T", 'v.T.status IN ["open", "done"]'),
            (ExpressionFlavor.GSQL, "s", 's.status IN ["open", "done"]'),
        ],
    )
    def test_graph_flavors_quote_each_member(
        self, kind: ExpressionFlavor, doc_name: str, expected: str
    ) -> None:
        expr = _in("status", ["open", "done"])

        assert expr(kind=kind, doc_name=doc_name) == expected

    def test_graph_flavor_one_member_is_still_a_list(self) -> None:
        expr = _in("status", "open")

        assert expr(kind=ExpressionFlavor.CYPHER, doc_name="n") == (
            'n.status IN ["open"]'
        )

    def test_the_rest_api_form_refuses_in(self) -> None:
        with pytest.raises(ValueError, match="IN"):
            _in("status", ["open"])(kind=ExpressionFlavor.GSQL, doc_name="")

    def test_python_evaluates_membership(self) -> None:
        expr = _in("status", ["open", "done"])

        assert expr(kind=ExpressionFlavor.PYTHON, status="done") is True
        assert expr(kind=ExpressionFlavor.PYTHON, status="closed") is False


class TestLoadErrors:
    def test_an_unknown_logical_key_is_refused(self) -> None:
        with pytest.raises(ValueError, match="XOR"):
            parse_filter_expression(
                {"XOR": [{"field": "a", "cmp_operator": "==", "value": 1}]}
            )

    def test_an_unknown_leaf_key_is_refused(self) -> None:
        with pytest.raises(ValueError, match="vlaue"):
            parse_filter_expression({"field": "a", "cmp_operator": "==", "vlaue": 1})

    def test_a_leaf_without_an_operator_is_refused(self) -> None:
        with pytest.raises(ValueError, match="operator"):
            parse_filter_expression({"field": "a", "value": 1})

    def test_two_logical_keys_in_one_entry_are_refused(self) -> None:
        leaf = {"field": "a", "cmp_operator": "==", "value": 1}
        with pytest.raises(ValueError, match="one logical operator"):
            parse_filter_expression({"OR": [leaf, leaf], "AND": [leaf, leaf]})

    def test_an_unknown_key_fails_when_the_bindings_load(self) -> None:
        with pytest.raises(ValueError, match=r"filters\[0\]"):
            BindingsConfig.model_validate(
                {
                    "connectors": [
                        {
                            "name": "t1",
                            "table_name": "events",
                            "filters": [
                                {
                                    "XOR": [
                                        {"field": "a", "cmp_operator": "==", "value": 1}
                                    ]
                                }
                            ],
                        }
                    ]
                }
            )

    def test_a_leaf_without_a_field_does_not_render_to_nothing_in_sql(self) -> None:
        expr = FilterExpression(
            kind="leaf", cmp_operator=ComparisonOperator.EQ, value=[1]
        )

        with pytest.raises(ValueError, match="field"):
            expr(kind=ExpressionFlavor.SQL)

    def test_the_legacy_unary_key_still_loads(self) -> None:
        expr = parse_filter_expression({"field": "a", "foo": "__gt__", "value": 1})

        assert expr.cmp_operator == ComparisonOperator.GT


class TestViewConditions:
    def _filtered(self, view: SelectSpec) -> TableConnector:
        return TableConnector(
            table_name="users", view=view, filters=[_eq("status", "active")]
        )

    def test_a_select_view_without_joins_names_no_alias(self) -> None:
        connector = self._filtered(SelectSpec(kind="select", select=[{"base": "id"}]))

        assert connector.build_query("public") == (
            'SELECT "id" FROM "public"."users" WHERE "status" = \'active\''
        )

    def test_a_select_view_without_joins_takes_the_time_filter_unqualified(
        self,
    ) -> None:
        connector = TableConnector(
            table_name="users",
            view=SelectSpec(kind="select", select=[{"base": "id"}]),
            time_filter=ColumnTimeFilter(column="dt", start="2024-01-01"),
        )

        assert _where(connector.build_query("public")) == "\"dt\" >= '2024-01-01'"

    def test_a_select_view_with_joins_qualifies_with_the_base_alias(self) -> None:
        view = SelectSpec(
            kind="select",
            joins=[
                JoinClause(table="orgs", alias="o", on_self="org_id", on_other="id")
            ],
        )

        assert _where(self._filtered(view).build_query("public")) == (
            "base.\"status\" = 'active'"
        )

    def test_a_type_lookup_view_qualifies_with_the_base_alias(self) -> None:
        view = SelectSpec(
            kind="type_lookup",
            table="objects",
            identity="id",
            type_column="type",
            source="parent",
            target="child",
        )

        assert _where(self._filtered(view).build_query("public")) == (
            's."id" IS NOT NULL AND t."id" IS NOT NULL AND base."status" = \'active\''
        )

    def test_effective_base_alias(self) -> None:
        join = JoinClause(table="orgs", alias="o", on_self="org_id", on_other="id")
        lookup = SelectSpec(
            kind="type_lookup",
            table="objects",
            identity="id",
            type_column="type",
            source="parent",
            target="child",
        )

        assert SelectSpec(kind="select").effective_base_alias() is None
        assert SelectSpec(kind="select", joins=[join]).effective_base_alias() == "base"
        assert lookup.effective_base_alias() == "base"

    def test_where_is_kept_as_authored_and_read_as_an_expression(self) -> None:
        authored = {"field": "a", "cmp_operator": "==", "value": 1}
        view = SelectSpec.from_dict({"where": authored})

        # Stored as written: the manifest's canonical form does not move.
        assert view.where == authored
        expression = view.where_expression()
        assert expression is not None
        assert expression(kind=ExpressionFlavor.SQL) == '"a" = 1'

    def test_an_invalid_where_fails_when_the_view_loads(self) -> None:
        with pytest.raises(ValueError, match="where"):
            SelectSpec.from_dict(
                {"where": {"XOR": [{"field": "a", "cmp_operator": "==", "value": 1}]}}
            )


class TestExtraFilters:
    def test_extra_filters_join_the_connector_conditions(self) -> None:
        connector = TableConnector(table_name="events", filters=[_eq("c", 3)])

        query = connector.build_query(
            "public", extra_filters=[_or(_eq("a", 1), _eq("b", 2))]
        )

        assert _where(query) == '"c" = 3 AND ("a" = 1 OR "b" = 2)'

    def test_extra_filters_are_qualified_under_joins(self) -> None:
        connector = TableConnector(
            table_name="events",
            joins=[JoinClause(table="users", alias="u", on_self="uid", on_other="id")],
        )

        query = connector.build_query("public", extra_filters=[_eq("c", 3)])

        assert _where(query) == 'base."c" = 3'

    def test_a_nested_or_survives_qualification_under_joins(self) -> None:
        """Qualifying re-parses the expression; its inner composite must survive."""
        nested = FilterExpression(
            kind="composite",
            operator=LogicalOperator.AND,
            deps=[_or(_eq("a", 1), _eq("b", 2)), _eq("c", 3)],
        )
        connector = TableConnector(
            table_name="events",
            filters=[nested],
            joins=[JoinClause(table="users", alias="u", on_self="uid", on_other="id")],
        )

        assert _where(connector.build_query("public")) == (
            '(base."a" = 1 OR base."b" = 2) AND base."c" = 3'
        )


class TestBackendQueryBuilders:
    def test_arango_match_query_parenthesises_an_or_filter(self) -> None:
        from graflo.db.arango.query import fetch_fields_query

        query = fetch_fields_query(
            "users",
            [{"email": "ada@example.com"}],
            ["email"],
            ["name"],
            filters=_or(_eq("a", 1), _eq("b", 2)),
        )

        assert '&& (_cdoc["a"] == 1 OR _cdoc["b"] == 2)' in query
