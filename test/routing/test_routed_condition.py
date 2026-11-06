"""Which rows a resource's routers keep, and where a connector reads that field from."""

from __future__ import annotations

from typing import Any

import pytest

from graflo.architecture.contract.bindings import TableConnector
from graflo.architecture.contract.ingestion.resource import (
    level_rewrites,
    routed_row_condition,
    router_accepted_values,
    router_keeps_subset,
)
from graflo.filter import JoinClause
from graflo.filter.onto import render_conjunct
from graflo.filter.select import SelectSpec
from graflo.filter.sql import values_in_filter
from graflo.onto import ExpressionFlavor

DECLARED = {"server", "switch", "rack"}


def _router(**options: Any) -> dict[str, Any]:
    return {"vertex_router": {"type_field": "kind", **options}}


class TestRouterAcceptedValues:
    def test_a_closed_router_accepts_its_keys_that_reach_a_declared_class(
        self,
    ) -> None:
        step = _router(
            type_map={"srv": "server", "sw": "switch", "pdu": "pdu"},
            type_map_only=True,
        )

        assert router_accepted_values(step, DECLARED) == {"srv", "sw"}

    def test_an_open_router_also_accepts_declared_names(self) -> None:
        step = _router(type_map={"srv": "server"})

        assert router_accepted_values(step, DECLARED) == {
            "srv",
            "server",
            "switch",
            "rack",
        }

    def test_vertex_types_bound_an_open_router(self) -> None:
        step = _router(type_map={"srv": "server"}, vertex_types=["server"])

        assert router_accepted_values(step, DECLARED) == {"srv", "server"}

    def test_a_name_mapped_elsewhere_routes_where_the_map_says(self) -> None:
        step = _router(type_map={"rack": "server"}, vertex_types=["server"])

        assert router_accepted_values(step, DECLARED) == {"rack", "server"}


class TestRoutedRowCondition:
    def test_a_router_alone_gives_its_field_and_values(self) -> None:
        pipeline = [_router(type_map={"srv": "server"}, type_map_only=True)]

        assert routed_row_condition(pipeline, DECLARED) == {"kind": {"srv"}}

    def test_routers_on_one_field_add_up(self) -> None:
        pipeline = [
            _router(type_map={"srv": "server"}, type_map_only=True, role="a"),
            _router(type_map={"sw": "switch"}, type_map_only=True, role="b"),
        ]

        assert routed_row_condition(pipeline, DECLARED) == {"kind": {"srv", "sw"}}

    def test_routers_on_two_fields_give_two_fields(self) -> None:
        pipeline = [
            _router(type_map={"srv": "server"}, type_map_only=True),
            {
                "vertex_router": {
                    "type_field": "parent_kind",
                    "type_map": {"rk": "rack"},
                    "type_map_only": True,
                }
            },
        ]

        assert routed_row_condition(pipeline, DECLARED) == {
            "kind": {"srv"},
            "parent_kind": {"rk"},
        }

    def test_an_edge_between_router_roles_keeps_the_condition(self) -> None:
        pipeline = [
            _router(type_map={"srv": "server"}, type_map_only=True, role="a"),
            {
                "vertex_router": {
                    "type_field": "other",
                    "type_map": {"rk": "rack"},
                    "type_map_only": True,
                    "role": "b",
                }
            },
            {"edge": {"source_role": "a", "target_role": "b"}},
        ]

        assert routed_row_condition(pipeline, DECLARED) == {
            "kind": {"srv"},
            "other": {"rk"},
        }

    def test_a_transform_that_writes_other_fields_keeps_the_condition(self) -> None:
        pipeline = [
            {"transform": {"rename": {"name": "label"}}},
            _router(type_map={"srv": "server"}, type_map_only=True),
        ]

        assert routed_row_condition(pipeline, DECLARED) == {"kind": {"srv"}}

    def test_a_router_routing_nothing_leaves_no_field(self) -> None:
        pipeline = [_router(type_map={"pdu": "pdu"}, type_map_only=True)]

        assert routed_row_condition(pipeline, DECLARED) == {}

    @pytest.mark.parametrize(
        "extra",
        [
            pytest.param({"vertex": "rack"}, id="static-vertex"),
            pytest.param(
                {"descend": {"key": "parts", "pipeline": [{"vertex": "rack"}]}},
                id="descend",
            ),
            pytest.param({"edge": {"from": "server", "to": "rack"}}, id="static-edge"),
            pytest.param(
                {"edge": {"source_role": "kind", "target_role": "elsewhere"}},
                id="edge-to-a-non-router-role",
            ),
            pytest.param(
                {"transform": {"rename": {"category": "kind"}}},
                id="rename-into-the-field",
            ),
            pytest.param(
                {"transform": {"call": {"use": "f", "input": ["a"]}}},
                id="transform-with-unknown-output",
            ),
        ],
    )
    def test_a_row_that_may_count_without_a_router_gives_none(
        self, extra: dict[str, Any]
    ) -> None:
        pipeline = [_router(type_map={"srv": "server"}, type_map_only=True), extra]

        assert routed_row_condition(pipeline, DECLARED) is None

    def test_no_router_gives_none(self) -> None:
        assert routed_row_condition([{"vertex": "rack"}], DECLARED) is None

    def test_a_transform_listed_after_the_router_still_counts(self) -> None:
        # Transforms of a level run before its routers, whatever the order listed.
        pipeline = [
            _router(type_map={"srv": "server"}, type_map_only=True),
            {"transform": {"rename": {"category": "kind"}}},
        ]

        assert level_rewrites(pipeline) == {"kind"}
        assert routed_row_condition(pipeline, DECLARED) is None


class TestRouterKeepsSubset:
    @pytest.mark.parametrize(
        "step,expected",
        [
            pytest.param(_router(type_map_only=True, type_map={}), True, id="closed"),
            pytest.param(_router(vertex_types=["rack"]), True, id="bounded"),
            pytest.param(_router(type_map={"srv": "server"}), False, id="open"),
        ],
    )
    def test_closed_or_bounded(self, step: dict[str, Any], expected: bool) -> None:
        assert router_keeps_subset([step]) is expected


class TestSourceColumn:
    def test_a_plain_table_reads_the_column_itself(self) -> None:
        assert TableConnector(table_name="t").source_column("kind") == "kind"

    def test_a_join_field_reads_the_joined_column(self) -> None:
        connector = TableConnector(
            table_name="t",
            joins=[
                JoinClause(
                    table="types",
                    alias="ty",
                    on_self="type_id",
                    on_other="id",
                    select_fields=["name"],
                )
            ],
        )

        assert connector.source_column("ty__name") == "ty.name"
        assert connector.source_column("kind") == "kind"

    def test_a_join_selecting_every_column_leaves_names_unknown(self) -> None:
        connector = TableConnector(
            table_name="t",
            joins=[JoinClause(table="types", on_self="type_id", on_other="id")],
        )

        assert connector.source_column("kind") is None

    def test_raw_select_columns_leave_names_unknown(self) -> None:
        connector = TableConnector(table_name="t", select_columns=["kind"])

        assert connector.source_column("kind") is None

    def test_a_view_resolves_through_its_columns(self) -> None:
        view = SelectSpec.model_validate(
            {
                "joins": [
                    {
                        "table": "cmdb_all_classes",
                        "alias": "ci_types",
                        "on_self": "cmdb_ci",
                        "on_other": "sys_id",
                    }
                ],
                "select": [
                    "all_base",
                    {"base": "cmdb_ci"},
                    {"base": "name", "as": "label"},
                    {
                        "from_join": "ci_types",
                        "column": "sys_class_name",
                        "as": "ci_type",
                    },
                    {"expr": "upper(name)", "as": "shout"},
                ],
            }
        )
        connector = TableConnector(table_name="t", view=view)

        assert connector.source_column("ci_type") == "ci_types.sys_class_name"
        assert connector.source_column("label") == "name"
        assert connector.source_column("cmdb_ci") == "cmdb_ci"
        assert connector.source_column("other") == "other"
        assert connector.source_column("shout") is None

    def test_a_view_without_all_base_knows_only_its_columns(self) -> None:
        view = SelectSpec.model_validate({"select": [{"base": "kind"}]})

        assert view.source_column("kind") == "kind"
        assert view.source_column("other") is None

    def test_an_unnamed_expression_leaves_other_names_unknown(self) -> None:
        view = SelectSpec.model_validate({"select": ["all_base", "lower(name)"]})

        assert view.source_column("kind") is None

    def test_a_type_lookup_view_names_its_outputs(self) -> None:
        view = SelectSpec.model_validate(
            {
                "kind": "type_lookup",
                "table": "nodes",
                "identity": "id",
                "type_column": "class",
                "source": "src",
                "target": "dst",
                "relation": "rel",
            }
        )

        assert view.source_column("source_type") == "s.class"
        assert view.source_column("target_type") == "t.class"
        assert view.source_column("source_id") == "src"
        assert view.source_column("relation") == "rel"
        assert view.source_column("other") is None


class TestValuesInFilter:
    @staticmethod
    def _sql(columns: dict[str, set[Any]]) -> str | None:
        expr = values_in_filter(columns)
        return (
            None if expr is None else render_conjunct(expr, kind=ExpressionFlavor.SQL)
        )

    def test_one_column_is_one_sorted_in(self) -> None:
        assert self._sql({"kind": {"sw", "srv"}}) == "\"kind\" IN ('srv', 'sw')"

    def test_two_columns_are_or_ed(self) -> None:
        assert self._sql({"b": {1}, "a": {"x"}}) == '("a" IN (\'x\') OR "b" IN (1))'

    @pytest.mark.parametrize(
        "values",
        [
            pytest.param({1, "server"}, id="mixed"),
            pytest.param({True}, id="bool"),
            pytest.param(set(), id="empty"),
        ],
    )
    def test_values_without_one_literal_type_give_none(self, values: set) -> None:
        assert values_in_filter({"kind": values}) is None
