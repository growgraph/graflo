"""Which records a resource's routers keep, and where a connector reads that field from."""

from __future__ import annotations

from typing import Any

import pytest

from graflo.architecture.contract.bindings import TableConnector
from graflo.architecture.contract.ingestion.routing import (
    RoutedRecordFilter,
    fields_written_before_routing,
    routed_record_filter,
    routers_keep_subset,
)
from graflo.architecture.contract.ingestion.steps import (
    ActorConfig,
    TransformActorConfig,
    VertexRouterActorConfig,
    as_actor_step,
)
from graflo.architecture.contract.ingestion.transform import ProtoTransform
from graflo.filter import JoinClause
from graflo.filter.onto import render_conjunct
from graflo.filter.select import SelectSpec
from graflo.filter.sql import values_in_filter
from graflo.onto import ExpressionFlavor

DECLARED = {"machine", "sensor", "line"}

#: A named transform the way ``ingestion_model.transforms`` holds one.
NAMED = {
    "lower_name": ProtoTransform(
        name="lower_name",
        module="builtins",
        foo="str",
        input=("name",),
        output=("label",),
    ),
    "relabel_kind": ProtoTransform(
        name="relabel_kind",
        module="builtins",
        foo="str",
        input=("raw",),
        output=("kind",),
    ),
}


def _router(**options: Any) -> VertexRouterActorConfig:
    step = as_actor_step({"vertex_router": {"type_field": "kind", **options}})
    assert isinstance(step, VertexRouterActorConfig)
    return step


def _steps(*raw: dict[str, Any] | ActorConfig) -> list[ActorConfig]:
    return [as_actor_step(step) for step in raw]


def _filter(*raw: dict[str, Any] | ActorConfig) -> RoutedRecordFilter:
    return routed_record_filter(_steps(*raw), DECLARED, NAMED)


CLOSED = {"type_map": {"mch": "machine"}, "type_map_only": True}


class TestRoutedValues:
    def test_a_closed_router_routes_its_keys_that_reach_a_declared_class(
        self,
    ) -> None:
        step = _router(
            type_map={"mch": "machine", "sen": "sensor", "valve": "valve"},
            type_map_only=True,
        )

        assert step.routed_values(DECLARED) == {"mch", "sen"}

    def test_an_open_router_also_routes_declared_names(self) -> None:
        step = _router(type_map={"mch": "machine"})

        assert step.routed_values(DECLARED) == {"mch", "machine", "sensor", "line"}

    def test_vertex_types_bound_an_open_router(self) -> None:
        step = _router(type_map={"mch": "machine"}, vertex_types=["machine"])

        assert step.routed_values(DECLARED) == {"mch", "machine"}

    def test_a_name_mapped_elsewhere_routes_where_the_map_says(self) -> None:
        step = _router(type_map={"line": "machine"}, vertex_types=["machine"])

        assert step.routed_values(DECLARED) == {"line", "machine"}


class TestFieldsWritten:
    @staticmethod
    def _written(raw: dict[str, Any]) -> frozenset[str] | None:
        step = as_actor_step({"transform": raw})
        assert isinstance(step, TransformActorConfig)
        return step.fields_written(NAMED)

    def test_a_rename_writes_its_targets(self) -> None:
        assert self._written({"rename": {"a": "b"}}) == {"b"}

    def test_an_explicit_output_is_what_is_written(self) -> None:
        call = {"module": "builtins", "foo": "str", "input": ["kind"], "output": ["k2"]}
        assert self._written({"call": call}) == {"k2"}

    def test_a_guard_on_the_field_only_reads_it(self) -> None:
        call = {"module": "builtins", "foo": "str", "input": ["id"], "output": ["key"]}
        raw = {"call": call, "when": {"field": "kind", "in": ["mch"]}}
        assert self._written(raw) == {"key"}

    def test_a_call_without_output_writes_back_into_its_input(self) -> None:
        call = {"module": "builtins", "foo": "str", "input": ["kind"]}
        assert self._written({"call": call}) == {"kind"}

    def test_a_named_transform_writes_its_declared_output(self) -> None:
        assert self._written({"call": {"use": "lower_name"}}) == {"label"}

    def test_the_call_overrides_the_named_output(self) -> None:
        raw = {"call": {"use": "lower_name", "output": ["shout"]}}
        assert self._written(raw) == {"shout"}

    def test_an_unknown_named_transform_is_unknown(self) -> None:
        assert self._written({"call": {"use": "missing"}}) is None

    def test_a_dress_writes_its_key_and_value(self) -> None:
        call = {
            "module": "builtins",
            "foo": "str",
            "input": ["open"],
            "dress": {"key": "name", "value": "value"},
        }
        assert self._written({"call": call}) == {"name", "value"}

    def test_output_groups_are_written(self) -> None:
        call = {
            "module": "builtins",
            "foo": "str",
            "input_groups": [["a"], ["b"]],
            "output_groups": [["x"], ["y"]],
        }
        assert self._written({"call": call}) == {"x", "y"}

    def test_a_key_target_is_unknown(self) -> None:
        call = {"module": "builtins", "foo": "str", "target": "keys"}
        assert self._written({"call": call}) is None


class TestRoutedRecordFilter:
    def test_a_router_alone_gives_its_field_and_values(self) -> None:
        assert _filter(_router(**CLOSED)).fields == {"kind": {"mch"}}

    def test_routers_on_one_field_add_up(self) -> None:
        verdict = _filter(
            _router(type_map={"mch": "machine"}, type_map_only=True, role="a"),
            _router(type_map={"sen": "sensor"}, type_map_only=True, role="b"),
        )

        assert verdict.fields == {"kind": {"mch", "sen"}}

    def test_routers_on_two_fields_give_two_fields(self) -> None:
        verdict = _filter(
            _router(**CLOSED),
            {
                "vertex_router": {
                    "type_field": "parent_kind",
                    "type_map": {"ln": "line"},
                    "type_map_only": True,
                }
            },
        )

        assert verdict.fields == {"kind": {"mch"}, "parent_kind": {"ln"}}

    def test_an_edge_between_router_roles_keeps_the_condition(self) -> None:
        verdict = _filter(
            _router(type_map={"mch": "machine"}, type_map_only=True, role="a"),
            {
                "vertex_router": {
                    "type_field": "other",
                    "type_map": {"ln": "line"},
                    "type_map_only": True,
                    "role": "b",
                }
            },
            {"edge": {"source_role": "a", "target_role": "b"}},
        )

        assert verdict.fields == {"kind": {"mch"}, "other": {"ln"}}

    @pytest.mark.parametrize(
        "transform",
        [
            pytest.param({"rename": {"name": "label"}}, id="rename-elsewhere"),
            pytest.param({"call": {"use": "lower_name"}}, id="named-output-elsewhere"),
            pytest.param(
                {
                    "call": {
                        "module": "builtins",
                        "foo": "str",
                        "input": ["id"],
                        "output": ["key"],
                    },
                    "when": {"field": "kind", "in": ["mch"]},
                },
                id="guarded-on-the-field",
            ),
            pytest.param(
                {
                    "call": {
                        "module": "builtins",
                        "foo": "str",
                        "input": ["open"],
                        "dress": {"key": "name", "value": "value"},
                    }
                },
                id="dress",
            ),
        ],
    )
    def test_a_transform_that_writes_other_fields_keeps_the_condition(
        self, transform: dict[str, Any]
    ) -> None:
        verdict = _filter({"transform": transform}, _router(**CLOSED))

        assert verdict.fields == {"kind": {"mch"}}
        assert verdict.blocked_by is None

    def test_a_router_routing_nothing_leaves_no_field(self) -> None:
        verdict = _filter(_router(type_map={"valve": "valve"}, type_map_only=True))

        assert verdict.fields == {}

    @pytest.mark.parametrize(
        "extra,blocked_by",
        [
            pytest.param({"vertex": "line"}, "step 1 (vertex)", id="static-vertex"),
            pytest.param(
                {"descend": {"key": "parts", "pipeline": [{"vertex": "line"}]}},
                "step 1 (descend)",
                id="descend",
            ),
            pytest.param(
                {"edge": {"from": "machine", "to": "line"}},
                "an edge end is not a router role",
                id="static-edge",
            ),
            pytest.param(
                {"edge": {"source_role": "kind", "target_role": "elsewhere"}},
                "an edge end is not a router role",
                id="edge-to-a-non-router-role",
            ),
            pytest.param(
                {"transform": {"rename": {"category": "kind"}}},
                "it writes ['kind']",
                id="rename-into-the-field",
            ),
            pytest.param(
                {"transform": {"call": {"use": "relabel_kind"}}},
                "step 1 (transform relabel_kind): it writes ['kind']",
                id="named-transform-into-the-field",
            ),
            pytest.param(
                {
                    "transform": {
                        "call": {"module": "builtins", "foo": "str", "input": ["kind"]}
                    }
                },
                "step 1 (transform builtins.str): it writes ['kind']",
                id="in-place-call-on-the-field",
            ),
            pytest.param(
                {"transform": {"call": {"use": "missing", "input": ["a"]}}},
                "cannot be known",
                id="unknown-named-transform",
            ),
        ],
    )
    def test_a_record_that_may_count_without_a_router_names_the_step(
        self, extra: dict[str, Any], blocked_by: str
    ) -> None:
        verdict = _filter(_router(**CLOSED), extra)

        assert verdict.fields is None
        assert verdict.blocked_by is not None and blocked_by in verdict.blocked_by

    def test_no_router_gives_none(self) -> None:
        verdict = _filter({"vertex": "line"})

        assert verdict.fields is None
        assert verdict.blocked_by == "no top-level vertex_router"

    def test_a_transform_listed_after_the_router_still_counts(self) -> None:
        # Transforms of a level run before its routers, whatever the order listed.
        pipeline = _steps(
            _router(**CLOSED), {"transform": {"rename": {"category": "kind"}}}
        )

        assert fields_written_before_routing(pipeline, NAMED) == {"kind"}
        assert routed_record_filter(pipeline, DECLARED, NAMED).fields is None


class TestRoutersKeepSubset:
    @pytest.mark.parametrize(
        "options,expected",
        [
            pytest.param({"type_map_only": True, "type_map": {}}, True, id="closed"),
            pytest.param({"vertex_types": ["line"]}, True, id="bounded"),
            pytest.param({"type_map": {"mch": "machine"}}, False, id="open"),
        ],
    )
    def test_closed_or_bounded(self, options: dict[str, Any], expected: bool) -> None:
        assert routers_keep_subset([_router(**options)]) is expected


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
                        "table": "all_classes",
                        "alias": "machine_types",
                        "on_self": "machine_id",
                        "on_other": "entity_id",
                    }
                ],
                "select": [
                    "all_base",
                    {"base": "machine_id"},
                    {"base": "name", "as": "label"},
                    {
                        "from_join": "machine_types",
                        "column": "type_name",
                        "as": "machine_type",
                    },
                    {"expr": "upper(name)", "as": "shout"},
                ],
            }
        )
        connector = TableConnector(table_name="t", view=view)

        assert connector.source_column("machine_type") == "machine_types.type_name"
        assert connector.source_column("label") == "name"
        assert connector.source_column("machine_id") == "machine_id"
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
        assert self._sql({"kind": {"sen", "mch"}}) == "\"kind\" IN ('mch', 'sen')"

    def test_two_columns_are_or_ed(self) -> None:
        assert self._sql({"b": {1}, "a": {"x"}}) == '("a" IN (\'x\') OR "b" IN (1))'

    @pytest.mark.parametrize(
        "values",
        [
            pytest.param({1, "machine"}, id="mixed"),
            pytest.param({True}, id="bool"),
            pytest.param(set(), id="empty"),
        ],
    )
    def test_values_without_one_literal_type_give_none(self, values: set) -> None:
        assert values_in_filter({"kind": values}) is None
