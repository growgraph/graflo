"""Edge property ops addressed at one edge triple (``per_edge``).

Two edges share the relation ``works_at`` and both carry ``weight``; a
per-edge change must reach the edge it names and leave its sibling alone, in
the schema, the profile and the pipeline.
"""

from __future__ import annotations

from typing import Any

import pytest

from graflo.architecture.contract import GraphManifest
from graflo.architecture.evolution import RemoveEdgePropertiesOp, apply_evolution
from graflo.architecture.evolution.codec import op_from_dict, op_to_dict
from graflo.architecture.evolution.merge3 import op_reads, op_slots, split_by_slot

TO_COMPANY = ("person", "company", "works_at")
TO_CITY = ("person", "city", "works_at")


def _selector(edge_id: tuple[str, str, str | None]) -> dict[str, Any]:
    return {"source": edge_id[0], "target": edge_id[1], "relation": edge_id[2]}


def _manifest(pipeline: list[dict] | None = None) -> GraphManifest:
    payload: dict[str, Any] = {
        "schema": {
            "metadata": {"name": "per-edge", "version": "1.0.0"},
            "graph": {
                "vertex_config": {
                    "vertices": [
                        {"name": name, "identity": ["id"], "properties": ["id"]}
                        for name in ("person", "company", "city")
                    ]
                },
                "edge_config": {
                    "edges": [
                        {**_selector(TO_COMPANY), "properties": ["since", "weight"]},
                        {**_selector(TO_CITY), "properties": ["since", "weight"]},
                    ]
                },
            },
            "db_profile": {
                "db_flavor": "tigergraph",
                "edge_specs": [
                    {**_selector(edge_id), "indexes": [{"fields": ["weight"]}]}
                    for edge_id in (TO_COMPANY, TO_CITY)
                ],
            },
        }
    }
    if pipeline is not None:
        payload["ingestion_model"] = {
            "resources": [{"name": "rows", "pipeline": pipeline}]
        }
    return GraphManifest.from_dict(payload)


def _static_steps() -> list[dict]:
    return [
        {"vertex": "person"},
        {"vertex": "company"},
        {"vertex": "city"},
        {"edge": {**_from_to(TO_COMPANY), "properties": ["weight"]}},
        {"edge": {**_from_to(TO_CITY), "properties": ["weight"]}},
    ]


def _from_to(edge_id: tuple[str, str, str | None]) -> dict[str, Any]:
    return {"from": edge_id[0], "to": edge_id[1], "relation": edge_id[2]}


def _properties(manifest: GraphManifest) -> dict[tuple, list[str]]:
    assert manifest.graph_schema is not None
    return {
        edge.edge_id: [f.name for f in edge.properties]
        for edge in manifest.graph_schema.core_schema.edge_config.edges
    }


def _index_fields(manifest: GraphManifest) -> dict[tuple, list[list[str]]]:
    assert manifest.graph_schema is not None
    return {
        spec.edge_id: [list(index.fields) for index in spec.indexes]
        for spec in manifest.graph_schema.db_profile.edge_specs
    }


def _step_properties(manifest: GraphManifest) -> list[Any]:
    assert manifest.ingestion_model is not None
    return [
        step["edge"]["properties"]
        for step in manifest.ingestion_model.resources[0].pipeline
        if "edge" in step
    ]


def _apply(manifest: GraphManifest, payload: dict) -> GraphManifest:
    return apply_evolution(manifest, [op_from_dict(payload)], bump_version=False)


class TestTheOpModels:
    @pytest.mark.parametrize(
        "kind",
        ["add_edge_properties", "remove_edge_properties", "rename_edge_properties"],
    )
    def test_an_op_with_no_entry_is_refused(self, kind: str) -> None:
        with pytest.raises(ValueError, match="at least one"):
            op_from_dict({"op": kind})

    def test_a_property_named_for_the_relation_and_one_edge_is_refused(self) -> None:
        with pytest.raises(ValueError, match="named both"):
            op_from_dict(
                {
                    "op": "remove_edge_properties",
                    "removals": {"works_at": ["weight"]},
                    "per_edge": [{**_selector(TO_CITY), "fields": ["weight"]}],
                }
            )

    def test_a_triple_named_twice_is_refused(self) -> None:
        entry = {**_selector(TO_CITY), "fields": ["weight"]}
        with pytest.raises(ValueError, match="unique"):
            op_from_dict({"op": "remove_edge_properties", "per_edge": [entry, entry]})

    def test_a_relation_wide_op_serializes_as_before(self) -> None:
        payload = {"op": "add_edge_properties", "additions": {"works_at": ["note"]}}
        assert op_to_dict(op_from_dict(payload)) == payload

    def test_a_per_edge_op_round_trips(self) -> None:
        payload = {
            "op": "change_field_types",
            "per_edge": [
                {**_selector(TO_CITY), "fields": {"weight": {"type": "FLOAT"}}}
            ],
        }
        op = op_from_dict(payload)
        assert op_from_dict(op_to_dict(op)) == op


class TestApplyReachesOneEdge:
    def test_an_addition_reaches_only_the_named_edge(self) -> None:
        out = _apply(
            _manifest(),
            {
                "op": "add_edge_properties",
                "per_edge": [{**_selector(TO_CITY), "properties": ["note"]}],
            },
        )
        assert _properties(out) == {
            TO_COMPANY: ["since", "weight"],
            TO_CITY: ["since", "weight", "note"],
        }

    def test_a_removal_trims_one_edge_its_spec_and_its_step(self) -> None:
        out = _apply(
            _manifest(_static_steps()),
            {
                "op": "remove_edge_properties",
                "per_edge": [{**_selector(TO_CITY), "fields": ["weight"]}],
            },
        )
        assert _properties(out) == {
            TO_COMPANY: ["since", "weight"],
            TO_CITY: ["since"],
        }
        assert _index_fields(out) == {TO_COMPANY: [["weight"]], TO_CITY: []}
        assert _step_properties(out) == [["weight"], []]

    def test_a_rename_renames_one_edge_its_spec_and_its_step(self) -> None:
        out = _apply(
            _manifest(_static_steps()),
            {
                "op": "rename_edge_properties",
                "per_edge": [{**_selector(TO_CITY), "renames": {"weight": "load"}}],
            },
        )
        assert _properties(out)[TO_CITY] == ["since", "load"]
        assert _properties(out)[TO_COMPANY] == ["since", "weight"]
        assert _index_fields(out) == {TO_COMPANY: [["weight"]], TO_CITY: [["load"]]}
        assert _step_properties(out) == [["weight"], ["load"]]

    def test_a_retype_reaches_only_the_named_edge(self) -> None:
        out = _apply(
            _manifest(),
            {
                "op": "change_field_types",
                "per_edge": [
                    {**_selector(TO_CITY), "fields": {"weight": {"type": "FLOAT"}}}
                ],
            },
        )
        assert out.graph_schema is not None
        types = {
            edge.edge_id: {f.name: f.type for f in edge.properties}
            for edge in out.graph_schema.core_schema.edge_config.edges
        }
        assert types[TO_CITY]["weight"] == "FLOAT"
        assert types[TO_COMPANY]["weight"] is None

    @pytest.mark.parametrize(
        "payload",
        [
            {"op": "add_edge_properties", "per_edge": [{"properties": ["p"]}]},
            {"op": "remove_edge_properties", "per_edge": [{"fields": ["since"]}]},
            {
                "op": "change_field_types",
                "per_edge": [{"fields": {"since": {"type": "INT"}}}],
            },
        ],
    )
    def test_an_unknown_edge_is_refused(self, payload: dict) -> None:
        payload["per_edge"][0] |= _selector(("company", "city", "works_at"))
        with pytest.raises(ValueError, match="unknown edge"):
            _apply(_manifest(), payload)

    def test_a_property_the_edge_lacks_is_refused(self) -> None:
        with pytest.raises(ValueError, match="unknown properties"):
            _apply(
                _manifest(),
                {
                    "op": "remove_edge_properties",
                    "per_edge": [{**_selector(TO_CITY), "fields": ["note"]}],
                },
            )


class TestAStepThatMayWriteSeveralEdges:
    """A per-edge change cannot edit a step that also writes other edges."""

    @pytest.mark.parametrize(
        "step",
        [
            {
                "edge": {
                    "from": "person",
                    "target_role": "place",
                    "relation": "works_at",
                }
            },
            {"edge": {**_from_to(TO_CITY), "emit_inverse": True}},
            {"edge": {"from": "person", "to": "city", "relation_field": "kind"}},
        ],
        ids=["role", "emit-inverse", "relation-field"],
    )
    def test_it_is_refused_when_it_declares_the_property(self, step: dict) -> None:
        step["edge"]["properties"] = ["weight"]
        with pytest.raises(ValueError, match="per-edge change cannot edit it"):
            _apply(
                _manifest([{"vertex": "person"}, {"vertex": "city"}, step]),
                {
                    "op": "remove_edge_properties",
                    "per_edge": [{**_selector(TO_CITY), "fields": ["weight"]}],
                },
            )

    def test_it_is_left_alone_when_it_does_not(self) -> None:
        step = {
            "edge": {
                "from": "person",
                "to": "city",
                "relation_field": "kind",
                "properties": ["since"],
            }
        }
        out = _apply(
            _manifest([{"vertex": "person"}, {"vertex": "city"}, step]),
            {
                "op": "remove_edge_properties",
                "per_edge": [{**_selector(TO_CITY), "fields": ["weight"]}],
            },
        )
        assert _step_properties(out) == [["since"]]

    def test_a_step_on_another_relation_is_not_its_concern(self) -> None:
        step = {
            "edge": {**_from_to(("person", "city", "visits")), "properties": ["weight"]}
        }
        out = _apply(
            _manifest([{"vertex": "person"}, {"vertex": "city"}, step]),
            {
                "op": "remove_edge_properties",
                "per_edge": [{**_selector(TO_CITY), "fields": ["weight"]}],
            },
        )
        assert _step_properties(out) == [["weight"]]


class TestMerge:
    def test_a_per_edge_change_slots_with_its_relation(self) -> None:
        per_edge = op_from_dict(
            {
                "op": "add_edge_properties",
                "per_edge": [{**_selector(TO_CITY), "properties": ["note"]}],
            }
        )
        wide = op_from_dict(
            {"op": "add_edge_properties", "additions": {"works_at": ["note"]}}
        )
        assert op_slots(per_edge) == op_slots(wide)
        assert op_reads(per_edge) == {
            ("vertex", "person"),
            ("vertex", "city"),
            ("relation", "works/at", "edge", "person", "city"),
        }

    def test_a_relation_less_edge_slots_under_its_own_root(self) -> None:
        op = op_from_dict(
            {
                "op": "add_edge_properties",
                "per_edge": [
                    {"source": "person", "target": "city", "properties": ["p"]}
                ],
            }
        )
        assert op_slots(op) == {("edge", "person", "city", "field")}

    def test_split_by_slot_separates_the_two_forms(self) -> None:
        op = op_from_dict(
            {
                "op": "remove_edge_properties",
                "removals": {"works_at": ["since"]},
                "per_edge": [
                    {**_selector(TO_CITY), "fields": ["weight"]},
                    {**_selector(TO_COMPANY), "fields": ["weight"]},
                ],
            }
        )
        pieces = split_by_slot(op)
        assert all(isinstance(p, RemoveEdgePropertiesOp) for p in pieces)
        assert [
            (p.removals, [e.edge_id() for e in p.per_edge])
            for p in pieces
            if isinstance(p, RemoveEdgePropertiesOp)
        ] == [
            ({"works_at": ["since"]}, []),
            ({}, [TO_CITY]),
            ({}, [TO_COMPANY]),
        ]
