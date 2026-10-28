"""The access aspect and the generic fields it rests on: reserved fields, union merge."""

from __future__ import annotations

from typing import Any

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import apply_evolution, invert_ops
from graflo.architecture.evolution.aspects import (
    ASPECTS,
    DEFAULT_ASPECTS,
    LiftError,
    LiftSpec,
    plan_lift,
)
from graflo.architecture.evolution.aspects.access import ACCESS_ZONES
from graflo.architecture.query import (
    AggregateQuery,
    NeighborQuery,
    NodeQuery,
    QueryCaps,
    ReservedFieldError,
)
from graflo.architecture.schema.context.element_text import iter_elements
from graflo.architecture.schema.context.instance_shape import instance_json_schema
from graflo.architecture.schema.context.type_sheet import render_type_sheet
from graflo.architecture.schema.vertex import Field, FieldMergeConflict, merge_fields
from graflo.rdf import ManifestRdfDeserializer, ManifestRdfSerializer

BASE_URI = "https://example.org/support/"


def _manifest() -> GraphManifest:
    manifest = GraphManifest.model_validate(
        {
            "schema": {
                "metadata": {"name": "support", "version": "1.0.0"},
                "core_schema": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "Ticket",
                                "properties": ["key", "summary"],
                                "identity": ["key"],
                            },
                            {
                                "name": "Product",
                                "properties": ["sku", "name"],
                                "identity": ["sku"],
                            },
                        ]
                    },
                    "edge_config": {
                        "edges": [
                            {
                                "source": "Ticket",
                                "target": "Product",
                                "relation": "about",
                            }
                        ]
                    },
                },
            }
        }
    )
    manifest.finish_init()
    return manifest


def _lifted(**spec: Any) -> GraphManifest:
    manifest = _manifest()
    ops = plan_lift(manifest, LiftSpec(**spec), aspects=["access"])
    lifted = apply_evolution(manifest, ops)
    lifted.finish_init()
    return lifted


def _field(manifest: GraphManifest, vertex: str, name: str) -> Field:
    schema = manifest.require_schema()
    return next(
        f for f in schema.core_schema.vertex_config.properties(vertex) if f.name == name
    )


# -- the aspect ------------------------------------------------------------------


def test_access_is_registered_and_not_lifted_by_default() -> None:
    assert ASPECTS["access"].needs_spec
    assert "access" not in DEFAULT_ASPECTS


def test_the_lift_adds_a_reserved_label_with_its_merge_mode() -> None:
    lifted = _lifted(
        access_for={"Ticket": "replace", "Product": "union"},
        access_edges=["about"],
    )

    ticket = _field(lifted, "Ticket", ACCESS_ZONES)
    product = _field(lifted, "Product", ACCESS_ZONES)
    edge = next(iter(lifted.require_schema().core_schema.edge_config.values()))
    edge_zones = next(f for f in edge.properties if f.name == ACCESS_ZONES)

    assert ticket.reserved and ticket.merge == "replace"
    assert product.reserved and product.merge == "union"
    assert edge_zones.reserved and edge_zones.merge == "replace"
    assert str(ticket.type) == "LIST" and str(ticket.item_type) == "INT"


def test_the_lift_inverts_back_to_the_original() -> None:
    manifest = _manifest()
    ops = plan_lift(
        manifest, LiftSpec(access_for={"Ticket": "replace"}), aspects=["access"]
    )
    inverses, reasons = invert_ops(ops, manifest=manifest)
    assert reasons == [], reasons

    restored = apply_evolution(apply_evolution(manifest, ops), inverses)

    def core(m: GraphManifest) -> dict:
        return m.require_schema().core_schema.to_minimal_canonical_dict()

    assert core(restored) == core(manifest)


def test_access_labels_the_types_provenance_mints_in_the_same_lift() -> None:
    manifest = _manifest()
    ops = plan_lift(
        manifest,
        LiftSpec(
            provenance_for=["Ticket"],
            access_for={"SourceRecord": "replace", "Agent": "union"},
            access_edges=["wasDerivedFrom"],
        ),
        aspects=["provenance", "access"],
    )
    lifted = apply_evolution(manifest, ops)
    lifted.finish_init()

    assert _field(lifted, "SourceRecord", ACCESS_ZONES).merge == "replace"
    assert _field(lifted, "Agent", ACCESS_ZONES).merge == "union"


@pytest.mark.parametrize(
    ("spec", "message"),
    [
        ({}, "needs access_for"),
        ({"access_for": {"Nope": "replace"}}, "lacks"),
        ({"access_edges": ["nope"]}, "lacks"),
    ],
)
def test_the_lift_refuses_what_it_cannot_plan(spec: dict, message: str) -> None:
    with pytest.raises(LiftError, match=message):
        plan_lift(_manifest(), LiftSpec(**spec), aspects=["access"])


def test_a_second_lift_adds_nothing() -> None:
    lifted = _lifted(access_for={"Ticket": "replace"}, access_edges=["about"])

    assert (
        plan_lift(
            lifted,
            LiftSpec(access_for={"Ticket": "replace"}, access_edges=["about"]),
            aspects=["access"],
        )
        == []
    )


def test_zones_declared_with_another_merge_mode_are_refused() -> None:
    lifted = _lifted(access_for={"Ticket": "replace"})

    with pytest.raises(LiftError, match="merges by 'replace', not 'union'"):
        plan_lift(lifted, LiftSpec(access_for={"Ticket": "union"}), aspects=["access"])


# -- reserved fields stay out of what a model sees ------------------------------


def test_a_reserved_field_is_left_out_of_every_model_facing_view() -> None:
    schema = _lifted(
        access_for={"Ticket": "replace"}, access_edges=["about"]
    ).require_schema()

    assert ACCESS_ZONES not in render_type_sheet(schema)
    assert ACCESS_ZONES not in str(instance_json_schema(schema))
    assert all(ACCESS_ZONES not in element.text for element in iter_elements(schema))
    assert schema.reserved_fields() == {ACCESS_ZONES}


# -- reserved fields cannot be queried ------------------------------------------


@pytest.mark.parametrize(
    "query",
    [
        NodeQuery(vertex_type="Ticket", projection=[ACCESS_ZONES]),
        NodeQuery(
            vertex_type="Ticket",
            filters={"field": ACCESS_ZONES, "cmp_operator": "==", "value": 1},
        ),
        NeighborQuery(
            vertex_type="Ticket",
            key="SUP-1",
            filters={"field": ACCESS_ZONES, "cmp_operator": "==", "value": 1},
        ),
        AggregateQuery(vertex_type="Ticket", group_by=ACCESS_ZONES),
    ],
)
def test_a_query_naming_a_reserved_field_is_refused(query) -> None:
    with pytest.raises(ReservedFieldError) as raised:
        query.refuse_reserved({ACCESS_ZONES})

    assert ACCESS_ZONES not in str(raised.value)


def test_a_query_naming_other_fields_passes() -> None:
    NodeQuery(vertex_type="Ticket", projection=["key"]).refuse_reserved({ACCESS_ZONES})


def test_an_omitted_projection_is_narrowed_to_the_allow_list() -> None:
    narrowed = NodeQuery(vertex_type="Ticket").narrowed(
        QueryCaps(projection_allow_list=["key"])
    )

    assert narrowed.projection == ["key"]


# -- the generic field flags -------------------------------------------------------


def test_union_needs_a_list_field() -> None:
    with pytest.raises(ValueError, match="needs type LIST"):
        Field(name="x", type="INT", merge="union")


def test_default_flags_are_left_out_of_a_dump() -> None:
    assert Field(name="tags").to_dict() == {"name": "tags"}
    dumped = Field(
        name="z", type="LIST", item_type="INT", reserved=True, merge="union"
    ).to_dict()
    assert dumped["reserved"] is True and dumped["merge"] == "union"


def test_a_fold_refuses_a_reserved_field_meeting_a_readable_one() -> None:
    with pytest.raises(FieldMergeConflict, match="reserved"):
        merge_fields(
            Field(name="z", type="LIST", item_type="INT", reserved=True),
            Field(name="z", type="LIST", item_type="INT"),
            owner="vertex 'Ticket'",
        )


def test_the_flags_round_trip_through_rdf() -> None:
    lifted = _lifted(access_for={"Product": "union"})

    turtle = ManifestRdfSerializer(include_ontology=False).to_turtle(lifted, BASE_URI)
    restored = ManifestRdfDeserializer().from_turtle(turtle, BASE_URI.rstrip("/"))
    restored.finish_init()
    zones = _field(restored, "Product", ACCESS_ZONES)

    assert zones.reserved and zones.merge == "union"
