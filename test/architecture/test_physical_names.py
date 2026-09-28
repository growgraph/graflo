"""Physical names live in ``DatabaseProfile``; the logical schema never changes.

Covers :mod:`graflo.architecture.evolution.sanitize` (naming and the two schema
views) and how the profile's property maps follow logical evolution ops.
"""

from __future__ import annotations

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import (
    RemoveVertexPropertiesOp,
    RenameEdgePropertiesOp,
    RenameVertexPropertiesOp,
    RenameVerticesOp,
    SanitizeOp,
    apply_evolution,
    apply_sanitize,
)
from graflo.architecture.evolution.sanitize import (
    materialize_physical_schema,
    physical_schema,
    with_physical_names,
)
from graflo.architecture.graph_types import Index
from graflo.architecture.schema import CoreSchema, GraphMetadata, Schema
from graflo.architecture.schema.database_features import (
    DatabaseProfile,
    DefaultPropertyValues,
    EdgePhysicalSpec,
)
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.vertex import Field, FieldType, Vertex, VertexConfig
from graflo.onto import DBType


def _schema(
    *,
    vertices: list[Vertex] | None = None,
    edges: list[Edge] | None = None,
    profile: DatabaseProfile | None = None,
) -> Schema:
    return Schema(
        metadata=GraphMetadata(name="physical_names"),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(
                vertices=vertices
                or [
                    Vertex(
                        name="route",
                        properties=[
                            Field(name="from", type=FieldType.INT),
                            Field(name="name"),
                        ],
                        identity=["from"],
                    ),
                    Vertex(name="stop", properties=[Field(name="id")], identity=["id"]),
                ]
            ),
            edge_config=EdgeConfig(
                edges=edges
                if edges is not None
                else [
                    Edge(
                        source="route",
                        target="stop",
                        relation="serves",
                        properties=[Field(name="type"), Field(name="since")],
                        identities=[["source", "target", "type"]],
                    )
                ]
            ),
        ),
        db_profile=profile or DatabaseProfile(db_flavor=DBType.TIGERGRAPH),
    )


def _manifest(schema: Schema) -> GraphManifest:
    manifest = GraphManifest.from_config(
        {
            "schema": schema.to_dict(skip_defaults=False),
            "ingestion_model": {
                "resources": [{"name": "routes", "pipeline": [{"vertex": "route"}]}]
            },
        }
    )
    manifest.finish_init()
    return manifest


# -- naming ---------------------------------------------------------------------


def test_edge_properties_get_stored_names():
    named = with_physical_names(_schema(), DBType.TIGERGRAPH)

    edge_id = ("route", "stop", "serves")
    assert named.db_profile.edge_property_map(edge_id) == {"type": "type_attr"}
    assert named.core_schema.edge_config.edge_for(edge_id).property_names == [
        "type",
        "since",
    ]


def test_edges_sharing_a_relation_share_stored_names():
    """TigerGraph declares one attribute list per edge type."""
    schema = _schema(
        vertices=[
            Vertex(name="a", properties=[Field(name="id")], identity=["id"]),
            Vertex(name="b", properties=[Field(name="id")], identity=["id"]),
        ],
        edges=[
            Edge(source="a", target="b", relation="r", properties=[Field(name="type")]),
            Edge(source="b", target="a", relation="r", properties=[Field(name="type")]),
        ],
    )

    named = with_physical_names(schema, DBType.TIGERGRAPH)

    assert named.db_profile.edge_property_map(("a", "b", "r")) == {"type": "type_attr"}
    assert named.db_profile.edge_property_map(("b", "a", "r")) == {"type": "type_attr"}


def test_a_sanitized_property_name_never_lands_on_a_declared_one():
    schema = _schema(
        vertices=[
            Vertex(
                name="user",
                properties=[
                    Field(name="user-id"),
                    Field(name="user__id"),
                    Field(name="from"),
                    Field(name="from_attr"),
                ],
                identity=["user__id"],
            )
        ],
        edges=[],
    )

    named = with_physical_names(schema, DBType.TIGERGRAPH)

    assert named.db_profile.vertex_property_map("user") == {
        "user-id": "user__id_1",
        "from": "from_attr_1",
    }


def test_sanitized_vertex_and_relation_names_stay_distinct():
    schema = _schema(
        vertices=[
            Vertex(name="a-b", properties=[Field(name="id")], identity=["id"]),
            Vertex(name="a__b", properties=[Field(name="id")], identity=["id"]),
        ],
        edges=[
            Edge(source="a-b", target="a__b", relation="a__b"),
            Edge(source="a__b", target="a-b", relation="x-y"),
            Edge(source="a-b", target="a-b", relation="x__y"),
        ],
    )

    profile = with_physical_names(schema, DBType.TIGERGRAPH).db_profile

    stored_vertices = {
        profile.vertex_storage_name("a-b"),
        profile.vertex_storage_name("a__b"),
    }
    assert stored_vertices == {"a__b", "a__b_1"}
    relations = {
        profile.edge_relation_name(edge_id, default_relation=edge_id[2])
        for edge_id in [
            ("a-b", "a__b", "a__b"),
            ("a__b", "a-b", "x-y"),
            ("a-b", "a-b", "x__y"),
        ]
    }
    assert relations == {"a__b_relation", "x__y", "x__y_1"}


def test_sanitize_is_idempotent():
    manifest = _manifest(_schema())
    apply_sanitize(manifest, SanitizeOp(db_flavor=DBType.TIGERGRAPH))
    once = manifest.require_schema().db_profile.to_dict(skip_defaults=False)

    apply_sanitize(manifest, SanitizeOp(db_flavor=DBType.TIGERGRAPH))

    assert manifest.require_schema().db_profile.to_dict(skip_defaults=False) == once


def test_names_already_in_the_profile_win():
    profile = DatabaseProfile(
        db_flavor=DBType.TIGERGRAPH,
        vertex_property_names={"route": {"from": "origin"}},
    )

    named = with_physical_names(_schema(profile=profile), DBType.TIGERGRAPH)

    assert named.db_profile.vertex_property_name("route", "from") == "origin"


def test_with_physical_names_does_not_touch_its_input():
    schema = _schema()

    with_physical_names(schema, DBType.TIGERGRAPH)

    assert schema.db_profile.vertex_property_names == {}
    assert not schema.db_profile.has_property_names()


def test_valid_names_need_no_profile_entry():
    schema = _schema(profile=DatabaseProfile(db_flavor=DBType.NEO4J))

    named = with_physical_names(schema, DBType.NEO4J)

    assert not named.db_profile.has_property_names()
    assert named.db_profile.vertex_storage_names == {}


# -- the stored view ------------------------------------------------------------


def test_materialized_schema_carries_stored_names_everywhere():
    profile = DatabaseProfile(
        db_flavor=DBType.TIGERGRAPH,
        vertex_indexes={"route": [Index(fields=["from", "name"])]},
        edge_specs=[
            EdgePhysicalSpec(
                source="route",
                target="stop",
                relation="serves",
                indexes=[Index(fields=["type"])],
            )
        ],
        default_property_values=DefaultPropertyValues(vertices={"route": {"from": 0}}),
    )

    stored = physical_schema(_schema(profile=profile), DBType.TIGERGRAPH)

    route = stored.core_schema.vertex_config["route"]
    assert route.property_names == ["from_attr", "name"]
    assert route.identity == ["from_attr"]
    assert stored.db_profile.vertex_indexes["route"][0].fields == ["from_attr", "name"]
    assert stored.db_profile.default_property_values is not None
    assert stored.db_profile.default_property_values.vertices == {
        "route": {"from_attr": 0}
    }
    edge = stored.core_schema.edge_config.edge_for(("route", "stop", "serves"))
    assert edge.property_names == ["type_attr", "since"]
    assert edge.identities == [["source", "target", "type_attr"]]
    spec = stored.db_profile.edge_name_spec(("route", "stop", "serves"))
    assert spec is not None
    assert spec.indexes[0].fields == ["type_attr"]
    assert not stored.db_profile.has_property_names()


def test_materializing_without_renames_returns_the_schema_itself():
    schema = _schema(profile=DatabaseProfile(db_flavor=DBType.NEO4J))

    assert materialize_physical_schema(schema) is schema


# -- the profile maps follow logical evolution ------------------------------------


def _sanitized_manifest() -> GraphManifest:
    manifest = _manifest(_schema())
    apply_sanitize(manifest, SanitizeOp(db_flavor=DBType.TIGERGRAPH))
    return manifest


def test_renaming_a_property_keeps_its_stored_name():
    manifest = apply_evolution(
        _sanitized_manifest(),
        [RenameVertexPropertiesOp(renames={"route": {"from": "origin"}})],
        bump_version=False,
    )

    profile = manifest.require_schema().db_profile
    assert profile.vertex_property_map("route") == {"origin": "from_attr"}


def test_renaming_a_vertex_keeps_its_stored_names():
    manifest = apply_evolution(
        _sanitized_manifest(),
        [RenameVerticesOp(renames={"route": "line"})],
        bump_version=False,
    )

    profile = manifest.require_schema().db_profile
    assert profile.vertex_property_map("line") == {"from": "from_attr"}
    assert "route" not in profile.vertex_property_names


def test_renaming_an_edge_property_keeps_its_stored_name():
    manifest = apply_evolution(
        _sanitized_manifest(),
        [RenameEdgePropertiesOp(renames={"serves": {"type": "mode"}})],
        bump_version=False,
    )

    profile = manifest.require_schema().db_profile
    assert profile.edge_property_map(("route", "stop", "serves")) == {
        "mode": "type_attr"
    }


def test_removing_a_property_drops_its_stored_name():
    manifest = apply_evolution(
        _sanitized_manifest(),
        [
            RenameVertexPropertiesOp(renames={"route": {"from": "origin"}}),
            RemoveVertexPropertiesOp(removals={"route": ["name"]}),
        ],
        bump_version=False,
    )
    manifest.require_schema().db_profile.vertex_property_names["route"]["ghost"] = "g"

    manifest.require_schema().finish_init()

    assert manifest.require_schema().db_profile.vertex_property_map("route") == {
        "origin": "from_attr"
    }


def test_two_properties_cannot_share_a_stored_name():
    profile = DatabaseProfile(
        db_flavor=DBType.TIGERGRAPH,
        vertex_property_names={"route": {"from": "name"}},
    )

    with pytest.raises(ValueError, match="collide"):
        _schema(profile=profile)


def test_stored_names_belong_on_the_base_edge_variant():
    profile = DatabaseProfile(
        edge_specs=[
            EdgePhysicalSpec(
                source="route",
                target="stop",
                relation="serves",
                purpose="audit",
                property_names={"type": "kind"},
            )
        ]
    )

    with pytest.raises(ValueError, match="base variant"):
        _schema(profile=profile)


def test_renaming_a_vertex_property_rewrites_its_filters():
    schema = _schema(
        vertices=[
            Vertex(
                name="route",
                properties=[Field(name="from"), Field(name="name")],
                identity=["from"],
                filters=[{"field": "name", "cmp_operator": "==", "value": "x"}],
            ),
        ],
        edges=[],
    )

    manifest = apply_evolution(
        _manifest(schema),
        [RenameVertexPropertiesOp(renames={"route": {"name": "title"}})],
        bump_version=False,
    )

    (expression,) = manifest.require_schema().core_schema.vertex_config["route"].filters
    assert expression.field == "title"
