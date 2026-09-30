"""Presence-only live drift: what is in the database but not in the schema."""

from __future__ import annotations

from typing import Any

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.schema import Schema
from graflo.db.graph_introspection import (
    GraphEdgeIntrospection,
    GraphIntrospectionResult,
    GraphSchemaInferencer,
    GraphVertexIntrospection,
)
from graflo.migrate.drift import compare_live_schema
from graflo.onto import DBType

DECLARED: dict[str, Any] = {
    "metadata": {"name": "plant", "version": "1.0.0"},
    "graph": {
        "vertex_config": {
            "vertices": [
                {
                    "name": "machine",
                    "properties": [
                        {"name": "key", "type": "STRING"},
                        {"name": "model", "type": "STRING"},
                    ],
                    "identity": ["key"],
                },
                {
                    "name": "line",
                    "properties": [{"name": "key", "type": "STRING"}],
                    "identity": ["key"],
                },
            ]
        },
        "edge_config": {
            "edges": [{"source": "line", "target": "machine", "relation": "uses"}]
        },
    },
}


def _declared(**db_profile: Any) -> Schema:
    config = {**DECLARED}
    if db_profile:
        config["db_profile"] = db_profile
    manifest = GraphManifest.model_validate({"schema": config})
    manifest.finish_init()
    return manifest.require_schema()


def _observed(
    vertices: dict[str, list[str]], edges: list[tuple[str, str, str]]
) -> Schema:
    """An introspected schema, built the way a Cypher backend builds one."""
    result = GraphIntrospectionResult(
        name="plant",
        vertices=[
            GraphVertexIntrospection(name=name, properties=props)
            for name, props in vertices.items()
        ],
        edges=[
            GraphEdgeIntrospection(source=s, target=t, relation=r) for s, r, t in edges
        ],
    )
    return GraphSchemaInferencer(db_flavor=DBType.NEO4J).infer_schema(result)


def test_a_database_matching_its_schema_has_no_drift() -> None:
    drift = compare_live_schema(
        _declared(),
        _observed(
            {"machine": ["key", "model"], "line": ["key"]},
            [("line", "uses", "machine")],
        ),
        db_flavor=DBType.NEO4J,
    )
    assert not drift.has_drift
    assert drift.missing_vertices == []
    assert drift.missing_properties == {}
    assert drift.missing_edges == []


def test_a_planted_property_is_reported_as_undeclared() -> None:
    drift = compare_live_schema(
        _declared(),
        _observed(
            {"machine": ["key", "model", "model_series"], "line": ["key"]},
            [("line", "uses", "machine")],
        ),
        db_flavor=DBType.NEO4J,
    )
    assert drift.has_drift
    assert drift.undeclared_properties == {"machine": ["model_series"]}


def test_types_edges_and_properties_are_compared_both_ways() -> None:
    drift = compare_live_schema(
        _declared(),
        _observed(
            {"machine": ["key"], "ghost": ["key"]},
            [("ghost", "haunts", "machine")],
        ),
        db_flavor=DBType.NEO4J,
        sampled=True,
    )
    assert drift.undeclared_vertices == ["ghost"]
    assert drift.missing_vertices == ["line"]
    assert drift.missing_properties == {"machine": ["model"]}
    assert drift.undeclared_edges == [("ghost", "haunts", "machine")]
    assert drift.missing_edges == [("line", "uses", "machine")]
    assert drift.sampled


def test_storage_names_are_mapped_back_to_logical_names() -> None:
    declared = _declared(vertex_storage_names={"machine": "Machine", "line": "Line"})
    drift = compare_live_schema(
        declared,
        _observed(
            {"Machine": ["key", "model"], "Line": ["key"]},
            [("Line", "uses", "Machine")],
        ),
        db_flavor=DBType.NEO4J,
    )
    assert not drift.has_drift
    assert drift.missing_vertices == []
    assert drift.missing_edges == []


def test_an_untyped_introspection_raises_no_type_drift() -> None:
    """Cypher introspection reports no property types; that is not drift."""
    observed = _observed(
        {"machine": ["key", "model"], "line": ["key"]}, [("line", "uses", "machine")]
    )
    assert all(
        field.type is None
        for vertex in observed.core_schema.vertex_config.vertices
        for field in vertex.properties
    )
    drift = compare_live_schema(_declared(), observed, db_flavor=DBType.NEO4J)
    assert not drift.has_drift
