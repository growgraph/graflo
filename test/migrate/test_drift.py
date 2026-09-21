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
    "metadata": {"name": "estate", "version": "1.0.0"},
    "graph": {
        "vertex_config": {
            "vertices": [
                {
                    "name": "server",
                    "properties": [
                        {"name": "key", "type": "STRING"},
                        {"name": "os", "type": "STRING"},
                    ],
                    "identity": ["key"],
                },
                {
                    "name": "app",
                    "properties": [{"name": "key", "type": "STRING"}],
                    "identity": ["key"],
                },
            ]
        },
        "edge_config": {
            "edges": [{"source": "app", "target": "server", "relation": "runs_on"}]
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
        name="estate",
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
            {"server": ["key", "os"], "app": ["key"]},
            [("app", "runs_on", "server")],
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
            {"server": ["key", "os", "os_family"], "app": ["key"]},
            [("app", "runs_on", "server")],
        ),
        db_flavor=DBType.NEO4J,
    )
    assert drift.has_drift
    assert drift.undeclared_properties == {"server": ["os_family"]}


def test_types_edges_and_properties_are_compared_both_ways() -> None:
    drift = compare_live_schema(
        _declared(),
        _observed(
            {"server": ["key"], "ghost": ["key"]},
            [("ghost", "haunts", "server")],
        ),
        db_flavor=DBType.NEO4J,
        sampled=True,
    )
    assert drift.undeclared_vertices == ["ghost"]
    assert drift.missing_vertices == ["app"]
    assert drift.missing_properties == {"server": ["os"]}
    assert drift.undeclared_edges == [("ghost", "haunts", "server")]
    assert drift.missing_edges == [("app", "runs_on", "server")]
    assert drift.sampled


def test_storage_names_are_mapped_back_to_logical_names() -> None:
    declared = _declared(vertex_storage_names={"server": "Server", "app": "App"})
    drift = compare_live_schema(
        declared,
        _observed(
            {"Server": ["key", "os"], "App": ["key"]},
            [("App", "runs_on", "Server")],
        ),
        db_flavor=DBType.NEO4J,
    )
    assert not drift.has_drift
    assert drift.missing_vertices == []
    assert drift.missing_edges == []


def test_an_untyped_introspection_raises_no_type_drift() -> None:
    """Cypher introspection reports no property types; that is not drift."""
    observed = _observed(
        {"server": ["key", "os"], "app": ["key"]}, [("app", "runs_on", "server")]
    )
    assert all(
        field.type is None
        for vertex in observed.core_schema.vertex_config.vertices
        for field in vertex.properties
    )
    drift = compare_live_schema(_declared(), observed, db_flavor=DBType.NEO4J)
    assert not drift.has_drift
