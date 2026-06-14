"""Tests for graph schema introspection utilities."""

from __future__ import annotations

from graflo.architecture.schema import Schema
from graflo.db.graph_introspection import (
    GraphEdgeIntrospection,
    GraphIntrospectionResult,
    GraphSchemaInferencer,
    GraphVertexIntrospection,
    infer_identity_fields,
    strip_internal_properties,
)
from graflo.onto import DBType


def test_infer_identity_fields_prefers_id() -> None:
    assert infer_identity_fields(["name", "id", "email"]) == ["id"]


def test_infer_identity_fields_fallback_first_property() -> None:
    assert infer_identity_fields(["name", "email"]) == ["name"]


def test_strip_internal_properties() -> None:
    doc = {"id": "1", "_id": "person/1", "_rev": "abc", "name": "Alice"}
    assert strip_internal_properties(doc) == {"id": "1", "name": "Alice"}


def test_graph_schema_inferencer_builds_schema() -> None:
    introspection = GraphIntrospectionResult(
        name="demo",
        vertices=[
            GraphVertexIntrospection(
                name="person",
                properties=["id", "name"],
                identity=["id"],
            ),
            GraphVertexIntrospection(
                name="department",
                properties=["id", "title"],
                identity=["id"],
            ),
        ],
        edges=[
            GraphEdgeIntrospection(
                source="person",
                target="department",
                relation="works_in",
                properties=["since"],
            )
        ],
    )
    schema = GraphSchemaInferencer(db_flavor=DBType.NEO4J).infer_schema(introspection)
    assert isinstance(schema, Schema)
    assert schema.metadata.name == "demo"
    assert len(schema.core_schema.vertex_config.vertices) == 2
    assert ("person", "department", "works_in") in schema.core_schema.edge_config
