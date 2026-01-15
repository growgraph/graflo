"""Tests for reserved word sanitization in PostgreSQL schema inference.

This module tests that reserved words are properly sanitized when inferring schemas
for TigerGraph, including:
- Vertex name sanitization
- Attribute name sanitization
- Edge reference updates
- Resource apply list updates

Note: TigerGraph does NOT support quoted identifiers for reserved words (unlike PostgreSQL).
Therefore, we must sanitize reserved words by appending suffixes like "_vertex" for vertex
names and "_attr" for attribute names. This is different from PostgreSQL which allows
quoted identifiers like "SELECT" to use reserved words as column names.

These tests create Schema objects directly without requiring PostgreSQL connections,
since the sanitization logic is independent of the database source.
"""

import logging

import pytest

from graflo.architecture.edge import Edge, EdgeConfig, WeightConfig
from graflo.architecture.resource import Resource
from graflo.architecture.schema import Schema, SchemaMetadata
from graflo.architecture.vertex import Field, FieldType, Vertex, VertexConfig
from graflo.db.postgres.schema_inference import PostgresSchemaInferencer
from graflo.onto import DBFlavor

logger = logging.getLogger(__name__)


@pytest.fixture
def schema_with_reserved_words():
    """Create a Schema object with reserved words for testing."""
    # Create vertices with reserved words
    package_vertex = Vertex(
        name="package",
        fields=[
            Field(name="id", type=FieldType.INT),
            Field(name="SELECT", type=FieldType.STRING),  # Reserved word
            Field(name="FROM", type=FieldType.STRING),  # Reserved word
            Field(name="WHERE", type=FieldType.STRING),  # Reserved word
            Field(name="name", type=FieldType.STRING),
        ],
    )

    users_vertex = Vertex(
        name="users",
        fields=[
            Field(name="id", type=FieldType.INT),
            Field(name="name", type=FieldType.STRING),
        ],
    )

    vertex_config = VertexConfig(
        vertices=[package_vertex, users_vertex], db_flavor=DBFlavor.TIGERGRAPH
    )

    # Create edge with reserved word attributes
    package_users_edge = Edge(
        source="package",
        target="users",
        weights=WeightConfig(
            direct=[
                Field(name="SELECT", type=FieldType.STRING),  # Reserved word
                Field(name="FROM", type=FieldType.STRING),  # Reserved word
            ]
        ),
    )

    edge_config = EdgeConfig(edges=[package_users_edge])

    # Create resource with vertex reference
    package_resource = Resource(
        resource_name="package",
        apply=[{"vertex": "package"}],  # Will be sanitized
    )

    schema = Schema(
        general=SchemaMetadata(name="test_schema"),
        vertex_config=vertex_config,
        edge_config=edge_config,
        resources=[package_resource],
    )

    return schema


def test_vertex_name_sanitization_for_tigergraph(schema_with_reserved_words):
    """Test that vertex names with reserved words are sanitized for TigerGraph."""
    schema = schema_with_reserved_words

    # Create inferencer with TigerGraph flavor
    inferencer = PostgresSchemaInferencer(db_flavor=DBFlavor.TIGERGRAPH)

    # Sanitize the schema
    sanitized_schema = inferencer._sanitize_schema_attributes(schema)

    vertex_dbnames = [v.dbname for v in sanitized_schema.vertex_config.vertices]
    assert "package_vertex" in vertex_dbnames, (
        f"Expected 'package_vertex' in vertices after sanitization, got {vertex_dbnames}"
    )
    assert "package" not in vertex_dbnames, (
        f"Original reserved word 'package' should not be in vertices, got {vertex_dbnames}"
    )
