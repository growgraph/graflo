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
    version_vertex = Vertex(
        name="version",  # "version" is a TigerGraph reserved word
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
        vertices=[version_vertex, users_vertex], db_flavor=DBFlavor.TIGERGRAPH
    )

    # Create edge with reserved word attributes
    version_users_edge = Edge(
        source="version",  # Will be sanitized
        target="users",
        weights=WeightConfig(
            direct=[
                Field(name="SELECT", type=FieldType.STRING),  # Reserved word
                Field(name="FROM", type=FieldType.STRING),  # Reserved word
            ]
        ),
    )

    edge_config = EdgeConfig(edges=[version_users_edge])

    # Create resource with vertex reference
    version_resource = Resource(
        resource_name="version",
        apply=[{"vertex": "version"}],  # Will be sanitized
    )

    schema = Schema(
        general=SchemaMetadata(name="test_schema"),
        vertex_config=vertex_config,
        edge_config=edge_config,
        resources=[version_resource],
    )

    return schema


def test_vertex_name_sanitization_for_tigergraph(schema_with_reserved_words):
    """Test that vertex names with reserved words are sanitized for TigerGraph."""
    schema = schema_with_reserved_words

    # Create inferencer with TigerGraph flavor
    inferencer = PostgresSchemaInferencer(db_flavor=DBFlavor.TIGERGRAPH)

    # Sanitize the schema
    sanitized_schema = inferencer._sanitize_schema_attributes(schema)

    # Check that "version" vertex was sanitized to "version_vertex"
    vertex_names = [v.name for v in sanitized_schema.vertex_config.vertices]
    assert "version_vertex" in vertex_names, (
        f"Expected 'version_vertex' in vertices after sanitization, got {vertex_names}"
    )
    assert "version" not in vertex_names, (
        f"Original reserved word 'version' should not be in vertices, got {vertex_names}"
    )

    # Verify the sanitized vertex exists and has correct fields
    version_vertex = next(
        v for v in sanitized_schema.vertex_config.vertices if v.name == "version_vertex"
    )
    assert version_vertex is not None, "version_vertex should exist"

    # Check that attribute names were also sanitized
    field_names = [f.name for f in version_vertex.fields]
    assert "SELECT_attr" in field_names, (
        f"Expected 'SELECT_attr' in fields after sanitization, got {field_names}"
    )
    assert "FROM_attr" in field_names, (
        f"Expected 'FROM_attr' in fields after sanitization, got {field_names}"
    )
    assert "WHERE_attr" in field_names, (
        f"Expected 'WHERE_attr' in fields after sanitization, got {field_names}"
    )
    assert "SELECT" not in field_names, (
        f"Original reserved word 'SELECT' should not be in fields, got {field_names}"
    )


def test_edge_references_updated_after_vertex_sanitization(schema_with_reserved_words):
    """Test that edge source/target references are updated when vertex names are sanitized."""
    schema = schema_with_reserved_words

    inferencer = PostgresSchemaInferencer(db_flavor=DBFlavor.TIGERGRAPH)
    sanitized_schema = inferencer._sanitize_schema_attributes(schema)

    # Find edge connecting version_vertex to users
    edges = list(sanitized_schema.edge_config.edges_list())
    version_users_edge = next(
        (
            e
            for e in edges
            if "version_vertex" in (e.source, e.target)
            and "users" in (e.source, e.target)
        ),
        None,
    )

    assert version_users_edge is not None, (
        "Edge between version_vertex and users should exist"
    )
    assert (
        version_users_edge.source == "version_vertex"
        or version_users_edge.target == "version_vertex"
    ), (
        f"Edge should reference 'version_vertex', got source={version_users_edge.source}, "
        f"target={version_users_edge.target}"
    )
    assert "version" not in (
        version_users_edge.source,
        version_users_edge.target,
    ), (
        f"Edge should not reference original 'version' name, "
        f"got source={version_users_edge.source}, target={version_users_edge.target}"
    )

    # Check that edge weight attributes were sanitized
    assert version_users_edge.weights is not None, "Edge should have weights"
    assert version_users_edge.weights.direct is not None, (
        "Edge should have direct weights"
    )
    weight_field_names = [f.name for f in version_users_edge.weights.direct]
    assert "SELECT_attr" in weight_field_names, (
        f"Expected 'SELECT_attr' in weight fields, got {weight_field_names}"
    )
    assert "FROM_attr" in weight_field_names, (
        f"Expected 'FROM_attr' in weight fields, got {weight_field_names}"
    )


def test_resource_apply_lists_updated_after_vertex_sanitization(
    schema_with_reserved_words,
):
    """Test that resource apply lists reference sanitized vertex names."""
    schema = schema_with_reserved_words

    inferencer = PostgresSchemaInferencer(db_flavor=DBFlavor.TIGERGRAPH)
    sanitized_schema = inferencer._sanitize_schema_attributes(schema)

    # Find resource for version table
    version_resource = next(
        (r for r in sanitized_schema.resources if r.resource_name == "version"), None
    )
    assert version_resource is not None, "version resource should exist"

    # Check that apply list references sanitized vertex name
    apply_str = str(version_resource.apply)
    assert "version_vertex" in apply_str, (
        f"Resource apply list should reference 'version_vertex', got {apply_str}"
    )
    # Check the actual apply item
    assert len(version_resource.apply) > 0, "Resource should have apply items"
    apply_item = version_resource.apply[0]
    assert isinstance(apply_item, dict), "Apply item should be a dict"
    assert apply_item.get("vertex") == "version_vertex", (
        f"Apply item should reference 'version_vertex', got {apply_item}"
    )


def test_arango_no_sanitization(schema_with_reserved_words):
    """Test that ArangoDB flavor does not sanitize names (no reserved words)."""
    schema = schema_with_reserved_words

    # Change schema to ArangoDB flavor
    schema.vertex_config.db_flavor = DBFlavor.ARANGO
    schema.edge_config.db_flavor = DBFlavor.ARANGO

    inferencer = PostgresSchemaInferencer(db_flavor=DBFlavor.ARANGO)
    sanitized_schema = inferencer._sanitize_schema_attributes(schema)

    # Check that "version" vertex name is NOT sanitized for ArangoDB
    vertex_names = [v.name for v in sanitized_schema.vertex_config.vertices]
    assert "version" in vertex_names, (
        f"Expected 'version' in vertices for ArangoDB (no sanitization), got {vertex_names}"
    )
    assert "version_vertex" not in vertex_names, (
        f"Should not have 'version_vertex' for ArangoDB, got {vertex_names}"
    )

    # Check that attribute names are NOT sanitized for ArangoDB
    version_vertex = next(
        v for v in sanitized_schema.vertex_config.vertices if v.name == "version"
    )
    field_names = [f.name for f in version_vertex.fields]
    assert "SELECT" in field_names, (
        f"Expected 'SELECT' in fields for ArangoDB (no sanitization), got {field_names}"
    )
    assert "SELECT_attr" not in field_names, (
        f"Should not have 'SELECT_attr' for ArangoDB, got {field_names}"
    )


def test_multiple_reserved_words_sanitization(schema_with_reserved_words):
    """Test that multiple reserved words are all sanitized correctly."""
    schema = schema_with_reserved_words

    inferencer = PostgresSchemaInferencer(db_flavor=DBFlavor.TIGERGRAPH)
    sanitized_schema = inferencer._sanitize_schema_attributes(schema)

    # Check vertex name sanitization
    vertex_names = [v.name for v in sanitized_schema.vertex_config.vertices]
    assert "version_vertex" in vertex_names, (
        f"Expected 'version_vertex' after sanitization, got {vertex_names}"
    )

    # Check attribute name sanitization
    version_vertex = next(
        v for v in sanitized_schema.vertex_config.vertices if v.name == "version_vertex"
    )
    field_names = [f.name for f in version_vertex.fields]

    # All reserved word attributes should be sanitized
    reserved_attrs = ["SELECT", "FROM", "WHERE"]
    sanitized_attrs = ["SELECT_attr", "FROM_attr", "WHERE_attr"]

    for reserved, sanitized in zip(reserved_attrs, sanitized_attrs):
        assert sanitized in field_names, (
            f"Expected '{sanitized}' in fields after sanitization, got {field_names}"
        )
        assert reserved not in field_names, (
            f"Original reserved word '{reserved}' should not be in fields, got {field_names}"
        )


def test_vertex_config_internal_mappings_updated(schema_with_reserved_words):
    """Test that VertexConfig internal mappings are updated after sanitization."""
    schema = schema_with_reserved_words

    inferencer = PostgresSchemaInferencer(db_flavor=DBFlavor.TIGERGRAPH)
    sanitized_schema = inferencer._sanitize_schema_attributes(schema)

    # Check that _vertices_map uses sanitized names
    assert "version_vertex" in sanitized_schema.vertex_config._vertices_map, (
        "VertexConfig._vertices_map should contain sanitized vertex name"
    )
    assert "version" not in sanitized_schema.vertex_config._vertices_map, (
        "VertexConfig._vertices_map should not contain original reserved word"
    )

    # Check that vertex_set uses sanitized names
    assert "version_vertex" in sanitized_schema.vertex_config.vertex_set, (
        "VertexConfig.vertex_set should contain sanitized vertex name"
    )
    assert "version" not in sanitized_schema.vertex_config.vertex_set, (
        "VertexConfig.vertex_set should not contain original reserved word"
    )

    # Verify we can look up the vertex by sanitized name
    version_vertex = sanitized_schema.vertex_config._vertices_map["version_vertex"]
    assert version_vertex is not None, (
        "Should be able to look up vertex by sanitized name"
    )
    assert version_vertex.name == "version_vertex", (
        f"Vertex name should be 'version_vertex', got {version_vertex.name}"
    )


def test_edge_finish_init_after_sanitization(schema_with_reserved_words):
    """Test that edges are properly re-initialized after vertex name sanitization."""
    schema = schema_with_reserved_words

    inferencer = PostgresSchemaInferencer(db_flavor=DBFlavor.TIGERGRAPH)
    sanitized_schema = inferencer._sanitize_schema_attributes(schema)

    # Find edge connecting version_vertex to users
    edges = list(sanitized_schema.edge_config.edges_list())
    version_users_edge = next(
        (
            e
            for e in edges
            if "version_vertex" in (e.source, e.target)
            and "users" in (e.source, e.target)
        ),
        None,
    )

    assert version_users_edge is not None, "Edge should exist"

    # After finish_init (called in _sanitize_schema_attributes), _source and _target should be set correctly
    assert version_users_edge._source is not None, (
        "Edge._source should be set after finish_init"
    )
    assert version_users_edge._target is not None, (
        "Edge._target should be set after finish_init"
    )

    # The _source and _target should reference the sanitized vertex name (via dbname lookup)
    # Since dbname defaults to name, they should be the sanitized names
    assert "version_vertex" in (
        version_users_edge._source,
        version_users_edge._target,
    ), (
        f"Edge internal references should use sanitized vertex name, "
        f"got _source={version_users_edge._source}, _target={version_users_edge._target}"
    )


def test_tigergraph_schema_validation_with_reserved_words(schema_with_reserved_words):
    """Test that sanitized schema has no reserved words."""
    schema = schema_with_reserved_words

    inferencer = PostgresSchemaInferencer(db_flavor=DBFlavor.TIGERGRAPH)
    sanitized_schema = inferencer._sanitize_schema_attributes(schema)

    # Verify schema structure is valid
    assert sanitized_schema is not None, "Schema should be sanitized"
    assert sanitized_schema.vertex_config is not None, (
        "Schema should have vertex_config"
    )
    assert sanitized_schema.edge_config is not None, "Schema should have edge_config"

    # Verify all vertex names are sanitized (no reserved words)
    vertex_names = [v.name for v in sanitized_schema.vertex_config.vertices]
    reserved_words = inferencer.reserved_words
    for vertex_name in vertex_names:
        assert vertex_name.upper() not in reserved_words, (
            f"Vertex name '{vertex_name}' should not be a reserved word"
        )

    # Verify all edge source/target names are sanitized
    edges = list(sanitized_schema.edge_config.edges_list())
    for edge in edges:
        assert edge.source.upper() not in reserved_words, (
            f"Edge source '{edge.source}' should not be a reserved word"
        )
        assert edge.target.upper() not in reserved_words, (
            f"Edge target '{edge.target}' should not be a reserved word"
        )

    # Verify all attribute names are sanitized
    for vertex in sanitized_schema.vertex_config.vertices:
        for field in vertex.fields:
            assert field.name.upper() not in reserved_words, (
                f"Field name '{field.name}' in vertex '{vertex.name}' should not be a reserved word"
            )

    # Verify edge weight names are sanitized
    for edge in edges:
        if edge.weights and edge.weights.direct:
            for weight_field in edge.weights.direct:
                assert weight_field.name.upper() not in reserved_words, (
                    f"Weight field name '{weight_field.name}' in edge "
                    f"'{edge.source}' -> '{edge.target}' should not be a reserved word"
                )

    logger.info("Schema validation passed - all names are sanitized for TigerGraph")


def test_indirect_edge_by_reference_sanitization():
    """Test that indirect edge 'by' references are sanitized."""
    # Create schema with indirect edge
    version_vertex = Vertex(
        name="version",
        fields=[Field(name="id", type=FieldType.INT)],
    )

    users_vertex = Vertex(
        name="users",
        fields=[Field(name="id", type=FieldType.INT)],
    )

    vertex_config = VertexConfig(
        vertices=[version_vertex, users_vertex], db_flavor=DBFlavor.TIGERGRAPH
    )

    # Create indirect edge with 'by' referencing reserved word vertex
    indirect_edge = Edge(
        source="users",
        target="users",
        by="version",  # Will be sanitized
    )

    edge_config = EdgeConfig(edges=[indirect_edge])

    schema = Schema(
        general=SchemaMetadata(name="test_schema"),
        vertex_config=vertex_config,
        edge_config=edge_config,
        resources=[],
    )

    inferencer = PostgresSchemaInferencer(db_flavor=DBFlavor.TIGERGRAPH)
    sanitized_schema = inferencer._sanitize_schema_attributes(schema)

    # Find the indirect edge
    edges = list(sanitized_schema.edge_config.edges_list())
    indirect_edge_sanitized = edges[0]

    # Check that 'by' reference was sanitized
    assert indirect_edge_sanitized.by == "version_vertex", (
        f"Indirect edge 'by' should reference 'version_vertex', got {indirect_edge_sanitized.by}"
    )
    assert indirect_edge_sanitized.by != "version", (
        f"Indirect edge 'by' should not reference original 'version', got {indirect_edge_sanitized.by}"
    )
