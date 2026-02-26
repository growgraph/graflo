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

from graflo.onto import DBType
from test.conftest import fetch_schema_obj
from graflo.hq.sanitizer import SchemaSanitizer


logger = logging.getLogger(__name__)


@pytest.fixture
def schema_with_reserved_words():
    schema_o = fetch_schema_obj("tigergraph-sanitize")
    return schema_o


@pytest.fixture
def schema_with_incompatible_edges():
    schema_o = fetch_schema_obj("tigergraph-sanitize-edges")
    return schema_o


def test_vertex_name_sanitization_for_tigergraph(schema_with_reserved_words):
    """Test that vertex names with reserved words are sanitized for TigerGraph."""
    schema = schema_with_reserved_words

    sanitizer = SchemaSanitizer(DBType.TIGERGRAPH)

    sanitized_schema = sanitizer.sanitize(schema)

    vertex_dbnames = [
        sanitized_schema.database_features.vertex_storage_name(v.name)
        for v in sanitized_schema.vertex_config.vertices
    ]
    assert "Package_vertex" in vertex_dbnames, (
        f"Expected 'package_vertex' in vertices after sanitization, got {vertex_dbnames}"
    )
    assert "package" not in vertex_dbnames, (
        f"Original reserved word 'package' should not be in vertices, got {vertex_dbnames}"
    )


def test_edges_sanitization_for_tigergraph(schema_with_incompatible_edges):
    """Test that vertex names with reserved words are sanitized for TigerGraph."""
    schema = schema_with_incompatible_edges

    sanitizer = SchemaSanitizer(DBType.TIGERGRAPH)

    sanitized_schema = sanitizer.sanitize(schema)

    # sanitized_schema.to_yaml_file(
    #     os.path.join(
    #         os.path.dirname(__file__),
    #         "../../config/schema/tigergraph-sanitize-edges.corrected.yaml",
    #     )
    # )

    assert sanitized_schema.resources[-1].root.actor.descendants[0].actor.t.map == {
        "container_name": "id"
    }

    assert sanitized_schema.vertex_config.vertices[-1].fields[0].name == "id"
    assert sanitized_schema.vertex_config.vertices[-1].identity[0] == "id"
    assert sanitized_schema.edge_config.edges[-2].relation_dbname == "package_relation"
    assert sanitized_schema.edge_config.edges[-1].relation_dbname == "box_relation"
