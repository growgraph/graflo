"""Inferring a graph from a relational source that is not PostgreSQL.

The classification heuristics were written against PostgreSQL and only ever
ran there. What these assert is that they were never actually PostgreSQL-
specific — given the same shape, SQLite yields the same graph.
"""

from __future__ import annotations

from graflo.db.sql import detect_edge_tables, detect_vertex_tables, introspect_schema
from graflo.hq.sql_inferencer import SQLInferenceManager
from graflo.onto import DBType


def test_entity_tables_become_vertices(sqlite_provider) -> None:
    names = {t.name for t in detect_vertex_tables(sqlite_provider)}
    assert names == {"author", "field"}


def test_the_junction_table_becomes_an_edge(sqlite_provider) -> None:
    edges = detect_edge_tables(sqlite_provider)
    assert [e.name for e in edges] == ["author_field"]
    # FK order is not semantically meaningful, so assert the pair, not the roles.
    assert {edges[0].source_table, edges[0].target_table} == {"author", "field"}
    assert {edges[0].source_column, edges[0].target_column} == {
        "author_id",
        "field_id",
    }


def test_the_junction_table_is_not_also_a_vertex(sqlite_provider) -> None:
    assert "author_field" not in {t.name for t in detect_vertex_tables(sqlite_provider)}


def test_raw_tables_carry_counts_and_samples(sqlite_provider) -> None:
    result = introspect_schema(sqlite_provider, include_raw_tables=True)
    by_name = {t.name: t for t in result.raw_tables}
    assert set(by_name) == {"author", "field", "author_field"}
    assert by_name["author"].row_count_estimate == 2
    sampled = {c.name: c.sample_values for c in by_name["author"].columns}
    assert "Ada Lovelace" in sampled["full_name"]


def test_a_full_manifest_is_inferred(sqlite_provider) -> None:
    """The end the whole path exists for: a Schema plus resources to load it."""
    manager = SQLInferenceManager(sqlite_provider, target_db_flavor=DBType.ARANGO)
    schema, ingestion = manager.infer_complete_schema()

    vertex_config = schema.core_schema.vertex_config
    assert {v.name for v in vertex_config.vertices} == {"author", "field"}
    assert vertex_config.identity_fields("author") == ["id"]

    fields = {f.name: f.type for f in vertex_config.vertices[0].properties}
    assert str(fields["hindex"]) == "INT", "declared INTEGER should not degrade"

    assert len(list(schema.core_schema.edge_config.values())) == 1
    assert {r.name for r in ingestion.resources} == {
        "author",
        "field",
        "author_field",
    }


def test_sampling_is_optional(sqlite_provider) -> None:
    """Inference without a provider still works — it just cannot refine types."""
    from graflo.db.postgres.schema_inference import PostgresSchemaInferencer

    result = introspect_schema(sqlite_provider)
    schema = PostgresSchemaInferencer(db_flavor=DBType.ARANGO).infer_schema(result)
    assert {v.name for v in schema.core_schema.vertex_config.vertices} == {
        "author",
        "field",
    }
