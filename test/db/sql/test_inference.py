"""Inferring a graph from a relational source that is not PostgreSQL.

The classification heuristics were written against PostgreSQL and only ever
ran there. What these assert is that they were never actually PostgreSQL-
specific — given the same shape, SQLite yields the same graph.
"""

from __future__ import annotations

from typing import Any, cast

from graflo.db.sql import detect_edge_tables, detect_vertex_tables, introspect_schema
from graflo.db.sql.introspect import foreign_keys_in_column_order
from graflo.hq.sql_inferencer import SQLInferenceManager
from graflo.onto import DBType


def test_entity_tables_become_vertices(sqlite_provider) -> None:
    names = {t.name for t in detect_vertex_tables(sqlite_provider)}
    assert names == {"author", "field"}


def test_the_junction_table_becomes_an_edge(sqlite_provider) -> None:
    edges = detect_edge_tables(sqlite_provider)
    assert [e.name for e in edges] == ["author_field"]
    # The roles follow the order the columns were declared in, whatever order
    # the engine lists its constraints in.
    assert (edges[0].source_table, edges[0].target_table) == ("author", "field")
    assert (edges[0].source_column, edges[0].target_column) == (
        "author_id",
        "field_id",
    )


class _ConstraintNameOrdered:
    """A provider that lists foreign keys by constraint name, as catalogs do.

    ``purchases_product_id_fkey`` sorts before ``purchases_user_id_fkey``, so the
    second declared column arrives first.
    """

    _columns = {
        "users": ["id", "name"],
        "products": ["id", "title"],
        "purchases": ["id", "user_id", "product_id", "quantity"],
    }

    def get_tables(self, schema_name: str | None = None) -> list[dict[str, Any]]:
        return [{"table_name": name} for name in self._columns]

    def get_table_columns(
        self, table_name: str, schema_name: str | None = None
    ) -> list[dict[str, Any]]:
        return [{"name": name, "type": "integer"} for name in self._columns[table_name]]

    def get_primary_keys(
        self, table_name: str, schema_name: str | None = None
    ) -> list[str]:
        return ["id"]

    def get_unique_columns(
        self, table_name: str, schema_name: str | None = None
    ) -> list[str]:
        return []

    def get_foreign_keys(
        self, table_name: str, schema_name: str | None = None
    ) -> list[dict[str, Any]]:
        if table_name != "purchases":
            return []
        return [
            {
                "column": "product_id",
                "references_table": "products",
                "constraint_name": "purchases_product_id_fkey",
            },
            {
                "column": "user_id",
                "references_table": "users",
                "constraint_name": "purchases_user_id_fkey",
            },
        ]


def test_the_edge_source_is_the_first_declared_foreign_key() -> None:
    (edge,) = detect_edge_tables(cast(Any, _ConstraintNameOrdered()))
    assert (edge.source_table, edge.target_table) == ("users", "products")
    assert (edge.source_column, edge.target_column) == ("user_id", "product_id")
    assert [fk.column for fk in edge.foreign_keys] == ["user_id", "product_id"]


class TestForeignKeysInColumnOrder:
    COLUMNS = [{"name": n} for n in ("id", "a_1", "a_2", "b", "c")]

    def test_a_composite_key_moves_as_one_and_keeps_its_own_order(self) -> None:
        rows = [
            {"column": "b", "constraint_name": "fk_b"},
            {"column": "a_2", "constraint_name": "fk_a"},
            {"column": "a_1", "constraint_name": "fk_a"},
        ]
        assert [
            r["column"] for r in foreign_keys_in_column_order(rows, self.COLUMNS)
        ] == [
            "a_2",
            "a_1",
            "b",
        ]

    def test_rows_without_a_constraint_name_are_placed_one_by_one(self) -> None:
        rows = [{"column": "c"}, {"column": "b"}]
        assert [
            r["column"] for r in foreign_keys_in_column_order(rows, self.COLUMNS)
        ] == [
            "b",
            "c",
        ]

    def test_a_column_the_table_does_not_list_sorts_last(self) -> None:
        rows = [{"column": "ghost"}, {"column": "c"}]
        assert [
            r["column"] for r in foreign_keys_in_column_order(rows, self.COLUMNS)
        ] == [
            "c",
            "ghost",
        ]


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
