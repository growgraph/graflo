"""What SQL schema inference makes of a relational schema, read from SQLite."""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import create_engine

from graflo.architecture.contract.bindings import Bindings, TableConnector
from graflo.architecture.onto_sql import ForeignKeyInfo, VertexTableInfo
from graflo.db.sql.alchemy import SqlAlchemyMetadataProvider
from graflo.db.sql.introspect import infer_reference_edges, introspect_schema
from graflo.hq.auto_join import enrich_edge_connector_with_joins
from graflo.hq.sql_inferencer import SQLInferenceArtifacts, SQLInferenceManager

DDL = """
CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE products (id INTEGER PRIMARY KEY, title TEXT NOT NULL);
CREATE TABLE employees (
    id INTEGER PRIMARY KEY,
    name TEXT,
    manager_id INTEGER REFERENCES employees(id)
);
CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    placed_at TEXT
);
CREATE TABLE tickets (
    id INTEGER PRIMARY KEY,
    title TEXT,
    opened_by INTEGER REFERENCES employees(id),
    closed_by INTEGER REFERENCES employees(id),
    customer_id INTEGER REFERENCES customers(id)
);
CREATE TABLE purchases (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    product_id INTEGER NOT NULL REFERENCES products(id),
    quantity INTEGER
);
CREATE TABLE audit_log (message TEXT, logged_at TEXT);
CREATE TABLE tags (id INTEGER PRIMARY KEY);
"""


@pytest.fixture(scope="module")
def provider(
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[SqlAlchemyMetadataProvider]:
    path: Path = tmp_path_factory.mktemp("sql_inference") / "source.db"
    connection = sqlite3.connect(path)
    connection.executescript(DDL)
    connection.commit()
    connection.close()
    engine = create_engine(f"sqlite:///{path}")
    try:
        yield SqlAlchemyMetadataProvider(engine)
    finally:
        engine.dispose()


@pytest.fixture(scope="module")
def inferred(provider: SqlAlchemyMetadataProvider) -> SQLInferenceArtifacts:
    return SQLInferenceManager(provider).infer_artifacts()


def _edge_ids(artifacts: SQLInferenceArtifacts) -> set[tuple[str, str, str | None]]:
    return {
        (e.source, e.target, e.relation)
        for e in artifacts.schema.core_schema.edge_config.edges
    }


def _ids(entities, edge_id: tuple[str, str, str]) -> list[tuple[int, int]]:
    return [(source["id"], target["id"]) for source, target, _ in entities[edge_id]]


class TestSkippedTables:
    def test_a_table_left_out_is_named_with_the_reason(self, provider) -> None:
        result = introspect_schema(provider)

        assert {t.name: t.reason for t in result.skipped_tables} == {
            "audit_log": "no primary key",
            "tags": "no columns besides its keys",
        }

    def test_a_table_left_out_is_logged(
        self, provider, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING, logger="graflo.db.sql.introspect"):
            introspect_schema(provider)

        assert "'audit_log'" in caplog.text
        assert "no primary key" in caplog.text

    def test_every_table_is_a_vertex_an_edge_or_skipped(self, provider) -> None:
        result = introspect_schema(provider)

        accounted = (
            [t.name for t in result.vertex_tables]
            + [t.name for t in result.edge_tables]
            + [t.name for t in result.skipped_tables]
        )
        assert sorted(accounted) == sorted(
            t["table_name"] for t in provider.get_tables(None)
        )


class TestReferenceEdges:
    def test_a_foreign_key_in_an_entity_table_is_an_edge(self, inferred) -> None:
        assert {
            ("orders", "customers", "customer"),
            ("employees", "employees", "manager"),
            ("tickets", "employees", "opened_by"),
            ("tickets", "employees", "closed_by"),
            ("tickets", "customers", "customer"),
        } <= _edge_ids(inferred)

    def test_a_row_is_linked_to_the_row_it_refers_to(self, inferred) -> None:
        orders = inferred.ingestion_model.fetch_resource("orders")

        entities = orders({"id": 1, "customer_id": 7, "placed_at": "2020-01-01"})

        assert _ids(entities, ("orders", "customers", "customer")) == [(1, 7)]

    def test_two_references_to_one_table_keep_their_own_rows(self, inferred) -> None:
        tickets = inferred.ingestion_model.fetch_resource("tickets")

        entities = tickets(
            {"id": 1, "title": "t", "opened_by": 3, "closed_by": 4, "customer_id": 7}
        )

        assert _ids(entities, ("tickets", "employees", "opened_by")) == [(1, 3)]
        assert _ids(entities, ("tickets", "employees", "closed_by")) == [(1, 4)]
        assert _ids(entities, ("tickets", "customers", "customer")) == [(1, 7)]

    def test_a_reference_to_the_same_table_starts_at_the_row(self, inferred) -> None:
        employees = inferred.ingestion_model.fetch_resource("employees")

        entities = employees({"id": 2, "name": "Bo", "manager_id": 1})

        assert _ids(entities, ("employees", "employees", "manager")) == [(2, 1)]

    def test_an_empty_reference_writes_no_edge(self, inferred) -> None:
        employees = inferred.ingestion_model.fetch_resource("employees")

        entities = employees({"id": 1, "name": "Al", "manager_id": None})

        assert entities["employees", "employees", "manager"] == []
        assert [v["id"] for v in entities["employees"]] == [1]

    def test_a_table_without_references_keeps_a_one_step_resource(
        self, inferred
    ) -> None:
        customers = inferred.ingestion_model.fetch_resource_config("customers")

        assert customers.pipeline == [{"vertex": "customers"}]

    def test_a_reference_to_a_column_that_is_not_the_key_is_no_edge(self) -> None:
        tables = [
            VertexTableInfo(
                name="users",
                schema_name="",
                columns=[],
                primary_key=["id"],
                foreign_keys=[],
            ),
            VertexTableInfo(
                name="logins",
                schema_name="",
                columns=[],
                primary_key=["id"],
                foreign_keys=[
                    ForeignKeyInfo(
                        column="user_email",
                        references_table="users",
                        references_column="email",
                    )
                ],
            ),
        ]

        assert infer_reference_edges(tables) == []

    def test_a_query_over_a_table_with_references_is_not_joined(self, inferred) -> None:
        connector = TableConnector(table_name="orders")
        bindings = Bindings()
        for table in ("orders", "customers"):
            table_connector = (
                connector if table == "orders" else TableConnector(table_name=table)
            )
            bindings.add_connector(table_connector)
            bindings.bind_resource(table, table_connector)

        enrich_edge_connector_with_joins(
            resource=inferred.ingestion_model.fetch_resource("orders"),
            connector=connector,
            bindings=bindings,
            vertex_config=inferred.schema.core_schema.vertex_config,
        )

        assert connector.joins == []
        assert connector.filters == []


class TestEntityTables:
    def test_a_table_with_two_foreign_keys_is_an_edge_by_default(
        self, inferred
    ) -> None:
        vertices = inferred.schema.core_schema.vertex_config.vertex_set

        assert "purchases" not in vertices
        assert any(
            (source, target) == ("customers", "products")
            for source, target, _ in _edge_ids(inferred)
        )

    def test_a_table_named_as_an_entity_is_a_vertex_with_its_references(
        self, provider
    ) -> None:
        artifacts = SQLInferenceManager(provider).infer_artifacts(
            entity_tables=["purchases"]
        )

        vertex_config = artifacts.schema.core_schema.vertex_config
        assert vertex_config.identity_fields("purchases") == ["id"]
        assert "quantity" in vertex_config.property_names("purchases")
        edges = _edge_ids(artifacts)
        assert ("purchases", "customers", "customer") in edges
        assert ("purchases", "products", "product") in edges
        assert not any(
            (source, target) == ("customers", "products") for source, target, _ in edges
        )

        purchases = artifacts.ingestion_model.fetch_resource("purchases")
        entities = purchases(
            {"id": 1, "customer_id": 7, "product_id": 9, "quantity": 2}
        )
        assert _ids(entities, ("purchases", "customers", "customer")) == [(1, 7)]
        assert _ids(entities, ("purchases", "products", "product")) == [(1, 9)]

    def test_an_unknown_entity_table_is_refused(self, provider) -> None:
        with pytest.raises(ValueError, match="'nowhere'"):
            introspect_schema(provider, entity_tables=["nowhere"])

    def test_an_entity_table_needs_a_primary_key(self, provider) -> None:
        with pytest.raises(ValueError, match="'audit_log'.*primary key"):
            introspect_schema(provider, entity_tables=["audit_log"])
