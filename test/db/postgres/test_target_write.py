"""Tests for PostgreSQL graph target helpers."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Self

from psycopg2 import sql

from graflo.db.postgres.target_write import (
    PostgresTargetWriteMixin,
    edge_table_name,
    split_edge_table_name,
    vertex_table_name,
)


def test_vertex_table_name() -> None:
    assert vertex_table_name("person") == "person"


def test_edge_table_name_with_relation() -> None:
    assert edge_table_name("person", "department", "works_in") == (
        "person_department_works_in_edges"
    )


def test_edge_table_name_without_relation() -> None:
    assert edge_table_name("person", "department", None) == (
        "person_department_relates_edges"
    )


def test_split_edge_table_name_round_trips() -> None:
    vertices = ["person", "department"]
    table = edge_table_name("person", "department", "works_in")
    assert split_edge_table_name(table, vertices) == (
        "person",
        "department",
        "works_in",
    )


def test_split_edge_table_name_handles_underscored_components() -> None:
    vertices = ["legal_entity", "cost_center"]
    table = edge_table_name("legal_entity", "cost_center", "charged_to")
    assert split_edge_table_name(table, vertices) == (
        "legal_entity",
        "cost_center",
        "charged_to",
    )


def test_split_edge_table_name_prefers_the_longest_vertex_match() -> None:
    """`person` is a prefix of `person_group`; the longer name must win."""
    vertices = ["person", "person_group"]
    table = edge_table_name("person_group", "person", "contains")
    assert split_edge_table_name(table, vertices) == (
        "person_group",
        "person",
        "contains",
    )


def test_split_edge_table_name_rejects_vertex_tables() -> None:
    assert split_edge_table_name("person", ["person"]) is None


def test_split_edge_table_name_returns_none_for_unknown_endpoints() -> None:
    """Unparseable is not a licence to guess -- callers must leave it alone."""
    assert split_edge_table_name("a_b_c_edges", ["person"]) is None


class _FakeCursor:
    def __init__(self, sink: list[Any]) -> None:
        self._sink = sink

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, query: Any, params: Any = None) -> None:
        self._sink.append(query)


class _FakeConn:
    def __init__(self) -> None:
        self.executed: list[Any] = []
        self.commits = 0

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self.executed)

    def commit(self) -> None:
        self.commits += 1

    def close(self) -> None:
        return None

    def rollback(self) -> None:
        return None


class _Target(PostgresTargetWriteMixin):
    """Minimal harness exposing only what `delete_graph_structure` touches."""

    def __init__(self, present: list[str]) -> None:
        self._fake = _FakeConn()
        self.conn = self._fake
        self.config = SimpleNamespace(schema_name="gf_test")
        self._present = present

    def get_tables(self, schema_name: str | None = None) -> list[dict[str, Any]]:
        return [{"table_name": t} for t in self._present]

    def dropped(self) -> set[str]:
        names: set[str] = set()
        for query in self._fake.executed:
            idents = [p for p in query.seq if isinstance(p, sql.Identifier)]
            names.add(idents[-1].strings[0])
        return names


def test_delete_graph_structure_drops_incident_edge_tables() -> None:
    """Regression: edge tables outlived every vertex drop and accumulated.

    PostgreSQL keeps edges in freestanding tables with no foreign key back to
    the vertex table, so nothing cascaded. Every other backend detaches them.
    """
    target = _Target(
        [
            "node",
            "other",
            "node_node_links_edges",
            "node_other_owns_edges",
            "other_other_peers_edges",
        ]
    )
    target.delete_graph_structure(vertex_types=("node",), delete_all=False)
    assert target.dropped() == {
        "node",
        "node_node_links_edges",
        "node_other_owns_edges",
    }


def test_delete_graph_structure_leaves_unrelated_edge_tables() -> None:
    target = _Target(["node", "other", "other_other_peers_edges"])
    target.delete_graph_structure(vertex_types=("node",), delete_all=False)
    assert target.dropped() == {"node"}


def test_delete_graph_structure_delete_all_drops_everything() -> None:
    present = ["node", "other", "node_other_owns_edges"]
    target = _Target(present)
    target.delete_graph_structure(delete_all=True)
    assert target.dropped() == set(present)
