"""Tests for PostgreSQL graph target helpers."""

from __future__ import annotations

from graflo.db.postgres.target_write import edge_table_name, vertex_table_name


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
