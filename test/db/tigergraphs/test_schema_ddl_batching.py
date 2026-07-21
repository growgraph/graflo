from __future__ import annotations

from types import SimpleNamespace

from graflo.db.tigergraph.conn import TigerGraphConnection
from graflo.db.tigergraph.schema_ddl import SchemaDdlBuilder


def _ddl_builder() -> SchemaDdlBuilder:
    conn = TigerGraphConnection.__new__(TigerGraphConnection)
    conn.config = SimpleNamespace(max_job_size=15000)
    return SchemaDdlBuilder(conn)


def _schema_change_batches(
    builder: SchemaDdlBuilder,
    *,
    vertex_stmts: list[str],
    edge_stmts: list[str],
    graph_name: str,
    max_job_size: int,
) -> list[list[str]]:
    max_size = max_job_size
    return builder._batch_schema_statements(
        vertex_stmts, graph_name, max_size
    ) + builder._batch_schema_statements(edge_stmts, graph_name, max_size)


def test_batch_schema_statements_preserves_statement_order() -> None:
    builder = _ddl_builder()
    stmts = ["stmt_a" + "x" * 100, "stmt_b", "stmt_c" + "y" * 200]

    batches = builder._batch_schema_statements(stmts, "g", max_job_size=250)
    flattened = [stmt for batch in batches for stmt in batch]

    assert flattened == stmts


def test_vertex_batches_precede_edge_batches_when_split() -> None:
    builder = _ddl_builder()
    vertex_stmts = [
        "vertex:1",
        "vertex:2" + "x" * 5000,
        "vertex:3",
    ]
    edge_stmts = ["edge:1", "edge:2"]

    batches = _schema_change_batches(
        builder,
        vertex_stmts=vertex_stmts,
        edge_stmts=edge_stmts,
        graph_name="g",
        max_job_size=3000,
    )
    flattened = [stmt for batch in batches for stmt in batch]

    first_edge_idx = next(
        i for i, stmt in enumerate(flattened) if stmt.startswith("edge:")
    )
    last_vertex_idx = max(
        i for i, stmt in enumerate(flattened) if stmt.startswith("vertex:")
    )

    assert last_vertex_idx < first_edge_idx
    assert flattened[: len(vertex_stmts)] == vertex_stmts
