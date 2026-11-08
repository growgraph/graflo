"""Memgraph bulk load: indexes first, create-only writes, conflicts retried."""

from __future__ import annotations

from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from graflo.db.bulk import BulkEdgeChunk, BulkVertexChunk
from graflo.db.cypher.bulk import create_nodes_query, scanned_counts
from graflo.db.memgraph import conn as memgraph
from graflo.db.memgraph.conn import MemgraphConnection

SCHEMA: Any = None  # the writes read no schema


class _Conflict(Exception):
    pass


def _connection(monkeypatch: pytest.MonkeyPatch, failures: int = 0) -> Any:
    monkeypatch.setattr(memgraph, "_DATABASE_ERROR", _Conflict)
    monkeypatch.setattr(memgraph.time, "sleep", lambda _: None)
    conn = cast(Any, MemgraphConnection.__new__(MemgraphConnection))
    calls: list[tuple[str, dict[str, Any]]] = []
    left = [failures]

    def execute(query: str, **kwargs: Any) -> Any:
        calls.append((query, kwargs))
        if left[0]:
            left[0] -= 1
            raise _Conflict("Cannot resolve conflicting transactions")
        return MagicMock(columns=[], result_set=[])

    conn.execute = execute
    conn.calls = calls
    conn.upsert_docs_batch = MagicMock()
    conn.insert_edges_batch = MagicMock()
    return conn


def _vertices(*, replay: bool = False) -> BulkVertexChunk:
    return BulkVertexChunk(
        chunk_id="v:0",
        class_name="Person",
        match_keys=["pid"],
        rows=[{"pid": "p1", "name": None}],
        replay=replay,
    )


def test_a_vertex_chunk_is_created_and_a_conflict_retried(monkeypatch) -> None:
    conn = _connection(monkeypatch, failures=2)

    conn.bulk_write_vertices(SCHEMA, _vertices())

    assert [q for q, _ in conn.calls] == [create_nodes_query("Person")] * 3
    assert conn.calls[-1][1] == {"rows": [{"pid": "p1", "name": None}]}


def test_a_conflict_that_outlasts_the_attempts_raises(monkeypatch) -> None:
    conn = _connection(monkeypatch, failures=MemgraphConnection.BULK_ATTEMPTS)

    with pytest.raises(_Conflict):
        conn.bulk_write_vertices(SCHEMA, _vertices())


def test_a_replay_merges_through_the_upsert_path(monkeypatch) -> None:
    conn = _connection(monkeypatch)
    conn.bulk_write_vertices(SCHEMA, _vertices(replay=True))
    conn.upsert_docs_batch.assert_called_once()

    conn.bulk_write_edges(
        SCHEMA,
        BulkEdgeChunk(
            chunk_id="e:0",
            source_class="Person",
            target_class="City",
            relation_name="lives_in",
            match_keys_source=["pid"],
            match_keys_target=["cid"],
            collection_name=None,
            merge_properties=["role"],
            rows=[({"pid": "p1"}, {"cid": "c1"}, {"role": "home"})],
            replay=True,
        ),
    )
    assert conn.insert_edges_batch.call_args.kwargs == {
        "relationship_merge_properties": ["role"]
    }
    assert conn.calls == []


def test_counts_are_scanned_per_label_and_per_endpoint_pair() -> None:
    def run(query: str) -> list[dict[str, Any]]:
        if query.startswith("MATCH (n)"):
            return [{"label": "Person", "c": 2}, {"label": "City", "c": 1}]
        return [{"s": "Person", "r": "lives_in", "t": "City", "c": 2}]

    counts = scanned_counts(run)

    assert counts.vertices == {"Person": 2, "City": 1}
    assert counts.edges == {("Person", "lives_in", "City"): 2}
