"""FalkorDB bulk load: the GRAPH.BULK wire format, and the writer's resume rules."""

from __future__ import annotations

from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest

from graflo.architecture.schema import Schema
from graflo.connections.bulk import BulkConfig
from graflo.connections.onto import FalkordbConfig
from graflo.db.bulk import BulkEdgeChunk, BulkVertexChunk
from graflo.db.conn import NamespaceNotFoundError, SchemaExistsError
from graflo.db.falkordb.bulk import (
    BulkEncodeError,
    bulk_args,
    edge_blob,
    encode_value,
    node_blob,
)
from graflo.db.falkordb.conn import FalkordbConnection

ZERO7 = b"\x00" * 7


def test_a_node_blob_is_a_header_then_each_nodes_values() -> None:
    blob = node_blob("P", [{"id": 1, "n": "a"}, {"id": 2}])
    assert blob == (
        b"P\x00" + b"\x02\x00\x00\x00" + b"id\x00n\x00"
        + b"\x04\x01" + ZERO7 + b"\x03a\x00"
        + b"\x04\x02" + ZERO7 + b"\x00"
    )  # fmt: skip


def test_an_edge_blob_carries_its_endpoint_node_ids() -> None:
    blob = edge_blob("knows", [(0, 1, {"w": 1.5, "tags": ["x", True]})])
    assert blob == (
        b"knows\x00" + b"\x02\x00\x00\x00" + b"w\x00tags\x00"
        + b"\x00" * 8 + b"\x01" + ZERO7
        + b"\x02" + b"\x00" * 6 + b"\xf8\x3f"
        + b"\x05\x02" + ZERO7 + b"\x03x\x00" + b"\x01\x01"
    )  # fmt: skip


def test_the_first_call_begins_the_graph_and_counts_what_it_sends() -> None:
    assert bulk_args(begin=True, nodes=[(b"n", 2)], edges=[(b"e", 3)]) == [
        "BEGIN", 2, 3, 1, 1, b"n", b"e",
    ]  # fmt: skip
    assert bulk_args(begin=False, edges=[(b"e", 3)]) == [0, 3, 0, 1, b"e"]


@pytest.mark.parametrize(
    "value", [{"a": 1}, float("nan"), 2**63, "a\x00b", [1, {"a": 1}]]
)
def test_a_value_the_format_cannot_carry_is_refused_before_sending(value) -> None:
    with pytest.raises(BulkEncodeError):
        encode_value(value)


# -- the writer ----------------------------------------------------------------


_SCHEMA: Any = None  # the writer reads no schema


def _connection(graphs: list[str], nodes: int = 0) -> tuple[FalkordbConnection, Any]:
    client = MagicMock()
    client.list_graphs.return_value = [g.encode() for g in graphs]
    with patch("graflo.db.falkordb.conn.FalkorDB", return_value=client):
        conn = FalkordbConnection(
            FalkordbConfig(uri="redis://localhost:6379", database="g")
        )
    cast(Any, conn.graph).query.return_value.result_set = [[nodes]]
    return conn, client


def _vertices(start: int, n: int, *, replay: bool = False) -> BulkVertexChunk:
    return BulkVertexChunk(
        chunk_id=f"v:{start}",
        class_name="P",
        match_keys=["id"],
        rows=[{"id": i} for i in range(start, start + n)],
        dense=list(range(start, start + n)),
        replay=replay,
    )


def test_the_first_vertex_chunk_creates_the_graph_key() -> None:
    conn, client = _connection([])
    conn.bulk_write_vertices(_SCHEMA, _vertices(0, 2))
    args = client.execute_command.call_args.args
    assert args[:3] == ("GRAPH.BULK", "g", "BEGIN")


def test_a_replayed_chunk_the_graph_holds_is_skipped() -> None:
    conn, client = _connection(["g"], nodes=4)
    conn.bulk_write_vertices(_SCHEMA, _vertices(2, 2, replay=True))
    client.execute_command.assert_not_called()


def test_a_chunk_out_of_dense_order_is_refused() -> None:
    conn, client = _connection(["g"], nodes=3)
    with pytest.raises(RuntimeError, match="holds 3 nodes"):
        conn.bulk_write_vertices(_SCHEMA, _vertices(2, 2))
    client.execute_command.assert_not_called()


def test_a_replayed_edge_chunk_merges_by_node_id() -> None:
    conn, client = _connection(["g"], nodes=4)
    chunk = BulkEdgeChunk(
        chunk_id="e:0",
        source_class="P",
        target_class="P",
        relation_name="knows",
        match_keys_source=["id"],
        match_keys_target=["id"],
        collection_name=None,
        rows=[({"id": 0}, {"id": 1}, {"w": 1})],
        dense=[(0, 1)],
        replay=True,
    )
    conn.bulk_write_edges(_SCHEMA, chunk)
    client.execute_command.assert_not_called()
    query = cast(Any, conn.graph).query.call_args.args[0]
    assert "id(source) = row[0]" in query and "MERGE" in query


SCHEMA: dict[str, Any] = {
    "metadata": {"name": "g", "version": "1.0.0"},
    "core_schema": {
        "vertex_config": {
            "vertices": [{"name": "P", "properties": ["id"], "identity": ["id"]}]
        },
        "edge_config": {"edges": []},
    },
}


def test_a_populated_graph_is_refused_and_an_empty_one_dropped() -> None:
    schema = Schema.model_validate(SCHEMA)
    conn, _ = _connection(["g"], nodes=1)
    with pytest.raises(SchemaExistsError):
        conn.bulk_begin(schema, BulkConfig())

    conn, _ = _connection(["g"], nodes=0)
    with patch.object(FalkordbConnection, "delete_database") as dropped:
        plan = conn.bulk_begin(schema, BulkConfig())
    dropped.assert_called_once_with("g")
    assert plan.max_parallel == 1 and not plan.restart


def test_a_resumed_load_whose_graph_is_gone_writes_everything_again() -> None:
    conn, _ = _connection([])
    plan = conn.bulk_begin(Schema.model_validate(SCHEMA), BulkConfig(), resume=True)
    assert plan.restart


def test_a_missing_graph_is_reported_without_creating_it() -> None:
    conn, _ = _connection([])
    with pytest.raises(NamespaceNotFoundError):
        conn.ensure_target_namespace(Schema.model_validate(SCHEMA), create=False)
    assert conn._node_count() == 0
    cast(Any, conn.graph).query.assert_not_called()
