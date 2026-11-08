"""The ArangoDB bulk writer without a server: import payloads, response checks, counts."""

from __future__ import annotations

import json
from typing import Any, cast

import pytest

from graflo.architecture.evolution.sanitize import physical_schema
from graflo.connections.bulk import BulkConfig
from graflo.db.arango.bulk import (
    ArangoImportError,
    check_import,
    edge_count_query,
    import_bodies,
)
from graflo.db.arango.conn import ArangoConnection
from graflo.db.arango.keys import edge_key, vertex_key
from graflo.db.bulk import BulkEdgeChunk, BulkVertexChunk
from graflo.db.conn import SchemaExistsError
from graflo.onto import DBType
from test.bulk.contract import manifest


class _Response:
    def __init__(self, body: dict[str, Any]) -> None:
        self.is_success = True
        self.status_code = 201
        self.error_message = None
        self.body = body


class _Http:
    """Answers every import as fully created, recording what was sent."""

    def __init__(self) -> None:
        self.requests: list[Any] = []

    def send_request(self, request: Any) -> _Response:
        self.requests.append(request)
        sent = request.data.count("\n")
        return _Response({"error": False, "created": sent, "errors": 0, "ignored": 0})


class _Collection:
    def __init__(self, count: int) -> None:
        self._count = count
        self.indexes_added: list[dict[str, Any]] = []

    def count(self) -> int:
        return self._count

    def indexes(self) -> list[Any]:
        return []

    def add_index(self, data: dict[str, Any]) -> None:
        self.indexes_added.append(data)


class _Database:
    def __init__(self, counts: dict[str, int] | None = None) -> None:
        self.conn = _Http()
        self.collections = {k: _Collection(v) for k, v in (counts or {}).items()}

    def collection(self, name: str) -> _Collection:
        return self.collections.setdefault(name, _Collection(0))


def _connection(db: _Database) -> ArangoConnection:
    conn = ArangoConnection.__new__(ArangoConnection)
    conn.conn = cast(Any, db)
    return conn


def _sent(db: _Database) -> list[tuple[dict[str, Any], list[dict[str, Any]]]]:
    return [
        (dict(r.params), [json.loads(line) for line in r.data.splitlines()])
        for r in db.conn.requests
    ]


VERTICES = BulkVertexChunk(
    chunk_id="v:0",
    class_name="Person",
    match_keys=["pid"],
    rows=[{"pid": "p1", "name": "Ann", "nick": None}, {"pid": "Ann Smith"}],
)

EDGES = BulkEdgeChunk(
    chunk_id="e:0",
    source_class="Person",
    target_class="City",
    relation_name="lives_in",
    match_keys_source=["pid"],
    match_keys_target=["cid"],
    collection_name="Person_City_edges",
    merge_properties=["role"],
    rows=[({"pid": "p1"}, {"cid": "c1"}, {"role": "home", "since": 2001})],
)


def test_vertices_are_imported_as_json_lines_with_their_keys() -> None:
    db = _Database()
    _connection(db).bulk_write_vertices(manifest().require_schema(), VERTICES)

    ((params, docs),) = _sent(db)
    assert params == {
        "collection": "Person",
        "type": "documents",
        "onDuplicate": "ignore",
        "details": "true",
    }
    assert docs == [
        {"pid": "p1", "name": "Ann", "nick": None, "_key": "p1"},
        {"pid": "Ann Smith", "_key": vertex_key(["Ann Smith"])},
    ]


def test_edges_are_imported_with_their_key_and_endpoints() -> None:
    db = _Database()
    _connection(db).bulk_write_edges(manifest().require_schema(), EDGES)

    ((params, docs),) = _sent(db)
    assert params["collection"] == "Person_City_edges"
    assert docs == [
        {
            "_key": edge_key("Person/p1", "City/c1", "lives_in", ["home"]),
            "_from": "Person/p1",
            "_to": "City/c1",
            "relation": "lives_in",
            "role": "home",
            "since": 2001,
        }
    ]


class _Capture:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def execute(self, query: str, **kwargs: Any) -> None:
        self.queries.append(query)


def test_upsert_and_bulk_write_the_same_keys() -> None:
    capture = _Capture()
    ArangoConnection.upsert_docs_batch(
        cast(Any, capture),
        [dict(r) for r in VERTICES.rows],
        "Person",
        ["pid"],
        update_keys="doc",
    )
    ArangoConnection.insert_edges_batch(
        cast(Any, capture),
        [list(r) for r in EDGES.rows],
        "Person",
        "City",
        "lives_in",
        ("pid",),
        ("cid",),
        collection_name="Person_City_edges",
        uniq_weight_fields=["role"],
    )
    vertex_query, edge_query = capture.queries
    upserted = json.loads(vertex_query.split("FOR doc in ", 1)[1].split("\n", 1)[0])
    upserted_edges = json.loads(
        edge_query.split("FOR doc IN ", 1)[1].split(" INSERT", 1)[0]
    )
    db = _Database()
    conn = _connection(db)
    conn.bulk_write_vertices(manifest().require_schema(), VERTICES)
    conn.bulk_write_edges(manifest().require_schema(), EDGES)
    (_, imported), (_, imported_edges) = _sent(db)

    assert [d["_key"] for d in upserted] == [d["_key"] for d in imported]
    assert upserted_edges == imported_edges
    assert "exclusive" not in vertex_query + edge_query


def test_a_bulk_chunk_whose_row_cannot_be_keyed_is_refused() -> None:
    chunk = VERTICES.model_copy(update={"rows": [{"pid": None}]})
    with pytest.raises(ValueError, match="cannot key"):
        _connection(_Database()).bulk_write_vertices(manifest().require_schema(), chunk)


def test_requests_stay_under_the_size_limit_and_keep_every_line() -> None:
    docs = [{"_key": str(i), "pad": "x" * 40} for i in range(100)]

    bodies = list(import_bodies(docs, limit=600))

    assert all(len(body) <= 600 for body, _ in bodies)
    assert sum(n for _, n in bodies) == 100
    lines = [json.loads(line) for body, _ in bodies for line in body.splitlines()]
    assert lines == docs


def test_a_line_longer_than_the_limit_goes_alone() -> None:
    bodies = list(import_bodies([{"a": "x" * 50}, {"b": 1}], limit=10))

    assert [n for _, n in bodies] == [1, 1]


def test_text_is_ascii_escaped_and_values_encoded_as_upsert_encodes_them() -> None:
    import datetime as dt

    ((body, _),) = import_bodies([{"name": "Zoé", "at": dt.date(2020, 1, 2)}])

    assert body.isascii()
    assert json.loads(body) == {"name": "Zoé", "at": "2020-01-02"}


@pytest.mark.parametrize(
    ("result", "replay", "match"),
    [
        (
            {"error": False, "created": 2, "errors": 1, "details": ["bad"]},
            False,
            "1 error",
        ),
        ({"created": 1, "ignored": 1}, False, "share a _key"),
        ({"created": 1, "ignored": 0}, True, "1 created"),
    ],
)
def test_an_import_that_did_not_store_everything_is_refused(
    result, replay, match
) -> None:
    with pytest.raises(ArangoImportError, match=match):
        check_import(result, sent=2, replay=replay, collection="Person")


@pytest.mark.parametrize(
    ("result", "replay"),
    [({"created": 2, "ignored": 0}, False), ({"created": 0, "ignored": 2}, True)],
)
def test_a_complete_import_passes(result, replay) -> None:
    check_import(result, sent=2, replay=replay, collection="Person")


def test_begin_refuses_a_database_that_holds_a_schema(monkeypatch) -> None:
    conn = _connection(_Database())
    monkeypatch.setattr(conn, "ensure_target_namespace", lambda *a, **k: None)
    monkeypatch.setattr(conn, "_schema_has_artifacts", lambda: True)
    monkeypatch.setattr(conn, "_resolve_db_name", lambda schema: "db")
    defined: list[bool] = []
    monkeypatch.setattr(
        conn,
        "define_schema",
        lambda schema, identity_indexes=True: defined.append(identity_indexes),
    )
    schema = manifest().require_schema()

    with pytest.raises(SchemaExistsError, match="already exist"):
        conn.bulk_begin(schema, BulkConfig())
    assert defined == []

    conn.bulk_begin(schema, BulkConfig(), resume=True)
    assert defined == [False]


def test_finish_builds_the_identity_indexes_and_counts_per_stored_edge(
    monkeypatch,
) -> None:
    schema = physical_schema(manifest().require_schema(), DBType.ARANGO)
    db = _Database({"Person": 3, "City": 2, "Person_City_edges": 5})
    conn = _connection(db)
    monkeypatch.setattr(conn, "define_indexes", lambda schema: None)

    counts = conn.bulk_finish(schema)

    assert counts.vertices == {"Person": 3, "City": 2}
    assert counts.edges == {("Person", "lives_in", "City"): 5}
    assert db.collections["Person"].indexes_added[0]["fields"] == ["pid"]
    assert db.collections["Person"].indexes_added[0]["unique"] is True


def test_finish_groups_a_collection_whose_rows_set_their_relation(
    monkeypatch,
) -> None:
    schema = physical_schema(manifest().require_schema(), DBType.ARANGO)
    conn = _connection(_Database({"Person": 3, "City": 2}))
    monkeypatch.setattr(conn, "define_indexes", lambda schema: None)
    monkeypatch.setattr(
        "graflo.db.arango.conn.edge_collections",
        lambda schema: {"Person_City_edges": [("Person", None, "City")]},
    )
    queries: list[str] = []

    def execute(query: str, bind_vars=None, **kwargs):
        queries.append(query)
        return iter([["Person", "a", "City", 2], ["Person", None, "City", 1]])

    monkeypatch.setattr(conn, "execute", execute)

    counts = conn.bulk_finish(schema)

    assert queries == [edge_count_query("Person_City_edges")]
    assert counts.edges == {
        ("Person", "a", "City"): 2,
        ("Person", None, "City"): 1,
    }
