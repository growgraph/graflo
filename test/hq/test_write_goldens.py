"""What :meth:`DBWriter.write` hands each backend, pinned per target flavor.

One manifest is cast for real (inline) and written through a recording fake
connection, once per flavor. The recorded ``upsert_docs_batch`` and
``insert_edges_batch`` calls are compared with a golden file per flavor, so a
refactor of the writer that changes what reaches a backend fails here.

The fixture covers natural, hash (digest), assigned and UUID-typed identities, a
derived field, a document with no identity value, a repeated identity within a
batch, a ``merge: union`` list holding a null, and reserved fields set by the
stamp. Generated UUIDs come from a counter, so the goldens are stable.

Regenerate after an intended change with
``GRAFLO_WRITE_WRITER_GOLDENS=1 uv run pytest test/hq/test_write_goldens.py``
and review the diff.
"""

from __future__ import annotations

import asyncio
import copy
import itertools
import json
import os
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.graph_types import GraphContainer
from graflo.connections.graflo_backend import GraFloBackendConfig
from graflo.connections.onto import (
    ArangoConfig,
    DBConfig,
    FalkordbConfig,
    MemgraphConfig,
    NebulaConfig,
    Neo4jConfig,
    PostgresConfig,
    TigergraphConfig,
)
from graflo.db.conn import ConnectionCapability
from graflo.db.manager import ConnectionManager
from graflo.hq.db_writer import DBWriter
from graflo.hq.document_caster import DocumentCaster
from graflo.hq.ingestion_parameters import IngestionParams
from graflo.onto import DBType

GOLDENS = Path(__file__).parent / "goldens" / "write"
WRITE_GOLDENS = os.environ.get("GRAFLO_WRITE_WRITER_GOLDENS") == "1"

EVENT_ID = "6f1c2a3b-0000-4000-8000-000000000001"
DEVICE_ID = "6f1c2a3b-0000-4000-8000-0000000000d1"
STAMP = {"zones": [7]}

MANIFEST: dict[str, Any] = {
    "schema": {
        "metadata": {"name": "goldens", "version": "1.0.0"},
        "core_schema": {
            "vertex_config": {
                "vertices": [
                    {
                        "name": "Person",
                        "properties": [
                            "pid",
                            "name",
                            {
                                "name": "zones",
                                "type": "LIST",
                                "item_type": "INT",
                                "reserved": True,
                            },
                        ],
                        "identity": ["pid"],
                    },
                    {
                        "name": "Org",
                        "properties": ["name", "country"],
                        "identity": ["id"],
                        "hash_identity_properties": ["name", "country"],
                    },
                    {
                        "name": "Event",
                        "properties": ["eid", "title"],
                        "identity": ["eid"],
                        "assigned": True,
                    },
                    {
                        "name": "Device",
                        "properties": [{"name": "uid", "type": "UUID"}, "model"],
                        "identity": ["uid"],
                    },
                    {
                        "name": "Alias",
                        "properties": [
                            {"name": "label", "match": {"normalizer": "label_v1"}},
                            {
                                "name": "normalized_label",
                                "derived": {"source": "label", "form": "normal"},
                            },
                        ],
                        "identity": ["id"],
                        "hash_identity_properties": ["normalized_label"],
                    },
                    {
                        "name": "Topic",
                        "properties": [
                            "name",
                            {
                                "name": "tags",
                                "type": "LIST",
                                "item_type": "STRING",
                                "merge": "union",
                            },
                        ],
                        "identity": ["name"],
                    },
                ]
            },
            "edge_config": {
                "edges": [
                    {
                        "source": "Person",
                        "target": "Org",
                        "relation": "works_at",
                        "properties": [
                            {
                                "name": "zones",
                                "type": "LIST",
                                "item_type": "INT",
                                "reserved": True,
                            }
                        ],
                    },
                    {"source": "Person", "target": "Event", "relation": "attended"},
                    {"source": "Device", "target": "Org", "relation": "owned_by"},
                    {"source": "Person", "target": "Alias", "relation": "known_as"},
                ]
            },
        },
    },
    "ingestion_model": {
        "resources": [
            {
                "name": "people",
                "apply": [
                    {"vertex": "Person", "from": {"pid": "pid", "name": "name"}},
                    {"vertex": "Org", "from": {"name": "org", "country": "country"}},
                    {"vertex": "Event", "from": {"eid": "eid", "title": "event"}},
                    {"vertex": "Device", "from": {"uid": "uid", "model": "model"}},
                    {"vertex": "Alias", "from": {"label": "alias"}},
                    {"edge": {"from": "Person", "to": "Org", "relation": "works_at"}},
                    {"edge": {"from": "Person", "to": "Event", "relation": "attended"}},
                    {"edge": {"from": "Device", "to": "Org", "relation": "owned_by"}},
                    {"edge": {"from": "Person", "to": "Alias", "relation": "known_as"}},
                ],
            },
            {
                "name": "topics",
                "apply": [
                    {"vertex": "Topic", "from": {"name": "topic", "tags": "tags"}}
                ],
            },
        ],
        "transforms": [],
    },
}

PEOPLE = [
    {
        "pid": "p1",
        "name": "Ann",
        "org": "Acme",
        "country": "FR",
        "eid": EVENT_ID,
        "event": "Kick-off",
        "uid": DEVICE_ID,
        "model": "X1",
        "alias": "Berlin pump",
    },
    # The same person again: one row per identity reaches the backend.
    {
        "pid": "p1",
        "name": "Ann B.",
        "org": "Acme",
        "country": "FR",
        "event": "Review",
        "alias": "BERLIN-pump",
    },
    # No identity value: the Person document is dropped, not written.
    {"name": "Ghost", "org": "Beta", "country": "DE", "event": "Wake"},
]

TOPICS = [
    {"topic": "graphs", "tags": ["a", None]},
    {"topic": "graphs", "tags": ["b"]},
    {"topic": "rdf", "tags": ["c"]},
]


def _configs() -> dict[DBType, DBConfig]:
    return {
        DBType.ARANGO: ArangoConfig(
            uri="http://localhost:8529", username="u", password="p"
        ),
        DBType.NEO4J: Neo4jConfig(
            uri="bolt://localhost:7687", username="u", password="p"
        ),
        DBType.TIGERGRAPH: TigergraphConfig(
            uri="http://localhost:14240", username="u", password="p"
        ),
        DBType.FALKORDB: FalkordbConfig(uri="redis://localhost:6379", database="g"),
        DBType.MEMGRAPH: MemgraphConfig(uri="bolt://localhost:7688"),
        DBType.NEBULA: NebulaConfig(
            uri="nebula://localhost:9669", username="u", password="p"
        ),
        DBType.POSTGRES: PostgresConfig(
            uri="postgresql://localhost:5432", database="d"
        ),
        DBType.GRAFLO_BACKEND: GraFloBackendConfig(output_dir=Path("unused")),
    }


FLAVORS = sorted(_configs(), key=str)


class _RecordingDB:
    """Records the batches a writer hands a backend, as JSON-ready values."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def upsert_docs_batch(self, docs, class_name, match_keys, **kwargs) -> None:
        self.calls.append(
            {
                "call": "upsert_docs_batch",
                "class_name": class_name,
                "match_keys": list(match_keys),
                "kwargs": _jsonable(kwargs),
                "docs": _jsonable(copy.deepcopy(docs)),
            }
        )

    def insert_edges_batch(self, docs_edges, **kwargs) -> None:
        self.calls.append(
            {
                "call": "insert_edges_batch",
                "kwargs": _jsonable(kwargs),
                "docs_edges": _jsonable(copy.deepcopy(docs_edges)),
            }
        )


def _manager(db: _RecordingDB) -> type:
    class _Manager:
        require = ConnectionManager.require
        flavors_supporting = ConnectionManager.flavors_supporting
        target_conn_mapping = ConnectionManager.target_conn_mapping

        def __init__(self, connection_config) -> None:
            pass

        def __enter__(self) -> _RecordingDB:
            return db

        def __exit__(self, *exc) -> bool:
            return False

    return _Manager


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _sort_key(call: dict[str, Any]) -> str:
    """Calls run concurrently, so the golden orders them by what they write."""
    if call["call"] == "upsert_docs_batch":
        return f"0 {call['class_name']}"
    kw = call["kwargs"]
    return f"1 {kw['source_class']} {kw['target_class']} {kw['relation_name']}"


@pytest.fixture
def counted_uuids(monkeypatch) -> None:
    counter = itertools.count(1)

    def _uuid4() -> UUID:
        return UUID(int=next(counter), version=4)

    monkeypatch.setattr("graflo.architecture.schema.identity_uuid.uuid4", _uuid4)
    monkeypatch.setattr("graflo.hq.write_prepare.uuid4", _uuid4)


def _manifest(flavor: DBType) -> GraphManifest:
    config = copy.deepcopy(MANIFEST)
    config["schema"]["db_profile"] = {"db_flavor": flavor.value}
    manifest = GraphManifest.from_config(config)
    manifest.finish_init()
    return manifest


def _cast(manifest: GraphManifest, resource: str, docs: list[dict]) -> GraphContainer:
    caster = DocumentCaster(manifest.require_ingestion_model())
    try:
        result = asyncio.run(
            caster.cast_batch(
                docs, resource, params=IngestionParams(cast_executor="inline")
            )
        )
    finally:
        caster.close()
    assert not result.failures
    return result.graph


@pytest.mark.parametrize("flavor", FLAVORS, ids=str)
def test_the_writer_hands_each_backend_the_pinned_batches(
    flavor: DBType, monkeypatch, counted_uuids
) -> None:
    recorded = _record(flavor, monkeypatch)
    golden = GOLDENS / f"{flavor.value}.json"
    text = json.dumps(recorded, indent=1, sort_keys=True) + "\n"
    if WRITE_GOLDENS:
        golden.parent.mkdir(parents=True, exist_ok=True)
        golden.write_text(text)
    assert golden.exists(), (
        f"no golden for {flavor}; run with GRAFLO_WRITE_WRITER_GOLDENS=1"
    )
    assert text == golden.read_text(), (
        f"what the writer hands {flavor} changed; if intended, regenerate with "
        "GRAFLO_WRITE_WRITER_GOLDENS=1 and review the diff"
    )


def _handmade() -> GraphContainer:
    """A container no cast produced, as a resource-less write receives it.

    Nothing is minted or dropped yet, so the writer's own steps do it: the
    digest and assigned ids, the derived field, and dropping the document with
    no identity value. Edge endpoints are separate dicts, as in a container
    loaded from JSON.
    """
    return GraphContainer(
        vertices={
            "Person": [{"pid": "p9", "name": "Bo"}, {"name": "Nobody"}],
            "Org": [{"name": "Gamma", "country": "IT"}],
            "Event": [{"title": "Unminted"}],
            "Alias": [{"label": "Rome pump"}],
        },
        edges={
            ("Person", "Org", "works_at"): [
                ({"pid": "p9"}, {"name": "Gamma", "country": "IT"}, {})
            ],
        },
    )


def _record(flavor: DBType, monkeypatch) -> dict[str, Any]:
    conn_conf = _configs()[flavor]
    manifest = _manifest(flavor)
    db = _RecordingDB()
    monkeypatch.setattr("graflo.hq.db_writer.ConnectionManager", _manager(db))
    writer = DBWriter(
        schema=manifest.require_schema(),
        ingestion_model=manifest.require_ingestion_model(),
    )
    union_ok = flavor in ConnectionManager.flavors_supporting(
        ConnectionCapability.UNION_MERGE
    )
    batches: list[tuple[str, str | None, GraphContainer]] = [
        ("people", "people", _cast(manifest, "people", PEOPLE)),
        ("topics", "topics", _cast(manifest, "topics", TOPICS)),
        ("handmade", None, _handmade()),
    ]
    out: dict[str, Any] = {}
    for label, resource, gc in batches:
        db.calls = []
        try:
            asyncio.run(writer.write(gc, conn_conf, resource, stamp=STAMP))
        except ValueError as exc:
            assert label == "topics" and not union_ok, exc
            out[label] = {"refused": str(exc)}
            continue
        out[label] = sorted(db.calls, key=_sort_key)
    return out


def test_cast_mints_digest_ids_onto_edge_endpoints(counted_uuids) -> None:
    """A digest vertex's id is on its edge endpoint documents before the writer runs.

    So a writer step that never sees the edges (bulk staging) can take endpoint
    keys from the cast as they are.
    """
    gc = _cast(_manifest(DBType.NEO4J), "people", PEOPLE)
    org_ids = {doc["id"] for doc in gc.vertices["Org"]}
    endpoints = [target for _, target, _ in gc.edges[("Person", "Org", "works_at")]]
    endpoints += [target for _, target, _ in gc.edges[("Device", "Org", "owned_by")]]
    assert endpoints
    assert {doc.get("id") for doc in endpoints} <= org_ids


def test_the_writer_does_not_mint_digest_ids_onto_edge_endpoints(
    monkeypatch, counted_uuids
) -> None:
    """The writer mints a digest id on vertex documents only.

    An edge endpoint of a container no cast produced keeps its source fields,
    so its key must be computed before the edges are built.
    """
    recorded = _record(DBType.NEO4J, monkeypatch)["handmade"]
    (org,) = [c for c in recorded if c.get("class_name") == "Org"]
    (works_at,) = [
        c for c in recorded if c["kwargs"].get("relation_name") == "works_at"
    ]
    assert "id" in org["docs"][0]
    ((_, target, _),) = works_at["docs_edges"]
    assert "id" not in target
