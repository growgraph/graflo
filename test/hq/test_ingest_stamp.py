"""Ingestion stamps and guards each record's output with that record's own values."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

import graflo.hq.caster as caster_module
from graflo.architecture.contract.bindings import Bindings
from graflo.architecture.contract.manifest import GraphManifest
from graflo.connections.graflo_backend import GraFloBackendConfig
from graflo.data_source.factory import DataSourceFactory
from graflo.data_source.registry import DataSourceRegistry
from graflo.db.manager import ConnectionManager
from graflo.filter.onto import ComparisonOperator, FilterExpression
from graflo.hq.caster import Caster
from graflo.hq.graph_engine import GraphEngine
from graflo.hq.ingestion_parameters import IngestionParams
from graflo.onto import DBType

ZONES = "access_zones"
ZONE_FIELD = {
    "name": ZONES,
    "type": "LIST",
    "item_type": "INT",
    "reserved": True,
    "merge": "replace",
}
ZONE_OF = {"C1": 1, "C2": 2, "C3": 3}
IN = ("Message", "Channel", "in")

CONFIG: dict[str, Any] = {
    "schema": {
        "metadata": {"name": "chat"},
        "core_schema": {
            "vertex_config": {
                "vertices": [
                    {
                        "name": "Message",
                        "properties": ["id", "text", ZONE_FIELD],
                        "identity": ["id"],
                    },
                    {
                        "name": "Channel",
                        "properties": ["id", ZONE_FIELD],
                        "identity": ["id"],
                    },
                ]
            },
            "edge_config": {
                "edges": [
                    {
                        "source": "Message",
                        "target": "Channel",
                        "relation": "in",
                        "properties": [ZONE_FIELD],
                    }
                ]
            },
        },
    },
    "ingestion_model": {
        "resources": [
            {
                "name": "messages",
                "pipeline": [
                    {"vertex": "Message", "from": {"id": "id", "text": "text"}},
                    {"vertex": "Channel", "from": {"id": "channel"}},
                    {"edge": {"from": "Message", "to": "Channel", "relation": "in"}},
                ],
            }
        ]
    },
}

RECORDS = [
    {"id": "m1", "text": "a", "channel": "C1"},
    {"id": "m2", "text": "b", "channel": "C2"},
    {"id": "m3", "text": "c", "channel": "C1"},
]


def _manifest() -> GraphManifest:
    manifest = GraphManifest.from_config(CONFIG)
    manifest.finish_init()
    return manifest


def _zone_stamp(resource: str, record: dict[str, Any]) -> dict[str, Any] | None:
    return {ZONES: [ZONE_OF[record["channel"]]]}


def _zone_guard(resource: str, record: dict[str, Any]) -> FilterExpression:
    return FilterExpression(
        kind="leaf",
        field=ZONES,
        cmp_operator=ComparisonOperator.ANY_IN,
        value=[[ZONE_OF[record["channel"]]]],
    )


def _ingest(tmp_path: Path, records: list[dict], params: IngestionParams):
    manifest = _manifest()
    backend = GraFloBackendConfig(output_dir=tmp_path)
    engine = GraphEngine(target_db_flavor=DBType.GRAFLO_BACKEND)
    engine.define_schema(manifest=manifest, target_db_config=backend)
    registry = DataSourceRegistry()
    registry.register(
        DataSourceFactory.create_in_memory_data_source(records),
        resource_name="messages",
    )
    engine.ingest(
        manifest=manifest,
        target_db_config=backend,
        ingestion_params=params,
        data_source_registry=registry,
    )
    return backend


def test_each_element_lands_with_its_own_records_stamp(tmp_path):
    seen: list[str] = []

    def stamp_for(resource: str, record: dict[str, Any]) -> dict[str, Any] | None:
        seen.append(resource)
        return _zone_stamp(resource, record)

    backend = _ingest(tmp_path, RECORDS, IngestionParams(stamp_for=stamp_for))

    with ConnectionManager(connection_config=backend) as conn:
        messages = {d["id"]: d[ZONES] for d in conn.fetch_all_docs("Message")}
        channels = {d["id"]: d[ZONES] for d in conn.fetch_all_docs("Channel")}
        edges = conn.fetch_all_edges(*IN)
    assert messages == {"m1": [1], "m2": [2], "m3": [1]}
    assert channels == {"C1": [1], "C2": [2]}
    assert {source["id"]: attrs[ZONES] for source, _, attrs in edges} == messages
    assert seen == ["messages"] * len(RECORDS)


@pytest.mark.parametrize(
    "params",
    [
        IngestionParams(
            stamp_for=lambda resource, record: (
                None if record["channel"] == "C2" else _zone_stamp(resource, record)
            )
        ),
        IngestionParams(guard_for=lambda resource, record: None),
    ],
    ids=["one-record-unstamped", "guard-only"],
)
def test_a_lifted_type_cast_from_an_unstamped_record_is_refused(tmp_path, params):
    with pytest.raises(ValueError, match="write it with a stamp"):
        _ingest(tmp_path, RECORDS, params)

    with ConnectionManager(
        connection_config=GraFloBackendConfig(output_dir=tmp_path)
    ) as conn:
        assert conn.fetch_all_docs("Message") == []


class _RecordingWriter:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def write(self, **kwargs: Any) -> None:
        self.calls.append(kwargs)


def _recorded_writes(tmp_path: Path, params: IngestionParams) -> list[dict[str, Any]]:
    manifest = _manifest()
    caster = Caster(
        manifest.require_schema(),
        manifest.require_ingestion_model(),
        ingestion_params=params,
    )
    backend = GraFloBackendConfig(output_dir=tmp_path)
    caster.validate_run(backend, Bindings(), params)
    writer = _RecordingWriter()
    caster._get_db_writer = lambda conn_conf: writer  # ty: ignore[invalid-assignment]
    asyncio.run(caster.process_batch(RECORDS, "messages", backend))
    caster.close()
    return writer.calls


def test_records_are_written_in_one_call_per_stamp_and_guard(tmp_path, monkeypatch):
    # The file backend applies no guard; the recording writer stands in for one that does.
    monkeypatch.setattr(caster_module, "require_read_guard", lambda conn_conf: None)
    calls = _recorded_writes(
        tmp_path, IngestionParams(stamp_for=_zone_stamp, guard_for=_zone_guard)
    )

    assert [call["stamp"] for call in calls] == [{ZONES: [1]}, {ZONES: [2]}]
    assert [call["guard"].value for call in calls] == [[[1]], [[2]]]
    assert [
        [doc["id"] for doc in call["gc"].vertices["Message"]] for call in calls
    ] == [["m1", "m3"], ["m2"]]
    assert [len(call["gc"].edges[IN]) for call in calls] == [2, 1]


def test_without_hooks_the_batch_is_written_once_unstamped(tmp_path):
    calls = _recorded_writes(tmp_path, IngestionParams())

    (call,) = calls
    assert set(call) == {"gc", "conn_conf", "resource_name", "bulk_session_id"}
    assert [doc["id"] for doc in call["gc"].vertices["Message"]] == ["m1", "m2", "m3"]


def test_the_hooks_are_never_serialized() -> None:
    params = IngestionParams(stamp_for=_zone_stamp, guard_for=_zone_guard)

    assert "stamp_for" not in params.model_dump()
    assert "guard_for" not in params.model_dump_json()
    assert "stamp_for" not in IngestionParams.model_json_schema()["properties"]


@pytest.mark.parametrize(
    "guard_for",
    [
        _zone_guard,
        lambda resource, record: (
            _zone_guard(resource, record) if record["channel"] == "C2" else None
        ),
    ],
    ids=["every-record", "a-later-record"],
)
def test_a_guard_on_a_backend_without_read_guards_writes_nothing(tmp_path, guard_for):
    params = IngestionParams(stamp_for=_zone_stamp, guard_for=guard_for)

    with pytest.raises(NotImplementedError, match="read guard"):
        _ingest(tmp_path, RECORDS, params)

    with ConnectionManager(
        connection_config=GraFloBackendConfig(output_dir=tmp_path)
    ) as conn:
        assert conn.fetch_all_docs("Message") == []
