"""The gate must serialize exactly the configurations where batch order is semantic."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.hq.concurrency_gate import (
    SerialReason,
    bulk_load_enabled,
    effective_in_flight,
)
from graflo.hq.ingestion_parameters import IngestionParams
from graflo.onto import DBType


def _runtime(
    *,
    extra_weights: tuple = (),
    blank: tuple[str, ...] = (),
    names: tuple[str, ...] = ("person", "org"),
):
    return SimpleNamespace(
        config=SimpleNamespace(extra_weights=list(extra_weights)),
        vertex_config=SimpleNamespace(blank_vertices=list(blank)),
        collect_vertex_names=lambda: set(names),
    )


def _conn(db_type: DBType = DBType.ARANGO):
    return SimpleNamespace(connection_type=db_type)


class TestGateConditions:
    def test_plain_configuration_allows_overlap(self) -> None:
        assert effective_in_flight(
            _runtime(), IngestionParams(), _conn(), bulk_enabled=False
        ) == (2, None)

    def test_requested_depth_is_respected(self) -> None:
        params = IngestionParams(max_in_flight_batches=5)
        assert effective_in_flight(_runtime(), params, _conn(), bulk_enabled=False) == (
            5,
            None,
        )

    def test_user_override_to_one(self) -> None:
        params = IngestionParams(max_in_flight_batches=1)
        assert effective_in_flight(_runtime(), params, _conn(), bulk_enabled=False) == (
            1,
            SerialReason.USER_OVERRIDE,
        )

    def test_dynamic_edges_serialize(self) -> None:
        params = IngestionParams(dynamic_edges=True)
        assert effective_in_flight(_runtime(), params, _conn(), bulk_enabled=False) == (
            1,
            SerialReason.DYNAMIC_EDGES,
        )

    def test_bulk_session_serializes(self) -> None:
        assert effective_in_flight(
            _runtime(), IngestionParams(), _conn(), bulk_enabled=True
        ) == (1, SerialReason.BULK_SESSION)

    def test_graflo_backend_serializes(self) -> None:
        assert effective_in_flight(
            _runtime(),
            IngestionParams(),
            _conn(DBType.GRAFLO_BACKEND),
            bulk_enabled=False,
        ) == (1, SerialReason.GRAFLO_BACKEND)

    def test_extra_weights_serialize(self) -> None:
        runtime = _runtime(extra_weights=(object(),))
        assert effective_in_flight(
            runtime, IngestionParams(), _conn(), bulk_enabled=False
        ) == (1, SerialReason.EXTRA_WEIGHTS)

    def test_blank_vertex_used_by_resource_serializes(self) -> None:
        runtime = _runtime(blank=("publication",), names=("publication", "ticker"))
        assert effective_in_flight(
            runtime, IngestionParams(), _conn(), bulk_enabled=False
        ) == (1, SerialReason.BLANK_VERTICES)

    def test_blank_vertex_not_touched_by_resource_allows_overlap(self) -> None:
        runtime = _runtime(blank=("publication",), names=("person", "org"))
        assert effective_in_flight(
            runtime, IngestionParams(), _conn(), bulk_enabled=False
        ) == (2, None)

    def test_blank_vertex_with_unknown_resource_names_stays_conservative(self) -> None:
        runtime = _runtime(blank=("publication",), names=())
        assert effective_in_flight(
            runtime, IngestionParams(), _conn(), bulk_enabled=False
        ) == (1, SerialReason.BLANK_VERTICES)

    def test_missing_runtime_or_conn_is_tolerated(self) -> None:
        assert effective_in_flight(
            None, IngestionParams(), None, bulk_enabled=False
        ) == (2, None)


class TestBulkLoadEnabled:
    @pytest.mark.parametrize(
        "conn, expected",
        [
            (None, False),
            (SimpleNamespace(), False),
            (SimpleNamespace(bulk_load=None), False),
            (SimpleNamespace(bulk_load=SimpleNamespace(enabled=False)), False),
            (SimpleNamespace(bulk_load=SimpleNamespace(enabled=True)), True),
        ],
    )
    def test_detection(self, conn, expected) -> None:
        assert bulk_load_enabled(conn) is expected


class TestRealRuntimeSurface:
    """Pin the ResourceRuntime attribute names the gate relies on."""

    def _manifest(self, *, blank: bool) -> GraphManifest:
        person: dict = {"name": "person", "properties": [{"name": "pid"}]}
        if blank:
            person["blank"] = True
        else:
            person["identity"] = ["pid"]
        out = GraphManifest.from_config(
            {
                "schema": {
                    "metadata": {"name": "g", "version": "1.0.0"},
                    "graph": {
                        "vertex_config": {"vertices": [person]},
                        "edge_config": {"edges": []},
                    },
                },
                "ingestion_model": {
                    "resources": [
                        {
                            "name": "r",
                            "apply": [{"vertex": "person", "from": {"pid": "pid"}}],
                        }
                    ],
                    "transforms": [],
                },
            }
        )
        out.finish_init()
        return out

    def test_plain_resource_allows_overlap(self) -> None:
        model = self._manifest(blank=False).require_ingestion_model()
        runtime = model.fetch_resource("r")
        assert effective_in_flight(
            runtime, IngestionParams(), _conn(), bulk_enabled=False
        ) == (2, None)

    def test_blank_resource_serializes(self) -> None:
        model = self._manifest(blank=True).require_ingestion_model()
        runtime = model.fetch_resource("r")
        assert effective_in_flight(
            runtime, IngestionParams(), _conn(), bulk_enabled=False
        ) == (1, SerialReason.BLANK_VERTICES)
