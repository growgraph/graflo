"""The gate must serialize exactly the configurations where batch order is semantic."""

from __future__ import annotations

from types import SimpleNamespace

from graflo.architecture.contract.manifest import GraphManifest
from graflo.hq.concurrency_gate import SerialReason, effective_in_flight
from graflo.hq.ingestion_parameters import IngestionParams


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


class TestGateConditions:
    def test_plain_configuration_allows_overlap(self) -> None:
        assert effective_in_flight(_runtime(), IngestionParams()) == (2, None)

    def test_requested_depth_is_respected(self) -> None:
        params = IngestionParams(max_in_flight_batches=5)
        assert effective_in_flight(_runtime(), params) == (
            5,
            None,
        )

    def test_user_override_to_one(self) -> None:
        params = IngestionParams(max_in_flight_batches=1)
        assert effective_in_flight(_runtime(), params) == (
            1,
            SerialReason.USER_OVERRIDE,
        )

    def test_dynamic_edges_serialize(self) -> None:
        params = IngestionParams(dynamic_edges=True)
        assert effective_in_flight(_runtime(), params) == (
            1,
            SerialReason.DYNAMIC_EDGES,
        )

    def test_extra_weights_serialize(self) -> None:
        runtime = _runtime(extra_weights=(object(),))
        assert effective_in_flight(runtime, IngestionParams()) == (
            1,
            SerialReason.EXTRA_WEIGHTS,
        )

    def test_blank_vertex_used_by_resource_serializes(self) -> None:
        runtime = _runtime(blank=("publication",), names=("publication", "ticker"))
        assert effective_in_flight(runtime, IngestionParams()) == (
            1,
            SerialReason.BLANK_VERTICES,
        )

    def test_blank_vertex_not_touched_by_resource_allows_overlap(self) -> None:
        runtime = _runtime(blank=("publication",), names=("person", "org"))
        assert effective_in_flight(runtime, IngestionParams()) == (2, None)

    def test_blank_vertex_with_unknown_resource_names_stays_conservative(self) -> None:
        runtime = _runtime(blank=("publication",), names=())
        assert effective_in_flight(runtime, IngestionParams()) == (
            1,
            SerialReason.BLANK_VERTICES,
        )

    def test_missing_runtime_is_tolerated(self) -> None:
        assert effective_in_flight(None, IngestionParams()) == (2, None)


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
        assert effective_in_flight(runtime, IngestionParams()) == (2, None)

    def test_blank_resource_serializes(self) -> None:
        model = self._manifest(blank=True).require_ingestion_model()
        runtime = model.fetch_resource("r")
        assert effective_in_flight(runtime, IngestionParams()) == (
            1,
            SerialReason.BLANK_VERTICES,
        )

    @staticmethod
    def _finding_manifest() -> GraphManifest:
        """``person`` is written; ``org`` is found by its ``by_name`` identity."""
        out = GraphManifest.from_config(
            {
                "schema": {
                    "metadata": {"name": "g", "version": "1.0.0"},
                    "graph": {
                        "vertex_config": {
                            "vertices": [
                                {"name": "person", "properties": ["pid"]},
                                {
                                    "name": "org",
                                    "properties": ["oid", "name"],
                                    "identity": ["oid"],
                                    "secondary_identities": [
                                        {"name": "by_name", "fields": ["name"]}
                                    ],
                                },
                            ]
                        },
                        "edge_config": {"edges": []},
                    },
                },
                "ingestion_model": {
                    "resources": [
                        {
                            "name": "r",
                            "apply": [
                                {"vertex": "person", "from": {"pid": "pid"}},
                                {"vertex": "org", "find": "by_name"},
                            ],
                        }
                    ],
                    "transforms": [],
                },
            }
        )
        out.finish_init()
        return out

    def test_a_find_step_serializes(self) -> None:
        runtime = self._finding_manifest().require_ingestion_model().fetch_resource("r")
        assert runtime.attached_selectors == {"org": "by_name"}
        assert effective_in_flight(runtime, IngestionParams()) == (
            1,
            SerialReason.SECONDARY_IDENTITY,
        )

    def test_a_find_step_for_a_class_excluded_from_the_run_allows_overlap(
        self,
    ) -> None:
        manifest = self._finding_manifest()
        model = manifest.require_ingestion_model()
        model.finish_init(
            manifest.require_schema().core_schema, allowed_vertex_names={"person"}
        )
        runtime = model.fetch_resource("r")
        assert runtime.attached_selectors == {}
        assert not runtime.edge_derivation.has_endpoint_matches()
        assert effective_in_flight(runtime, IngestionParams()) == (2, None)
