"""Under ``strict_references`` a pipeline writes only what the schema declares.

An edge is declared when the schema lists it, or lists a relation-less template
between its endpoints for relations read from the data. A step that writes any
other edge, or maps a property its vertex does not declare, is refused when the
manifest loads; a relation found in the data that no edge declares is skipped
with a warning. Loose manifests (``strict_references=False``) keep loading.
"""

from __future__ import annotations

import asyncio
import copy
import logging
from pathlib import Path

import pytest
from suthing import FileHandle

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.hq.document_caster import DocumentCaster
from graflo.hq.ingestion_parameters import IngestionParams

_EXAMPLE = (
    Path(__file__).resolve().parents[2]
    / "examples"
    / "07-vertex-router-type-map"
    / "manifest.yaml"
)


def _config() -> dict:
    return copy.deepcopy(FileHandle.load(_EXAMPLE))


def _load(config: dict, *, strict: bool) -> GraphManifest:
    manifest = GraphManifest.from_config(config)
    manifest.finish_init(strict_references=strict)
    return manifest


def _cast(manifest: GraphManifest, resource: str, rows: list[dict]) -> dict:
    caster = DocumentCaster(manifest.require_ingestion_model())
    result = asyncio.run(caster.cast_batch(rows, resource, params=IngestionParams()))
    return result.graph.edges


def _add_resource(config: dict, name: str, pipeline: list[dict]) -> dict:
    config["ingestion_model"]["resources"].append({"name": name, "pipeline": pipeline})
    return config


def _undeclared_relation() -> dict:
    return _add_resource(
        _config(),
        "pilots",
        [
            {"vertex": "person", "from": {"id": "person_id"}},
            {"vertex": "vehicle", "from": {"id": "vehicle_id"}},
            {"edge": {"from": "person", "to": "vehicle", "relation": "pilots"}},
        ],
    )


def _undeclared_property() -> dict:
    return _add_resource(
        _config(),
        "people",
        [{"vertex": "person", "from": {"id": "pid", "no_such_prop": "x"}}],
    )


class TestDeclared:
    def test_an_exact_edge_and_a_relation_less_template_declare(self) -> None:
        config = EdgeConfig(
            edges=[
                Edge(source="a", target="b", relation="r"),
                Edge(source="a", target="a"),
            ]
        )

        assert config.declared(("a", "b", "r")) is not None
        assert config.declared(("a", "a", "anything")) is not None
        assert config.declared(("a", "b", "other")) is None


class TestStaticEdges:
    def test_an_undeclared_relation_is_refused_at_load(self) -> None:
        with pytest.raises(ValueError, match="does not declare"):
            _load(_undeclared_relation(), strict=True)

    def test_a_loose_manifest_still_loads_it(self) -> None:
        _load(_undeclared_relation(), strict=False)


class TestPropertyMappings:
    def test_an_undeclared_from_target_is_refused_at_load(self) -> None:
        with pytest.raises(ValueError, match="no_such_prop"):
            _load(_undeclared_property(), strict=True)

    def test_a_loose_manifest_still_loads_it(self) -> None:
        _load(_undeclared_property(), strict=False)

    def test_a_router_from_target_is_checked_against_its_routed_types(self) -> None:
        config = _config()
        router = config["ingestion_model"]["resources"][1]["pipeline"][0]
        router["vertex_router"]["vertex_from_map"] = {
            "vehicle": {"id": "source_id", "wheels": "n_wheels"}
        }

        with pytest.raises(ValueError, match="wheels"):
            _load(config, strict=True)


class TestDynamicEdges:
    @staticmethod
    def _with_pilots() -> dict:
        config = _config()
        edge = config["ingestion_model"]["resources"][1]["pipeline"][2]["edge"]
        edge["relation_map"]["PILOTS"] = "pilots"
        return config

    _ROWS = [
        {
            "source_type": "Person",
            "source_id": "1",
            "relation": "OWNS",
            "target_type": "Vehicle",
            "target_id": "4",
        },
        {
            "source_type": "Person",
            "source_id": "1",
            "relation": "PILOTS",
            "target_type": "Vehicle",
            "target_id": "4",
        },
    ]

    def test_an_undeclared_pair_is_skipped_with_a_warning(self, caplog) -> None:
        manifest = _load(self._with_pilots(), strict=True)

        with caplog.at_level(logging.WARNING):
            edges = _cast(manifest, "relations", self._ROWS)

        assert ("person", "vehicle", "owns") in edges
        assert ("person", "vehicle", "pilots") not in edges
        assert any("pilots" in r.getMessage() for r in caplog.records)

    def test_a_loose_manifest_still_casts_it(self) -> None:
        manifest = _load(self._with_pilots(), strict=False)

        edges = _cast(manifest, "relations", self._ROWS)

        assert ("person", "vehicle", "pilots") in edges


class TestRelationsReadFromTheData:
    def test_a_declared_template_casts_every_relation(self) -> None:
        config = _config()
        config["schema"]["graph"]["edge_config"]["edges"].append(
            {"source": "person", "target": "vehicle"}
        )
        _add_resource(
            config,
            "uses",
            [
                {"vertex": "person", "from": {"id": "person_id"}},
                {"vertex": "vehicle", "from": {"id": "vehicle_id"}},
                {"edge": {"from": "person", "to": "vehicle", "relation_field": "how"}},
            ],
        )
        manifest = _load(config, strict=True)

        edges = _cast(
            manifest, "uses", [{"person_id": "1", "vehicle_id": "4", "how": "rents"}]
        )

        assert ("person", "vehicle", "rents") in edges
