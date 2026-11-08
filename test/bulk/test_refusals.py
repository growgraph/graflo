"""Bulk mode refuses, by resource and step, what reads the target while ingesting."""

from __future__ import annotations

import copy
from typing import Any

import pytest

from graflo.architecture.contract.bindings import Bindings
from graflo.architecture.contract.manifest import GraphManifest
from graflo.connections.onto import DBConfig, Neo4jConfig, PostgresConfig
from graflo.hq.bulk.refusals import bulk_refusals
from graflo.hq.caster import Caster
from graflo.hq.ingestion_parameters import IngestionParams
from test.bulk import contract
from test.hq.test_write_goldens import MANIFEST


def _refusals(config: dict[str, Any]) -> list[str]:
    manifest = GraphManifest.from_config(config)
    manifest.finish_init()
    return bulk_refusals(manifest.require_schema(), manifest.require_ingestion_model())


def test_a_manifest_that_reads_nothing_is_accepted() -> None:
    assert _refusals(copy.deepcopy(MANIFEST)) == []


def test_a_lookup_only_step_is_refused() -> None:
    config = copy.deepcopy(MANIFEST)
    config["ingestion_model"]["resources"][0]["apply"][1]["lookup_only"] = True
    assert _refusals(config) == ["people: vertex step 'Org' is lookup_only"]


def test_a_blank_vertex_is_refused() -> None:
    config = copy.deepcopy(MANIFEST)
    vertices = config["schema"]["core_schema"]["vertex_config"]["vertices"]
    vertices.append({"name": "Note", "properties": ["text"], "blank": True})
    config["ingestion_model"]["resources"][1]["apply"].append(
        {"vertex": "Note", "from": {"text": "topic"}}
    )
    assert _refusals(config) == [
        "topics: blank vertices ['Note'] are keyed by the target"
    ]


def test_only_the_named_resources_are_checked() -> None:
    config = copy.deepcopy(MANIFEST)
    config["ingestion_model"]["resources"][0]["apply"][1]["lookup_only"] = True
    manifest = GraphManifest.from_config(config)
    manifest.finish_init()
    assert (
        bulk_refusals(
            manifest.require_schema(), manifest.require_ingestion_model(), ["topics"]
        )
        == []
    )


def _validate_bulk(conf: DBConfig, config: dict[str, Any]) -> None:
    manifest = GraphManifest.from_config(copy.deepcopy(config))
    manifest.finish_init()
    params = IngestionParams(mode="bulk", staging_dir="unused")
    Caster(
        manifest.require_schema(),
        manifest.require_ingestion_model(),
        ingestion_params=params,
    ).validate_run(conf, Bindings(), params)


def test_a_union_field_is_refused_where_an_upsert_could_not_extend_it() -> None:
    with pytest.raises(ValueError, match=r"union fields \['Person.tags'\]"):
        _validate_bulk(
            PostgresConfig(uri="postgresql://localhost/x"), contract.MANIFEST
        )


def test_a_union_field_is_loaded_where_an_upsert_writes_one() -> None:
    _validate_bulk(Neo4jConfig(uri="bolt://localhost:7687"), contract.MANIFEST)
    _validate_bulk(
        PostgresConfig(uri="postgresql://localhost/x"),
        contract.without_union(contract.MANIFEST),
    )
