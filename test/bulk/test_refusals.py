"""Bulk mode refuses, by resource and step, what reads the target while ingesting."""

from __future__ import annotations

import copy
from typing import Any

from graflo.architecture.contract.manifest import GraphManifest
from graflo.hq.bulk.refusals import bulk_refusals
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
