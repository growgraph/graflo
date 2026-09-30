"""An edge step that names no relation takes the one declared between its endpoints."""

from __future__ import annotations

import asyncio

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.hq.document_caster import DocumentCaster
from graflo.hq.ingestion_parameters import IngestionParams


def _manifest(edges: list[dict], step: dict) -> GraphManifest:
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "staff", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "person",
                                "properties": ["pid"],
                                "identity": ["pid"],
                            },
                            {"name": "org", "properties": ["oid"], "identity": ["oid"]},
                        ]
                    },
                    "edge_config": {"edges": edges},
                },
            },
            "ingestion_model": {
                "resources": [
                    {
                        "name": "staff",
                        "pipeline": [{"vertex": "person"}, {"vertex": "org"}, step],
                    }
                ]
            },
        }
    )
    manifest.finish_init()
    return manifest


def _edge_ids(manifest: GraphManifest) -> list:
    caster = DocumentCaster(manifest.require_ingestion_model())
    result = asyncio.run(
        caster.cast_batch(
            [{"pid": "p1", "oid": "o1"}], "staff", params=IngestionParams()
        )
    )
    return sorted(result.graph.edges, key=str)


WORKS_AT = {"source": "person", "target": "org", "relation": "works_at"}
OWNS = {"source": "person", "target": "org", "relation": "owns"}


def test_a_step_without_a_relation_writes_the_declared_one() -> None:
    manifest = _manifest([WORKS_AT], {"edge": {"from": "person", "to": "org"}})

    assert _edge_ids(manifest) == [("person", "org", "works_at")]


def test_the_step_does_not_declare_a_relation_less_edge() -> None:
    manifest = _manifest([WORKS_AT], {"edge": {"from": "person", "to": "org"}})

    edges = manifest.require_schema().core_schema.edge_config.edges
    assert [e.edge_id for e in edges] == [("person", "org", "works_at")]


def test_several_declared_relations_are_refused_at_load() -> None:
    with pytest.raises(ValueError, match="names no relation"):
        _manifest([WORKS_AT, OWNS], {"edge": {"from": "person", "to": "org"}})


def test_a_named_relation_is_kept() -> None:
    manifest = _manifest(
        [WORKS_AT, OWNS],
        {"edge": {"from": "person", "to": "org", "relation": "owns"}},
    )

    assert _edge_ids(manifest) == [("person", "org", "owns")]


def test_no_declared_relation_keeps_a_relation_less_edge() -> None:
    manifest = _manifest(
        [{"source": "person", "target": "org"}],
        {"edge": {"from": "person", "to": "org"}},
    )

    assert _edge_ids(manifest) == [("person", "org", None)]
