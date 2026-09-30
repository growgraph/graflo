"""Inference reads the edges a resource declares, not ones a record registered.

An edge step that names its edge per document registers that edge type while
it casts. Inferring over the live config would then write the type for every
later record, so a record's edges would depend on which records came first.
"""

from __future__ import annotations

import asyncio

from graflo.architecture.contract.manifest import GraphManifest
from graflo.hq.document_caster import DocumentCaster
from graflo.hq.ingestion_parameters import IngestionParams

ADVISES = {"pid": "p1", "oid": "o1", "rel": "advises"}
UNMAPPED = {"pid": "p2", "oid": "o2", "rel": "unknown"}


def _manifest() -> GraphManifest:
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
                    "edge_config": {"edges": []},
                },
            },
            "ingestion_model": {
                "resources": [
                    {
                        "name": "staff",
                        "pipeline": [
                            {"vertex": "person", "from": {"pid": "pid"}, "role": "S"},
                            {"vertex": "org", "from": {"oid": "oid"}, "role": "T"},
                            {
                                "edge": {
                                    "source_role": "S",
                                    "target_role": "T",
                                    "relation_field": "rel",
                                    "relation_map": {"advises": "advises"},
                                    "relation_map_only": True,
                                }
                            },
                        ],
                    }
                ]
            },
        }
    )
    manifest.finish_init()
    return manifest


def _edges(docs: list[dict]) -> set[tuple]:
    caster = DocumentCaster(_manifest().require_ingestion_model())
    graph = asyncio.run(
        caster.cast_batch(docs, "staff", params=IngestionParams(dynamic_edges=True))
    ).graph
    return {
        (edge_id[2], u["pid"], v["oid"])
        for edge_id, rows in graph.edges.items()
        for u, v, _ in rows
    }


def test_a_record_the_step_skips_gets_no_edge_whatever_came_before() -> None:
    assert _edges([ADVISES, UNMAPPED]) == {("advises", "p1", "o1")}
    assert _edges([UNMAPPED, ADVISES]) == {("advises", "p1", "o1")}
