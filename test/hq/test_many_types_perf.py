"""Per-document cast cost must not grow with the number of declared types.

A document touching two vertex types is cast against schemas of growing size;
its cost should track the types it touches, not the types the schema declares.
"""

from __future__ import annotations

import time

import pytest

from graflo.architecture.contract.ingestion.resource import Resource
from graflo.architecture.pipeline.runtime.resource import build_resource_runtime
from graflo.architecture.schema.edge import EdgeConfig
from graflo.architecture.schema.vertex import VertexConfig


def _chain_schema(n: int) -> tuple[VertexConfig, EdgeConfig]:
    """``n`` vertex types and about ``2n`` edges: ``v{i} -> v{i+1}``, ``v{i+2}``."""
    vertex_config = VertexConfig.from_dict(
        {
            "vertices": [
                {"name": f"v{i}", "properties": ["id"], "identity": ["id"]}
                for i in range(n)
            ]
        }
    )
    edges = [
        {"source": f"v{i}", "target": f"v{j}", "relation": f"r{i}_{j}"}
        for i in range(n)
        for j in (i + 1, i + 2)
        if j < n
    ]
    return vertex_config, EdgeConfig.from_dict({"edges": edges})


def _seconds_per_document(n: int, documents: int = 300) -> float:
    vertex_config, edge_config = _chain_schema(n)
    runtime = build_resource_runtime(
        Resource.from_dict(
            {
                "name": "pairs",
                "pipeline": [
                    {"vertex": "v0"},
                    {"vertex": "v1", "from": {"id": "other"}},
                ],
            }
        ),
        vertex_config,
        edge_config,
        {},
    )
    runtime({"id": "warm", "other": "up"})
    start = time.perf_counter()
    for k in range(documents):
        entities = runtime({"id": f"a{k}", "other": f"b{k}"})
    elapsed = time.perf_counter() - start
    assert entities["v0", "v1", "r0_1"]
    return elapsed / documents


@pytest.mark.performance
def test_cast_cost_tracks_touched_types_not_declared_types():
    small = _seconds_per_document(100)
    large = _seconds_per_document(2000)
    print(f"\nper document: n=100 {small * 1e6:.0f}us, n=2000 {large * 1e6:.0f}us")

    # 20x the declared types; a per-document walk over every edge shows as
    # a ratio near 20, an indexed lookup near 1.
    assert large / small < 4
