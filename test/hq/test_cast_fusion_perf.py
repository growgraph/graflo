"""Opt-in casting timings with value checks, without a noisy CI time limit.

Run with ``uv run pytest test/hq/test_cast_fusion_perf.py --run-performance -s``.
Compare raw timings from clean before/after checkouts under the same CPU and
dependency configuration; a single checkout does not establish a speedup.
"""

from __future__ import annotations

import asyncio
import gc
import hashlib
import json
import platform
import time
from pathlib import Path

import pytest

from graflo.architecture.graph_types import merge
from graflo.hq.document_caster import DocumentCaster
from graflo.hq.ingestion_parameters import IngestionParams
from test.hq.test_cast_dispatch import manifest

__all__ = ["manifest"]


@pytest.mark.performance
def test_cast_fusion_throughput_preserves_values(manifest) -> None:
    documents = 10_000
    batch_size = 1_000
    docs = [
        {"pid": f"p{i}", "name": f"n{i}", "oid": f"o{i % 7}", "rank": str(i)}
        for i in range(documents)
    ]
    caster = DocumentCaster(manifest.require_ingestion_model())
    params = IngestionParams(cast_executor="inline", n_cores=1)

    async def cast():
        return [
            await caster.cast_batch(
                docs[offset : offset + batch_size], "r", params=params
            )
            for offset in range(0, documents, batch_size)
        ]

    async def measure():
        await caster.cast_batch(docs[:batch_size], "r", params=params)
        samples = []
        for _ in range(7):
            gc.collect()
            wall = time.perf_counter()
            cpu = time.process_time()
            results = await cast()
            samples.append(
                {
                    "cpu_seconds": time.process_time() - cpu,
                    "seconds": time.perf_counter() - wall,
                }
            )
            people = [
                row for result in results for row in result.graph.vertices["person"]
            ]
            assert people == [
                {"pid": f"p{i}", "name": f"n{i}"} for i in range(documents)
            ]
            edges = [
                row
                for result in results
                for row in result.graph.edges["person", "org", "works_at"]
            ]
            assert edges == [
                ({"pid": f"p{i}"}, {"oid": f"o{i % 7}"}, {}) for i in range(documents)
            ]
            assert all(not result.failures for result in results)
            assert all(type(doc["rank"]) is str for doc in docs)
        return samples

    try:
        samples = asyncio.run(measure())
    finally:
        caster.close()
    print(
        json.dumps(
            {
                "documents": documents,
                "batch_size": batch_size,
                "python": platform.python_version(),
                "cast_executor": params.cast_executor,
                "n_cores": params.n_cores,
                "fusion_sha256": hashlib.sha256(
                    Path(merge.__file__).read_bytes()
                ).hexdigest(),
                "samples": samples,
                "values_checked": True,
            }
        )
    )
