"""Several writers on one file-backend directory keep every record."""

from __future__ import annotations

import multiprocessing
from pathlib import Path

from graflo.architecture.backend import GraFloBackendReader, GraFloBackendWriter
from graflo.architecture.schema import CoreSchema, GraphMetadata, Schema
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig

KNOWS = ("person", "person", "knows")


def _schema() -> Schema:
    return Schema(
        metadata=GraphMetadata(name="demo"),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(
                vertices=[
                    Vertex(
                        name="person", properties=[Field(name="id")], identity=["id"]
                    )
                ]
            ),
            edge_config=EdgeConfig(
                edges=[Edge(source="person", target="person", relation="knows")]
            ),
        ),
    )


def _ids(root: Path) -> list[str]:
    reader = GraFloBackendReader(root)
    return sorted(
        doc["id"] for batch in reader.iter_vertex_batches("person") for doc in batch
    )


def _write(root: Path, prefix: str, batches: int) -> None:
    for n in range(batches):
        with GraFloBackendWriter(root, resume=True) as writer:
            writer.write_vertex_batch("person", [{"id": f"{prefix}{n}"}])


def test_two_open_writers_keep_both_batches(tmp_path: Path) -> None:
    with GraFloBackendWriter(tmp_path) as setup:
        setup.write_schema(_schema())
    first = GraFloBackendWriter(tmp_path, resume=True)
    second = GraFloBackendWriter(tmp_path, resume=True)

    first.write_vertex_batch("person", [{"id": "a"}])
    second.write_vertex_batch("person", [{"id": "b"}])
    second.write_edge_batch(KNOWS, [[{"id": "b"}, {"id": "a"}, {}]])
    first.flush_index()
    index = second.flush_index()

    assert _ids(tmp_path) == ["a", "b"]
    entry = index.vertices["person"]
    assert entry.record_count == 2
    assert len(set(entry.chunks)) == 2
    assert index.edges["person__knows__person"].record_count == 1


def test_a_writer_flushed_twice_counts_its_records_once(tmp_path: Path) -> None:
    with GraFloBackendWriter(tmp_path) as writer:
        writer.write_schema(_schema())
        writer.write_vertex_batch("person", [{"id": "a"}, {"id": "b"}])
        writer.flush_index()

    index = GraFloBackendReader(tmp_path).read_index()
    assert index.vertices["person"].record_count == 2
    assert len(index.vertices["person"].chunks) == 1


def test_writers_in_several_processes_keep_every_record(tmp_path: Path) -> None:
    with GraFloBackendWriter(tmp_path) as setup:
        setup.write_schema(_schema())
    context = multiprocessing.get_context("spawn")
    workers = [
        context.Process(target=_write, args=(tmp_path, prefix, 5))
        for prefix in ("p", "q", "r")
    ]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=120)
        assert worker.exitcode == 0

    expected = sorted(f"{prefix}{n}" for prefix in ("p", "q", "r") for n in range(5))
    assert _ids(tmp_path) == expected
    index = GraFloBackendReader(tmp_path).read_index()
    assert index.vertices["person"].record_count == 15
