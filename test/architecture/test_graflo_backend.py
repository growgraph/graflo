"""Tests for GraFlo file backend I/O and pipeline integration."""

from __future__ import annotations

from pathlib import Path

import pytest

from graflo.architecture.backend import (
    GraFloBackendReader,
    GraFloBackendWriter,
    GraFloIndex,
    GraFloLayout,
)
from graflo.architecture.graph_types import GraphContainer
from graflo.architecture.schema import CoreSchema, GraphMetadata, Schema
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig
from graflo.db.graflo_backend.config import GraFloBackendConfig
from graflo.db.graflo_backend.connection import GraFloBackendConnection
from graflo.db.manager import ConnectionManager
from graflo.hq.graph_engine import GraphEngine
from graflo.onto import DBType


def _sample_schema() -> Schema:
    return Schema(
        metadata=GraphMetadata(name="demo"),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(
                vertices=[
                    Vertex(
                        name="person",
                        properties=[Field(name="id"), Field(name="name")],
                        identity=["id"],
                    )
                ]
            ),
            edge_config=EdgeConfig(
                edges=[
                    Edge(source="person", target="person", relation="knows"),
                ]
            ),
        ),
    )


def test_graflo_layout_edge_key_roundtrip() -> None:
    edge_key = ("person", "person", "knows")
    name = GraFloLayout.edge_key_to_index_name(edge_key)
    assert GraFloLayout.index_name_to_edge_key(name) == edge_key


def test_backend_writer_reader_roundtrip(tmp_path: Path) -> None:
    schema = _sample_schema()
    data = GraphContainer(
        vertices={"person": [{"id": "1", "name": "Alice"}]},
        edges={("person", "person", "knows"): [[{"id": "1"}, {"id": "2"}, {}]]},
    )
    with GraFloBackendWriter(tmp_path, chunk_size=1) as writer:
        writer.write_schema(schema)
        writer.write_vertex_batch("person", data.vertices["person"])
        for edge_key, edge_docs in data.edges.items():
            writer.write_edge_batch(edge_key, edge_docs)
        index = writer.flush_index()

    assert isinstance(index, GraFloIndex)
    assert index.vertices["person"].record_count == 1
    assert index.edges["person__knows__person"].record_count == 1

    reader = GraFloBackendReader(tmp_path)
    restored_schema = reader.read_schema()
    restored_data = reader.load_graph_container()
    assert restored_schema.metadata.name == "demo"
    assert restored_data.vertices["person"][0]["name"] == "Alice"
    assert ("person", "person", "knows") in restored_data.edges


def test_backend_reader_iter_batches(tmp_path: Path) -> None:
    schema = _sample_schema()
    with GraFloBackendWriter(tmp_path, chunk_size=1) as writer:
        writer.write_schema(schema)
        writer.write_vertex_batch("person", [{"id": "1", "name": "Alice"}])
        writer.write_edge_batch(
            ("person", "person", "knows"),
            [[{"id": "1"}, {"id": "2"}, {}]],
        )
        writer.flush_index()
    reader = GraFloBackendReader(tmp_path)

    vertex_batches = list(reader.iter_vertex_batches("person", batch_size=1))
    assert vertex_batches == [[{"id": "1", "name": "Alice"}]]

    edge_batches = list(
        reader.iter_edge_batches(("person", "person", "knows"), batch_size=1)
    )
    assert edge_batches == [[[{"id": "1"}, {"id": "2"}, {}]]]


def test_graflo_backend_connection_write_and_read(tmp_path: Path) -> None:
    schema = _sample_schema()
    config = GraFloBackendConfig(output_dir=tmp_path, chunk_size=10)

    with ConnectionManager(connection_config=config) as conn:
        assert isinstance(conn, GraFloBackendConnection)
        conn.init_db(schema, recreate_schema=True)
        conn.upsert_docs_batch(
            [{"id": "1", "name": "Alice"}],
            "person",
            match_keys=["id"],
        )
        conn.insert_edges_batch(
            [[{"id": "1"}, {"id": "2"}, {}]],
            "person",
            "person",
            "knows",
            match_keys_source=("id",),
            match_keys_target=("id",),
        )

    with ConnectionManager(connection_config=config) as conn:
        docs = conn.fetch_all_docs("person")
        edges = conn.fetch_all_edges("person", "person", "knows")
        assert docs[0]["name"] == "Alice"
        assert edges[0][0]["id"] == "1"


def test_graflo_backend_registered_as_source_and_target() -> None:
    config = GraFloBackendConfig(output_dir=Path("/tmp/graflo-backend-test"))
    assert config.can_be_source()
    assert config.can_be_target()
    assert config.connection_type == DBType.GRAFLO_BACKEND
    assert DBType.GRAFLO_BACKEND in ConnectionManager.graph_export_flavors()


def test_resolve_target_schema_skips_sanitization_for_file_backend() -> None:
    engine = GraphEngine(target_db_flavor=DBType.ARANGO)
    schema = _sample_schema()
    target = GraFloBackendConfig(output_dir=Path("/tmp/graflo-backend-test"))
    resolved = engine._resolve_target_schema(schema, target)
    assert resolved is schema


def test_resolve_target_schema_honors_target_flavor_hint() -> None:
    engine = GraphEngine(target_db_flavor=DBType.ARANGO)
    schema = _sample_schema()
    target = GraFloBackendConfig(
        output_dir=Path("/tmp/graflo-backend-test"),
        target_flavor_hint=DBType.ARANGO,
    )
    resolved = engine._resolve_target_schema(schema, target)
    assert resolved.db_profile.db_flavor == DBType.ARANGO


def test_ingest_manifest_to_file_backend(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from suthing import FileHandle

    from graflo import GraphManifest
    from graflo.hq.caster import IngestionParams

    example_dir = (
        Path(__file__).resolve().parents[2] / "examples" / "13-graph-export-migration"
    )
    manifest = GraphManifest.from_config(FileHandle.load(example_dir / "manifest.yaml"))
    manifest.finish_init()
    backend = GraFloBackendConfig(output_dir=tmp_path / "csv-backend")
    engine = GraphEngine(target_db_flavor=DBType.GRAFLO_BACKEND)
    monkeypatch.chdir(example_dir)
    engine.define_and_ingest(
        manifest=manifest,
        target_db_config=backend,
        ingestion_params=IngestionParams(clear_data=True),
        recreate_schema=True,
    )

    reader = GraFloBackendReader(backend.output_dir)
    people = [doc for batch in reader.iter_vertex_batches("person") for doc in batch]
    assert len(people) >= 3
    index = reader.read_index()
    assert index.vertices["person"].record_count >= 3


def test_writer_resume_appends_chunks(tmp_path: Path) -> None:
    schema = _sample_schema()
    config = GraFloBackendConfig(output_dir=tmp_path, chunk_size=1)

    with ConnectionManager(connection_config=config) as conn:
        conn.init_db(schema, recreate_schema=True)
        conn.upsert_docs_batch([{"id": "1", "name": "Alice"}], "person", ["id"])

    with ConnectionManager(connection_config=config) as conn:
        conn.upsert_docs_batch([{"id": "2", "name": "Bob"}], "person", ["id"])

    reader = GraFloBackendReader(tmp_path)
    docs = [doc for batch in reader.iter_vertex_batches("person") for doc in batch]
    assert {doc["id"] for doc in docs} == {"1", "2"}
    index = reader.read_index()
    assert index.vertices["person"].record_count == 2
    assert len(index.vertices["person"].chunks) == 2
