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
from graflo.connections.graflo_backend import GraFloBackendConfig
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

    from graflo.architecture import GraphManifest
    from graflo.hq.caster import IngestionParams

    example_dir = (
        Path(__file__).resolve().parents[2] / "examples" / "14-file-backend-export"
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


def test_ingest_reads_a_given_registry_in_the_target_flavor(tmp_path: Path) -> None:
    from graflo.architecture import GraphManifest
    from graflo.data_source.factory import DataSourceFactory
    from graflo.data_source.registry import DataSourceRegistry

    config = {
        "schema": {
            "metadata": {"name": "hr"},
            "graph": {
                "vertex_config": {
                    "vertices": [
                        {"name": "person", "properties": ["id"], "identity": ["id"]}
                    ]
                },
                "edge_config": {"edges": []},
            },
            "db_profile": {},
        },
        "ingestion_model": {
            "resources": [{"name": "people", "pipeline": [{"vertex": "person"}]}]
        },
    }
    backend = GraFloBackendConfig(output_dir=tmp_path / "graph")
    engine = GraphEngine(target_db_flavor=DBType.GRAFLO_BACKEND)
    declared = GraphManifest.from_config(config)
    declared.finish_init()
    engine.define_schema(manifest=declared, target_db_config=backend)

    # A second manifest object: nothing has pointed it at the target yet.
    manifest = GraphManifest.from_config(config)
    manifest.finish_init()
    registry = DataSourceRegistry()
    registry.register(
        DataSourceFactory.create_in_memory_data_source([{"id": "a"}, {"id": "b"}]),
        resource_name="people",
    )
    engine.ingest(
        manifest=manifest, target_db_config=backend, data_source_registry=registry
    )

    assert manifest.require_schema().db_profile.db_flavor == DBType.GRAFLO_BACKEND
    reader = GraFloBackendReader(backend.output_dir)
    people = [doc for batch in reader.iter_vertex_batches("person") for doc in batch]
    assert {doc["id"] for doc in people} == {"a", "b"}


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


def _renamed_schema() -> Schema:
    """A vertex type and a relation TigerGraph stores under other names."""
    return Schema(
        metadata=GraphMetadata(name="demo"),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(
                vertices=[
                    Vertex(
                        name="vertex",
                        properties=[Field(name="id"), Field(name="colour")],
                        identity=["id"],
                    )
                ]
            ),
            edge_config=EdgeConfig(
                edges=[Edge(source="vertex", target="vertex", relation="to")]
            ),
        ),
    )


@pytest.fixture
def hinted_backend(tmp_path: Path) -> GraFloBackendConfig:
    """A file backend written for TigerGraph by migrating a plain one into it."""
    source = tmp_path / "plain"
    with GraFloBackendWriter(source) as writer:
        writer.write_schema(_renamed_schema())
        writer.write_vertex_batch(
            "vertex", [{"id": "1", "colour": "x"}, {"id": "2", "colour": "y"}]
        )
        writer.write_edge_batch(
            ("vertex", "vertex", "to"), [[{"id": "1"}, {"id": "2"}, {}]]
        )
        writer.flush_index()
    target = GraFloBackendConfig(
        output_dir=tmp_path / "hinted", target_flavor_hint=DBType.TIGERGRAPH
    )
    GraphEngine(target_db_flavor=DBType.GRAFLO_BACKEND).migrate_graph(
        GraFloBackendConfig(output_dir=source), target
    )
    return target


class TestRenamedNamesReadBack:
    def test_the_data_is_stored_under_the_stored_names(
        self, hinted_backend: GraFloBackendConfig
    ) -> None:
        index = GraFloBackendReader(hinted_backend.output_dir).read_index()

        assert list(index.vertices) == ["vertex_vertex"]

    def test_the_reader_returns_it_under_the_logical_names(
        self, hinted_backend: GraFloBackendConfig
    ) -> None:
        data = GraFloBackendReader(hinted_backend.output_dir).load_graph_container()

        assert data.vertices["vertex"] == [
            {"id": "1", "colour": "x"},
            {"id": "2", "colour": "y"},
        ]
        assert data.edges[("vertex", "vertex", "to")] == [
            [{"id": "1"}, {"id": "2"}, {}]
        ]

    def test_an_export_reads_it_back(self, hinted_backend: GraFloBackendConfig) -> None:
        exported = GraphEngine(target_db_flavor=DBType.GRAFLO_BACKEND).export_graph(
            hinted_backend
        )

        assert [doc["colour"] for doc in exported.data.vertices["vertex"]] == ["x", "y"]
        assert len(exported.data.edges[("vertex", "vertex", "to")]) == 1

    def test_the_export_limit_holds(self, hinted_backend: GraFloBackendConfig) -> None:
        exported = GraphEngine(target_db_flavor=DBType.GRAFLO_BACKEND).export_graph(
            hinted_backend, data_limit=1
        )

        assert len(exported.data.vertices["vertex"]) == 1

    def test_a_walk_follows_its_edges(
        self, hinted_backend: GraFloBackendConfig
    ) -> None:
        schema = GraFloBackendReader(hinted_backend.output_dir).read_schema()
        with ConnectionManager(connection_config=hinted_backend) as conn:
            reached = conn.graph_neighbors("vertex", {"id": "1"}, schema=schema)

        assert sum(len(rows) for rows in reached.edges.values()) == 1


def test_a_walk_follows_the_edges_of_a_plain_directory(tmp_path: Path) -> None:
    schema = _sample_schema()
    with GraFloBackendWriter(tmp_path) as writer:
        writer.write_schema(schema)
        writer.write_vertex_batch("person", [{"id": "1"}, {"id": "2"}])
        writer.write_edge_batch(
            ("person", "person", "knows"), [[{"id": "1"}, {"id": "2"}, {}]]
        )
        writer.flush_index()

    with ConnectionManager(
        connection_config=GraFloBackendConfig(output_dir=tmp_path)
    ) as conn:
        reached = conn.graph_neighbors("person", {"id": "1"}, schema=schema)

    assert [doc["id"] for doc in reached.vertices["person"]] == ["2"]


class TestAggregate:
    @pytest.fixture
    def conn(self, tmp_path: Path):
        schema = Schema(
            metadata=GraphMetadata(name="shop"),
            core_schema=CoreSchema(
                vertex_config=VertexConfig(
                    vertices=[
                        Vertex(
                            name="item",
                            properties=[
                                Field(name="id"),
                                Field(name="kind"),
                                Field(name="price"),
                            ],
                            identity=["id"],
                        )
                    ]
                ),
                edge_config=EdgeConfig(edges=[]),
            ),
        )
        with GraFloBackendWriter(tmp_path) as writer:
            writer.write_schema(schema)
            writer.write_vertex_batch(
                "item",
                [
                    {"id": "1", "kind": "tool", "price": 4},
                    {"id": "2", "kind": "tool", "price": 2},
                    {"id": "3", "kind": "food", "price": 3},
                    {"id": "4", "kind": "food"},
                ],
            )
            writer.flush_index()
        with ConnectionManager(
            connection_config=GraFloBackendConfig(output_dir=tmp_path)
        ) as conn:
            yield conn

    def test_count(self, conn) -> None:
        from graflo.onto import AggregationType

        assert conn.aggregate("item", AggregationType.COUNT) == 4

    def test_count_by_a_field(self, conn) -> None:
        from graflo.onto import AggregationType

        assert conn.aggregate("item", AggregationType.COUNT, discriminant="kind") == {
            "tool": 2,
            "food": 2,
        }

    def test_count_with_a_filter(self, conn) -> None:
        from graflo.onto import AggregationType

        count = conn.aggregate(
            "item",
            AggregationType.COUNT,
            filters={"field": "kind", "cmp_operator": "==", "value": "food"},
        )
        assert count == 2

    def test_max_min_and_average_skip_missing_values(self, conn) -> None:
        from graflo.onto import AggregationType

        assert (
            conn.aggregate("item", AggregationType.MAX, aggregated_field="price") == 4
        )
        assert (
            conn.aggregate("item", AggregationType.MIN, aggregated_field="price") == 2
        )
        assert (
            conn.aggregate("item", AggregationType.AVERAGE, aggregated_field="price")
            == 3
        )

    def test_sorted_unique(self, conn) -> None:
        from graflo.onto import AggregationType

        assert conn.aggregate(
            "item", AggregationType.SORTED_UNIQUE, aggregated_field="kind"
        ) == ["food", "tool"]

    def test_nothing_to_aggregate_is_none(self, conn) -> None:
        from graflo.onto import AggregationType

        assert (
            conn.aggregate("item", AggregationType.MAX, aggregated_field="size") is None
        )

    def test_a_field_is_required_beyond_a_count(self, conn) -> None:
        from graflo.onto import AggregationType

        with pytest.raises(ValueError, match="aggregated_field"):
            conn.aggregate("item", AggregationType.MAX)


def test_an_edge_filter_keeps_the_matching_edges(tmp_path: Path) -> None:
    with GraFloBackendWriter(tmp_path) as writer:
        writer.write_schema(_sample_schema())
        writer.write_vertex_batch("person", [{"id": "1"}, {"id": "2"}, {"id": "3"}])
        writer.write_edge_batch(
            ("person", "person", "knows"),
            [
                [{"id": "1"}, {"id": "2"}, {"since": 2020}],
                [{"id": "1"}, {"id": "3"}, {"since": 2010}],
            ],
        )
        writer.flush_index()

    with ConnectionManager(
        connection_config=GraFloBackendConfig(output_dir=tmp_path)
    ) as conn:
        rows = conn.fetch_edges(
            "person",
            "1",
            edge_type="knows",
            filters={"field": "since", "cmp_operator": ">", "value": 2015},
        )

    assert [row["_to_key"] for row in rows] == ["2"]
