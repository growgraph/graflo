"""Discovery: what a connection offers to read, as connectors, for every kind."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from graflo.architecture.contract.bindings import (
    Bindings,
    FileConnector,
    KafkaConnector,
    SparqlConnector,
    TableConnector,
)
from graflo.architecture.onto_sql import (
    ColumnInfo,
    ForeignKeyInfo,
    RawTableInfo,
    SchemaIntrospectionResult,
)
from graflo.connections.kinds import ConnectionKind
from graflo.connections.onto import Neo4jConfig, PostgresConfig, SparqlEndpointConfig
from graflo.connections.provider import InMemoryConnectionProvider
from graflo.connections.sources import (
    FilesConnConfig,
    KafkaConnConfig,
    RestApiConnConfig,
)
from graflo.data_source.files import S3ObjectDataSource
from graflo.hq import discovery
from graflo.hq.discovery import discover
from graflo.hq.registry_builder import RegistryBuilder
from graflo.hq.sampler import ResourceSampler


class FakeS3:
    """A bucket held in memory, answering the calls files connections make."""

    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects

    def get_paginator(self, name: str) -> FakeS3:
        assert name == "list_objects_v2"
        return self

    def paginate(self, *, Bucket: str, Prefix: str, Delimiter: str | None = None):
        contents = []
        for key, body in sorted(self.objects.items()):
            if not key.startswith(Prefix):
                continue
            if Delimiter and Delimiter in key[len(Prefix) :]:
                continue
            contents.append({"Key": key, "Size": len(body)})
        yield {"Contents": contents}

    def download_file(self, bucket: str, key: str, path: str) -> None:
        Path(path).write_bytes(self.objects[key])


@pytest.fixture
def s3(monkeypatch: pytest.MonkeyPatch) -> FakeS3:
    fake = FakeS3(
        {
            "raw/orders/2026.csv": b"id,total\n1,9.5\n2,3.0\n",
            "raw/customers.jsonl": b'{"id": "c1"}\n{"id": "c2"}\n',
            "raw/README.md": b"notes",
            "other/ignored.csv": b"id\n1\n",
        }
    )
    from graflo.object_storage import s3_client

    monkeypatch.setattr(s3_client, "boto3_s3_client", lambda cfg: fake)
    return fake


S3_FILES = FilesConnConfig(root="s3://lake/raw", endpoint_url="http://minio:9000")


class TestFiles:
    def test_s3_files_under_the_root_are_candidates(self, s3: FakeS3) -> None:
        found = discover(S3_FILES)

        assert found.kind == ConnectionKind.FILES
        assert [r.name for r in found.resources] == ["customers", "orders_2026"]
        orders = found.resources[1]
        assert isinstance(orders.connector, FileConnector)
        assert str(orders.connector.sub_path) == "orders"
        assert orders.size_bytes == len(s3.objects["raw/orders/2026.csv"])

    def test_a_pick_of_s3_files_samples_through_its_connection(
        self, s3: FakeS3
    ) -> None:
        bindings = discover(S3_FILES).bindings("lake", ["orders_2026"])
        sample = ResourceSampler(max_docs=10).sample_bindings(
            bindings, connection_provider=InMemoryConnectionProvider({"lake": S3_FILES})
        )

        (orders,) = sample.samples
        assert orders.resource_name == "orders_2026"
        assert [doc["id"] for doc in orders.docs] == ["1", "2"]

    def test_ingest_reads_the_same_s3_files(self, s3: FakeS3) -> None:
        bindings = discover(S3_FILES).bindings("lake", ["customers"])
        (connector,) = bindings.connectors
        assert isinstance(connector, FileConnector)

        sources = RegistryBuilder.file_sources(
            connector, bindings, InMemoryConnectionProvider({"lake": S3_FILES})
        )

        (source,) = sources
        assert isinstance(source, S3ObjectDataSource)
        assert source.key == "raw/customers.jsonl"
        rows = [row for batch in source.iter_batches() for row in batch]
        assert rows == [{"id": "c1"}, {"id": "c2"}]

    def test_local_files_are_candidates_too(self, tmp_path: Path) -> None:
        (tmp_path / "a").mkdir()
        (tmp_path / "a" / "events.jsonl").write_text(json.dumps({"id": 1}) + "\n")
        (tmp_path / "notes.md").write_text("not data")

        found = discover(FilesConnConfig(root=str(tmp_path)))

        assert [r.name for r in found.resources] == ["a_events"]

    def test_a_numbered_run_of_s3_files_is_one_candidate(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        run = FakeS3(
            {
                "raw/events/part-0001.jsonl": b'{"id": 1}\n',
                "raw/events/part-0002.jsonl": b'{"id": 2}\n',
                "raw/events/schema.json": b'{"id": "s"}',
                "other/events/part-0003.jsonl": b'{"id": 3}\n',
            }
        )
        from graflo.object_storage import s3_client

        monkeypatch.setattr(s3_client, "boto3_s3_client", lambda cfg: run)

        found = discover(S3_FILES)

        assert [r.name for r in found.resources] == ["events_part", "events_schema"]
        events = found.resources[0]
        assert isinstance(events.connector, FileConnector)
        assert events.connector.regex == r"^part\-\d+\.jsonl$"
        assert (events.file_count, events.size_bytes) == (2, 20)

        bindings = found.bindings("lake", ["events_part"])
        sample = ResourceSampler(max_docs=10).sample_bindings(
            bindings, connection_provider=InMemoryConnectionProvider({"lake": S3_FILES})
        )
        assert [doc["id"] for doc in sample.samples[0].docs] == [1]
        (connector,) = bindings.connectors
        assert isinstance(connector, FileConnector)
        sources = RegistryBuilder.file_sources(
            connector, bindings, InMemoryConnectionProvider({"lake": S3_FILES})
        )
        assert [s.key for s in sources if isinstance(s, S3ObjectDataSource)] == [
            "raw/events/part-0001.jsonl",
            "raw/events/part-0002.jsonl",
        ]

    def test_a_numbered_run_of_local_files_is_one_candidate(
        self, tmp_path: Path
    ) -> None:
        for day in ("2026-11-04", "2026-11-05"):
            (tmp_path / f"orders-{day}.csv").write_text("id\n1\n")
        (tmp_path / "2026.csv").write_text("id\n1\n")
        (tmp_path / "notes.txt").write_text("not a table")

        found = discover(FilesConnConfig(root=str(tmp_path)))

        assert [(r.name, r.file_count) for r in found.resources] == [
            ("2026", 1),
            ("orders", 2),
        ]
        regexes = [
            r.connector.regex
            for r in found.resources
            if isinstance(r.connector, FileConnector)
        ]
        assert regexes == [r"^2026\.csv$", r"^orders\-\d+\-\d+\-\d+\.csv$"]

    def test_the_limit_marks_the_listing_truncated(self, s3: FakeS3) -> None:
        found = discover(S3_FILES, limit=1)
        assert len(found.resources) == 1 and found.truncated


def test_postgres_tables_are_candidates_with_what_they_declare(monkeypatch) -> None:
    from graflo.hq.graph_engine import GraphEngine

    table = RawTableInfo(
        name="orders",
        schema_name="shop",
        description="What customers bought",
        columns=[
            ColumnInfo(name="id", type="integer", is_pk=True, is_nullable="NO"),
            ColumnInfo(name="user_id", type="integer"),
        ],
        primary_key=["id"],
        foreign_keys=[ForeignKeyInfo(column="user_id", references_table="users")],
        row_count_estimate=-1,
    )
    monkeypatch.setattr(
        GraphEngine,
        "introspect",
        lambda self, conn, **kw: SchemaIntrospectionResult(
            vertex_tables=[], edge_tables=[], raw_tables=[table], schema_name="shop"
        ),
    )

    (orders,) = discover(PostgresConfig(uri="postgresql://h/db")).resources

    assert orders.connector == TableConnector(
        name="orders", table_name="orders", schema_name="shop"
    )
    assert orders.row_estimate is None
    assert orders.description == "What customers bought"
    assert [(c.name, c.is_pk, c.nullable) for c in orders.columns] == [
        ("id", True, False),
        ("user_id", False, True),
    ]
    assert orders.foreign_keys[0].references_resource == "users"


def test_a_table_in_no_named_schema_reads_the_connection_default(monkeypatch) -> None:
    """An empty schema in introspection is no schema, not a schema called ''."""
    from graflo.hq.graph_engine import GraphEngine

    table = RawTableInfo(
        name="orders", schema_name="", columns=[], primary_key=[], foreign_keys=[]
    )
    monkeypatch.setattr(
        GraphEngine,
        "introspect",
        lambda self, conn, **kw: SchemaIntrospectionResult(
            vertex_tables=[], edge_tables=[], raw_tables=[table], schema_name=""
        ),
    )

    (orders,) = discover(PostgresConfig(uri="postgresql://h/db")).resources

    assert isinstance(orders.connector, TableConnector)
    assert orders.connector.schema_name is None


def test_sparql_classes_are_candidates_by_local_name(monkeypatch) -> None:
    rows: list[dict[str, Any]] = [
        {"class": {"value": "http://ex.org/Person"}, "n": {"value": "12"}},
        {"class": {"value": "http://other.org/ns#Person"}, "n": {"value": "3"}},
    ]
    monkeypatch.setattr(discovery, "sparql_select", lambda conn, query: rows)

    found = discover(SparqlEndpointConfig(uri="http://h:3030", dataset="kb"))

    assert [r.name for r in found.resources] == ["Person", "Person_2"]
    assert found.resources[1].connector == SparqlConnector(
        name="Person_2", rdf_class="http://other.org/ns#Person"
    )
    assert found.resources[0].row_estimate == 12


def test_a_sparql_connector_reads_the_endpoint_of_its_connection() -> None:
    endpoint = SparqlEndpointConfig(uri="http://h:3030", dataset="kb")
    connector = SparqlConnector(name="people", rdf_class="http://ex.org/Person")
    bindings = Bindings(
        connectors=[connector],
        connector_connection=[{"connector": "people", "conn_proxy": "kb"}],
    )

    source: Any = RegistryBuilder.sparql_source(
        connector,
        bindings,
        resource_name="people",
        page_size=10,
        connection_provider=InMemoryConnectionProvider({"kb": endpoint}),
    )

    assert source.config.endpoint_url == endpoint.query_endpoint


class TestKafka:
    def test_topics_are_candidates_and_internal_ones_are_not(self, monkeypatch):
        class Metadata:
            topics = {"orders": None, "__consumer_offsets": None, "audit.log": None}

        class Admin:
            def __init__(self, config: dict[str, str]) -> None:
                assert config["bootstrap.servers"] == "k:9092"

            def list_topics(self, timeout: float) -> Metadata:
                return Metadata()

        import confluent_kafka.admin

        monkeypatch.setattr(confluent_kafka.admin, "AdminClient", Admin)

        found = discover(KafkaConnConfig(bootstrap_servers="k:9092"))

        assert [r.name for r in found.resources] == ["audit_log", "orders"]
        assert found.resources[0].connector == KafkaConnector(
            name="audit_log", topics=["audit.log"], group_id="graflo-audit_log"
        )

    def test_a_sample_reads_in_a_consumer_group_of_its_own(self, monkeypatch):
        seen: dict[str, Any] = {}

        class FakeKafkaSource:
            def __init__(self, config) -> None:
                seen["config"] = config

            def iter_batches(self, batch_size: int, limit: int):
                yield [{"id": 1}]

            def close(self) -> None:
                seen["closed"] = True

        import graflo.data_source.kafka

        monkeypatch.setattr(
            graflo.data_source.kafka, "KafkaDataSource", FakeKafkaSource
        )
        connector = KafkaConnector(name="orders", topics=["orders"], group_id="etl")
        bindings = Bindings(
            connectors=[connector],
            connector_connection=[{"connector": "orders", "conn_proxy": "events"}],
        )

        sample = ResourceSampler(max_docs=5).sample_connector(
            connector,
            resource_name="orders",
            bindings=bindings,
            connection_provider=InMemoryConnectionProvider(
                {"events": KafkaConnConfig(bootstrap_servers="k:9092")}
            ),
        )

        assert sample.docs == [{"id": 1}]
        assert seen["closed"]
        assert seen["config"].group_id.startswith("graflo-sample-")
        assert seen["config"].max_wait_ms is not None


def test_an_api_connection_lists_nothing_by_itself() -> None:
    found = discover(RestApiConnConfig(base_url="http://h"))
    assert found.kind == ConnectionKind.API and found.resources == []


def test_a_graph_target_is_not_discovered() -> None:
    with pytest.raises(ValueError, match="written to, not read from"):
        discover(Neo4jConfig(uri="bolt://h:7687"))


def test_a_pick_names_only_discovered_candidates(s3: FakeS3) -> None:
    with pytest.raises(ValueError, match="not discovered here: invoices"):
        discover(S3_FILES).bindings("lake", ["invoices"])
