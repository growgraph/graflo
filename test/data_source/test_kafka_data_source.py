"""Tests for Kafka data source (unit decode + optional live broker)."""

from __future__ import annotations

import json
import os
import time
import uuid

import pytest

from graflo.architecture.contract.bindings import Bindings, KafkaConnector
from graflo.connections.provider import InMemoryConnectionProvider
from graflo.connections.sources import KafkaConnConfig, KafkaGeneralizedConnConfig
from graflo.data_source.kafka import (
    KafkaConfig,
    KafkaDataSource,
    decode_kafka_json_value,
)
from graflo.hq.ingestion_parameters import IngestionParams
from graflo.hq.registry_builder import RegistryBuilder
from test.conftest import fetch_manifest_obj

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def test_decode_kafka_json_object() -> None:
    assert decode_kafka_json_value(b'{"a": 1, "b": "x"}') == {"a": 1, "b": "x"}
    assert decode_kafka_json_value('{"a": 1}') == {"a": 1}


def test_decode_kafka_json_rejects_non_objects() -> None:
    assert decode_kafka_json_value(b"[1, 2]") is None
    assert decode_kafka_json_value(b'"str"') is None
    assert decode_kafka_json_value(b"not-json") is None
    assert decode_kafka_json_value(None) is None
    assert decode_kafka_json_value(b"\xff\xfe") is None


def test_kafka_connector_build_config() -> None:
    connector = KafkaConnector(
        name="events",
        topics=["graflo.test.events"],
        group_id="graflo-test",
        row_annotations={"source": "kafka"},
        idle_ms=1000,
    )
    conn = KafkaConnConfig(bootstrap_servers="localhost:9092")
    cfg = connector.build_kafka_config(conn=conn)
    assert isinstance(cfg, KafkaConfig)
    assert cfg.topics == ["graflo.test.events"]
    assert cfg.group_id == "graflo-test"
    assert cfg.bootstrap_servers == "localhost:9092"
    assert cfg.row_annotations == {"source": "kafka"}
    assert cfg.idle_ms == 1000


def test_kafka_conn_config_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAFKA_LOCAL_BOOTSTRAP_SERVERS", "broker:9092")
    monkeypatch.setenv("KAFKA_LOCAL_SECURITY_PROTOCOL", "PLAINTEXT")
    monkeypatch.setenv("KAFKA_LOCAL_CLIENT_ID", "graflo")
    cfg = KafkaConnConfig.from_env("KAFKA_LOCAL_")
    assert cfg.bootstrap_servers == "broker:9092"
    assert cfg.security_protocol == "PLAINTEXT"
    assert cfg.client_id == "graflo"


def test_kafka_conn_config_from_env_requires_bootstrap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("MISSING_BOOTSTRAP_SERVERS", raising=False)
    with pytest.raises(ValueError, match="BOOTSTRAP_SERVERS"):
        KafkaConnConfig.from_env("MISSING_")


def _require_confluent_kafka():
    pytest.importorskip("confluent_kafka")


def _wait_for_kafka(bootstrap: str, timeout_s: float = 30.0) -> None:
    _require_confluent_kafka()
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": bootstrap})
    deadline = time.monotonic() + timeout_s
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            meta = admin.list_topics(timeout=2.0)
            if meta.brokers:
                return
        except Exception as e:
            last_err = e
            time.sleep(0.5)
    raise RuntimeError(f"Kafka not reachable at {bootstrap}: {last_err}")


def _ensure_topic(topic: str, *, bootstrap: str) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=1, replication_factor=1)]
    )
    for fut in futures.values():
        try:
            fut.result(timeout=10)
        except Exception as e:
            # Topic already exists is fine.
            if (
                "TOPIC_ALREADY_EXISTS" not in str(e)
                and "already exists" not in str(e).lower()
            ):
                # Some brokers raise differently; proceed if topic is listed.
                meta = admin.list_topics(timeout=5)
                if topic not in meta.topics:
                    raise
    # Wait until metadata sees the topic.
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        meta = admin.list_topics(timeout=2)
        if topic in meta.topics:
            return
        time.sleep(0.2)
    raise RuntimeError(f"Topic {topic!r} not visible after create")


def _produce_json(topic: str, docs: list[dict], *, bootstrap: str) -> None:
    from confluent_kafka import Producer

    _ensure_topic(topic, bootstrap=bootstrap)
    producer = Producer({"bootstrap.servers": bootstrap})
    for doc in docs:
        producer.produce(topic, json.dumps(doc).encode("utf-8"))
    remaining = producer.flush(10)
    assert remaining == 0, f"Failed to flush {remaining} Kafka messages"


@pytest.mark.kafka
def test_kafka_datasource_consumes_json_messages() -> None:
    _require_confluent_kafka()
    _wait_for_kafka(KAFKA_BOOTSTRAP)

    topic = f"graflo.test.{uuid.uuid4().hex[:12]}"
    group_id = f"graflo-ds-{uuid.uuid4().hex[:8]}"
    docs = [{"id": i, "name": f"item-{i}"} for i in range(5)]
    _produce_json(topic, docs, bootstrap=KAFKA_BOOTSTRAP)

    source = KafkaDataSource(
        config=KafkaConfig(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            topics=[topic],
            group_id=group_id,
            auto_offset_reset="earliest",
            idle_ms=5000,
            max_wait_ms=20000,
            poll_timeout_ms=500,
            row_annotations={"pipeline": "test"},
        )
    )
    batches = list(source.iter_batches(batch_size=2, limit=5))
    flat = [row for batch in batches for row in batch]
    assert len(flat) == 5
    assert all(row["pipeline"] == "test" for row in flat)
    assert {row["id"] for row in flat} == {0, 1, 2, 3, 4}
    assert all("_kafka_topic" in row and row["_kafka_topic"] == topic for row in flat)
    assert all("_kafka_offset" in row for row in flat)
    assert all(len(b) <= 2 for b in batches)


@pytest.mark.kafka
def test_kafka_datasource_respects_limit() -> None:
    _require_confluent_kafka()
    _wait_for_kafka(KAFKA_BOOTSTRAP)

    topic = f"graflo.test.limit.{uuid.uuid4().hex[:12]}"
    group_id = f"graflo-limit-{uuid.uuid4().hex[:8]}"
    _produce_json(
        topic,
        [{"n": i} for i in range(10)],
        bootstrap=KAFKA_BOOTSTRAP,
    )

    source = KafkaDataSource(
        config=KafkaConfig(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            topics=[topic],
            group_id=group_id,
            auto_offset_reset="earliest",
            idle_ms=5000,
            max_wait_ms=20000,
        )
    )
    flat = [
        row for batch in source.iter_batches(batch_size=100, limit=3) for row in batch
    ]
    assert len(flat) == 3


@pytest.mark.kafka
def test_registry_builder_registers_kafka_source() -> None:
    _require_confluent_kafka()
    _wait_for_kafka(KAFKA_BOOTSTRAP)

    topic = f"graflo.test.reg.{uuid.uuid4().hex[:12]}"
    group_id = f"graflo-reg-{uuid.uuid4().hex[:8]}"
    _produce_json(topic, [{"arxiv": "1", "doi": "x"}], bootstrap=KAFKA_BOOTSTRAP)

    manifest = fetch_manifest_obj("kg")
    schema = manifest.require_schema()
    ingestion_model = manifest.require_ingestion_model()
    resource_name = "kg"

    connector = KafkaConnector(
        name="pubs_kafka",
        topics=[topic],
        group_id=group_id,
        idle_ms=5000,
        max_wait_ms=20000,
    )
    bindings = Bindings(
        connectors=[connector],
        resource_connector=[
            {"resource": resource_name, "connector": "pubs_kafka"},
        ],
        connector_connection=[
            {"connector": "pubs_kafka", "conn_proxy": "kafka_local"},
        ],
    )
    provider = InMemoryConnectionProvider()
    provider.register_generalized_config(
        conn_proxy="kafka_local",
        config=KafkaGeneralizedConnConfig(
            config=KafkaConnConfig(bootstrap_servers=KAFKA_BOOTSTRAP)
        ),
    )
    provider.bind_from_bindings(bindings=bindings)

    registry = RegistryBuilder(schema, ingestion_model).build(
        bindings=bindings,
        ingestion_params=IngestionParams(n_cores=1, batch_size=10),
        connection_provider=provider,
    )
    sources = registry.get_data_sources(resource_name)
    assert len(sources) == 1
    assert isinstance(sources[0], KafkaDataSource)
    assert sources[0].config.topics == [topic]
