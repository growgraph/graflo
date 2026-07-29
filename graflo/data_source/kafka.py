"""Kafka topic data source implementation.

Runtime consumer for Kafka connectors. Configuration is built by
:class:`~graflo.architecture.contract.bindings.KafkaConnector.build_kafka_config`
from contract fields plus runtime credentials from a connection provider.

Requires ``confluent-kafka`` (default package dependency).
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator
from typing import Any, Literal

from pydantic import Field

from graflo.architecture.base import ConfigBaseModel
from graflo.connections.sources import KafkaSecurityProtocol
from graflo.data_source.base import AbstractDataSource, DataSourceType

logger = logging.getLogger(__name__)


class KafkaConfig(ConfigBaseModel):
    """Merged runtime configuration for Kafka consumers.

    Built exclusively via :meth:`KafkaConnector.build_kafka_config`; not intended
    for direct construction in manifests or factory helpers.
    """

    bootstrap_servers: str
    security_protocol: KafkaSecurityProtocol = "PLAINTEXT"
    client_id: str | None = None
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: str | None = None
    topics: list[str]
    group_id: str
    auto_offset_reset: Literal["earliest", "latest"] = "earliest"
    value_encoding: Literal["json"] = "json"
    include_headers: bool = False
    idle_ms: int = 2000
    max_wait_ms: int | None = None
    poll_timeout_ms: int = 500
    row_annotations: dict[str, Any] = Field(default_factory=dict)

    def to_consumer_config(self) -> dict[str, str]:
        """Build a confluent-kafka consumer config dict."""
        cfg: dict[str, str] = {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": self.security_protocol,
            "group.id": self.group_id,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": "false",
        }
        if self.client_id:
            cfg["client.id"] = self.client_id
        if self.sasl_mechanism:
            cfg["sasl.mechanism"] = self.sasl_mechanism
        if self.sasl_username is not None:
            cfg["sasl.username"] = self.sasl_username
        if self.sasl_password is not None:
            cfg["sasl.password"] = self.sasl_password
        return cfg


def _import_confluent_kafka():
    try:
        from confluent_kafka import Consumer, KafkaError, KafkaException
    except ImportError as e:
        raise ImportError(
            "Kafka support requires confluent-kafka. "
            "Install package dependencies (e.g. uv sync) so confluent-kafka is available."
        ) from e
    return Consumer, KafkaError, KafkaException


def decode_kafka_json_value(raw: bytes | str | None) -> dict[str, Any] | None:
    """Decode a UTF-8 JSON object payload into a dict.

    Non-object JSON (arrays, scalars) and invalid UTF-8/JSON return ``None``.
    """
    if raw is None:
        return None
    if isinstance(raw, bytes):
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            return None
    else:
        text = raw
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _decode_headers(headers: list[tuple[str, bytes | None]] | None) -> dict[str, str]:
    if not headers:
        return {}
    out: dict[str, str] = {}
    for key, value in headers:
        if value is None:
            out[key] = ""
        else:
            try:
                out[key] = value.decode("utf-8")
            except UnicodeDecodeError:
                out[key] = value.hex()
    return out


def _message_to_doc(
    msg: Any,
    *,
    row_annotations: dict[str, Any],
    include_headers: bool,
) -> dict[str, Any] | None:
    decoded = decode_kafka_json_value(msg.value())
    if decoded is None:
        return None

    key = msg.key()
    if key is None:
        key_str: str | None = None
    elif isinstance(key, bytes):
        try:
            key_str = key.decode("utf-8")
        except UnicodeDecodeError:
            key_str = key.hex()
    else:
        key_str = str(key)

    meta: dict[str, Any] = {
        "_kafka_topic": msg.topic(),
        "_kafka_partition": msg.partition(),
        "_kafka_offset": msg.offset(),
        "_kafka_key": key_str,
    }
    if include_headers:
        meta["_kafka_headers"] = _decode_headers(msg.headers())

    return {**row_annotations, **meta, **decoded}


class KafkaDataSource(AbstractDataSource):
    """Data source that consumes JSON messages from Kafka topics."""

    config: KafkaConfig
    source_type: DataSourceType = DataSourceType.KAFKA

    def iter_batches(
        self, batch_size: int = 1000, limit: int | None = None
    ) -> Iterator[list[dict]]:
        """Poll Kafka until limit, idle timeout, or max wait is reached.

        Offsets are committed after each yielded batch (at-least-once).
        Non-JSON-object payloads are skipped with a warning.
        """
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")

        Consumer, KafkaError, KafkaException = _import_confluent_kafka()
        consumer = Consumer(self.config.to_consumer_config())
        consumer.subscribe(list(self.config.topics))

        batch: list[dict] = []
        total = 0
        started = time.monotonic()
        last_message_at: float | None = None
        poll_timeout_s = self.config.poll_timeout_ms / 1000.0
        idle_s = self.config.idle_ms / 1000.0 if self.config.idle_ms > 0 else None
        max_wait_s = (
            self.config.max_wait_ms / 1000.0
            if self.config.max_wait_ms is not None
            else None
        )

        try:
            while True:
                now = time.monotonic()
                if max_wait_s is not None and (now - started) >= max_wait_s:
                    break
                # Idle stop only after at least one message (avoids exiting during
                # consumer-group assignment before backlog is readable).
                if (
                    idle_s is not None
                    and last_message_at is not None
                    and (now - last_message_at) >= idle_s
                ):
                    break
                if limit is not None and total >= limit:
                    break

                try:
                    msg = consumer.poll(poll_timeout_s)
                except KafkaException as e:
                    logger.error("Kafka poll failed: %s", e)
                    raise

                if msg is None:
                    continue

                err = msg.error()
                if err is not None:
                    if err.code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(err)

                last_message_at = time.monotonic()
                doc = _message_to_doc(
                    msg,
                    row_annotations=self.config.row_annotations,
                    include_headers=self.config.include_headers,
                )
                if doc is None:
                    logger.warning(
                        "Skipping non-JSON-object Kafka message topic=%s "
                        "partition=%s offset=%s",
                        msg.topic(),
                        msg.partition(),
                        msg.offset(),
                    )
                    continue

                batch.append(doc)
                total += 1

                if len(batch) >= batch_size or (limit is not None and total >= limit):
                    yield batch
                    consumer.commit(asynchronous=False)
                    batch = []

                if limit is not None and total >= limit:
                    break

            if batch:
                yield batch
                consumer.commit(asynchronous=False)
        finally:
            consumer.close()
