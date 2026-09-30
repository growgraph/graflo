"""Which Kafka offsets are committed, and when: only for batches the caller wrote."""

from __future__ import annotations

import json
from typing import Any

import pytest

from graflo.data_source import kafka as kafka_module
from graflo.data_source.kafka import KafkaConfig, KafkaDataSource, _OffsetLedger

TOPIC = "events"


class TestOffsetLedger:
    def test_nothing_is_committable_before_an_acknowledgement(self) -> None:
        ledger = _OffsetLedger()
        ledger.record({(TOPIC, 0): 2})
        assert ledger.take_committable() is None
        assert ledger.awaiting

    def test_an_acknowledged_batch_gives_its_positions(self) -> None:
        ledger = _OffsetLedger()
        assert ledger.record({(TOPIC, 0): 2}) == 0
        ledger.acknowledge(0)
        assert ledger.take_committable() == {(TOPIC, 0): 2}
        assert not ledger.awaiting

    def test_positions_are_handed_out_once(self) -> None:
        ledger = _OffsetLedger()
        ledger.record({(TOPIC, 0): 2})
        ledger.acknowledge(0)
        ledger.take_committable()
        assert ledger.take_committable() is None

    def test_a_later_batch_waits_for_the_earlier_one(self) -> None:
        ledger = _OffsetLedger()
        ledger.record({(TOPIC, 0): 2})
        ledger.record({(TOPIC, 0): 4})
        ledger.acknowledge(1)
        assert ledger.take_committable() is None
        ledger.acknowledge(0)
        assert ledger.take_committable() == {(TOPIC, 0): 4}

    def test_a_gap_stops_the_commit_before_it(self) -> None:
        ledger = _OffsetLedger()
        for end in (2, 4, 6):
            ledger.record({(TOPIC, 0): end})
        ledger.acknowledge(0)
        ledger.acknowledge(2)
        assert ledger.take_committable() == {(TOPIC, 0): 2}
        assert ledger.awaiting

    def test_recorded_positions_are_a_snapshot(self) -> None:
        ledger = _OffsetLedger()
        positions = {(TOPIC, 0): 2}
        ledger.record(positions)
        positions[(TOPIC, 0)] = 9
        ledger.acknowledge(0)
        assert ledger.take_committable() == {(TOPIC, 0): 2}

    def test_an_unknown_batch_is_refused(self) -> None:
        ledger = _OffsetLedger()
        ledger.record({(TOPIC, 0): 2})
        with pytest.raises(ValueError, match="batch 3"):
            ledger.acknowledge(3)


class _Message:
    def __init__(self, partition: int, offset: int, value: bytes) -> None:
        self._partition, self._offset, self._value = partition, offset, value

    def topic(self) -> str:
        return TOPIC

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def value(self) -> bytes:
        return self._value

    def key(self) -> None:
        return None

    def error(self) -> None:
        return None

    def headers(self) -> None:
        return None


def _doc(partition: int, offset: int) -> _Message:
    return _Message(partition, offset, json.dumps({"n": offset}).encode())


class _Consumer:
    """A consumer that hands out a fixed list of messages and records commits."""

    def __init__(self, messages: list[_Message]) -> None:
        self._messages = list(messages)
        self.commits: list[dict[tuple[str, int], int]] = []
        self.closed = False

    def subscribe(self, topics: list[str]) -> None:
        del topics

    def poll(self, timeout: float) -> _Message | None:
        del timeout
        assert not self.closed
        return self._messages.pop(0) if self._messages else None

    def commit(self, offsets: list[Any], asynchronous: bool) -> None:
        assert not asynchronous and not self.closed
        self.commits.append({(tp.topic, tp.partition): tp.offset for tp in offsets})

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def consume(monkeypatch: pytest.MonkeyPatch):
    """Build a source over scripted messages; returns ``(source, consumer)``."""
    confluent_kafka = pytest.importorskip("confluent_kafka")

    def build(messages: list[_Message]) -> tuple[KafkaDataSource, _Consumer]:
        consumer = _Consumer(messages)
        monkeypatch.setattr(
            kafka_module,
            "_import_confluent_kafka",
            lambda: (
                lambda config: consumer,
                confluent_kafka.KafkaError,
                confluent_kafka.KafkaException,
                confluent_kafka.TopicPartition,
            ),
        )
        source = KafkaDataSource(
            config=KafkaConfig(
                bootstrap_servers="broker.invalid:9092",
                topics=[TOPIC],
                group_id="g",
                idle_ms=1,
                poll_timeout_ms=1,
            )
        )
        return source, consumer

    return build


class TestCommits:
    def test_reading_alone_commits_nothing(self, consume) -> None:
        source, consumer = consume([_doc(0, n) for n in range(4)])

        batches = list(source.iter_batches(batch_size=2))
        source.close()

        assert [len(batch) for batch in batches] == [2, 2]
        assert consumer.commits == []
        assert consumer.closed

    def test_an_acknowledged_batch_is_committed(self, consume) -> None:
        source, consumer = consume([_doc(0, n) for n in range(4)])
        batches = source.iter_batches(batch_size=2)

        next(batches)
        source.acknowledge(0)
        next(batches)

        assert consumer.commits == [{(TOPIC, 0): 2}]

    def test_a_batch_written_out_of_order_waits_for_the_one_before(
        self, consume
    ) -> None:
        source, consumer = consume([_doc(0, n) for n in range(6)])

        list(source.iter_batches(batch_size=2))
        source.acknowledge(0)
        source.acknowledge(2)
        source.close()

        assert consumer.commits == [{(TOPIC, 0): 2}]

    def test_close_commits_the_batches_acknowledged_after_the_last_read(
        self, consume
    ) -> None:
        source, consumer = consume([_doc(0, n) for n in range(5)])

        batches = list(source.iter_batches(batch_size=2))
        for index in range(len(batches)):
            source.acknowledge(index)
        assert not consumer.closed
        source.close()

        assert consumer.commits[-1] == {(TOPIC, 0): 5}
        assert consumer.closed

    def test_every_partition_read_so_far_is_committed(self, consume) -> None:
        source, consumer = consume(
            [_doc(0, 0), _doc(1, 0), _doc(0, 1), _doc(0, 2)],
        )

        list(source.iter_batches(batch_size=2))
        source.acknowledge(0)
        source.acknowledge(1)
        source.close()

        assert consumer.commits == [{(TOPIC, 0): 3, (TOPIC, 1): 1}]

    def test_a_skipped_message_is_covered_by_the_next_batch(self, consume) -> None:
        source, consumer = consume(
            [_doc(0, 0), _Message(0, 1, b"not json"), _doc(0, 2)]
        )

        batches = list(source.iter_batches(batch_size=2))
        source.acknowledge(0)
        source.close()

        assert [[row["n"] for row in batch] for batch in batches] == [[0, 2]]
        assert consumer.commits == [{(TOPIC, 0): 3}]

    def test_an_empty_topic_leaves_nothing_open(self, consume) -> None:
        source, consumer = consume([])
        source.config.max_wait_ms = 5

        assert list(source.iter_batches(batch_size=2)) == []
        assert consumer.closed

    def test_close_ends_a_read_in_progress(self, consume) -> None:
        source, consumer = consume([_doc(0, n) for n in range(6)])
        batches = source.iter_batches(batch_size=2)
        next(batches)

        source.close()

        assert list(batches) == []
        assert consumer.closed

    def test_close_twice_is_harmless(self, consume) -> None:
        source, consumer = consume([_doc(0, 0)])
        list(source.iter_batches(batch_size=1))
        source.close()
        source.close()
        assert consumer.closed
