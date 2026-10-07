"""Bounded batch pipelining: overlap when safe, serial when gated, errors bare.

The consumer in ``Caster.process_data_source`` may run several cast+write tasks
concurrently (``max_in_flight_batches``). These tests pin the three contracts:
overlap actually happens, gated configurations stay strictly serial, and the
first exception propagates bare (not wrapped in an ``ExceptionGroup``) with
in-flight siblings cancelled.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from graflo.data_source.base import AbstractDataSource, DataSourceType
from graflo.hq.caster import Caster
from graflo.hq.ingestion_parameters import DocErrorBudgetExceeded, IngestionParams


class _NBatchSource(AbstractDataSource):
    source_type: DataSourceType = DataSourceType.IN_MEMORY

    def __init__(self, n_batches: int) -> None:
        super().__init__(source_type=DataSourceType.IN_MEMORY)
        self._n_batches = n_batches
        self.resource_name = "fake_resource"

    def iter_batches(self, batch_size: int = 1000, limit: int | None = None):
        del batch_size, limit
        for i in range(self._n_batches):
            yield [{"id": i}]


class _BatchSpy:
    """Replacement for ``Caster.process_batch`` measuring concurrency."""

    def __init__(self, delay: float = 0.02, fail_on_call: int | None = None) -> None:
        self.delay = delay
        self.fail_on_call = fail_on_call
        self.active = 0
        self.high_water = 0
        self.calls = 0

    async def __call__(self, batch, resource_name=None, conn_conf=None):
        del batch, resource_name, conn_conf
        self.calls += 1
        call_index = self.calls
        self.active += 1
        self.high_water = max(self.high_water, self.active)
        try:
            await asyncio.sleep(self.delay)
            if self.fail_on_call is not None and call_index == self.fail_on_call:
                raise ConnectionError("db write failed")
        finally:
            self.active -= 1


def _caster(**params) -> Caster:
    im = MagicMock()
    # A gate-neutral runtime: no extra_weights, no blank vertices.
    im.fetch_resource = MagicMock(
        return_value=SimpleNamespace(
            config=SimpleNamespace(extra_weights=[]),
            vertex_config=SimpleNamespace(blank_vertices=[]),
            collect_vertex_names=lambda: {"v_test"},
        )
    )
    return Caster(MagicMock(), im, ingestion_params=IngestionParams(**params))


def _run(caster: Caster, spy, n_batches: int = 6) -> None:
    caster.process_batch = spy
    asyncio.run(caster.process_data_source(data_source=_NBatchSource(n_batches)))


class TestOverlap:
    def test_two_batches_are_in_flight_by_default(self) -> None:
        spy = _BatchSpy()
        _run(_caster(), spy)
        assert spy.calls == 6
        assert spy.high_water == 2

    def test_requested_depth_is_the_bound(self) -> None:
        spy = _BatchSpy()
        _run(_caster(max_in_flight_batches=3, batch_prefetch=4), spy)
        assert spy.calls == 6
        assert 2 <= spy.high_water <= 3

    def test_user_override_stays_serial(self) -> None:
        spy = _BatchSpy()
        _run(_caster(max_in_flight_batches=1), spy)
        assert spy.calls == 6
        assert spy.high_water == 1

    def test_gated_configuration_stays_serial(self) -> None:
        spy = _BatchSpy()
        _run(_caster(dynamic_edges=True), spy)
        assert spy.calls == 6
        assert spy.high_water == 1


class TestSourceFanOutRespectsTheGate:
    """A gate-serialized resource must not fan its sources out either —
    concurrent sources share the same runtime and reintroduce the very
    cross-batch hazards the gate prevents."""

    class _SourceSpy:
        """Replacement for ``Caster.process_data_source`` measuring concurrency."""

        def __init__(self) -> None:
            self.active = 0
            self.high_water = 0
            self.calls = 0

        async def __call__(self, data_source, resource_name=None, conn_conf=None):
            del data_source, resource_name, conn_conf
            self.calls += 1
            self.active += 1
            self.high_water = max(self.high_water, self.active)
            try:
                await asyncio.sleep(0.02)
            finally:
                self.active -= 1

    def _fan_out(self, caster: Caster, n_sources: int = 3) -> _SourceSpy:
        spy = self._SourceSpy()
        caster.process_data_source = spy  # ty: ignore[invalid-assignment]
        asyncio.run(
            caster._process_source_group(
                [_NBatchSource(1) for _ in range(n_sources)],
                conn_conf=None,
                resource_name="fake_resource",
            )
        )
        return spy

    def test_plain_resource_fans_sources_out(self) -> None:
        spy = self._fan_out(_caster())
        assert spy.calls == 3
        assert spy.high_water > 1

    def test_dynamic_edges_serializes_sources(self) -> None:
        spy = self._fan_out(_caster(dynamic_edges=True))
        assert spy.calls == 3
        assert spy.high_water == 1

    def test_user_batch_override_does_not_serialize_sources(self) -> None:
        """max_in_flight_batches=1 targets batches; sources have their own knob."""
        spy = self._fan_out(_caster(max_in_flight_batches=1))
        assert spy.calls == 3
        assert spy.high_water > 1


class TestErrors:
    def test_first_error_propagates_bare_and_stops_the_source(self) -> None:
        spy = _BatchSpy(fail_on_call=2)
        caster = _caster()
        caster.process_batch = spy  # ty: ignore[invalid-assignment]
        with pytest.raises(ConnectionError, match="db write failed"):
            asyncio.run(caster.process_data_source(data_source=_NBatchSource(20)))
        # The pipeline must not keep draining the source after the failure.
        assert spy.calls < 20

    def test_doc_error_budget_type_survives(self) -> None:
        """Callers catch DocErrorBudgetExceeded directly; no ExceptionGroup."""

        async def _fail(batch, resource_name=None, conn_conf=None):
            raise DocErrorBudgetExceeded(
                total_failures=3, limit=1, doc_error_sink_path=None
            )

        caster = _caster()
        caster.process_batch = _fail  # ty: ignore[invalid-assignment]
        with pytest.raises(DocErrorBudgetExceeded):
            asyncio.run(caster.process_data_source(data_source=_NBatchSource(4)))

    def test_fetch_error_still_propagates_when_pipelined(self) -> None:
        class _ExplodingSource(AbstractDataSource):
            source_type: DataSourceType = DataSourceType.IN_MEMORY

            def __init__(self) -> None:
                super().__init__(source_type=DataSourceType.IN_MEMORY)
                self.resource_name = "fake_resource"

            def iter_batches(self, batch_size: int = 1000, limit: int | None = None):
                del batch_size, limit
                yield [{"id": 1}]
                raise RuntimeError("fetch exploded")

        spy = _BatchSpy()
        caster = _caster()
        caster.process_batch = spy  # ty: ignore[invalid-assignment]
        with pytest.raises(RuntimeError, match="fetch exploded"):
            asyncio.run(caster.process_data_source(data_source=_ExplodingSource()))


class _AcknowledgedSource(_NBatchSource):
    """Records what the caster tells the source about its batches."""

    def __init__(self, n_batches: int) -> None:
        super().__init__(n_batches)
        self._acknowledged: list[int] = []
        self._closed = 0

    @property
    def acknowledged(self) -> list[int]:
        return self._acknowledged

    @property
    def closed(self) -> int:
        return self._closed

    def acknowledge(self, batch_index: int) -> None:
        self._acknowledged.append(batch_index)

    def close(self) -> None:
        self._closed += 1


#: A target, as far as the batch gate looks at one.
_TARGET = SimpleNamespace(connection_type="neo4j", bulk_load=None)


def _process(caster: Caster, source: _AcknowledgedSource, spy, target=_TARGET) -> None:
    caster.process_batch = spy
    asyncio.run(
        caster.process_data_source(data_source=source, conn_conf=target)  # ty: ignore[invalid-argument-type]
    )


class TestAcknowledgement:
    """A source hears about a batch once it is written, and is closed at the end."""

    def test_every_written_batch_is_acknowledged(self) -> None:
        source = _AcknowledgedSource(5)
        _process(_caster(), source, _BatchSpy())
        assert sorted(source.acknowledged) == [0, 1, 2, 3, 4]
        assert source.closed == 1

    def test_serial_processing_acknowledges_in_order(self) -> None:
        source = _AcknowledgedSource(4)
        _process(_caster(max_in_flight_batches=1), source, _BatchSpy())
        assert source.acknowledged == [0, 1, 2, 3]

    def test_a_batch_is_acknowledged_only_after_its_write_returns(self) -> None:
        source = _AcknowledgedSource(3)
        seen: list[tuple[int, list[int]]] = []

        async def _write(batch, resource_name=None, conn_conf=None):
            del resource_name, conn_conf
            await asyncio.sleep(0.01)
            seen.append((batch[0]["id"], list(source.acknowledged)))

        _process(_caster(max_in_flight_batches=1), source, _write)
        assert seen == [(0, []), (1, [0]), (2, [0, 1])]

    def test_a_failed_batch_is_not_acknowledged(self) -> None:
        source = _AcknowledgedSource(6)
        caster = _caster(max_in_flight_batches=1)
        with pytest.raises(ConnectionError):
            _process(caster, source, _BatchSpy(fail_on_call=3))
        assert source.acknowledged == [0, 1]
        assert source.closed == 1

    def test_a_dry_run_acknowledges_nothing(self) -> None:
        source = _AcknowledgedSource(3)
        _process(_caster(dry=True), source, _BatchSpy())
        assert source.acknowledged == []
        assert source.closed == 1

    def test_casting_without_a_target_acknowledges_nothing(self) -> None:
        source = _AcknowledgedSource(3)
        _process(_caster(), source, _BatchSpy(), target=None)
        assert source.acknowledged == []
        assert source.closed == 1

    def test_a_bulk_session_acknowledges_after_it_is_loaded(self) -> None:
        source = _AcknowledgedSource(3)
        caster = _caster()
        caster._bulk_coordinator._session_id = "session"
        _process(caster, source, _BatchSpy())
        assert (source.acknowledged, source.closed) == ([], 0)

        asyncio.run(caster._release_staged_sources(loaded=True))
        assert (sorted(source.acknowledged), source.closed) == ([0, 1, 2], 1)

    def test_a_bulk_session_that_fails_to_load_acknowledges_nothing(self) -> None:
        source = _AcknowledgedSource(3)
        caster = _caster()
        caster._bulk_coordinator._session_id = "session"
        _process(caster, source, _BatchSpy())

        asyncio.run(caster._release_staged_sources(loaded=False))
        assert (source.acknowledged, source.closed) == ([], 1)


class TestWriteResolutionLog:
    """The run's write-resolution counts are logged however the run ends."""

    @staticmethod
    def _ingest(caster: Caster) -> None:
        from graflo.hq.endpoint_resolve import AttachStats, WriteStats

        stats = WriteStats()
        stats.add_attached("ci", AttachStats(documents=3, unmatched=2))

        async def _write(batch, resource_name=None, conn_conf=None):
            del batch, resource_name, conn_conf
            caster._db_writer = SimpleNamespace(stats=stats)  # ty: ignore[invalid-assignment]

        caster.process_batch = _write  # ty: ignore[invalid-assignment]
        caster.ingestion_model._resources = {"fake_resource": MagicMock()}
        registry = SimpleNamespace(
            get_data_sources=lambda name: [_AcknowledgedSource(1)]
        )
        asyncio.run(
            caster.ingest_data_sources(registry, conn_conf=_TARGET)  # ty: ignore[invalid-argument-type]
        )

    @staticmethod
    def _logged(caplog) -> list[str]:
        return [
            r.getMessage()
            for r in caplog.records
            if r.name == "graflo.hq.caster" and "Write resolution" in r.getMessage()
        ]

    def test_the_counts_are_logged_at_the_end_of_the_run(self, caplog) -> None:
        with caplog.at_level("INFO", logger="graflo.hq.caster"):
            self._ingest(_caster())

        (message,) = self._logged(caplog)
        assert "attached ci" in message and "unmatched=2" in message

    def test_the_counts_are_logged_when_finalizing_fails(self, caplog) -> None:
        caster = _caster()

        async def _finalize(conn_conf) -> None:
            del conn_conf
            raise ConnectionError("bulk load failed")

        caster._finalize_bulk_session = _finalize  # ty: ignore[invalid-assignment]
        with (
            caplog.at_level("INFO", logger="graflo.hq.caster"),
            pytest.raises(ConnectionError, match="bulk load failed"),
        ):
            self._ingest(caster)

        (message,) = self._logged(caplog)
        assert "attached ci" in message and "unmatched=2" in message
