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
