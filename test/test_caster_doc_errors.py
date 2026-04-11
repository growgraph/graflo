"""Tests for per-document cast error handling (skip vs fail, doc error sink, budget)."""

from __future__ import annotations

import asyncio
import gzip
import json
import threading
import time
from collections import defaultdict
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from graflo.data_source.base import AbstractDataSource, DataSourceType
from graflo.hq.caster import (
    CastBatchResult,
    Caster,
    DocErrorBudgetExceeded,
    IngestionParams,
)


def _read_all_jsonl_gz_lines(path: Path) -> list[str]:
    """Read every line from a file that may contain concatenated gzip members."""

    lines: list[str] = []
    with path.open("rb") as raw:
        while True:
            gzf = gzip.GzipFile(fileobj=raw, mode="rb")
            try:
                chunk = gzf.read()
            finally:
                gzf.close()
            if not chunk:
                break
            lines.extend(chunk.decode("utf-8").splitlines())
    return [ln for ln in lines if ln.strip()]


class _FakeResource:
    """Callable resource that fails documents containing ``_fail``."""

    name = "fake_resource"

    def __call__(self, doc: dict) -> defaultdict:
        if doc.get("_fail"):
            raise ValueError("intentional document failure")
        out: defaultdict = defaultdict(list)
        out["v_test"] = [{"id": doc.get("id")}]
        return out


@pytest.fixture
def mock_ingestion_model() -> MagicMock:
    im = MagicMock()
    im.fetch_resource = MagicMock(return_value=_FakeResource())
    return im


@pytest.fixture
def mock_schema() -> MagicMock:
    return MagicMock()


def test_skip_continues_batch_and_doc_error_sink(
    mock_schema: MagicMock,
    mock_ingestion_model: MagicMock,
    tmp_path: Path,
) -> None:
    sink = tmp_path / "errors.jsonl.gz"
    params = IngestionParams(
        n_cores=1,
        on_doc_error="skip",
        doc_error_sink_path=sink,
    )
    caster = Caster(mock_schema, mock_ingestion_model, ingestion_params=params)
    data = [{"id": 1}, {"id": 2, "_fail": True}, {"id": 3}]

    result = asyncio.run(caster.cast_normal_resource(data))

    assert isinstance(result, CastBatchResult)
    assert len(result.failures) == 1
    assert result.failures[0].doc_index == 1
    assert result.failures[0].exception_type == "ValueError"
    assert "v_test" in result.graph.vertices
    assert len(result.graph.vertices["v_test"]) == 2

    lines = _read_all_jsonl_gz_lines(sink)
    assert len(lines) == 1
    rec = json.loads(lines[0])
    assert rec["doc_index"] == 1
    assert rec["resource_name"] == "fake_resource"
    assert rec["exception_type"] == "ValueError"


def test_fail_propagates(
    mock_schema: MagicMock,
    mock_ingestion_model: MagicMock,
) -> None:
    params = IngestionParams(n_cores=1, on_doc_error="fail")
    caster = Caster(mock_schema, mock_ingestion_model, ingestion_params=params)
    data = [{"id": 1}, {"id": 2, "_fail": True}]

    with pytest.raises(ValueError, match="intentional"):
        asyncio.run(caster.cast_normal_resource(data))


def test_max_doc_errors_exceeded(
    mock_schema: MagicMock,
    mock_ingestion_model: MagicMock,
    tmp_path: Path,
) -> None:
    sink = tmp_path / "errors.jsonl.gz"
    params = IngestionParams(
        n_cores=1,
        on_doc_error="skip",
        max_doc_errors=1,
        doc_error_sink_path=sink,
    )
    caster = Caster(mock_schema, mock_ingestion_model, ingestion_params=params)
    data = [{"_fail": True}, {"_fail": True}]

    with pytest.raises(DocErrorBudgetExceeded) as exc_info:
        asyncio.run(caster.cast_normal_resource(data))

    assert exc_info.value.limit == 1
    assert exc_info.value.total_failures == 2
    assert exc_info.value.doc_error_sink_path == sink
    lines = _read_all_jsonl_gz_lines(sink)
    assert len(lines) == 2


async def _two_concurrent_failed_docs(caster: Caster) -> None:
    await asyncio.gather(
        caster.cast_normal_resource([{"_fail": True}]),
        caster.cast_normal_resource([{"_fail": True}]),
    )


def test_concurrent_doc_error_sink_lines(
    mock_schema: MagicMock,
    mock_ingestion_model: MagicMock,
    tmp_path: Path,
) -> None:
    sink = tmp_path / "errors.jsonl.gz"
    params = IngestionParams(
        n_cores=2,
        on_doc_error="skip",
        doc_error_sink_path=sink,
    )
    caster = Caster(mock_schema, mock_ingestion_model, ingestion_params=params)
    asyncio.run(_two_concurrent_failed_docs(caster))

    lines = _read_all_jsonl_gz_lines(sink)
    assert len(lines) == 2
    for ln in lines:
        json.loads(ln)


def test_batch_prefetch_default_and_validation() -> None:
    params = IngestionParams()
    assert params.batch_prefetch == 2

    with pytest.raises(ValueError):
        IngestionParams(batch_prefetch=0)


class _PrefetchProbeDataSource(AbstractDataSource):
    source_type: DataSourceType = DataSourceType.IN_MEMORY

    def __init__(self, second_fetch_started: threading.Event) -> None:
        super().__init__(source_type=DataSourceType.IN_MEMORY)
        self._second_fetch_started = second_fetch_started
        self.resource_name = "fake_resource"

    def iter_batches(self, batch_size: int = 1000, limit: int | None = None):
        del batch_size, limit
        time.sleep(0.01)
        yield [{"id": 1}]
        self._second_fetch_started.set()
        time.sleep(0.01)
        yield [{"id": 2}]


def test_process_data_source_prefetch_overlaps_fetch_and_processing(
    mock_schema: MagicMock,
    mock_ingestion_model: MagicMock,
) -> None:
    params = IngestionParams(n_cores=1, batch_prefetch=2)
    caster = Caster(mock_schema, mock_ingestion_model, ingestion_params=params)
    second_fetch_started = threading.Event()
    data_source = _PrefetchProbeDataSource(second_fetch_started)

    async def _fake_process_batch(*args, **kwargs) -> None:
        del args, kwargs
        if not second_fetch_started.wait(timeout=0.2):
            raise AssertionError("next batch was not prefetched while processing")
        await asyncio.sleep(0.01)

    caster.process_batch = _fake_process_batch  # type: ignore[method-assign]
    asyncio.run(caster.process_data_source(data_source=data_source))


class _ErrorAfterFirstBatchDataSource(AbstractDataSource):
    source_type: DataSourceType = DataSourceType.IN_MEMORY

    def __init__(self) -> None:
        super().__init__(source_type=DataSourceType.IN_MEMORY)
        self.resource_name = "fake_resource"

    def iter_batches(self, batch_size: int = 1000, limit: int | None = None):
        del batch_size, limit
        yield [{"id": 1}]
        raise RuntimeError("fetch exploded")


def test_process_data_source_propagates_fetch_errors(
    mock_schema: MagicMock,
    mock_ingestion_model: MagicMock,
) -> None:
    params = IngestionParams(n_cores=1, batch_prefetch=2)
    caster = Caster(mock_schema, mock_ingestion_model, ingestion_params=params)
    data_source = _ErrorAfterFirstBatchDataSource()

    async def _fake_process_batch(*args, **kwargs) -> None:
        del args, kwargs
        await asyncio.sleep(0.01)

    caster.process_batch = _fake_process_batch  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="fetch exploded"):
        asyncio.run(caster.process_data_source(data_source=data_source))


def test_process_data_source_propagates_processing_errors(
    mock_schema: MagicMock,
    mock_ingestion_model: MagicMock,
) -> None:
    params = IngestionParams(n_cores=1, batch_prefetch=2)
    caster = Caster(mock_schema, mock_ingestion_model, ingestion_params=params)
    data_source = _PrefetchProbeDataSource(threading.Event())

    async def _fake_process_batch(*args, **kwargs) -> None:
        del args, kwargs
        raise ConnectionError("db write failed")

    caster.process_batch = _fake_process_batch  # type: ignore[method-assign]
    with pytest.raises(ConnectionError, match="db write failed"):
        asyncio.run(caster.process_data_source(data_source=data_source))
