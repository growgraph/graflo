"""Tests for per-row cast error handling (skip vs fail, dead-letter, budget)."""

from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from graflo.hq.caster import (
    CastBatchResult,
    Caster,
    IngestionParams,
    RowErrorBudgetExceeded,
)


class _FakeResource:
    """Callable resource that fails rows containing ``_fail``."""

    name = "fake_resource"

    def __call__(self, doc: dict) -> defaultdict:
        if doc.get("_fail"):
            raise ValueError("intentional row failure")
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


def test_skip_continues_batch_and_dead_letter(
    mock_schema: MagicMock,
    mock_ingestion_model: MagicMock,
    tmp_path: Path,
) -> None:
    dl = tmp_path / "errors.jsonl"
    params = IngestionParams(
        n_cores=1,
        on_row_error="skip",
        row_error_dead_letter_path=dl,
    )
    caster = Caster(mock_schema, mock_ingestion_model, ingestion_params=params)
    data = [{"id": 1}, {"id": 2, "_fail": True}, {"id": 3}]

    result = asyncio.run(caster.cast_normal_resource(data))

    assert isinstance(result, CastBatchResult)
    assert len(result.failures) == 1
    assert result.failures[0].row_index == 1
    assert result.failures[0].exception_type == "ValueError"
    assert "v_test" in result.graph.vertices
    assert len(result.graph.vertices["v_test"]) == 2

    lines = dl.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    row = json.loads(lines[0])
    assert row["row_index"] == 1
    assert row["resource_name"] == "fake_resource"
    assert row["exception_type"] == "ValueError"


def test_fail_propagates(
    mock_schema: MagicMock,
    mock_ingestion_model: MagicMock,
) -> None:
    params = IngestionParams(n_cores=1, on_row_error="fail")
    caster = Caster(mock_schema, mock_ingestion_model, ingestion_params=params)
    data = [{"id": 1}, {"id": 2, "_fail": True}]

    with pytest.raises(ValueError, match="intentional"):
        asyncio.run(caster.cast_normal_resource(data))


def test_max_row_errors_exceeded(
    mock_schema: MagicMock,
    mock_ingestion_model: MagicMock,
    tmp_path: Path,
) -> None:
    dl = tmp_path / "errors.jsonl"
    params = IngestionParams(
        n_cores=1,
        on_row_error="skip",
        max_row_errors=1,
        row_error_dead_letter_path=dl,
    )
    caster = Caster(mock_schema, mock_ingestion_model, ingestion_params=params)
    data = [{"_fail": True}, {"_fail": True}]

    with pytest.raises(RowErrorBudgetExceeded) as exc_info:
        asyncio.run(caster.cast_normal_resource(data))

    assert exc_info.value.limit == 1
    assert exc_info.value.total_failures == 2
    lines = dl.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2


async def _two_concurrent_failed_rows(caster: Caster) -> None:
    await asyncio.gather(
        caster.cast_normal_resource([{"_fail": True}]),
        caster.cast_normal_resource([{"_fail": True}]),
    )


def test_concurrent_dead_letter_lines(
    mock_schema: MagicMock,
    mock_ingestion_model: MagicMock,
    tmp_path: Path,
) -> None:
    dl = tmp_path / "errors.jsonl"
    params = IngestionParams(
        n_cores=2,
        on_row_error="skip",
        row_error_dead_letter_path=dl,
    )
    caster = Caster(mock_schema, mock_ingestion_model, ingestion_params=params)
    asyncio.run(_two_concurrent_failed_rows(caster))

    lines = [ln for ln in dl.read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert len(lines) == 2
    for ln in lines:
        json.loads(ln)
