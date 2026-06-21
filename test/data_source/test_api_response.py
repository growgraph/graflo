"""Unit tests for API response envelope parsing and auto-detection."""

from __future__ import annotations

import pytest

from graflo.architecture.contract.bindings import ApiResponseStructure
from graflo.data_source.api_response import (
    ResolvedApiResponse,
    detect_response_shape,
    extract_records,
    get_at_path,
    get_batch_metadata,
    has_more_pages,
    next_offset_value,
)


def test_get_at_path_nested() -> None:
    body = {"results": {"items": [{"id": 1}]}}
    assert get_at_path(body, "results.items") == [{"id": 1}]


def test_detect_response_shape_envelope() -> None:
    body = {
        "count": 100,
        "offset": 0,
        "results": [{"id": 1}],
        "next_offset": 50,
        "result_id": "batch-1",
        "next": "https://example.com/items?offset=50",
    }
    detected = detect_response_shape(body)
    assert detected["records_path"] == "results"
    assert detected["total_count_path"] == "count"
    assert detected["offset_path"] == "offset"
    assert detected["next_offset_path"] == "next_offset"
    assert "next" not in detected.values()


def test_detect_records_path_single_list_of_dicts() -> None:
    body = {"payload": [{"id": 1}], "meta": {"total": 1}}
    detected = detect_response_shape(body)
    assert detected["records_path"] == "payload"


def test_resolved_api_response_merges_config_and_detection() -> None:
    body = {
        "count": 10,
        "offset": 0,
        "results": [{"id": 1}],
        "next_offset": 1,
    }
    config = ApiResponseStructure(
        auto_detect=True,
        next_offset_path="next_offset",
    )
    resolved = ResolvedApiResponse.resolve(config, body)
    assert resolved.records_path == "results"
    assert resolved.next_offset_path == "next_offset"
    assert resolved.total_count_path == "count"


def test_extract_records_with_records_path() -> None:
    body = {"results": [{"id": 1}, {"id": 2}]}
    resolved = ResolvedApiResponse(records_path="results")
    assert extract_records(body, resolved) == [{"id": 1}, {"id": 2}]


def test_extract_records_top_level_array_without_pagination() -> None:
    body = [{"id": 1}]
    resolved = ResolvedApiResponse()
    assert extract_records(body, resolved) == [{"id": 1}]


def test_extract_records_envelope_without_path_raises() -> None:
    body = {"results": [{"id": 1}]}
    resolved = ResolvedApiResponse()
    with pytest.raises(ValueError, match="records_path"):
        extract_records(body, resolved)


def test_get_batch_metadata() -> None:
    body = {"result_id": "batch-abc", "results": [{"id": 1}]}
    resolved = ResolvedApiResponse(
        records_path="results",
        batch_metadata_paths={"_batch_id": "result_id"},
    )
    assert get_batch_metadata(body, resolved) == {"_batch_id": "batch-abc"}


def test_has_more_pages_from_has_more_path() -> None:
    body = {"data": [{"id": 1}], "has_more": False}
    resolved = ResolvedApiResponse(records_path="data", has_more_path="has_more")
    assert has_more_pages(body, resolved, [{"id": 1}], strategy="offset") is False


def test_has_more_pages_from_next_offset_path() -> None:
    body = {"results": [{"id": 1}], "next_offset": 100}
    resolved = ResolvedApiResponse(
        records_path="results",
        next_offset_path="next_offset",
    )
    assert has_more_pages(body, resolved, [{"id": 1}], strategy="offset") is True

    body_last = {"results": [{"id": 2}]}
    assert has_more_pages(body_last, resolved, [{"id": 2}], strategy="offset") is False


def test_has_more_pages_from_count_and_offset() -> None:
    body = {"count": 3, "offset": 0, "results": [{"id": 1}, {"id": 2}]}
    resolved = ResolvedApiResponse(
        records_path="results",
        total_count_path="count",
        offset_path="offset",
    )
    assert (
        has_more_pages(body, resolved, [{"id": 1}, {"id": 2}], strategy="offset")
        is True
    )

    body_last = {"count": 3, "offset": 2, "results": [{"id": 3}]}
    assert has_more_pages(body_last, resolved, [{"id": 3}], strategy="offset") is False


def test_next_offset_value() -> None:
    body = {"next_offset": 100}
    resolved = ResolvedApiResponse(next_offset_path="next_offset")
    assert next_offset_value(body, resolved) == 100
