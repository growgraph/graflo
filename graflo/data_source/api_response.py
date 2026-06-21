"""API response envelope parsing and optional first-response auto-detection."""

from __future__ import annotations

import logging
from typing import Any, cast

from graflo.architecture.contract.bindings import ApiResponseStructure

logger = logging.getLogger(__name__)

RECORDS_CANDIDATES = ("results", "data", "items", "records", "entries", "rows")
NEXT_OFFSET_CANDIDATES = ("next_offset", "nextOffset")
TOTAL_COUNT_CANDIDATES = ("count", "total", "total_count")
OFFSET_CANDIDATES = ("offset", "skip")
HAS_MORE_CANDIDATES = ("has_more", "hasMore")
CURSOR_CANDIDATES = ("next_cursor", "cursor", "next_page_token")


class ResolvedApiResponse(ApiResponseStructure):
    """Response paths resolved after optional auto-detection on the first body."""

    @classmethod
    def resolve(
        cls,
        config: ApiResponseStructure,
        body: dict[str, Any] | list[Any],
    ) -> ResolvedApiResponse:
        detected: dict[str, str] = {}
        if config.auto_detect and isinstance(body, dict):
            detected = detect_response_shape(body)
            if detected:
                logger.info("Auto-detected API response paths: %s", detected)

        return cls(
            records_path=config.records_path or detected.get("records_path"),
            total_count_path=config.total_count_path
            or detected.get("total_count_path"),
            offset_path=config.offset_path or detected.get("offset_path"),
            next_offset_path=config.next_offset_path
            or detected.get("next_offset_path"),
            has_more_path=config.has_more_path or detected.get("has_more_path"),
            cursor_path=config.cursor_path or detected.get("cursor_path"),
            batch_metadata_paths=dict(config.batch_metadata_paths),
            auto_detect=False,
        )


def get_at_path(obj: object, path: str | None) -> object | None:
    """Resolve a dot-separated path against a JSON-like object."""
    if path is None:
        return None

    data: object = obj
    for part in path.split("."):
        if isinstance(data, dict):
            mapping = cast(dict[str, Any], data)
            data = mapping.get(part)
        elif isinstance(data, list):
            try:
                data = data[int(part)]
            except (ValueError, IndexError):
                return None
        else:
            return None
        if data is None:
            return None
    return data


def detect_response_shape(body: dict[str, Any]) -> dict[str, str]:
    """Infer unset response path keys from a top-level response envelope."""
    detected: dict[str, str] = {}

    records_path = _detect_records_path(body)
    if records_path is not None:
        detected["records_path"] = records_path

    for field, candidates in (
        ("next_offset_path", NEXT_OFFSET_CANDIDATES),
        ("total_count_path", TOTAL_COUNT_CANDIDATES),
        ("offset_path", OFFSET_CANDIDATES),
        ("has_more_path", HAS_MORE_CANDIDATES),
        ("cursor_path", CURSOR_CANDIDATES),
    ):
        key = _detect_top_level_key(body, candidates)
        if key is not None:
            detected[field] = key

    return detected


def extract_records(
    body: dict[str, Any] | list[Any],
    resolved: ResolvedApiResponse,
) -> list[dict[str, Any]]:
    """Extract record dicts from a parsed JSON body."""
    if resolved.records_path is None:
        if isinstance(body, list):
            return [_as_record(item) for item in body]
        raise ValueError(
            "API response is an object envelope but response.records_path is not "
            "configured. Set response.records_path or enable response.auto_detect."
        )

    data = get_at_path(body, resolved.records_path)
    if data is None:
        return []
    if isinstance(data, list):
        return [_as_record(item) for item in data]
    if isinstance(data, dict):
        return [cast(dict[str, Any], data)]
    return []


def get_batch_metadata(
    body: dict[str, Any] | list[Any],
    resolved: ResolvedApiResponse,
) -> dict[str, Any]:
    """Read batch-level metadata from the response envelope."""
    if not isinstance(body, dict) or not resolved.batch_metadata_paths:
        return {}

    metadata: dict[str, Any] = {}
    for annotation_key, response_path in resolved.batch_metadata_paths.items():
        value = get_at_path(body, response_path)
        if value is not None:
            metadata[annotation_key] = value
    return metadata


def has_more_pages(
    body: dict[str, Any] | list[Any],
    resolved: ResolvedApiResponse,
    items: list[dict[str, Any]],
    *,
    strategy: str,
) -> bool:
    """Return whether another HTTP page should be fetched."""
    if not isinstance(body, dict):
        return len(items) > 0

    if resolved.has_more_path is not None:
        return bool(get_at_path(body, resolved.has_more_path))

    if resolved.next_offset_path is not None:
        return get_at_path(body, resolved.next_offset_path) is not None

    if resolved.total_count_path is not None and resolved.offset_path is not None:
        count = get_at_path(body, resolved.total_count_path)
        offset = get_at_path(body, resolved.offset_path)
        if count is not None and offset is not None:
            return _as_int(offset) + len(items) < _as_int(count)

    if strategy == "cursor" and resolved.cursor_path is not None:
        cursor = get_at_path(body, resolved.cursor_path)
        return cursor is not None and str(cursor) != ""

    return len(items) > 0


def next_offset_value(
    body: dict[str, Any] | list[Any],
    resolved: ResolvedApiResponse,
) -> int | None:
    """Read the next offset from the response when configured."""
    if not isinstance(body, dict) or resolved.next_offset_path is None:
        return None

    value = get_at_path(body, resolved.next_offset_path)
    if value is None:
        return None
    return _as_int(value)


def next_cursor_value(
    body: dict[str, Any] | list[Any],
    resolved: ResolvedApiResponse,
) -> str | None:
    """Read the next cursor token from the response when configured."""
    if not isinstance(body, dict) or resolved.cursor_path is None:
        return None

    value = get_at_path(body, resolved.cursor_path)
    if value is None:
        return None
    token = str(value)
    return token if token else None


def _detect_records_path(body: dict[str, Any]) -> str | None:
    for key in RECORDS_CANDIDATES:
        value = body.get(key)
        if isinstance(value, list) and _is_list_of_dicts(value):
            return key

    list_of_dict_keys = [
        key
        for key, value in body.items()
        if isinstance(value, list) and _is_list_of_dicts(value)
    ]
    if len(list_of_dict_keys) == 1:
        return list_of_dict_keys[0]
    return None


def _detect_top_level_key(
    body: dict[str, Any], candidates: tuple[str, ...]
) -> str | None:
    for key in candidates:
        if key in body:
            return key
    return None


def _is_list_of_dicts(value: list[Any]) -> bool:
    return bool(value) and all(isinstance(item, dict) for item in value)


def _as_record(item: object) -> dict[str, Any]:
    if isinstance(item, dict):
        return cast(dict[str, Any], item)
    return {"value": item}


def _as_int(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        return int(value)
    raise TypeError(f"Expected numeric pagination metadata, got {type(value)!r}")
