"""Unit tests for TigerGraph API token process cache."""

from __future__ import annotations

import time
from unittest.mock import patch

import pytest

from graflo.db.connection import TigergraphConfig
from graflo.db.tigergraph.conn import (
    TigerGraphConnection,
    _CachedToken,
    _TigerGraphTokenCache,
    _make_token_cache_key,
    _parse_tg_expiration,
    reset_tigergraph_token_cache,
)


@pytest.fixture(autouse=True)
def _clear_token_cache():
    reset_tigergraph_token_cache()
    yield
    reset_tigergraph_token_cache()


def test_parse_tg_expiration_numeric() -> None:
    assert _parse_tg_expiration("1700000000") == 1700000000.0


def test_parse_tg_expiration_iso() -> None:
    ts = _parse_tg_expiration("2024-01-15T12:00:00Z")
    assert ts is not None
    assert ts > 0


def test_cache_hit_calls_fetch_once() -> None:
    cache = _TigerGraphTokenCache.instance()
    key = _make_token_cache_key("http://localhost:14240", "g1", "secret")
    calls = 0

    def fetch() -> tuple[str, str | None]:
        nonlocal calls
        calls += 1
        return "token-a", None

    token1, hit1 = cache.get_or_fetch(key, fetch)
    token2, hit2 = cache.get_or_fetch(key, fetch)

    assert token1 == "token-a"
    assert token2 == "token-a"
    assert hit1 is False
    assert hit2 is True
    assert calls == 1


def test_cache_miss_different_graph() -> None:
    cache = _TigerGraphTokenCache.instance()
    key1 = _make_token_cache_key("http://localhost:14240", "g1", "secret")
    key2 = _make_token_cache_key("http://localhost:14240", "g2", "secret")
    calls = 0

    def fetch() -> tuple[str, str | None]:
        nonlocal calls
        calls += 1
        return f"token-{calls}", None

    cache.get_or_fetch(key1, fetch)
    cache.get_or_fetch(key2, fetch)

    assert calls == 2


def test_expiry_buffer_evicts_stale_entry() -> None:
    cache = _TigerGraphTokenCache.instance()
    key = _make_token_cache_key("http://localhost:14240", "g1", "secret")
    calls = 0

    def fetch() -> tuple[str, str | None]:
        nonlocal calls
        calls += 1
        return "token-a", None

    cache.get_or_fetch(key, fetch)
    assert calls == 1

    with cache._lock:
        cache._cache[key] = _CachedToken(token="token-a", expires_at=time.time() - 1)

    token, hit = cache.get_or_fetch(key, fetch)
    assert token == "token-a"
    assert hit is False
    assert calls == 2


def test_reset_clears_cache() -> None:
    cache = _TigerGraphTokenCache.instance()
    key = _make_token_cache_key("http://localhost:14240", "g1", "secret")
    calls = 0

    def fetch() -> tuple[str, str | None]:
        nonlocal calls
        calls += 1
        return "token-a", None

    cache.get_or_fetch(key, fetch)
    reset_tigergraph_token_cache()
    cache2 = _TigerGraphTokenCache.instance()
    cache2.get_or_fetch(key, fetch)

    assert calls == 2


@patch.object(TigerGraphConnection, "_get_version", return_value=None)
@patch.object(
    TigerGraphConnection,
    "_get_token_from_secret",
    return_value=("cached-token", None),
)
def test_connection_init_uses_token_cache(
    mock_get_token: object,
    mock_get_version: object,
) -> None:
    config = TigergraphConfig(
        uri="http://localhost:14240",
        username="tigergraph",
        password="tigergraph",
        secret="my-secret",
        database="my_graph",
    )
    TigerGraphConnection(config)
    TigerGraphConnection(config)

    assert mock_get_token.call_count == 1  # type: ignore[attr-defined]


@patch.object(TigerGraphConnection, "_get_version", return_value=None)
@patch.object(
    TigerGraphConnection,
    "_get_token_from_secret",
    return_value=("cached-token", None),
)
def test_call_restpp_api_401_invalidates_cache(
    mock_get_token: object,
    mock_get_version: object,
) -> None:
    config = TigergraphConfig(
        uri="http://localhost:14240",
        username="tigergraph",
        password="tigergraph",
        secret="my-secret",
        database="my_graph",
    )
    conn = TigerGraphConnection(config)
    assert conn._token_cache_key is not None
    assert mock_get_token.call_count == 1  # type: ignore[attr-defined]

    cache = _TigerGraphTokenCache.instance()
    assert cache._get_valid_unlocked(conn._token_cache_key) == "cached-token"

    from requests.exceptions import HTTPError
    from requests.models import Response

    response = Response()
    response.status_code = 401
    err = HTTPError(response=response)

    with patch("graflo.db.tigergraph.rest_client.requests.get", side_effect=err):
        result = conn._call_restpp_api("/graph/my_graph/vertices/Person")

    assert isinstance(result, dict) and result.get("error") is True
    assert cache._get_valid_unlocked(conn._token_cache_key) is None

    TigerGraphConnection(config)
    assert mock_get_token.call_count == 2  # type: ignore[attr-defined]
