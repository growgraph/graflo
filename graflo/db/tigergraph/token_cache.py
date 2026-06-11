"""Process-scoped TigerGraph API token cache."""

from __future__ import annotations

import hashlib
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone

_TOKEN_EXPIRY_BUFFER_SECONDS = 120

TokenCacheKey = tuple[str, str, str]


@dataclass(frozen=True)
class _CachedToken:
    token: str
    expires_at: float | None


class _TigerGraphTokenCache:
    """Process-scoped, thread-safe cache for TigerGraph API tokens from secrets."""

    _instance: _TigerGraphTokenCache | None = None
    _instance_lock = threading.Lock()

    def __init__(self) -> None:
        self._cache: dict[TokenCacheKey, _CachedToken] = {}
        self._lock = threading.Lock()

    @classmethod
    def instance(cls) -> _TigerGraphTokenCache:
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def get_or_fetch(
        self,
        key: TokenCacheKey,
        fetch_fn: Callable[[], tuple[str, str | None]],
    ) -> tuple[str, bool]:
        """Return ``(token, cache_hit)``; only one thread fetches per cold key."""
        with self._lock:
            cached = self._get_valid_unlocked(key)
            if cached is not None:
                return cached, True
            token, expiration = fetch_fn()
            expires_at = parse_tg_expiration(expiration)
            self._cache[key] = _CachedToken(token=token, expires_at=expires_at)
            return token, False

    def invalidate(self, key: TokenCacheKey) -> None:
        with self._lock:
            self._cache.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()

    def _get_valid_unlocked(self, key: TokenCacheKey) -> str | None:
        cached = self._cache.get(key)
        if cached is None:
            return None
        if (
            cached.expires_at is not None
            and time.time() >= cached.expires_at - _TOKEN_EXPIRY_BUFFER_SECONDS
        ):
            del self._cache[key]
            return None
        return cached.token


def parse_tg_expiration(raw: str | None) -> float | None:
    """Convert TigerGraph expiration field to a Unix timestamp."""
    if raw is None:
        return None
    try:
        return float(raw)
    except (ValueError, TypeError):
        pass
    try:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except (ValueError, TypeError):
        return None


def make_token_cache_key(gsql_url: str, graphname: str, secret: str) -> TokenCacheKey:
    secret_fingerprint = hashlib.sha256(secret.encode()).hexdigest()[:16]
    return (gsql_url, graphname, secret_fingerprint)


def reset_tigergraph_token_cache() -> None:
    """Clear the process token cache (for tests and long-lived REPL sessions)."""
    with _TigerGraphTokenCache._instance_lock:
        if _TigerGraphTokenCache._instance is not None:
            _TigerGraphTokenCache._instance.clear()
        _TigerGraphTokenCache._instance = None
