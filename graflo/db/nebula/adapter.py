"""Version-specific client adapters for NebulaGraph.

Provides a uniform ``execute / close / use_space`` interface over
``nebula3-python`` (v3.x, Thrift) and ``nebula5-python`` (v5.x, gRPC).
"""

from __future__ import annotations

import abc
import logging
from typing import Any

from graflo.connections.onto import NebulaConfig

logger = logging.getLogger(__name__)


class NebulaResultSet:
    """Thin wrapper around driver-specific result objects.

    Normalises access so that ``NebulaConnection`` never has to know which
    driver is in use.
    """

    def __init__(self, raw: Any, *, is_v3: bool = True):
        self._raw = raw
        self._is_v3 = is_v3

    @property
    def raw(self) -> Any:
        return self._raw

    def is_succeeded(self) -> bool:
        """Whether the statement succeeded.

        The two drivers report failure differently: v3 returns a status on the
        result, while v5 raises from :meth:`ResultSet.raise_on_error`, which
        :meth:`NebulaV5Adapter.execute` calls before wrapping. So a v5 result
        that reaches here has already been checked -- ``True`` is a fact about
        an error-free path, not an assumption that nothing failed.
        """
        if self._is_v3:
            return self._raw.is_succeeded()
        return True

    def error_msg(self) -> str:
        """Driver error message; empty on v5, where failures raise instead."""
        if self._is_v3:
            return self._raw.error_msg()
        return ""

    def column_values(self, col: str) -> list[Any]:
        if self._is_v3:
            return [v.cast() for v in self._raw.column_values(col)]
        return self._raw.as_primitive_by_column().get(col, [])

    def rows_as_dicts(self) -> list[dict[str, Any]]:
        """Return all rows as list of primitive-type dicts."""
        if self._is_v3:
            return self._raw.as_primitive()
        # nebula5-python exposes no as_primitive(); rows come back as an
        # iterator, so materialise it to match the v3 contract.
        return list(self._raw.as_primitive_by_row())


class NebulaClientAdapter(abc.ABC):
    """Abstract adapter that hides driver differences."""

    @abc.abstractmethod
    def connect(self, config: NebulaConfig) -> None: ...

    @abc.abstractmethod
    def execute(self, statement: str) -> NebulaResultSet: ...

    @abc.abstractmethod
    def close(self) -> None: ...

    @abc.abstractmethod
    def use_space(self, space_name: str) -> None: ...


class NebulaV3Adapter(NebulaClientAdapter):
    """Adapter for ``nebula3-python`` (NebulaGraph 3.x, Thrift)."""

    def __init__(self) -> None:
        self._pool: Any = None
        self._session: Any = None

    def connect(self, config: NebulaConfig) -> None:
        from nebula3.Config import Config as N3Config
        from nebula3.gclient.net import ConnectionPool

        hostname = config.hostname or "localhost"
        port = config.port or 9669
        username = config.username or "root"
        password = config.password or "nebula"

        n3_cfg = N3Config()
        n3_cfg.max_connection_pool_size = 10
        n3_cfg.timeout = int(config.request_timeout * 1000)

        self._pool = ConnectionPool()
        ok = self._pool.init([(hostname, port)], n3_cfg)
        if not ok:
            raise ConnectionError(
                f"Failed to connect to NebulaGraph at {hostname}:{port}"
            )

        self._session = self._pool.get_session(username, password)
        logger.info("Connected to NebulaGraph 3.x at %s:%s", hostname, port)

    def execute(self, statement: str) -> NebulaResultSet:
        if self._session is None:
            raise RuntimeError("Not connected")
        result = self._session.execute(statement)
        rs = NebulaResultSet(result, is_v3=True)
        if not rs.is_succeeded():
            raise RuntimeError(
                f"nGQL execution failed: {rs.error_msg()}\nStatement: {statement}"
            )
        return rs

    def use_space(self, space_name: str) -> None:
        self.execute(f"USE `{space_name}`")

    def close(self) -> None:
        if self._session is not None:
            self._session.release()
            self._session = None
        if self._pool is not None:
            self._pool.close()
            self._pool = None


class NebulaV5Adapter(NebulaClientAdapter):
    """Adapter for ``nebula5-python`` (NebulaGraph 5.x, gRPC / ISO GQL).

    .. warning::
       **Experimental and unverified.** NebulaGraph 5.x ships no open-source
       server image, so this path is exercised by no test and no CI job. Most
       statement builders in :mod:`graflo.db.nebula.query` still emit nGQL
       rather than GQL, so DDL, writes and introspection are expected to fail
       here. Treat 5.x as unsupported until it can be run against a real
       instance; 3.x is the validated target.
    """

    def __init__(self) -> None:
        self._client: Any = None

    def connect(self, config: NebulaConfig) -> None:
        from nebulagraph_python.client import NebulaClient

        hostname = config.hostname or "localhost"
        port = config.port or 9669
        username = config.username or "root"
        password = config.password or "nebula"

        self._client = NebulaClient(
            addresses=f"{hostname}:{port}",
            user_name=username,
            password=password,
        )
        logger.warning(
            "Connected to NebulaGraph 5.x at %s:%s using the experimental v5 "
            "adapter. This path is unverified -- no open-source 5.x server "
            "image exists, so it is covered by no test, and most statement "
            "builders still emit nGQL rather than GQL. Use NebulaGraph 3.x for "
            "supported behaviour.",
            hostname,
            port,
        )

    def execute(self, statement: str) -> NebulaResultSet:
        if self._client is None:
            raise RuntimeError("Not connected")
        result = self._client.execute(statement)
        # Without this the driver's error status is never inspected and every
        # failed statement is reported as a success.
        result.raise_on_error()
        return NebulaResultSet(result, is_v3=False)

    def use_space(self, space_name: str) -> None:
        self.execute(f"USE `{space_name}`")

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None


def create_adapter(config: NebulaConfig) -> NebulaClientAdapter:
    """Factory: instantiate and connect the correct adapter for *config.version*."""
    adapter: NebulaClientAdapter
    if config.is_v3:
        adapter = NebulaV3Adapter()
    else:
        adapter = NebulaV5Adapter()
    adapter.connect(config)
    return adapter
