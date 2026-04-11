"""Backend-agnostic coordinator for optional native bulk ingestion.

The coordinator keeps begin/finalize lifecycle out of :class:`Caster` and
delegates feature support decisions to database connections.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from graflo.architecture.schema import Schema
from graflo.db import ConnectionManager, DBConfig
from graflo.db.bulk_exc import UnsupportedBulkLoad

if TYPE_CHECKING:
    from graflo.architecture.contract.bindings import Bindings
    from graflo.hq.connection_provider import ConnectionProvider


class BulkSessionCoordinator:
    """Coordinate a single optional native bulk session for an ingest run."""

    def __init__(self, schema: Schema):
        self._schema = schema
        self._session_id: str | None = None
        self._begin_lock = asyncio.Lock()

    async def ensure_session(self, conn_conf: DBConfig) -> str | None:
        """Return an active bulk session id, or ``None`` when unsupported/disabled."""
        async with self._begin_lock:
            if self._session_id is not None:
                return self._session_id

            def _begin() -> str | None:
                with ConnectionManager(connection_config=conn_conf) as db:
                    bulk_cfg = getattr(conn_conf, "bulk_load", None)
                    if bulk_cfg is None or not getattr(bulk_cfg, "enabled", False):
                        return None
                    try:
                        return db.bulk_load_begin(self._schema, bulk_cfg)
                    except UnsupportedBulkLoad:
                        return None

            self._session_id = await asyncio.to_thread(_begin)
            return self._session_id

    async def finalize(
        self,
        conn_conf: DBConfig,
        *,
        bindings: Bindings | None,
        connection_provider: ConnectionProvider | None,
    ) -> None:
        """Finalize the active session if one exists."""
        session_id = self._session_id
        self._session_id = None
        if session_id is None:
            return

        def _finalize() -> None:
            with ConnectionManager(connection_config=conn_conf) as db:
                db.bulk_load_finalize(
                    session_id,
                    self._schema,
                    bindings=bindings,
                    connection_provider=connection_provider,
                )

        await asyncio.to_thread(_finalize)
