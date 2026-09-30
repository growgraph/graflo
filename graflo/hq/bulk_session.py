"""Backend-agnostic coordinator for optional native bulk ingestion.

The coordinator keeps begin/finalize lifecycle out of :class:`Caster` and
delegates feature support decisions to database connections.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from graflo.architecture.evolution.sanitize import physical_schema
from graflo.architecture.schema import Schema
from graflo.db.bulk_exc import UnsupportedBulkLoad

if TYPE_CHECKING:
    from graflo.architecture.contract.bindings import Bindings
    from graflo.connections.onto import DBConfig
    from graflo.connections.provider import ConnectionProvider


class BulkSessionCoordinator:
    """Coordinate a single optional native bulk session for an ingest run.

    Backends receive the schema as the target stores it
    (:func:`~graflo.architecture.evolution.sanitize.physical_schema`), the same
    one DDL declared and the writer's appended batches are keyed by.
    """

    def __init__(self, schema: Schema):
        self._schema = schema
        self._stored_schema: Schema | None = None
        self._session_id: str | None = None
        self._begin_lock = asyncio.Lock()

    @property
    def active(self) -> bool:
        """Whether a session is open: batches are staged, not yet in the target."""
        return self._session_id is not None

    def _stored_for(self, conn_conf: DBConfig) -> Schema:
        if self._stored_schema is None:
            self._stored_schema = physical_schema(
                self._schema, conn_conf.connection_type
            )
        return self._stored_schema

    async def ensure_session(self, conn_conf: DBConfig) -> str | None:
        """Return an active bulk session id, or ``None`` when unsupported/disabled."""
        async with self._begin_lock:
            if self._session_id is not None:
                return self._session_id

            def _begin() -> str | None:
                from graflo.db.manager import ConnectionManager

                with ConnectionManager(connection_config=conn_conf) as db:
                    bulk_cfg = getattr(conn_conf, "bulk_load", None)
                    if bulk_cfg is None or not getattr(bulk_cfg, "enabled", False):
                        return None
                    try:
                        return db.bulk_load_begin(self._stored_for(conn_conf), bulk_cfg)
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
            from graflo.db.manager import ConnectionManager

            with ConnectionManager(connection_config=conn_conf) as db:
                db.bulk_load_finalize(
                    session_id,
                    self._stored_for(conn_conf),
                    bindings=bindings,
                    connection_provider=connection_provider,
                )

        await asyncio.to_thread(_finalize)
