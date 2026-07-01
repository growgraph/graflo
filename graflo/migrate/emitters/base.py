"""Base interfaces for migration emitters."""

from __future__ import annotations

from abc import ABC, abstractmethod

from graflo.architecture.schema import Schema
from graflo.db import Connection
from graflo.migrate.models import MigrationOperation


class BaseEmitter(ABC):
    """Backend adapter contract for migration execution."""

    @property
    @abstractmethod
    def backend_name(self) -> str:
        """Backend identifier."""

    @abstractmethod
    def execute(
        self,
        conn: Connection,
        operation: MigrationOperation,
        *,
        target_schema: Schema,
    ) -> str:
        """Execute operation and return a concise action description."""

    @abstractmethod
    def supports(self, operation: MigrationOperation) -> bool:
        """Return whether operation is supported for this backend in v1."""

    @abstractmethod
    def dry_run_message(
        self, operation: MigrationOperation, *, target_schema: Schema
    ) -> str:
        """Describe what would happen for the operation."""

    def _ensure_schema(self, conn: Connection, schema: Schema) -> None:
        """Ensure target schema artifacts exist (idempotent where supported)."""
        conn.apply_target_schema(schema, recreate=False)

    @staticmethod
    def _is_additive_operation(operation: MigrationOperation) -> bool:
        return operation.op_type.startswith("ADD_")
