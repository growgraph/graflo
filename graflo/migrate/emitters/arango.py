"""ArangoDB migration emitter."""

from __future__ import annotations

from graflo.architecture.schema import Schema
from graflo.db.conn import Connection
from graflo.migrate.emitters.base import BaseEmitter
from graflo.migrate.models import MigrationOperation, OperationType

SUPPORTED_OPS = {
    OperationType.ADD_VERTEX,
    OperationType.ADD_EDGE,
    OperationType.ADD_VERTEX_FIELD,
    OperationType.ADD_EDGE_FIELD,
    OperationType.ADD_VERTEX_INDEX,
    OperationType.ADD_EDGE_INDEX,
}


class ArangoEmitter(BaseEmitter):
    """Safe v1 Arango executor: additive operations only."""

    @property
    def backend_name(self) -> str:
        return "arango"

    def supports(self, operation: MigrationOperation) -> bool:
        return operation.op_type in SUPPORTED_OPS

    def dry_run_message(
        self, operation: MigrationOperation, *, target_schema: Schema
    ) -> str:
        _ = target_schema
        return f"[arango] would apply {operation.op_type} on {operation.target}"

    def execute(
        self,
        conn: Connection,
        operation: MigrationOperation,
        *,
        target_schema: Schema,
    ) -> str:
        if not self.supports(operation):
            raise ValueError(
                f"Operation not supported by arango v1 emitter: {operation.op_type}"
            )
        self._ensure_schema(conn, target_schema)
        return f"[arango] applied {operation.op_type} on {operation.target}"
