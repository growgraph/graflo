"""Migration execution orchestrator."""

from __future__ import annotations

from dataclasses import dataclass, field

from graflo.architecture.schema import Schema
from graflo.connections.onto import DBConfig
from graflo.db.manager import ConnectionManager
from graflo.migrate.emitters import ArangoEmitter, BaseEmitter, Neo4jEmitter
from graflo.migrate.models import MigrationPlan, MigrationRecord, RiskLevel
from graflo.migrate.store import FileMigrationStore
from graflo.onto import DBType


class MigrationExecutionError(RuntimeError):
    """Raised when migration execution cannot proceed."""


@dataclass
class ExecutionReport:
    """Execution summary."""

    applied: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    blocked: list[str] = field(default_factory=list)


class MigrationExecutor:
    """Execute migration plans through backend adapters."""

    def __init__(
        self,
        *,
        allow_high_risk: bool = False,
        store: FileMigrationStore | None = None,
    ):
        self.allow_high_risk = allow_high_risk
        self.store = store or FileMigrationStore()
        self._emitters: dict[DBType, BaseEmitter] = {
            DBType.ARANGO: ArangoEmitter(),
            DBType.NEO4J: Neo4jEmitter(),
        }

    def execute_plan(
        self,
        *,
        revision: str,
        schema_hash: str,
        target_schema: Schema,
        plan: MigrationPlan,
        conn_conf: DBConfig,
        dry_run: bool = True,
    ) -> ExecutionReport:
        """Execute migration plan and persist revision on success."""
        db_type = conn_conf.connection_type
        if db_type not in self._emitters:
            raise MigrationExecutionError(
                f"Backend '{db_type}' is not supported by v1 executor."
            )
        emitter = self._emitters[db_type]

        report = ExecutionReport()
        for blocked in plan.blocked_operations:
            report.blocked.append(f"{blocked.op_type}:{blocked.target}")

        if report.blocked and not self.allow_high_risk:
            raise MigrationExecutionError(
                "Plan contains blocked operations. v1 executor permits only low-risk operations."
            )

        if self.store.has_revision(revision=revision, backend=db_type.value):
            existing = self.store.get_revision(revision=revision, backend=db_type.value)
            if existing is not None and existing.schema_hash != schema_hash:
                raise MigrationExecutionError(
                    "Revision already exists with a different schema hash. "
                    "Use a new revision id or reconcile history."
                )
            report.skipped.append(
                f"Revision '{revision}' already applied for backend '{db_type.value}'."
            )
            return report

        if self.store.has_schema_hash(schema_hash=schema_hash, backend=db_type.value):
            report.skipped.append(
                f"Schema hash already present in history for backend '{db_type.value}'."
            )
            return report

        if dry_run:
            for operation in plan.operations:
                if operation.risk != RiskLevel.LOW and not self.allow_high_risk:
                    raise MigrationExecutionError(
                        f"Blocked operation {operation.op_type} ({operation.risk.value})"
                    )
                if not emitter.supports(operation):
                    raise MigrationExecutionError(
                        f"Operation {operation.op_type} unsupported by backend {db_type.value}."
                    )
                report.applied.append(
                    emitter.dry_run_message(operation, target_schema=target_schema)
                )
            return report

        with ConnectionManager(connection_config=conn_conf) as conn:
            for operation in plan.operations:
                if operation.risk != RiskLevel.LOW and not self.allow_high_risk:
                    raise MigrationExecutionError(
                        f"Blocked operation {operation.op_type} ({operation.risk.value})"
                    )
                if not emitter.supports(operation):
                    raise MigrationExecutionError(
                        f"Operation {operation.op_type} unsupported by backend {db_type.value}."
                    )
                report.applied.append(
                    emitter.execute(conn, operation, target_schema=target_schema)
                )

        record = MigrationRecord(
            revision=revision,
            schema_hash=schema_hash,
            backend=db_type.value,
            operations=[str(op.op_type) for op in plan.operations],
            reversible=all(op.reversible for op in plan.operations),
        )
        self.store.add_record(record)

        return report
