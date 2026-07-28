"""Migration plan ordering and policy filters."""

from __future__ import annotations

from graflo.migrate.models import (
    MigrationOperation,
    MigrationPlan,
    OperationType,
    SchemaDiffResult,
)
from graflo.migrate.risk import is_low_risk

OP_ORDER = {
    OperationType.ADD_VERTEX: 10,
    OperationType.ADD_EDGE: 20,
    OperationType.ADD_VERTEX_FIELD: 30,
    OperationType.ADD_EDGE_FIELD: 40,
    OperationType.ADD_VERTEX_INDEX: 50,
    OperationType.ADD_EDGE_INDEX: 60,
    OperationType.CHANGE_VERTEX_FIELD_TYPE: 70,
    OperationType.CHANGE_EDGE_FIELD_TYPE: 80,
    OperationType.REMOVE_EDGE_INDEX: 90,
    OperationType.REMOVE_VERTEX_INDEX: 100,
    OperationType.REMOVE_EDGE_FIELD: 110,
    OperationType.REMOVE_VERTEX_FIELD: 120,
    OperationType.REMOVE_EDGE: 130,
    OperationType.REMOVE_VERTEX: 140,
    OperationType.CHANGE_EDGE_IDENTITY: 150,
    OperationType.CHANGE_VERTEX_IDENTITY: 160,
    OperationType.REKEY_VERTEX: 170,
}


class MigrationPlanner:
    """Translate schema diff result into ordered execution plan."""

    def __init__(self, allow_high_risk: bool = False):
        self.allow_high_risk = allow_high_risk

    def build(self, diff_result: SchemaDiffResult) -> MigrationPlan:
        """Build ordered plan with risk gate filtering."""
        ordered_ops = sorted(
            diff_result.operations,
            key=lambda op: (OP_ORDER.get(op.op_type, 9999), op.target),
        )
        runnable: list[MigrationOperation] = []
        blocked: list[MigrationOperation] = []

        for op in ordered_ops:
            if self.allow_high_risk or is_low_risk(op):
                runnable.append(op)
            else:
                blocked.append(op)

        warnings = list(diff_result.warnings)
        if blocked and not self.allow_high_risk:
            warnings.append(
                "High-risk operations are blocked by default. Re-run with explicit allow flag in future guarded workflow."
            )
        return MigrationPlan(
            operations=runnable,
            blocked_operations=blocked,
            warnings=warnings,
        )
