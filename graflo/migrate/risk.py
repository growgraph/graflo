"""Risk classification and compatibility helpers."""

from __future__ import annotations

from graflo.migrate.models import MigrationOperation, OperationType, RiskLevel

LOW_RISK_OPS = {
    OperationType.ADD_VERTEX,
    OperationType.ADD_EDGE,
    OperationType.ADD_VERTEX_FIELD,
    OperationType.ADD_EDGE_FIELD,
    OperationType.ADD_VERTEX_INDEX,
    OperationType.ADD_EDGE_INDEX,
}

MEDIUM_RISK_OPS = {
    OperationType.REMOVE_VERTEX_INDEX,
    OperationType.REMOVE_EDGE_INDEX,
    OperationType.CHANGE_INDEX,
}

HIGH_RISK_OPS = {
    OperationType.REMOVE_VERTEX_FIELD,
    OperationType.REMOVE_EDGE_FIELD,
    OperationType.CHANGE_VERTEX_FIELD_TYPE,
    OperationType.CHANGE_EDGE_FIELD_TYPE,
    OperationType.REMOVE_VERTEX,
    OperationType.REMOVE_EDGE,
}

CRITICAL_RISK_OPS = {
    OperationType.CHANGE_VERTEX_IDENTITY,
    OperationType.CHANGE_EDGE_IDENTITY,
    OperationType.REKEY_VERTEX,
}


def classify_operation(op_type: OperationType) -> RiskLevel:
    """Map operation type to risk level."""
    if op_type in LOW_RISK_OPS:
        return RiskLevel.LOW
    if op_type in MEDIUM_RISK_OPS:
        return RiskLevel.MEDIUM
    if op_type in HIGH_RISK_OPS:
        return RiskLevel.HIGH
    if op_type in CRITICAL_RISK_OPS:
        return RiskLevel.CRITICAL
    return RiskLevel.MEDIUM


def is_low_risk(op: MigrationOperation) -> bool:
    """Return True when operation is safe for v1 executor."""
    return op.risk == RiskLevel.LOW


def is_backward_compatible_operations(operations: list[MigrationOperation]) -> bool:
    """A migration is backward compatible when all ops are additive low-risk."""
    return all(op.risk == RiskLevel.LOW for op in operations)
