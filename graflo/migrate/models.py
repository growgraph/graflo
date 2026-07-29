"""Typed migration domain models."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class RiskLevel(StrEnum):
    """Risk levels attached to migration operations."""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class OperationType(StrEnum):
    """Canonical migration operation type identifiers."""

    ADD_VERTEX = "ADD_VERTEX"
    ADD_EDGE = "ADD_EDGE"
    ADD_VERTEX_FIELD = "ADD_VERTEX_FIELD"
    ADD_EDGE_FIELD = "ADD_EDGE_FIELD"
    ADD_VERTEX_INDEX = "ADD_VERTEX_INDEX"
    ADD_EDGE_INDEX = "ADD_EDGE_INDEX"
    CHANGE_VERTEX_FIELD_TYPE = "CHANGE_VERTEX_FIELD_TYPE"
    CHANGE_EDGE_FIELD_TYPE = "CHANGE_EDGE_FIELD_TYPE"
    REMOVE_EDGE_INDEX = "REMOVE_EDGE_INDEX"
    REMOVE_VERTEX_INDEX = "REMOVE_VERTEX_INDEX"
    REMOVE_EDGE_FIELD = "REMOVE_EDGE_FIELD"
    REMOVE_VERTEX_FIELD = "REMOVE_VERTEX_FIELD"
    REMOVE_EDGE = "REMOVE_EDGE"
    REMOVE_VERTEX = "REMOVE_VERTEX"
    CHANGE_EDGE_IDENTITY = "CHANGE_EDGE_IDENTITY"
    CHANGE_VERTEX_IDENTITY = "CHANGE_VERTEX_IDENTITY"
    CHANGE_SECONDARY_IDENTITY = "CHANGE_SECONDARY_IDENTITY"
    REKEY_VERTEX = "REKEY_VERTEX"
    CHANGE_INDEX = "CHANGE_INDEX"


class MigrationOperation(BaseModel):
    """A typed migration operation emitted from schema diff."""

    op_type: OperationType = Field(..., description="Operation type identifier.")
    target: str = Field(..., description="Operation target path.")
    old_value: Any = Field(default=None, description="Old value, when relevant.")
    new_value: Any = Field(default=None, description="New value, when relevant.")
    risk: RiskLevel = Field(..., description="Operation risk level.")
    reversible: bool = Field(
        default=True,
        description="Whether operation is reversible without external backup.",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Additional operation metadata."
    )


class SchemaConflict(BaseModel):
    """A conflict discovered during planning or validation."""

    key: str
    message: str
    risk: RiskLevel = RiskLevel.MEDIUM


class SchemaDiffResult(BaseModel):
    """Structured schema diff with operations and diagnostics."""

    operations: list[MigrationOperation] = Field(default_factory=list)
    conflicts: list[SchemaConflict] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class MigrationPlan(BaseModel):
    """Ordered migration operations ready for preview or execution."""

    operations: list[MigrationOperation] = Field(default_factory=list)
    blocked_operations: list[MigrationOperation] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)

    def is_empty(self) -> bool:
        return len(self.operations) == 0 and len(self.blocked_operations) == 0


class MigrationRecord(BaseModel):
    """Applied migration metadata persisted by migration store."""

    revision: str
    schema_hash: str
    backend: str
    operations: list[str] = Field(default_factory=list)
    reversible: bool = True
    applied_at: str = Field(default_factory=lambda: datetime.now(UTC).isoformat())
