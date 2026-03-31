"""Ingestion parameters and row-error models for the caster.

This module exists to keep `graflo/hq/caster.py` focused on casting logic, while
keeping ingestion-policy types stable and importable.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from graflo.architecture.graph_types import GraphContainer


class RowErrorBudgetExceeded(RuntimeError):
    """Raised when total row cast failures exceed ``IngestionParams.max_row_errors``."""

    def __init__(
        self,
        *,
        total_failures: int,
        limit: int,
        dead_letter_path: Path | None,
    ) -> None:
        self.total_failures = total_failures
        self.limit = limit
        self.dead_letter_path = dead_letter_path
        dl = str(dead_letter_path) if dead_letter_path else "(not configured)"
        super().__init__(
            f"Row error budget exceeded: {total_failures} total failures "
            f"(limit {limit}). Dead letter: {dl}"
        )


class RowCastFailure(BaseModel):
    """Structured record for a single row that failed during resource casting."""

    resource_name: str
    row_index: int
    exception_type: str
    message: str
    traceback: str = Field(
        default="",
        description="Formatted traceback, truncated to the configured max length.",
    )
    doc_preview: Any = Field(
        default=None,
        description="Subset or truncated JSON of the source document for debugging.",
    )


class CastBatchResult(BaseModel):
    """Outcome of casting a batch through a resource (possibly with skipped rows)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    graph: GraphContainer
    failures: list[RowCastFailure] = Field(default_factory=list)


class IngestionParams(BaseModel):
    """Parameters for controlling the ingestion process.

    ``max_items`` caps how many **source items** (rows, JSON objects, grouped
    RDF subjects, …) are read per resource run. It maps to
    ``AbstractDataSource.iter_batches(..., limit=...)``. ``batch_size`` is only
    the maximum number of items per yielded batch, not a cap on total volume.
    """

    clear_data: bool = False
    n_cores: int = 1
    max_items: int | None = Field(
        default=None,
        ge=1,
        description=(
            "Maximum number of source items (rows / JSON objects / grouped "
            "RDF subjects) to ingest for each resource. Not a batch count."
        ),
    )
    batch_size: int = Field(
        default=10000,
        ge=1,
        description="Number of source items to group per batch for casting and writes.",
    )
    dry: bool = False
    init_only: bool = False
    limit_files: int | None = None
    resources: list[str] | None = None
    vertices: list[str] | None = None
    max_concurrent_db_ops: int | None = None
    datetime_after: str | None = None
    datetime_before: str | None = None
    datetime_column: str | None = None

    # Strict contract checks for major-release style validation workflows.
    strict_references: bool = True
    strict_registry: bool = True
    dynamic_edges: bool = False
    on_row_error: Literal["skip", "fail"] = "skip"
    row_error_dead_letter_path: Path | None = None
    max_row_errors: int | None = None
    row_error_doc_preview_max_bytes: int = 4096
    row_error_doc_keys: tuple[str, ...] | None = None
