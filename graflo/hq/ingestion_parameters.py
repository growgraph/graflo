"""Ingestion parameters and per-document cast-failure models for the caster.

This module exists to keep `graflo/hq/caster.py` focused on casting logic, while
keeping ingestion-policy types stable and importable.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from graflo.architecture.graph_types import GraphContainer


class DocErrorBudgetExceeded(RuntimeError):
    """Raised when total document cast failures exceed ``IngestionParams.max_doc_errors``."""

    def __init__(
        self,
        *,
        total_failures: int,
        limit: int,
        doc_error_sink_path: Path | None,
    ) -> None:
        self.total_failures = total_failures
        self.limit = limit
        self.doc_error_sink_path = doc_error_sink_path
        sink = str(doc_error_sink_path) if doc_error_sink_path else "(not configured)"
        super().__init__(
            f"Document error budget exceeded: {total_failures} total failures "
            f"(limit {limit}). Doc error sink (jsonl.gz): {sink}"
        )


class DocCastFailure(BaseModel):
    """Structured record for one source document that failed during resource casting."""

    resource_name: str
    doc_index: int
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
    """Outcome of casting a batch through a resource (possibly with skipped documents)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    graph: GraphContainer
    failures: list[DocCastFailure] = Field(default_factory=list)


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
    batch_prefetch: int = Field(
        default=2,
        ge=1,
        description=(
            "How many batches to prefetch ahead while processing current batch. "
            "Keeps ingestion lazy with bounded memory."
        ),
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
    on_doc_error: Literal["skip", "fail"] = "skip"
    doc_error_sink_path: Path | None = Field(
        default=None,
        description=(
            "Append gzip-compressed JSONL cast-failure records (typical suffix .jsonl.gz)."
        ),
    )
    max_doc_errors: int | None = None
    doc_error_preview_max_bytes: int = 4096
    doc_error_preview_keys: tuple[str, ...] | None = None
