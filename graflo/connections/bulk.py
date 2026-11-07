"""Tuning of a bulk load, carried on a database config as ``bulk``.

Nothing here turns bulk on: a bulk run is asked for by the ingestion mode.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class BulkConfig(BaseModel):
    """How a bulk load folds and writes; every field has a working default."""

    model_config = ConfigDict(extra="forbid")

    chunk_size: int = Field(
        default=50_000,
        gt=0,
        description="Folded rows per chunk: the unit a writer commits and resume skips.",
    )
    parallelism: int = Field(
        default=4,
        gt=0,
        description="Chunks written at once; a backend may allow fewer.",
    )
    memory_limit: str | None = Field(
        default=None,
        description="Memory the fold may use before spilling to the staging directory "
        "(DuckDB syntax, e.g. '8GB'); DuckDB's default when unset.",
    )
    fold_threads: int | None = Field(
        default=None,
        gt=0,
        description="Threads the fold uses; DuckDB's default when unset.",
    )
