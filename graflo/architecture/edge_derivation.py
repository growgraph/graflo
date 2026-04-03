"""Ingestion-time wiring: how an edge step binds to extracted locations and documents.

These fields do **not** belong in schema ``edge_config`` / :class:`~graflo.architecture.schema.edge.Edge`.
They are set on edge pipeline steps (:class:`~graflo.architecture.pipeline.runtime.actor.config.models.EdgeActorConfig`),
threaded through :class:`~graflo.architecture.graph_types.EdgeIntent`, and used in
:func:`~graflo.architecture.pipeline.runtime.actor.edge_render.render_edge`.

When :attr:`EdgeDerivation.relation_from_key` is true, :class:`~graflo.architecture.schema.edge.EdgeConfig`
records the edge id via :meth:`~graflo.architecture.schema.edge.EdgeConfig.mark_relation_derived_from_key`
so :class:`~graflo.architecture.schema.db_aware.EdgeConfigDBAware` can align TigerGraph DDL with runtime.
"""

from __future__ import annotations

from pydantic import Field

from graflo.architecture.base import ConfigBaseModel


class EdgeDerivation(ConfigBaseModel):
    """How this edge step selects vertex locations and reads per-row relation from data."""

    match_source: str | None = Field(
        default=None,
        description="Require this path segment in source vertex locations.",
    )
    match_target: str | None = Field(
        default=None,
        description="Require this path segment in target vertex locations.",
    )
    exclude_source: str | None = Field(
        default=None,
        description="Exclude source locations containing this path segment.",
    )
    exclude_target: str | None = Field(
        default=None,
        description="Exclude target locations containing this path segment.",
    )
    match: str | None = Field(
        default=None,
        description="Require this segment in both source and target locations.",
    )
    relation_field: str | None = Field(
        default=None,
        description="Document/ctx field name for per-row relationship label when schema relation is unset.",
    )
    relation_from_key: bool = Field(
        default=False,
        description="If True, derive the per-row relation label from the location key during assembly.",
    )

    def is_empty(self) -> bool:
        if self.relation_from_key:
            return False
        return all(
            getattr(self, name) is None
            for name in (
                "match_source",
                "match_target",
                "exclude_source",
                "exclude_target",
                "match",
                "relation_field",
            )
        )
