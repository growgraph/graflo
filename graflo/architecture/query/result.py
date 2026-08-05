"""What a read returned, and what it left out.

`GraphContainer` alone cannot say whether a result is the whole answer. Callers
would otherwise infer truncation from ``len(rows) == limit``, which is wrong in
both directions: a result of exactly `limit` rows may be complete, and a result
under `limit` may still have been cut by an element or timeout bound.
"""

from __future__ import annotations

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.graph_types import GraphContainer


class QueryResult(ConfigBaseModel):
    """A query's answer plus the honest caveats."""

    container: GraphContainer = PydanticField(
        default_factory=GraphContainer,
        description="DB-agnostic vertices and edges, identical across backends.",
    )
    element_count: int = PydanticField(
        default=0, ge=0, description="Vertices plus edges carried."
    )
    truncated: bool = PydanticField(
        default=False, description="Whether a cap cut the answer short."
    )
    caps_hit: list[str] = PydanticField(
        default_factory=list,
        description=(
            "Which caps bound this answer, by name. Empty when nothing bound "
            "it — the only way a caller can tell a complete answer from one "
            "that happens to fit."
        ),
    )
    elapsed_ms: int = PydanticField(default=0, ge=0)

    @classmethod
    def of(
        cls,
        container: GraphContainer,
        *,
        caps_hit: list[str] | None = None,
        elapsed_ms: int = 0,
    ) -> QueryResult:
        """Build a result, deriving the element count from the container."""
        hit = caps_hit or []
        count = sum(len(docs) for docs in container.vertices.values()) + sum(
            len(rows) for rows in container.edges.values()
        )
        return cls(
            container=container,
            element_count=count,
            truncated=bool(hit),
            caps_hit=hit,
            elapsed_ms=elapsed_ms,
        )
