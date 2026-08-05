"""What a bounded projection left out, and how to go and get it.

A slice without an elision report is indistinguishable from a complete schema,
which is the failure mode this whole wave exists to avoid: an agent that believes
it has seen everything asks confidently wrong questions.
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.graph_types import EdgeId
from graflo.architecture.schema.context.budget import BudgetAccounting

VertexElisionReason = Literal["budget", "unreachable", "not_selected"]
EdgeElisionReason = Literal["budget", "endpoint_elided", "not_selected"]


class ElidedVertex(ConfigBaseModel):
    """A vertex type left out of the slice."""

    name: str = PydanticField(..., description="Vertex type name.")
    reason: VertexElisionReason = PydanticField(..., description="Why it was dropped.")
    degree: int = PydanticField(..., description="Incident edges in the full schema.")
    hop_distance: int | None = PydanticField(
        default=None, description="Hops from the nearest seed; None when unreachable."
    )
    description: str | None = PydanticField(
        default=None,
        description="Authored description, kept because it may be the reason to drill in.",
    )
    drill_in: str = PydanticField(
        ..., description="Call that would bring this type into a slice."
    )


class ElidedEdge(ConfigBaseModel):
    """An edge left out of the slice."""

    edge_id: EdgeId = PydanticField(..., description="(source, target, relation).")
    reason: EdgeElisionReason = PydanticField(..., description="Why it was dropped.")
    description: str | None = PydanticField(
        default=None, description="Authored description, kept for the same reason."
    )


class ElisionReport(ConfigBaseModel):
    """Everything the slice does not contain, plus the budget that caused it."""

    elided_vertices: list[ElidedVertex] = PydanticField(
        default_factory=list, description="Vertex types not in the slice."
    )
    elided_edges: list[ElidedEdge] = PydanticField(
        default_factory=list, description="Edges not in the slice."
    )
    elided_properties: dict[str, list[str]] = PydanticField(
        default_factory=dict,
        description="Vertex type -> property names dropped from a surviving type.",
    )
    budget: BudgetAccounting = PydanticField(
        ..., description="Measured cost of the slice."
    )

    @property
    def truncated(self) -> bool:
        """Whether anything at all was left out."""
        return bool(self.elided_vertices or self.elided_edges or self.elided_properties)
