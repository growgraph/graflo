"""Edges whose endpoints are known only by a secondary identity.

A producer that builds a :class:`~graflo.architecture.graph_types.container.GraphContainer`
itself (an extractor reading text, say) may know an edge's endpoint only by an
alternate key: a phone number rather than the email that identifies a person.
Such an edge cannot sit in the container, whose edges carry primary
identities; it waits as a :class:`PendingEdge` until the endpoint is looked up
in the target database at write time.
"""

from __future__ import annotations

from typing import Any

from pydantic import Field

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.graph_types.identifiers import EdgeId


class EdgeEndpoint(ConfigBaseModel):
    """How an edge reaches one vertex: by its primary identity, or by values of
    a secondary identity to look it up by."""

    vertex: str = Field(description="Vertex type.")
    identity: dict[str, Any] | None = Field(
        default=None, description="Primary identity, when known."
    )
    match: dict[str, Any] | None = Field(
        default=None, description="Secondary identity values to look the vertex up by."
    )


class PendingEdge(ConfigBaseModel):
    """An edge with at least one endpoint known only by a secondary identity."""

    edge_id: EdgeId
    source: EdgeEndpoint
    target: EdgeEndpoint
    props: dict[str, Any] = Field(default_factory=dict)
