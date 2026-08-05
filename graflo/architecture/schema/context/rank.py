"""Ranking vertex types by how useful they are to an agent orienting itself.

Local signals only — degree, identity policy, property count, index presence, hop
distance from a seed. No embeddings and no corpus statistics: semantic ranking is
the server's business (it has the index), core stays dependency-free.
"""

from __future__ import annotations

import math
from collections.abc import Sequence

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.graph_types import EdgeDirection
from graflo.architecture.schema.context.graph import SchemaGraph

#: Relative usefulness of each identity mode to an agent forming a query.
#: A ``blank`` vertex has no natural key to filter on, so it is nearly
#: unqueryable — ranking it last is correct behaviour, not a fudge factor.
IDENTITY_MODE_STRENGTH: dict[str, float] = {
    "natural": 1.0,
    "hash": 0.75,
    "assigned": 0.5,
    "blank": 0.1,
}


class RankingWeights(ConfigBaseModel):
    """Relative weight of each local signal. Weights need not sum to 1."""

    hop_decay: float = PydanticField(
        default=0.55,
        description="Score multiplier per hop of distance from the nearest seed.",
        gt=0.0,
        le=1.0,
    )
    degree: float = PydanticField(
        default=0.20, description="Weight of normalized incident-edge count.", ge=0.0
    )
    identity: float = PydanticField(
        default=0.15, description="Weight of identity-mode strength.", ge=0.0
    )
    properties: float = PydanticField(
        default=0.10, description="Weight of log-scaled property count.", ge=0.0
    )
    indexed: float = PydanticField(
        default=0.10,
        description="Weight of secondary-index presence (cheap to filter on).",
        ge=0.0,
    )


class VertexSignals(ConfigBaseModel):
    """Per-vertex-type ranking inputs and the score derived from them."""

    name: str = PydanticField(..., description="Vertex type name.")
    hop_distance: int | None = PydanticField(
        default=None,
        description="Hops from the nearest seed; None when unreachable.",
    )
    degree: int = PydanticField(..., description="Incident edges (out + in).")
    identity_mode: str = PydanticField(
        ..., description="One of natural / hash / assigned / blank."
    )
    property_count: int = PydanticField(..., description="Declared property count.")
    has_secondary_index: bool = PydanticField(
        ..., description="Whether db_profile declares a secondary index for this type."
    )
    score: float = PydanticField(..., description="Composite rank; higher is better.")


def score_vertices(
    graph: SchemaGraph,
    seeds: Sequence[str] = (),
    *,
    weights: RankingWeights | None = None,
    max_hops: int = 3,
    direction: EdgeDirection = EdgeDirection.ANY,
) -> list[VertexSignals]:
    """Rank every vertex type in *graph*, highest score first.

    With no *seeds*, ranking is seed-independent (structure only) and answers
    "what are the important types here" — which is what the orientation card
    needs. With seeds, hop distance dominates and answers "what is near what I
    asked about".

    Ties break by vertex name ascending. This is not cosmetic: without a total
    order the elision report is not reproducible across runs, and the budget tests
    become flaky.
    """
    weights = weights or RankingWeights()
    schema = graph.schema
    vertex_config = schema.core_schema.vertex_config
    db_profile = schema.db_profile

    distances: dict[str, int] = {}
    for seed in seeds:
        neighborhood = graph.schema_neighbors(seed, hops=max_hops, direction=direction)
        for name, distance in neighborhood.distances.items():
            if name not in distances or distance < distances[name]:
                distances[name] = distance

    degrees = {name: graph.degree(name) for name in graph.vertex_types}
    max_degree = max(degrees.values(), default=0)
    property_counts = {
        name: len(vertex_config[name].property_names) for name in graph.vertex_types
    }
    max_properties = max(property_counts.values(), default=0)

    signals: list[VertexSignals] = []
    for name in sorted(graph.vertex_types):
        vertex = vertex_config[name]
        hop_distance = distances.get(name) if seeds else None
        degree = degrees[name]
        property_count = property_counts[name]
        has_index = bool(db_profile.vertex_secondary_indexes(name))

        structural = (
            weights.degree * (degree / max_degree if max_degree else 0.0)
            + weights.identity * IDENTITY_MODE_STRENGTH.get(vertex.identity_mode, 0.5)
            + weights.properties
            * (
                math.log1p(property_count) / math.log1p(max_properties)
                if max_properties
                else 0.0
            )
            + weights.indexed * (1.0 if has_index else 0.0)
        )
        if not seeds:
            score = structural
        elif hop_distance is None:
            score = 0.0
        else:
            score = (weights.hop_decay**hop_distance) + structural

        signals.append(
            VertexSignals(
                name=name,
                hop_distance=hop_distance,
                degree=degree,
                identity_mode=vertex.identity_mode,
                property_count=property_count,
                has_secondary_index=has_index,
                score=score,
            )
        )

    signals.sort(key=lambda item: (-item.score, item.name))
    return signals
