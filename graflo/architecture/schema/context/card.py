"""Compact orientation card: the cheapest useful thing to hand an agent first.

Answers "what kind of graph is this, where do I start, how much is here" in a
payload that stays small no matter how large the schema is.
"""

from __future__ import annotations

from collections import Counter

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.schema.context.budget import estimate_tokens
from graflo.architecture.schema.context.graph import SchemaGraph
from graflo.architecture.schema.context.rank import VertexSignals, score_vertices
from graflo.architecture.schema.document import Schema


class EntryPoint(ConfigBaseModel):
    """A vertex type an agent can look up directly.

    The single most useful fact about an unfamiliar graph: a type with a natural
    identity *and* a secondary index is one you can filter on cheaply, which is
    where a query should start.
    """

    name: str = PydanticField(..., description="Vertex type name.")
    identity: list[str] = PydanticField(
        ..., description="Primary identity field names."
    )
    identity_mode: str = PydanticField(
        ..., description="natural / hash / assigned / blank."
    )
    secondary_identities: list[str] = PydanticField(
        default_factory=list, description="Declared secondary identity names."
    )
    indexed_fields: list[list[str]] = PydanticField(
        default_factory=list, description="Secondary index field-sets on this type."
    )
    description: str | None = PydanticField(
        default=None, description="Authored description, if any."
    )


class SchemaCard(ConfigBaseModel):
    """Bounded orientation summary of a whole schema."""

    name: str = PydanticField(..., description="Schema name.")
    version: str | None = PydanticField(default=None, description="Schema version.")
    description: str | None = PydanticField(
        default=None, description="Authored schema description."
    )
    db_flavor: str = PydanticField(
        ..., description="Target database flavor from the db profile."
    )
    vertex_count: int = PydanticField(..., description="Declared vertex types.")
    edge_count: int = PydanticField(..., description="Declared edges.")
    total_property_count: int = PydanticField(
        ..., description="Declared properties across all vertex types."
    )
    hub_types: list[VertexSignals] = PydanticField(
        default_factory=list, description="Highest-ranked types, most central first."
    )
    entry_points: list[EntryPoint] = PydanticField(
        default_factory=list, description="Types that can be looked up directly."
    )
    identity_modes: dict[str, int] = PydanticField(
        default_factory=dict, description="Histogram of vertex identity modes."
    )
    isolated_types: list[str] = PydanticField(
        default_factory=list,
        description="Vertex types with no incident edge, truncated to ``max_names``.",
    )
    isolated_type_count: int = PydanticField(
        default=0, description="Total isolated types, including any not listed."
    )
    relation_vocabulary: list[str] = PydanticField(
        default_factory=list,
        description="Distinct edge relation names, truncated to ``max_names``.",
    )
    relation_count: int = PydanticField(
        default=0, description="Total distinct relations, including any not listed."
    )
    estimated_tokens: int = PydanticField(
        ..., description="Estimated token cost of this card."
    )


def build_card(
    schema: Schema,
    *,
    top_n: int = 10,
    max_names: int = 25,
    graph: SchemaGraph | None = None,
) -> SchemaCard:
    """Summarize *schema* for an agent's first contact with it.

    Every list on the card is bounded, with a count reported alongside. A card
    whose size grows with the schema is not a card — it is the problem this wave
    exists to solve, wearing a summary's clothes.

    Args:
        schema: Schema to summarize. Never mutated.
        top_n: How many hub types and entry points to list.
        max_names: How many isolated types and relation names to list.
        graph: Prebuilt index, if the caller already has one.
    """
    graph = graph or SchemaGraph.from_schema(schema)
    vertex_config = schema.core_schema.vertex_config
    db_profile = schema.db_profile

    ranked = score_vertices(graph)
    isolated = graph.isolated_types()
    relations = graph.relation_vocabulary()
    identity_modes = Counter(
        vertex_config[name].identity_mode for name in graph.vertex_types
    )

    entry_points: list[EntryPoint] = []
    for signal in ranked:
        if len(entry_points) >= top_n:
            break
        vertex = vertex_config[signal.name]
        indexes = db_profile.vertex_secondary_indexes(signal.name)
        # A blank type has no natural key and nothing to filter on: it is not an
        # entry point, whatever its centrality.
        if vertex.identity_mode == "blank" and not indexes:
            continue
        if not vertex.identity and not indexes:
            continue
        entry_points.append(
            EntryPoint(
                name=signal.name,
                identity=list(vertex.identity),
                identity_mode=vertex.identity_mode,
                secondary_identities=vertex.secondary_identity_names,
                indexed_fields=[list(index.fields) for index in indexes],
                description=vertex.description,
            )
        )

    card = SchemaCard(
        name=schema.metadata.name,
        version=schema.metadata.version,
        description=schema.metadata.description,
        db_flavor=str(db_profile.db_flavor),
        vertex_count=len(graph.vertex_types),
        edge_count=len(graph.edge_ids),
        total_property_count=sum(
            len(vertex_config[name].property_names) for name in graph.vertex_types
        ),
        hub_types=ranked[:top_n],
        entry_points=entry_points,
        identity_modes=dict(sorted(identity_modes.items())),
        isolated_types=isolated[:max_names],
        isolated_type_count=len(isolated),
        relation_vocabulary=relations[:max_names],
        relation_count=len(relations),
        estimated_tokens=0,
    )
    card.estimated_tokens = estimate_tokens(card.to_minimal_canonical_dict())
    return card
