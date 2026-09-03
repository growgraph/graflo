"""Compact orientation card: the cheapest useful thing to hand an agent first.

Answers "what kind of graph is this, where do I start, how much is here" in a
payload that stays small no matter how large the schema is.
"""

from __future__ import annotations

from collections import Counter
from typing import TYPE_CHECKING

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.schema.context.budget import estimate_tokens
from graflo.architecture.schema.context.graph import SchemaGraph
from graflo.architecture.schema.context.rank import VertexSignals, score_vertices
from graflo.architecture.schema.document import Schema

if TYPE_CHECKING:
    from graflo.architecture.contract.bindings.core import AnyConnector
    from graflo.architecture.contract.ingestion.resource import ResourceConfig
    from graflo.architecture.contract.ingestion.transform import Transform
    from graflo.architecture.contract.manifest import GraphManifest
    from graflo.architecture.schema.database_features import DatabaseProfile
    from graflo.architecture.schema.edge import Edge
    from graflo.architecture.schema.vertex import Vertex


class BaseCard(ConfigBaseModel):
    """Common foundation for all card types."""

    id: str | None = PydanticField(
        default=None, description="Optional stable identifier (UUID)."
    )
    estimated_tokens: int = PydanticField(
        default=0, description="Estimated token cost of this card."
    )


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


class SchemaCard(BaseCard):
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


def build_card(
    schema: Schema,
    *,
    top_n: int = 10,
    max_names: int = 25,
    graph: SchemaGraph | None = None,
    id: str | None = None,
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
        id=id,
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


class VertexCard(BaseCard):
    """Summary of a single vertex type."""

    name: str = PydanticField(..., description="Vertex type name.")
    identity_mode: str = PydanticField(
        ..., description="natural / hash / blank / assigned."
    )
    property_count: int = PydanticField(..., description="Total declared properties.")
    identity_fields: list[str] = PydanticField(
        default_factory=list, description="Primary identity field names."
    )
    secondary_identity_count: int = PydanticField(
        default=0, description="Number of secondary identity field-sets."
    )
    description: str | None = PydanticField(
        default=None, description="Authored description."
    )


class EdgeCard(BaseCard):
    """Summary of a single edge type."""

    source: str = PydanticField(..., description="Source vertex type name.")
    target: str = PydanticField(..., description="Target vertex type name.")
    relation: str | None = PydanticField(
        default=None, description="Relation type name."
    )
    directed: bool = PydanticField(
        default=True, description="True if source->target order matters."
    )
    identity_count: int = PydanticField(
        default=0, description="Number of logical uniqueness keys."
    )
    property_count: int = PydanticField(
        default=0, description="Total declared properties."
    )
    description: str | None = PydanticField(
        default=None, description="Authored description."
    )


class ResourceCard(BaseCard):
    """Summary of an ingestion resource."""

    name: str = PydanticField(..., description="Resource name.")
    actor_count: int = PydanticField(default=0, description="Number of pipeline steps.")
    vertex_targets: list[str] = PydanticField(
        default_factory=list, description="Top vertex types this resource writes to."
    )
    edge_targets: list[str] = PydanticField(
        default_factory=list, description="Top edge types this resource writes to."
    )
    encoding: str = PydanticField(default="utf-8", description="Character encoding.")
    infer_edges: bool = PydanticField(
        default=True, description="True if greedy edge inference is enabled."
    )


class TransformCard(BaseCard):
    """Summary of a data transform."""

    name: str | None = PydanticField(default=None, description="Transform name.")
    functional: bool = PydanticField(
        ..., description="True if it wraps a Python function."
    )
    module: str | None = PydanticField(default=None, description="Python module path.")
    foo: str | None = PydanticField(default=None, description="Python function name.")
    input_count: int = PydanticField(default=0, description="Number of input fields.")
    output_count: int = PydanticField(default=0, description="Number of output fields.")
    strategy: str = PydanticField(
        default="single", description="Functional call strategy (single, each, all)."
    )


class ConnectorCard(BaseCard):
    """Summary of a data connector (File, Table, etc.)."""

    type: str = PydanticField(
        ..., description="Connector class name (FileConnector, etc.)."
    )
    name: str | None = PydanticField(default=None, description="Authored name.")
    resource_name: str | None = PydanticField(
        default=None, description="Primary resource name bound to this connector."
    )
    summary: str | None = PydanticField(
        default=None, description="Type-specific summary (e.g. path, table name)."
    )


class DatabaseProfileCard(BaseCard):
    """Summary of physical DB features."""

    db_flavor: str = PydanticField(..., description="Target DB flavor.")
    vertex_index_count: int = PydanticField(
        default=0, description="Total secondary vertex indexes."
    )
    edge_spec_count: int = PydanticField(
        default=0, description="Total physical edge specifications."
    )
    target_namespace: str | None = PydanticField(
        default=None, description="LPG namespace override."
    )


class ManifestCard(BaseCard):
    """Summary of a whole GraphManifest."""

    name: str | None = PydanticField(default=None, description="Manifest name.")
    version: str | None = PydanticField(default=None, description="Manifest version.")
    has_schema: bool = PydanticField(..., description="Presence of schema block.")
    has_ingestion: bool = PydanticField(..., description="Presence of ingestion block.")
    has_bindings: bool = PydanticField(..., description="Presence of bindings block.")
    vertex_count: int | None = PydanticField(
        default=None, description="Vertex count if schema present."
    )
    edge_count: int | None = PydanticField(
        default=None, description="Edge count if schema present."
    )
    resource_count: int | None = PydanticField(
        default=None, description="Resource count if ingestion present."
    )


def build_vertex_card(vertex: Vertex, id: str | None = None) -> VertexCard:
    """Build a summary card for a logical vertex type."""
    card = VertexCard(
        id=id,
        name=vertex.name,
        identity_mode=vertex.identity_mode,
        property_count=len(vertex.properties),
        identity_fields=list(vertex.identity),
        secondary_identity_count=len(vertex.secondary_identities),
        description=vertex.description,
    )
    card.estimated_tokens = estimate_tokens(card.to_minimal_canonical_dict())
    return card


def build_edge_card(edge: Edge, id: str | None = None) -> EdgeCard:
    """Build a summary card for a logical edge type."""
    card = EdgeCard(
        id=id,
        source=edge.source,
        target=edge.target,
        relation=edge.relation,
        directed=edge.directed,
        identity_count=len(edge.identities),
        property_count=len(edge.properties),
        description=edge.description,
    )
    card.estimated_tokens = estimate_tokens(card.to_minimal_canonical_dict())
    return card


def build_resource_card(
    resource: ResourceConfig, id: str | None = None, top_n: int = 5
) -> ResourceCard:
    """Build a summary card for an ingestion resource."""
    # This is a bit expensive if we bind, but here we just collect names.
    from graflo.architecture.contract.ingestion.resource import (
        collect_vertex_names_from_pipeline,
    )

    pipeline_vertices = list(collect_vertex_names_from_pipeline(resource.pipeline))
    edge_targets = [
        f"({e.edge.source}, {e.edge.target}, {e.edge.relation})"
        for e in resource.extra_weights
    ]

    card = ResourceCard(
        id=id,
        name=resource.name,
        actor_count=resource.pipeline_actor_count(),
        vertex_targets=pipeline_vertices[:top_n],
        edge_targets=edge_targets[:top_n],
        encoding=str(resource.encoding),
        infer_edges=resource.infer_edges,
    )
    card.estimated_tokens = estimate_tokens(card.to_minimal_canonical_dict())
    return card


def build_transform_card(transform: Transform, id: str | None = None) -> TransformCard:
    """Build a summary card for a data transform."""
    card = TransformCard(
        id=id,
        name=transform.name,
        functional=transform.functional_transform,
        module=transform.module,
        foo=transform.foo,
        input_count=len(transform.input),
        output_count=len(transform.output),
        strategy=str(transform.strategy),
    )
    card.estimated_tokens = estimate_tokens(card.to_minimal_canonical_dict())
    return card


def build_connector_card(
    connector: AnyConnector, id: str | None = None
) -> ConnectorCard:
    """Build a summary card for a data connector."""
    from graflo.architecture.contract.bindings.connectors import (
        APIConnector,
        FileConnector,
        KafkaConnector,
        SparqlConnector,
        TableConnector,
    )

    summary: str | None = None
    if isinstance(connector, FileConnector):
        summary = connector.regex or str(connector.sub_path)
    elif isinstance(connector, TableConnector):
        summary = connector.table_name
    elif isinstance(connector, SparqlConnector):
        summary = connector.rdf_class
    elif isinstance(connector, APIConnector):
        summary = connector.path
    elif isinstance(connector, KafkaConnector):
        summary = ",".join(connector.topics)

    card = ConnectorCard(
        id=id,
        type=connector.__class__.__name__,
        name=connector.name,
        resource_name=connector.resource_name,
        summary=summary,
    )
    card.estimated_tokens = estimate_tokens(card.to_minimal_canonical_dict())
    return card


def build_database_profile_card(
    profile: DatabaseProfile, id: str | None = None
) -> DatabaseProfileCard:
    """Build a summary card for a database profile."""
    card = DatabaseProfileCard(
        id=id,
        db_flavor=str(profile.db_flavor),
        vertex_index_count=sum(len(idx) for idx in profile.vertex_indexes.values()),
        edge_spec_count=len(profile.edge_specs),
        target_namespace=profile.target_namespace,
    )
    card.estimated_tokens = estimate_tokens(card.to_minimal_canonical_dict())
    return card


def build_manifest_card(manifest: GraphManifest, id: str | None = None) -> ManifestCard:
    """Build a summary card for a full graph manifest."""
    schema = manifest.graph_schema
    ingestion = manifest.ingestion_model

    card = ManifestCard(
        id=id,
        name=schema.metadata.name if schema else None,
        version=schema.metadata.version if schema else None,
        has_schema=schema is not None,
        has_ingestion=ingestion is not None,
        has_bindings=manifest.bindings is not None,
        vertex_count=len(schema.core_schema.vertex_config.vertices) if schema else None,
        edge_count=len(schema.core_schema.edge_config.edges) if schema else None,
        resource_count=len(ingestion.resources) if ingestion else None,
    )
    card.estimated_tokens = estimate_tokens(card.to_minimal_canonical_dict())
    return card
