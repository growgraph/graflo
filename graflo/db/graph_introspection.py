"""Graph database schema introspection and Schema inference."""

from __future__ import annotations

import logging
from typing import Any

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.schema import CoreSchema, GraphMetadata, Schema
from graflo.architecture.schema.database_features import DatabaseProfile
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.vertex import Field, FieldType, Vertex, VertexConfig
from graflo.onto import DBType

logger = logging.getLogger(__name__)

_IDENTITY_CANDIDATES = ("id", "_key", "uuid", "key", "name")


class GraphVertexIntrospection(ConfigBaseModel):
    """Sampled vertex type from a graph database."""

    name: str
    properties: list[str] = PydanticField(default_factory=list)
    identity: list[str] = PydanticField(default_factory=list)
    #: Declared property types, where the backend has a catalogue to report them
    #: (``DESCRIBE TAG``, GSQL DDL, ``information_schema``). Sampling backends
    #: leave this empty rather than guess, and the inferencer falls back to
    #: ``STRING`` only for what is genuinely unknown.
    property_types: dict[str, FieldType] = PydanticField(default_factory=dict)


class GraphEdgeIntrospection(ConfigBaseModel):
    """Sampled edge pattern from a graph database."""

    source: str
    target: str
    relation: str | None = None
    properties: list[str] = PydanticField(default_factory=list)
    collection_name: str | None = None
    #: ``False`` only when the backend *states* the edge is undirected -- today
    #: that is TigerGraph's ``UNDIRECTED EDGE`` DDL. Sampling backends observe
    #: edges in one orientation and cannot tell the difference, so they leave
    #: the default: an unproven ``directed: false`` would silently widen every
    #: query built on the recovered schema.
    directed: bool = True
    property_types: dict[str, FieldType] = PydanticField(default_factory=dict)


class GraphIntrospectionResult(ConfigBaseModel):
    """Serializable graph introspection snapshot."""

    name: str
    vertices: list[GraphVertexIntrospection] = PydanticField(default_factory=list)
    edges: list[GraphEdgeIntrospection] = PydanticField(default_factory=list)
    #: Rows examined per type where the result came from sampling, ``None`` where
    #: it came from a DDL catalogue and is therefore complete. A modeller needs
    #: this to know how far to trust the recovered schema: a low sample against a
    #: large graph misses rare properties and rare edge patterns entirely.
    sample_limit: int | None = None


def infer_identity_fields(properties: list[str]) -> list[str]:
    """Pick identity fields from sampled property names."""
    for candidate in _IDENTITY_CANDIDATES:
        if candidate in properties:
            return [candidate]
    if properties:
        return [properties[0]]
    return ["id"]


def _field(name: str, types: dict[str, FieldType]) -> Field:
    """Build a ``Field``, typed when the backend reported a type for it."""
    declared = types.get(name)
    return Field(name=name) if declared is None else Field(name=name, type=declared)


def merge_property_names(existing: list[str], sampled: list[str]) -> list[str]:
    """Merge property name lists preserving order and uniqueness."""
    seen: set[str] = set()
    merged: list[str] = []
    for name in existing + sampled:
        if name not in seen:
            seen.add(name)
            merged.append(name)
    return merged


class GraphSchemaInferencer:
    """Build a graflo :class:`Schema` from graph introspection results."""

    def __init__(self, db_flavor: DBType = DBType.ARANGO):
        self.db_flavor = db_flavor

    def infer_vertex_config(
        self, introspection: GraphIntrospectionResult
    ) -> VertexConfig:
        vertices: list[Vertex] = []
        for vertex_info in introspection.vertices:
            identity = (
                vertex_info.identity
                if vertex_info.identity
                else infer_identity_fields(vertex_info.properties)
            )
            types = vertex_info.property_types
            fields = [_field(p, types) for p in vertex_info.properties]
            for ident in identity:
                if ident not in vertex_info.properties:
                    fields.insert(0, _field(ident, types))
            vertices.append(
                Vertex(
                    name=vertex_info.name,
                    properties=fields,
                    identity=identity,
                )
            )
        return VertexConfig(vertices=vertices)

    def infer_edge_config(
        self,
        introspection: GraphIntrospectionResult,
        vertex_config: VertexConfig,
    ) -> EdgeConfig:
        vertex_names = vertex_config.vertex_set
        edges: list[Edge] = []
        for edge_info in introspection.edges:
            if edge_info.source not in vertex_names:
                logger.warning(
                    "Skipping edge %s -> %s: source vertex undefined",
                    edge_info.source,
                    edge_info.target,
                )
                continue
            if edge_info.target not in vertex_names:
                logger.warning(
                    "Skipping edge %s -> %s: target vertex undefined",
                    edge_info.source,
                    edge_info.target,
                )
                continue
            weight_fields = [
                _field(p, edge_info.property_types) for p in edge_info.properties
            ]
            edges.append(
                Edge(
                    source=edge_info.source,
                    target=edge_info.target,
                    relation=edge_info.relation,
                    properties=weight_fields,
                    directed=edge_info.directed,
                )
            )
        return EdgeConfig(edges=edges)

    def infer_schema(
        self,
        introspection: GraphIntrospectionResult,
        *,
        schema_name: str | None = None,
    ) -> Schema:
        name = schema_name or introspection.name
        vertex_config = self.infer_vertex_config(introspection)
        edge_config = self.infer_edge_config(introspection, vertex_config)
        metadata = GraphMetadata(name=name)
        return Schema(
            metadata=metadata,
            core_schema=CoreSchema(
                vertex_config=vertex_config,
                edge_config=edge_config,
            ),
            db_profile=DatabaseProfile(
                db_flavor=self.db_flavor,
                vertex_storage_names={v.name: v.name for v in vertex_config.vertices},
            ),
        )


def strip_internal_properties(doc: dict[str, Any]) -> dict[str, Any]:
    """Remove common database-internal keys from a document."""
    skip = frozenset(
        {
            "_id",
            "_rev",
            "_from",
            "_to",
            "elementId",
            "identity",
            "labels",
        }
    )
    return {k: v for k, v in doc.items() if k not in skip}
