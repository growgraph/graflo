"""Bounded schema context: the schema graph as a navigable, budgetable object.

Answers *"what can I ask?"* about a schema without touching a database. Every
export here is layer 2 — pure logical model, no ``db``, no ``data_source``, no
embeddings, no tokenizer.

Eager re-exports (this is a package boundary, not a lazy façade).
"""

from __future__ import annotations

from graflo.architecture.schema.context.budget import (
    Budget,
    BudgetAccounting,
    estimate_tokens,
)
from graflo.architecture.schema.context.card import (
    BaseCard,
    ConnectorCard,
    DatabaseProfileCard,
    EdgeCard,
    EntryPoint,
    ManifestCard,
    ResourceCard,
    SchemaCard,
    TransformCard,
    VertexCard,
    build_card,
    build_connector_card,
    build_database_profile_card,
    build_edge_card,
    build_manifest_card,
    build_resource_card,
    build_transform_card,
    build_vertex_card,
)
from graflo.architecture.schema.context.element_text import (
    ELEMENT_TEXT_VERSION,
    ElementText,
    edge_text,
    extractable_properties,
    field_text,
    iter_elements,
    minted_key_fields,
    vertex_text,
)
from graflo.architecture.schema.context.elision import (
    ElidedEdge,
    ElidedVertex,
    ElisionReport,
)
from graflo.architecture.schema.context.graph import (
    SchemaGraph,
    SchemaNeighborhood,
    SchemaPath,
    neighborhood_distances,
)
from graflo.architecture.schema.context.instance_shape import instance_json_schema
from graflo.architecture.schema.context.rank import (
    RankingWeights,
    VertexSignals,
    score_vertices,
)
from graflo.architecture.schema.context.subschema import subschema
from graflo.architecture.schema.context.type_sheet import render_type_sheet

__all__ = [
    "ELEMENT_TEXT_VERSION",
    "BaseCard",
    "Budget",
    "BudgetAccounting",
    "ConnectorCard",
    "DatabaseProfileCard",
    "EdgeCard",
    "ElementText",
    "ElidedEdge",
    "ElidedVertex",
    "ElisionReport",
    "EntryPoint",
    "ManifestCard",
    "RankingWeights",
    "ResourceCard",
    "SchemaCard",
    "SchemaGraph",
    "SchemaNeighborhood",
    "SchemaPath",
    "TransformCard",
    "VertexCard",
    "VertexSignals",
    "build_card",
    "build_connector_card",
    "build_database_profile_card",
    "build_edge_card",
    "build_manifest_card",
    "build_resource_card",
    "build_transform_card",
    "build_vertex_card",
    "edge_text",
    "estimate_tokens",
    "extractable_properties",
    "field_text",
    "instance_json_schema",
    "iter_elements",
    "minted_key_fields",
    "neighborhood_distances",
    "render_type_sheet",
    "score_vertices",
    "subschema",
    "vertex_text",
]
