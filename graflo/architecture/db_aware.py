"""DB-aware projections for logical graph configs.

These wrappers materialize database-specific naming/defaults from logical
`VertexConfig`/`EdgeConfig` and `DPProfile` without mutating logical models.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator

from graflo.architecture.database_features import DatabaseProfile
from graflo.architecture.edge import (
    DEFAULT_TIGERGRAPH_RELATION,
    DEFAULT_TIGERGRAPH_RELATION_WEIGHTNAME,
    Edge,
    EdgeConfig,
    WeightConfig,
)
from graflo.architecture.onto import EdgeId, Index
from graflo.architecture.vertex import Field, FieldType, VertexConfig
from graflo.onto import DBType


@dataclass(frozen=True)
class EdgeRuntime:
    """Resolved DB-facing runtime data for one logical edge."""

    edge: Edge
    source_storage: str
    target_storage: str
    relation_name: str | None
    store_extracted_relation_as_weight: bool
    effective_relation_field: str | None
    db_profile: DatabaseProfile

    @property
    def edge_id(self) -> EdgeId:
        return self.edge.edge_id

    def storage_name(self, *, purpose: str | None = None) -> str | None:
        return self.db_profile.edge_storage_name(
            self.edge.edge_id,
            source_storage=self.source_storage,
            target_storage=self.target_storage,
            purpose=purpose,
        )

    def graph_name(self, *, purpose: str | None = None) -> str | None:
        return self.db_profile.edge_graph_name(
            self.edge.edge_id,
            source_storage=self.source_storage,
            target_storage=self.target_storage,
            purpose=purpose,
        )

    def physical_variants(self) -> list[dict[str, str | None | list[Index]]]:
        return self.db_profile.edge_physical_variants(
            self.edge.edge_id,
            source_storage=self.source_storage,
            target_storage=self.target_storage,
            logical_relation=self.edge.relation,
        )


class VertexConfigDBAware:
    """DB-aware projection wrapper for `VertexConfig`."""

    def __init__(self, logical: VertexConfig, database_features: DatabaseProfile):
        self.logical = logical
        self.db_profile = database_features

    @property
    def vertex_set(self):
        return self.logical.vertex_set

    @property
    def blank_vertices(self):
        return self.logical.blank_vertices

    @property
    def vertices(self):
        return self.logical.vertices

    def vertex_dbname(self, vertex_name: str) -> str:
        return self.db_profile.vertex_storage_name(vertex_name)

    def index(self, vertex_name: str) -> Index:
        """Get primary index for a vertex (DB layer needs Index for collection setup)."""
        return Index(fields=self.identity_fields(vertex_name))

    def identity_fields(self, vertex_name: str) -> list[str]:
        identity = self.logical.identity_fields(vertex_name)
        if identity:
            return identity
        if vertex_name in self.logical.blank_vertices:
            return ["_key"] if self.db_profile.db_flavor == DBType.ARANGO else ["id"]
        return identity

    def fields(self, vertex_name: str) -> list[Field]:
        fields = self.logical.fields(vertex_name)
        if self.db_profile.db_flavor != DBType.TIGERGRAPH:
            return fields
        # TigerGraph needs explicit scalar defaults for schema definition.
        return [
            Field(name=f.name, type=FieldType.STRING if f.type is None else f.type)
            for f in fields
        ]

    def fields_names(self, vertex_name: str) -> list[str]:
        return [f.name for f in self.fields(vertex_name)]


class EdgeConfigDBAware:
    """DB-aware projection wrapper for `EdgeConfig`."""

    def __init__(
        self,
        logical: EdgeConfig,
        vertex_config: VertexConfigDBAware,
        database_features: DatabaseProfile,
    ):
        self.logical = logical
        self.vertex_config = vertex_config
        self.db_profile = database_features

    @property
    def edges(self) -> list[Edge]:
        return self.logical.edges

    def __iter__(self) -> Iterator[Edge]:
        return self.values()

    def values(self) -> Iterator[Edge]:
        return self.logical.values()

    def items(self) -> Iterator[tuple[EdgeId, Edge]]:
        return self.logical.items()

    @property
    def vertices(self):
        return self.logical.vertices

    def relation_dbname(self, edge: Edge) -> str | None:
        relation = edge.relation
        if self.db_profile.db_flavor == DBType.TIGERGRAPH and relation is None:
            relation = DEFAULT_TIGERGRAPH_RELATION
        return self.db_profile.edge_relation_name(
            edge.edge_id,
            default_relation=relation,
            logical_relation=edge.relation,
        )

    def effective_weights(self, edge: Edge) -> WeightConfig | None:
        if self.db_profile.db_flavor != DBType.TIGERGRAPH:
            return edge.weights

        relation_field = edge.relation_field
        if relation_field is None and edge.relation_from_key:
            relation_field = DEFAULT_TIGERGRAPH_RELATION_WEIGHTNAME

        if relation_field is None:
            return edge.weights

        base = (
            edge.weights.model_copy(deep=True)
            if edge.weights is not None
            else WeightConfig()
        )
        if relation_field not in base.direct_names:
            base.direct.append(Field(name=relation_field, type=FieldType.STRING))
        return base

    def runtime(self, edge: Edge) -> EdgeRuntime:
        runtime = EdgeRuntime(
            edge=edge,
            source_storage=self.vertex_config.vertex_dbname(edge.source),
            target_storage=self.vertex_config.vertex_dbname(edge.target),
            relation_name=self.relation_dbname(edge),
            store_extracted_relation_as_weight=(
                self.db_profile.db_flavor == DBType.TIGERGRAPH
            ),
            effective_relation_field=(
                edge.relation_field
                if edge.relation_field is not None
                else (
                    DEFAULT_TIGERGRAPH_RELATION_WEIGHTNAME
                    if self.db_profile.db_flavor == DBType.TIGERGRAPH
                    and edge.relation_from_key
                    else None
                )
            ),
            db_profile=self.db_profile,
        )
        return runtime

    def compile_identity_indexes(self) -> None:
        db_flavor = self.db_profile.db_flavor
        for edge in self.logical.edges:
            for identity_key in edge.identities:
                identity_fields = self._identity_key_index_fields(
                    identity_key, db_flavor
                )
                if not identity_fields:
                    continue
                self.db_profile.add_edge_index(
                    edge.edge_id,
                    Index(fields=identity_fields, unique=True),
                    logical_relation=edge.relation,
                    purpose=None,
                )

    def _identity_key_index_fields(
        self, identity_key: list[str], db_flavor: DBType
    ) -> list[str]:
        fields: list[str] = []
        for token in identity_key:
            if token == "source":
                if db_flavor == DBType.ARANGO:
                    fields.append("_from")
            elif token == "target":
                if db_flavor == DBType.ARANGO:
                    fields.append("_to")
            elif token == "relation":
                if db_flavor != DBType.TIGERGRAPH:
                    fields.append("relation")
            else:
                fields.append(token)
        deduped: list[str] = []
        for field in fields:
            if field not in deduped:
                deduped.append(field)
        return deduped


@dataclass(frozen=True)
class SchemaDBAware:
    """DB-aware schema runtime view."""

    vertex_config: VertexConfigDBAware
    edge_config: EdgeConfigDBAware
    db_profile: DatabaseProfile
