"""DB-aware projections for logical graph configs.

These wrappers materialize database-specific naming/defaults from logical
`VertexConfig`/`EdgeConfig` and `DPProfile` without mutating logical models.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator, Protocol, runtime_checkable, Any

from pydantic import Field as PydanticField, field_validator

from graflo.architecture.database_features import DatabaseProfile
from graflo.architecture.graph_types import EdgeId, Index, Weight
from graflo.onto import DBType

from .edge import (
    DEFAULT_TIGERGRAPH_RELATION,
    DEFAULT_TIGERGRAPH_RELATION_WEIGHTNAME,
    Edge,
    EdgeConfig,
    _normalize_direct_item,
)
from .vertex import Field, FieldType, VertexConfig
from ..base import ConfigBaseModel


@runtime_checkable
class EdgeIngestionOverlay(Protocol):
    """Ingestion-only signals that affect DB projection (e.g. TigerGraph DDL)."""

    def uses_relation_from_key(self, edge_id: EdgeId) -> bool: ...


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

    def properties(self, vertex_name: str) -> list[Field]:
        props = self.logical.properties(vertex_name)
        if self.db_profile.db_flavor != DBType.TIGERGRAPH:
            return props
        # TigerGraph needs explicit scalar defaults for schema definition.
        return [
            Field(name=f.name, type=FieldType.STRING if f.type is None else f.type)
            for f in props
        ]

    def property_names(self, vertex_name: str) -> list[str]:
        return [f.name for f in self.properties(vertex_name)]


class WeightConfig(ConfigBaseModel):
    """Configuration for edge weights and relationships.

    This class manages the configuration of weights and relationships for edges,
    including source and target field mappings.

    Attributes:
        vertices: List of weight configurations
        direct: List of direct field mappings. Can be specified as strings, Field objects, or dicts.
               Will be normalized to Field objects by the validator.
               After initialization, this is always list[Field] (type checker sees this).

    Examples:
        >>> # List of strings
        >>> wc1 = WeightConfig(direct=["date", "weight"])

        >>> # Typed fields: list of Field objects
        >>> wc2 = WeightConfig(direct=[
        ...     Field(name="date", type="DATETIME"),
        ...     Field(name="weight", type="FLOAT")
        ... ])

        >>> # From dicts (e.g., from YAML/JSON)
        >>> wc3 = WeightConfig(direct=[
        ...     {"name": "date", "type": "DATETIME"},
        ...     {"name": "weight"}  # defaults to None type
        ... ])
    """

    vertices: list[Weight] = PydanticField(
        default_factory=list,
        description="List of weight definitions for vertex-based edge attributes.",
    )
    direct: list[Field] = PydanticField(
        default_factory=list,
        description="Direct edge attributes (field names, Field objects, or dicts). Normalized to Field objects.",
    )

    @field_validator("direct", mode="before")
    @classmethod
    def normalize_direct(cls, v: Any) -> Any:
        if not isinstance(v, list):
            return v
        return [_normalize_direct_item(item) for item in v]

    @property
    def direct_names(self) -> list[str]:
        """Get list of direct field names (as strings).

        Returns:
            list[str]: List of field names
        """
        return [field.name for field in self.direct]


class EdgeConfigDBAware:
    """DB-aware projection wrapper for `EdgeConfig`."""

    def __init__(
        self,
        logical: EdgeConfig,
        vertex_config: VertexConfigDBAware,
        database_features: DatabaseProfile,
        ingestion_overlay: EdgeIngestionOverlay | None = None,
    ):
        self.logical = logical
        self.vertex_config = vertex_config
        self.db_profile = database_features
        self.ingestion_overlay = ingestion_overlay

    def _uses_relation_from_key(self, edge_id: EdgeId) -> bool:
        if self.ingestion_overlay is not None:
            return self.ingestion_overlay.uses_relation_from_key(edge_id)
        return False

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
        )

    def effective_weights(self, edge: Edge) -> WeightConfig | None:
        def _as_weight_config() -> WeightConfig | None:
            if not edge.properties:
                return None
            return WeightConfig(
                direct=[f.model_copy(deep=True) for f in edge.properties],
            )

        if self.db_profile.db_flavor != DBType.TIGERGRAPH:
            return _as_weight_config()

        # Typed TigerGraph edge: per-row relation label stored under a stable attribute.
        needs_relation_attr = edge.relation is None or self._uses_relation_from_key(
            edge.edge_id
        )
        if not needs_relation_attr:
            return _as_weight_config()

        base = _as_weight_config() or WeightConfig()
        if DEFAULT_TIGERGRAPH_RELATION_WEIGHTNAME not in base.direct_names:
            base.direct.append(
                Field(
                    name=DEFAULT_TIGERGRAPH_RELATION_WEIGHTNAME, type=FieldType.STRING
                )
            )
        return base

    def runtime(self, edge: Edge) -> EdgeRuntime:
        needs_tg_relation_attr = self.db_profile.db_flavor == DBType.TIGERGRAPH and (
            edge.relation is None or self._uses_relation_from_key(edge.edge_id)
        )
        runtime = EdgeRuntime(
            edge=edge,
            source_storage=self.vertex_config.vertex_dbname(edge.source),
            target_storage=self.vertex_config.vertex_dbname(edge.target),
            relation_name=self.relation_dbname(edge),
            store_extracted_relation_as_weight=needs_tg_relation_attr,
            effective_relation_field=(
                DEFAULT_TIGERGRAPH_RELATION_WEIGHTNAME
                if needs_tg_relation_attr
                else None
            ),
            db_profile=self.db_profile,
        )
        return runtime

    def relationship_merge_property_names(self, edge: Edge) -> list[str]:
        """Relationship properties used for edge upsert/MERGE keys (per backend).

        Uniqueness is ``(source_id, *identity_fields, target_id)`` for the **first**
        logical ``identities`` key (endpoints are matched separately on vertices).
        Additional ``identities`` keys are compiled into separate unique indexes
        via :meth:`compile_identity_indexes` but do not change the writer merge key.

        If that key yields no relationship fields, or ``identities`` is empty,
        falls back to all declared edge attribute names.
        """
        db_flavor = self.db_profile.db_flavor
        if edge.identities:
            props = self._identity_tokens_to_relationship_properties(
                edge.identities[0], db_flavor
            )
            if props:
                return props
        if edge.property_names:
            return list(edge.property_names)
        return []

    @staticmethod
    def _identity_tokens_to_relationship_properties(
        identity_key: list[str], db_flavor: DBType
    ) -> list[str]:
        fields: list[str] = []
        for token in identity_key:
            if token in ("source", "target"):
                continue
            if token == "relation":
                if db_flavor != DBType.TIGERGRAPH:
                    fields.append("relation")
                continue
            fields.append(token)
        deduped: list[str] = []
        for field in fields:
            if field not in deduped:
                deduped.append(field)
        return deduped

    def compile_identity_indexes(self) -> None:
        db_flavor = self.db_profile.db_flavor
        for edge in self.logical.edges:
            for identity_key in edge.identities:
                identity_fields = self._identity_key_index_fields(
                    identity_key, db_flavor
                )
                if not identity_fields:
                    continue
                fields, unique = self._normalize_edge_identity_index(
                    identity_fields, db_flavor
                )
                if not fields:
                    continue
                self.db_profile.add_edge_index(
                    edge.edge_id,
                    Index(fields=fields, unique=unique),
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

    @staticmethod
    def _normalize_edge_identity_index(
        fields: list[str], db_flavor: DBType
    ) -> tuple[list[str], bool]:
        """Map logical edge identity to physical index fields and DB uniqueness.

        Logical uniqueness is always ``(source, *relationship_fields, target)``.

        * **ArangoDB** — Edge documents carry ``_from`` / ``_to``. Unique persistent
          indexes must include them before other fields, even when the YAML
          ``identities`` entry lists only relationship tokens (e.g. ``_role``).
        * **Neo4j, FalkorDB, Memgraph, Nebula** — Indexed columns are relationship /
          edge-type properties only; they cannot express endpoint scope. We still
          register the property fields for lookups but set ``unique=False`` so the
          database is not asked to enforce a misleading global uniqueness on those
          properties alone. (Application MERGE / ingest semantics remain authoritative.)
        * **TigerGraph** — Edge secondary indexes are not applied by the driver today;
          fields are kept for profiling; uniqueness is preserved for consistency.
        """
        rest = [f for f in fields if f not in ("_from", "_to")]
        if db_flavor == DBType.ARANGO:
            return (["_from", "_to", *rest], True)
        if db_flavor in (
            DBType.NEO4J,
            DBType.FALKORDB,
            DBType.MEMGRAPH,
            DBType.NEBULA,
        ):
            return (fields, False)
        return (fields, True)


@dataclass(frozen=True)
class SchemaDBAware:
    """DB-aware schema runtime view."""

    vertex_config: VertexConfigDBAware
    edge_config: EdgeConfigDBAware
    db_profile: DatabaseProfile
