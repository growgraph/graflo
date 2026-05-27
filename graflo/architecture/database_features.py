"""Database-specific schema features.

This module stores physical DB features that are separate from logical graph identity.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from pydantic import AliasChoices, Field as PydanticField, model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.graph_types import EdgeId, EdgePhysicalKey, Index
from graflo.architecture.schema.vertex import VertexName
from graflo.onto import DBType

if TYPE_CHECKING:
    from graflo.architecture.schema.edge import EdgeConfig


class EdgeRef(ConfigBaseModel):
    """Reference to a logical edge identity."""

    source: VertexName = PydanticField(..., description="Edge source vertex name.")
    target: VertexName = PydanticField(..., description="Edge target vertex name.")
    relation: str | None = PydanticField(
        default=None,
        description="Logical relation for edge identity (source, target, relation).",
    )

    @property
    def edge_id(self) -> EdgeId:
        return (self.source, self.target, self.relation)


class EdgePhysicalSpec(EdgeRef):
    """Unified edge physical spec keyed by edge identity + purpose."""

    purpose: str | None = PydanticField(
        default=None,
        description="DB-only purpose identifier for physical edge variant.",
    )
    relation_name: str | None = PydanticField(
        default=None,
        description="Database-specific relation/type name override for the variant.",
    )
    indexes: list[Index] = PydanticField(
        default_factory=list,
        description="Secondary indexes for this variant (or overrides based on indexes_mode).",
    )
    indexes_mode: Literal["inherit", "append", "replace"] = PydanticField(
        default="inherit",
        description=(
            "How variant indexes relate to base (purpose=None): "
            "inherit=base only, append=base+variant, replace=variant only."
        ),
    )

    @property
    def physical_key(self) -> EdgePhysicalKey:
        return (self.source, self.target, self.relation, self.purpose)


class EdgePropertyDefaults(EdgeRef):
    """Per logical edge type: optional GSQL ``DEFAULT`` values for edge attributes."""

    values: dict[str, Any] = PydanticField(
        default_factory=dict,
        description="Edge attribute name to default value (YAML/JSON literals).",
        validation_alias=AliasChoices("values", "properties"),
    )


class DefaultPropertyValues(ConfigBaseModel):
    """TigerGraph-style attribute defaults for physical schema DDL (covariant profile).

    Maps to GSQL ``attribute_name type [DEFAULT default_value]`` on vertices and edges;
    see TigerGraph `Defining a Graph Schema`_.

    .. _Defining a Graph Schema: https://docs.tigergraph.com/gsql-ref/4.2/ddl-and-loading/defining-a-graph-schema
    """

    vertices: dict[str, dict[str, Any]] = PydanticField(
        default_factory=dict,
        description="Logical vertex name -> property name -> default value for GSQL DEFAULT.",
    )
    edges: list[EdgePropertyDefaults] = PydanticField(
        default_factory=list,
        description="Per (source, target, relation) edge type: attribute defaults.",
    )


class DatabaseProfile(ConfigBaseModel):
    """Container for DB-only physical features such as secondary indexes."""

    db_flavor: DBType = PydanticField(
        default=DBType.ARANGO,
        description="Target DB flavor used for physical naming and defaults.",
    )
    target_namespace: str | None = PydanticField(
        default=None,
        description=(
            "Runtime target LPG namespace when the connection config leaves it unset: "
            "Arango/Neo4j/FalkorDB/Memgraph database, TigerGraph graph name, Nebula space. "
            "GraphEngine uses this before falling back to schema.metadata.name."
        ),
    )
    vertex_storage_names: dict[VertexName, str] = PydanticField(
        default_factory=dict,
        description="Physical vertex collection/label names keyed by logical vertex name.",
    )
    vertex_indexes: dict[VertexName, list[Index]] = PydanticField(
        default_factory=dict,
        description="Secondary indexes per vertex name (identity excluded).",
    )
    edge_specs: list[EdgePhysicalSpec] = PydanticField(
        default_factory=list,
        description="Unified edge physical specs keyed by edge identity + purpose.",
    )
    default_property_values: DefaultPropertyValues | None = PydanticField(
        default=None,
        description=(
            "Optional per-attribute GSQL DEFAULT values for TigerGraph (and similar) DDL. "
            "Vertex keys are logical vertex names; edge entries match logical (source, target, relation). "
            "Does not change logical LPG types—only physical schema projection."
        ),
    )

    @model_validator(mode="after")
    def _normalize_edge_specs(self) -> "DatabaseProfile":
        def _variant_key(
            spec: EdgePhysicalSpec,
        ) -> EdgePhysicalKey:
            return spec.physical_key

        def _ensure_variant(
            merged: dict[EdgePhysicalKey, EdgePhysicalSpec],
            *,
            source: str,
            target: str,
            relation: str | None,
            purpose: str | None,
        ) -> EdgePhysicalSpec:
            key = (source, target, relation, purpose)
            if key not in merged:
                merged[key] = EdgePhysicalSpec(
                    source=source,
                    target=target,
                    relation=relation,
                    purpose=purpose,
                )
            return merged[key]

        merged: dict[EdgePhysicalKey, EdgePhysicalSpec] = {}

        for item in self.edge_specs:
            variant = _ensure_variant(
                merged,
                source=item.source,
                target=item.target,
                relation=item.relation,
                purpose=item.purpose,
            )
            if item.relation_name is not None:
                variant.relation_name = item.relation_name
            existing = {tuple(ix.fields) for ix in variant.indexes}
            for idx in item.indexes:
                if tuple(idx.fields) not in existing:
                    variant.indexes.append(idx)
            if item.indexes_mode != "inherit" or variant.indexes_mode == "inherit":
                variant.indexes_mode = item.indexes_mode

        object.__setattr__(self, "edge_specs", list(merged.values()))
        return self

    def validate_against_schema(self, edge_config: "EdgeConfig") -> None:
        """Assert all edge specs reference declared logical edges."""
        for spec in self.edge_specs:
            if spec.edge_id not in edge_config:
                raise ValueError(
                    f"EdgePhysicalSpec {spec.physical_key!r} references undeclared "
                    f"edge {spec.edge_id!r}"
                )

    def vertex_property_default(
        self, vertex_name: str, property_name: str
    ) -> Any | None:
        """Return declared default for a vertex property, or None if not specified."""
        dpv = self.default_property_values
        if dpv is None:
            return None
        per_vertex = dpv.vertices.get(vertex_name)
        if per_vertex is None:
            return None
        return per_vertex.get(property_name)

    def has_vertex_property_default(self, vertex_name: str, property_name: str) -> bool:
        dpv = self.default_property_values
        if dpv is None:
            return False
        per_vertex = dpv.vertices.get(vertex_name)
        return per_vertex is not None and property_name in per_vertex

    def edge_property_default(self, edge_id: EdgeId, property_name: str) -> Any | None:
        """Return declared default for an edge attribute, or None if not specified."""
        dpv = self.default_property_values
        if dpv is None or not dpv.edges:
            return None
        source, target, relation = edge_id
        for spec in reversed(dpv.edges):
            if spec.source != source or spec.target != target:
                continue
            if spec.relation != relation:
                continue
            if property_name not in spec.values:
                continue
            return spec.values[property_name]
        return None

    def has_edge_property_default(self, edge_id: EdgeId, property_name: str) -> bool:
        dpv = self.default_property_values
        if dpv is None or not dpv.edges:
            return False
        source, target, relation = edge_id
        for spec in reversed(dpv.edges):
            if spec.source != source or spec.target != target:
                continue
            if spec.relation != relation:
                continue
            if property_name in spec.values:
                return True
        return False

    def _edge_variant_spec(
        self,
        edge_id: EdgeId,
        purpose: str | None = None,
    ) -> EdgePhysicalSpec | None:
        for item in self.edge_specs:
            if item.edge_id != edge_id:
                continue
            if item.purpose != purpose:
                continue
            return item
        return None

    def edge_purposes(self, edge_id: EdgeId) -> list[str | None]:
        """Return declared physical purposes for an edge.

        The base variant (`None`) is always included; additional purposes are
        collected from matching edge variant specs.
        """
        purposes: list[str | None] = [None]
        seen: set[str | None] = {None}

        for item in self.edge_specs:
            if item.edge_id != edge_id:
                continue
            if item.purpose not in seen:
                seen.add(item.purpose)
                purposes.append(item.purpose)

        return purposes

    def edge_physical_variants(
        self,
        edge_id: EdgeId,
        *,
        source_storage: str,
        target_storage: str,
    ) -> list[dict[str, str | None | list[Index]]]:
        """Return resolved physical variants (base + purpose copies) for one edge."""
        variants: list[dict[str, str | None | list[Index]]] = []
        for purpose in self.edge_purposes(edge_id):
            variants.append(
                {
                    "purpose": purpose,
                    "storage_name": self.edge_storage_name(
                        edge_id,
                        source_storage=source_storage,
                        target_storage=target_storage,
                        purpose=purpose,
                    ),
                    "graph_name": self.edge_graph_name(
                        edge_id,
                        source_storage=source_storage,
                        target_storage=target_storage,
                        purpose=purpose,
                    ),
                    "indexes": self.edge_secondary_indexes(
                        edge_id,
                        purpose=purpose,
                    ),
                }
            )
        return variants

    def vertex_secondary_indexes(self, vertex_name: str) -> list[Index]:
        return list(self.vertex_indexes.get(vertex_name, []))

    def vertex_storage_name(self, vertex_name: str) -> str:
        return self.vertex_storage_names.get(vertex_name, vertex_name)

    def edge_secondary_indexes(
        self,
        edge_id: EdgeId,
        purpose: str | None = None,
    ) -> list[Index]:
        base_spec = self._edge_variant_spec(edge_id=edge_id, purpose=None)
        base_indexes = list(base_spec.indexes) if base_spec is not None else []

        if purpose is None:
            effective = base_indexes
        else:
            purpose_spec = self._edge_variant_spec(edge_id=edge_id, purpose=purpose)
            if purpose_spec is None:
                effective = base_indexes
            elif purpose_spec.indexes_mode == "replace":
                effective = list(purpose_spec.indexes)
            elif purpose_spec.indexes_mode == "append":
                effective = base_indexes + list(purpose_spec.indexes)
            else:
                effective = base_indexes

        deduped: list[Index] = []
        seen: set[tuple[str, ...]] = set()
        for idx in effective:
            key = tuple(idx.fields)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(idx)
        return deduped

    def has_edge_index_spec(
        self,
        edge_id: EdgeId,
        *,
        purpose: str | None = None,
    ) -> bool:
        """Return True when an exact purpose edge index spec exists."""
        spec = self._edge_variant_spec(edge_id=edge_id, purpose=purpose)
        return spec is not None

    def has_explicit_edge_indexes(
        self,
        edge_id: EdgeId,
        *,
        purpose: str | None = None,
    ) -> bool:
        spec = self._edge_variant_spec(edge_id=edge_id, purpose=purpose)
        return spec is not None and len(spec.indexes) > 0

    def edge_index_spec(
        self,
        edge_id: EdgeId,
        purpose: str | None = None,
    ) -> EdgePhysicalSpec | None:
        spec = self._edge_variant_spec(edge_id=edge_id, purpose=purpose)
        if spec is None and purpose is not None:
            spec = self._edge_variant_spec(edge_id=edge_id, purpose=None)
        return spec

    def add_edge_index(
        self,
        edge_id: EdgeId,
        index: Index,
        *,
        purpose: str | None = None,
    ) -> None:
        spec = self._edge_variant_spec(edge_id=edge_id, purpose=purpose)
        if spec is None:
            source, target, relation = edge_id
            spec = EdgePhysicalSpec(
                source=source,
                target=target,
                relation=relation,
                purpose=purpose,
            )
            # Auto-added indexes are additive by default.
            spec.indexes_mode = "append" if purpose is not None else "inherit"
            self.edge_specs.append(spec)
        existing = {tuple(ix.fields) for ix in spec.indexes}
        if tuple(index.fields) not in existing:
            spec.indexes.append(index)

    def edge_name_spec(
        self,
        edge_id: EdgeId,
        purpose: str | None = None,
    ) -> EdgePhysicalSpec | None:
        spec = self._edge_variant_spec(edge_id=edge_id, purpose=purpose)
        if spec is None and purpose is not None:
            spec = self._edge_variant_spec(edge_id=edge_id, purpose=None)
        return spec

    def set_edge_name_spec(
        self,
        edge_id: EdgeId,
        *,
        relation_name: str | None = None,
        purpose: str | None = None,
    ) -> None:
        spec = self._edge_variant_spec(edge_id=edge_id, purpose=purpose)
        if spec is None:
            source, target, relation = edge_id
            spec = EdgePhysicalSpec(
                source=source,
                target=target,
                relation=relation,
                purpose=purpose,
            )
            self.edge_specs.append(spec)
        if relation_name is not None:
            spec.relation_name = relation_name
        if purpose is not None:
            spec.purpose = purpose

    def edge_relation_name(
        self,
        edge_id: EdgeId,
        default_relation: str | None = None,
        purpose: str | None = None,
    ) -> str | None:
        spec = self.edge_name_spec(edge_id, purpose=purpose)
        if spec is not None and spec.relation_name is not None:
            return spec.relation_name
        return default_relation

    def edge_storage_name(
        self,
        edge_id: EdgeId,
        *,
        source_storage: str,
        target_storage: str,
        purpose: str | None = None,
    ) -> str | None:
        spec = self._edge_variant_spec(edge_id, purpose=purpose)
        if self.db_flavor != DBType.ARANGO:
            return None
        tokens = [source_storage, target_storage]
        purpose = spec.purpose if spec is not None else purpose
        if purpose is not None:
            tokens.append(purpose)
        return "_".join(tokens + ["edges"])

    def edge_graph_name(
        self,
        edge_id: EdgeId,
        *,
        source_storage: str,
        target_storage: str,
        purpose: str | None = None,
    ) -> str | None:
        return self.edge_storage_name(
            edge_id,
            source_storage=source_storage,
            target_storage=target_storage,
            purpose=purpose,
        )
