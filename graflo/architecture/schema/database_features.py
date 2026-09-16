"""Database-specific schema features.

This module stores physical DB features that are separate from logical graph identity.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from pydantic import AliasChoices, model_validator
from pydantic import Field as PydanticField

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


def index_identity(index: Index) -> tuple:
    """Everything about an index except its cosmetic name.

    Indexes are addressed by field list (``RemoveVertexIndexesOp``), so two
    entries differing only by name are one index; two differing on uniqueness,
    type or sparsity are two contradictory claims about one field-set.
    """
    return (
        tuple(index.fields),
        bool(index.unique),
        str(index.type),
        bool(index.deduplicate),
        bool(index.sparse),
        bool(index.exclude_edge_endpoints),
    )


def append_index(indexes: list[Index], index: Index, *, owner: str) -> None:
    """Register *index* unless already present, refusing a contradictory restatement.

    Idempotent on the full definition so repeated schema resolution does not
    accumulate duplicates. Deduplicating on the field-set alone -- as this did
    -- silently kept whichever ``unique`` arrived first for a field-set indexed
    two ways, and which one that was depended on resolution order.
    """
    for existing in indexes:
        if index_identity(existing) == index_identity(index):
            return
        if tuple(existing.fields) == tuple(index.fields):
            raise ValueError(
                f"Conflicting index on {owner} fields {list(index.fields)!r}: "
                f"unique={existing.unique}/type={existing.type}/sparse={existing.sparse} "
                f"vs unique={index.unique}/type={index.type}/sparse={index.sparse}"
            )
    indexes.append(index)


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
            "Validated against the flavor, never rewritten. When unset, the namespace "
            "is schema.metadata.name sanitized per flavor (Schema.effective_namespace)."
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
    native_inverses: list[str] = PydanticField(
        default_factory=list,
        description=(
            "TigerGraph only: relations whose declared inverse the database "
            "maintains as a paired type (GSQL ``WITH REVERSE_EDGE``). Keyed by "
            "relation because TigerGraph sets the reverse type on the edge type, "
            "which spans every (source, target) pair of the relation. The paired "
            "type is named by ``edge_config.inverses``, never here. Mutually "
            "exclusive with explicit inverse edges."
        ),
    )

    @model_validator(mode="after")
    def _normalize_native_inverses(self) -> DatabaseProfile:
        # A set of relation names: order and repetition carry no meaning.
        object.__setattr__(self, "native_inverses", sorted(set(self.native_inverses)))
        return self

    @model_validator(mode="after")
    def _normalize_edge_specs(self) -> DatabaseProfile:
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
                # Refused rather than last-wins: the physical name is what DDL
                # emits, and two descriptions of one physical edge cannot both
                # be it.
                if (
                    variant.relation_name is not None
                    and variant.relation_name != item.relation_name
                ):
                    raise ValueError(
                        f"Conflicting relation_name for edge spec {variant.physical_key!r}: "
                        f"{variant.relation_name!r} vs {item.relation_name!r}"
                    )
                variant.relation_name = item.relation_name
            for idx in item.indexes:
                append_index(
                    variant.indexes, idx, owner=f"edge spec {variant.physical_key!r}"
                )
            if item.indexes_mode != "inherit" or variant.indexes_mode == "inherit":
                variant.indexes_mode = item.indexes_mode

        object.__setattr__(self, "edge_specs", list(merged.values()))
        return self

    def validate_against_schema(self, edge_config: EdgeConfig) -> None:
        """Assert all edge specs reference declared logical edges."""
        for spec in self.edge_specs:
            if spec.edge_id not in edge_config:
                raise ValueError(
                    f"EdgePhysicalSpec {spec.physical_key!r} references undeclared "
                    f"edge {spec.edge_id!r}"
                )

    def validate_native_inverses(
        self, edge_config: EdgeConfig, vertex_names: set[str]
    ) -> None:
        """Check every native inverse against the declared inverses and edges.

        A native inverse is the database realizing a *declared* pair for a whole
        relation, so it needs the declaration, must not coexist with explicit
        inverse edges, and exists only where the database can maintain one: on a
        single TigerGraph edge type whose reverse name is free.

        Raises:
            ValueError: naming the rule broken and the relation that breaks it.
        """
        if not self.native_inverses:
            return
        errors: list[str] = []
        if self.db_flavor != DBType.TIGERGRAPH:
            errors.append(
                f"native_inverses {self.native_inverses} are TigerGraph-only "
                f"(db_flavor is {str(self.db_flavor)!r}); use explicit inverse edges "
                "(add_inverse_edges) for a portable inverse"
            )
        edges_by_relation: dict[str, list[EdgeId]] = {}
        for edge in edge_config.edges:
            if edge.relation is not None:
                edges_by_relation.setdefault(edge.relation, []).append(edge.edge_id)
        native = set(self.native_inverses)
        for relation in self.native_inverses:
            edge_ids = edges_by_relation.get(relation)
            if not edge_ids:
                errors.append(f"{relation!r}: names no declared edge")
                continue
            if edge_config.is_symmetric(relation):
                errors.append(
                    f"{relation!r}: a symmetric relation has no reverse type; its "
                    "edges are undirected"
                )
                continue
            inverse = edge_config.inverse_of(relation)
            if inverse is None:
                errors.append(
                    f"{relation!r}: a native inverse needs a declared pair; add "
                    f"{{relation: {relation}, inverse: <name>}} to edge_config.inverses"
                )
                continue
            if inverse in native and relation < inverse:
                errors.append(
                    f"{relation!r} and {inverse!r}: only one side of a pair can be "
                    "native; the other is the reverse type the database creates"
                )
            # TigerGraph edge type names share one namespace with each other and
            # with vertex types, so the reverse type must not shadow either --
            # including an explicit inverse edge, which would store the fact twice.
            if inverse in edges_by_relation:
                errors.append(
                    f"{relation!r}: native inverse {inverse!r} collides with the "
                    "declared edges of that name; keep one realization of the pair"
                )
            elif inverse in vertex_names:
                errors.append(
                    f"{relation!r}: native inverse {inverse!r} collides with a "
                    "vertex type"
                )
            # WITH REVERSE_EDGE belongs to one edge type; a relation spread over
            # several physical names would ask for one reverse name twice.
            physical = sorted(
                {
                    self.edge_relation_name(edge_id, default_relation=relation)
                    or relation
                    for edge_id in edge_ids
                }
            )
            if len(physical) > 1:
                errors.append(
                    f"{relation!r}: a native inverse needs the relation stored as "
                    f"one edge type, but relation_name splits it into {physical}"
                )
        if errors:
            raise ValueError("invalid native inverse: " + "; ".join(errors))

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

    def add_vertex_index(self, vertex_name: VertexName, index: Index) -> None:
        """Register a secondary index for *vertex_name* if not already present.

        Idempotent on the **field-set**, so repeated schema resolution does not
        accumulate duplicates -- deliberately weaker than :func:`append_index`.
        The caller here is a generator (`compile_secondary_identity_indexes`
        derives a lookup index from a declared secondary identity), so an
        authored unique index over the same fields is not a competing claim to
        refuse but the stronger one to leave alone. Two *authored* profiles
        meeting is the case that refuses, and that goes through
        :func:`append_index`.
        """
        indexes = self.vertex_indexes.setdefault(vertex_name, [])
        if tuple(index.fields) not in {tuple(ix.fields) for ix in indexes}:
            indexes.append(index)

    def prune_empty_vertex_indexes(self) -> None:
        """Drop vertex entries whose index list is empty.

        An empty list and an absent key describe the same profile, but they
        serialize — and therefore hash — differently. Since content hashes are
        what verify a replayed manifest against an authored one, leaving the
        empty entry behind would make identical schemas compare unequal.
        """
        for vertex_name in [
            name for name, indexes in self.vertex_indexes.items() if not indexes
        ]:
            del self.vertex_indexes[vertex_name]

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

    def has_native_inverse(self, relation: str | None) -> bool:
        """Whether the database maintains the declared inverse of ``relation``."""
        return relation is not None and relation in self.native_inverses

    def native_inverse_of(
        self, relation: str | None, edge_config: EdgeConfig
    ) -> str | None:
        """Name of the database-maintained reverse type of ``relation``, if any.

        The name is the declared inverse (``edge_config.inverses``); the profile
        only says that the database, rather than explicit edges, realizes it.
        """
        if self.db_flavor != DBType.TIGERGRAPH or not self.has_native_inverse(relation):
            return None
        return edge_config.inverse_of(relation)

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
