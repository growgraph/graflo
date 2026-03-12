"""Database-specific schema features.

This module stores physical DB features that are separate from logical graph identity.
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field as PydanticField
from pydantic import model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.onto import EdgeId, Index
from graflo.onto import DBType


class EdgeIndexSpec(ConfigBaseModel):
    """Secondary indexes for one edge definition."""

    source: str = PydanticField(..., description="Edge source vertex name.")
    target: str = PydanticField(..., description="Edge target vertex name.")
    relation: str | None = PydanticField(
        default=None,
        description="Logical relation for edge identity (source, target, relation).",
    )
    purpose: str | None = PydanticField(
        default=None,
        description="DB-only purpose identifier for auxiliary physical storage naming.",
    )
    logical_relation: str | None = PydanticField(
        default=None,
        description="Legacy relation discriminator; merged into relation when omitted.",
    )
    indexes: list[Index] = PydanticField(
        default_factory=list, description="Physical indexes for this edge."
    )

    @model_validator(mode="after")
    def _normalize_relation(self) -> "EdgeIndexSpec":
        if self.relation is None and self.logical_relation is not None:
            self.relation = self.logical_relation
        return self

    @property
    def edge_id(self) -> EdgeId:
        return (self.source, self.target, self.relation)


class EdgeNameSpec(ConfigBaseModel):
    """Physical naming overrides for one edge definition."""

    source: str = PydanticField(..., description="Edge source vertex name.")
    target: str = PydanticField(..., description="Edge target vertex name.")
    relation: str | None = PydanticField(
        default=None,
        description="Logical relation for edge identity (source, target, relation).",
    )
    purpose: str | None = PydanticField(
        default=None,
        description="DB-only purpose identifier for auxiliary physical storage naming.",
    )
    logical_relation: str | None = PydanticField(
        default=None,
        description="Legacy relation discriminator; merged into relation when omitted.",
    )
    relation_name: str | None = PydanticField(
        default=None,
        description="Database-specific relation/type name for the edge.",
    )

    @model_validator(mode="after")
    def _normalize_relation(self) -> "EdgeNameSpec":
        if self.relation is None and self.logical_relation is not None:
            self.relation = self.logical_relation
        return self

    @property
    def edge_id(self) -> EdgeId:
        return (self.source, self.target, self.relation)


class EdgeVariantSpec(ConfigBaseModel):
    """Unified edge physical variant spec keyed by edge identity + purpose."""

    source: str = PydanticField(..., description="Edge source vertex name.")
    target: str = PydanticField(..., description="Edge target vertex name.")
    relation: str | None = PydanticField(
        default=None,
        description="Logical relation for edge identity (source, target, relation).",
    )
    purpose: str | None = PydanticField(
        default=None,
        description="DB-only purpose identifier for physical edge variant.",
    )
    logical_relation: str | None = PydanticField(
        default=None,
        description="Legacy relation discriminator; merged into relation when omitted.",
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

    @model_validator(mode="after")
    def _normalize_relation(self) -> "EdgeVariantSpec":
        if self.relation is None and self.logical_relation is not None:
            self.relation = self.logical_relation
        return self

    @property
    def edge_id(self) -> EdgeId:
        return (self.source, self.target, self.relation)


class DatabaseProfile(ConfigBaseModel):
    """Container for DB-only physical features such as secondary indexes."""

    db_flavor: DBType = PydanticField(
        default=DBType.ARANGO,
        description="Target DB flavor used for physical naming and defaults.",
    )
    vertex_storage_names: dict[str, str] = PydanticField(
        default_factory=dict,
        description="Physical vertex collection/label names keyed by logical vertex name.",
    )
    vertex_indexes: dict[str, list[Index]] = PydanticField(
        default_factory=dict,
        description="Secondary indexes per vertex name (identity excluded).",
    )
    edge_indexes: list[EdgeIndexSpec] = PydanticField(
        default_factory=list,
        description="Deprecated: legacy secondary indexes per edge identity.",
    )
    edge_names: list[EdgeNameSpec] = PydanticField(
        default_factory=list,
        description="Deprecated: legacy edge naming overrides keyed by edge identity.",
    )
    edge_specs: list[EdgeVariantSpec] = PydanticField(
        default_factory=list,
        description="Unified edge physical specs keyed by edge identity + purpose.",
    )

    @model_validator(mode="after")
    def _migrate_legacy_edge_specs(self) -> "DatabaseProfile":
        def _variant_key(
            spec: EdgeVariantSpec,
        ) -> tuple[str, str, str | None, str | None, str | None]:
            return (
                spec.source,
                spec.target,
                spec.relation,
                spec.purpose,
                spec.logical_relation,
            )

        def _ensure_variant(
            merged: dict[
                tuple[str, str, str | None, str | None, str | None], EdgeVariantSpec
            ],
            *,
            source: str,
            target: str,
            relation: str | None,
            purpose: str | None,
            logical_relation: str | None,
        ) -> EdgeVariantSpec:
            key = (source, target, relation, purpose, logical_relation)
            if key not in merged:
                merged[key] = EdgeVariantSpec(
                    source=source,
                    target=target,
                    relation=relation,
                    purpose=purpose,
                    logical_relation=logical_relation,
                )
            return merged[key]

        merged: dict[
            tuple[str, str, str | None, str | None, str | None], EdgeVariantSpec
        ] = {}

        for item in self.edge_specs:
            variant = _ensure_variant(
                merged,
                source=item.source,
                target=item.target,
                relation=item.relation,
                purpose=item.purpose,
                logical_relation=item.logical_relation,
            )
            if item.relation_name is not None:
                variant.relation_name = item.relation_name
            existing = {tuple(ix.fields) for ix in variant.indexes}
            for idx in item.indexes:
                if tuple(idx.fields) not in existing:
                    variant.indexes.append(idx)
            if item.indexes_mode != "inherit" or variant.indexes_mode == "inherit":
                variant.indexes_mode = item.indexes_mode

        for item in self.edge_names:
            variant = _ensure_variant(
                merged,
                source=item.source,
                target=item.target,
                relation=item.relation,
                purpose=item.purpose,
                logical_relation=item.logical_relation,
            )
            if item.relation_name is not None:
                variant.relation_name = item.relation_name

        for item in self.edge_indexes:
            variant = _ensure_variant(
                merged,
                source=item.source,
                target=item.target,
                relation=item.relation,
                purpose=item.purpose,
                logical_relation=item.logical_relation,
            )
            existing = {tuple(ix.fields) for ix in variant.indexes}
            for idx in item.indexes:
                if tuple(idx.fields) not in existing:
                    variant.indexes.append(idx)
            # Legacy purpose-specific edge_indexes behaved as override when non-empty.
            if item.purpose is not None:
                variant.indexes_mode = "replace" if len(item.indexes) > 0 else "inherit"

        object.__setattr__(self, "edge_specs", list(merged.values()))
        return self

    def _edge_variant_spec(
        self,
        edge_id: EdgeId,
        logical_relation: str | None = None,
        purpose: str | None = None,
    ) -> EdgeVariantSpec | None:
        fallback: EdgeVariantSpec | None = None
        for item in self.edge_specs:
            if item.edge_id != edge_id:
                continue
            if item.purpose != purpose:
                continue
            if item.logical_relation == logical_relation:
                return item
            if item.logical_relation is None:
                fallback = item
        return fallback

    def edge_purposes(
        self, edge_id: EdgeId, logical_relation: str | None = None
    ) -> list[str | None]:
        """Return declared physical purposes for an edge.

        The base variant (`None`) is always included; additional purposes are
        collected from matching edge naming/index specs.
        """
        purposes: list[str | None] = [None]
        seen: set[str | None] = {None}

        def _matches(item_edge_id: EdgeId, item_logical_relation: str | None) -> bool:
            if item_edge_id != edge_id:
                return False
            if logical_relation is None:
                return item_logical_relation is None
            return item_logical_relation in (None, logical_relation)

        for item in self.edge_specs:
            if not _matches(item.edge_id, item.logical_relation):
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
        logical_relation: str | None = None,
    ) -> list[dict[str, str | None | list[Index]]]:
        """Return resolved physical variants (base + purpose copies) for one edge."""
        variants: list[dict[str, str | None | list[Index]]] = []
        for purpose in self.edge_purposes(edge_id, logical_relation=logical_relation):
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
                        logical_relation=logical_relation,
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
        logical_relation: str | None = None,
        purpose: str | None = None,
    ) -> list[Index]:
        base_spec = self._edge_variant_spec(
            edge_id=edge_id, logical_relation=logical_relation, purpose=None
        )
        base_indexes = list(base_spec.indexes) if base_spec is not None else []

        if purpose is None:
            effective = base_indexes
        else:
            purpose_spec = self._edge_variant_spec(
                edge_id=edge_id, logical_relation=logical_relation, purpose=purpose
            )
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
        logical_relation: str | None = None,
        purpose: str | None = None,
    ) -> bool:
        """Return True when an exact purpose/logical_relation edge index spec exists."""
        spec = self._edge_variant_spec(
            edge_id=edge_id, logical_relation=logical_relation, purpose=purpose
        )
        return spec is not None

    def has_explicit_edge_indexes(
        self,
        edge_id: EdgeId,
        *,
        logical_relation: str | None = None,
        purpose: str | None = None,
    ) -> bool:
        spec = self._edge_variant_spec(
            edge_id=edge_id, logical_relation=logical_relation, purpose=purpose
        )
        return spec is not None and len(spec.indexes) > 0

    def edge_index_spec(
        self,
        edge_id: EdgeId,
        logical_relation: str | None = None,
        purpose: str | None = None,
    ) -> EdgeVariantSpec | None:
        spec = self._edge_variant_spec(
            edge_id=edge_id, logical_relation=logical_relation, purpose=purpose
        )
        if spec is None and purpose is not None:
            spec = self._edge_variant_spec(
                edge_id=edge_id, logical_relation=logical_relation, purpose=None
            )
        return spec

    def add_edge_index(
        self,
        edge_id: EdgeId,
        index: Index,
        *,
        logical_relation: str | None = None,
        purpose: str | None = None,
    ) -> None:
        spec = self._edge_variant_spec(
            edge_id=edge_id, logical_relation=logical_relation, purpose=purpose
        )
        if spec is None:
            source, target, relation = edge_id
            spec = EdgeVariantSpec(
                source=source,
                target=target,
                relation=relation,
                logical_relation=logical_relation,
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
        logical_relation: str | None = None,
        purpose: str | None = None,
    ) -> EdgeVariantSpec | None:
        spec = self._edge_variant_spec(
            edge_id=edge_id, logical_relation=logical_relation, purpose=purpose
        )
        if spec is None and purpose is not None:
            spec = self._edge_variant_spec(
                edge_id=edge_id, logical_relation=logical_relation, purpose=None
            )
        return spec

    def set_edge_name_spec(
        self,
        edge_id: EdgeId,
        *,
        logical_relation: str | None = None,
        relation_name: str | None = None,
        purpose: str | None = None,
    ) -> None:
        spec = self._edge_variant_spec(
            edge_id=edge_id, logical_relation=logical_relation, purpose=purpose
        )
        if spec is None:
            source, target, relation = edge_id
            spec = EdgeVariantSpec(
                source=source,
                target=target,
                relation=relation,
                logical_relation=logical_relation,
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
        logical_relation: str | None = None,
        purpose: str | None = None,
    ) -> str | None:
        spec = self.edge_name_spec(edge_id, logical_relation, purpose=purpose)
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
