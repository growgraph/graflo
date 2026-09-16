"""Edge configuration and management for graph databases.

This module provides classes and utilities for managing edges in graph databases.
It handles edge configuration, weight management, indexing, and relationship operations.
The module supports both ArangoDB and Neo4j through the DBType enum.

Key Components:
    - Edge: Abstract graph edge kind (schema / ``edge_config`` only)
    - EdgeDerivation: Ingestion wiring (see ``graflo.architecture.graph_types.edge_derivation``)
    - EdgeConfig: Manages collections of edges and their configurations
    - WeightConfig: DTO for DB projection helpers (e.g. effective weights); schema uses ``properties``

Direction semantics:
    ``Edge.directed`` is a statement about the *model*, not about storage: when
    false, endpoint order carries no meaning and the two orientations denote one
    relationship. Backends express that to very different degrees, and only
    TigerGraph has an undirected edge type. What the flag costs elsewhere is a
    read-path question — reaching an edge from its target endpoint:

    =============  ==============  =============================================
    Backend        Native          Reverse traversal
    =============  ==============  =============================================
    TigerGraph     yes             schema-time only (``WITH REVERSE_EDGE``)
    Arango         no              free — both endpoints indexed
    Neo4j          no              cheap — relationships are doubly linked
    Memgraph       no              cheap
    FalkorDB       no              cheap — matrices stored with transposes
    Nebula         no              cheap, but needs an explicit reverse clause
    PostgreSQL     no              needs an index on the target column
    graflo backend no              direction is the storage partition key
    =============  ==============  =============================================

    The authoritative table lives in :mod:`graflo.db.edge_direction_support`,
    which also reports per-schema diagnostics at schema-apply time.

Example:
    >>> edge = Edge(source="user", target="post")
    >>> config = EdgeConfig(edges=[edge])
    >>> edge.finish_init(vertex_config=vertex_config)
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Iterator
from typing import Any, cast

from pydantic import (
    Field as PydanticField,
)
from pydantic import (
    PrivateAttr,
    field_validator,
    model_validator,
)

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.graph_types import (
    EdgeId,
    EdgeType,
)
from graflo.architecture.schema.semantics import Semantics
from graflo.architecture.schema.vertex import (
    Field,
    VertexConfig,
    VertexName,
    union_field_lists,
)

# Default relation name for TigerGraph edges when relation is not specified
DEFAULT_TIGERGRAPH_RELATION = "relates"

# Default field name for storing extracted relations in TigerGraph weights
DEFAULT_TIGERGRAPH_RELATION_WEIGHTNAME = "relation"


def _normalize_direct_item(item: str | Field | dict[str, Any]) -> Field:
    """Convert a single direct field item (str, Field, or dict) to Field.

    A dict is handed to the model whole rather than copied key by key: an
    enumerated constructor silently drops every field it was not written for
    (``item_type`` and ``semantics`` were lost that way), and ``extra="forbid"``
    only protects what actually reaches validation.
    """
    if isinstance(item, Field):
        return item
    if isinstance(item, str):
        return Field(name=item, type=None)
    if isinstance(item, dict):
        if item.get("name") is None:
            raise ValueError(f"Field dict must have 'name' key: {item}")
        return Field.model_validate(item)
    raise TypeError(f"Field must be str, Field, or dict, got {type(item)}")


class Edge(ConfigBaseModel):
    """Abstract graph edge kind (schema / ``edge_config`` only).

    Ingestion-only behavior (location filters, relation column, relation from
    key, etc.) belongs on :class:`~graflo.architecture.graph_types.edge_derivation.EdgeDerivation`
    in pipeline edge steps, not on this model.

    .. note::
       ``identities`` keys are endpoint-order-sensitive even when ``directed``
       is false: the ``source`` and ``target`` tokens resolve positionally, so
       ``(a, b)`` and ``(b, a)`` count as two identities on an edge whose whole
       premise is that they are one. Canonical endpoint ordering for undirected
       identity keys is not implemented — declare such edges from a consistent
       side until it is.
    """

    source: VertexName = PydanticField(
        ...,
        description="Source vertex type name (e.g. user, company).",
    )
    target: VertexName = PydanticField(
        ...,
        description="Target vertex type name (e.g. post, company).",
    )
    relation: str | None = PydanticField(
        default=None,
        description="Relation/edge type name (e.g. Neo4j relationship type). For ArangoDB used as weight.",
    )
    description: str | None = PydanticField(
        default=None,
        description="Optional semantic description of edge intent, direction semantics, and business meaning.",
    )
    semantics: Semantics | None = PydanticField(
        default=None,
        description="Optional external-vocabulary anchors for this edge type.",
    )
    directed: bool = PydanticField(
        default=True,
        description=(
            "When True (default), source→target order is meaningful. When False, the "
            "edge is logically undirected: the two orientations denote one "
            "relationship, so inverse-edge ops must not duplicate it and traversal "
            "may follow it either way. This asserts something about the model, not "
            "about storage — only TigerGraph has an undirected edge type, and every "
            "other backend honours it to the extent it can (see "
            "``graflo.db.edge_direction_support``). An undirected edge has no "
            "declared inverse, and so no native inverse either."
        ),
    )

    identities: list[list[str]] = PydanticField(
        default_factory=list,
        description=(
            "Logical uniqueness keys for this edge: each key names fields that, "
            "together with the resolved source and target vertex ids, must be unique "
            "(``source`` / ``target`` tokens stand for endpoints; other tokens are edge "
            "attributes). Multiple keys define multiple uniqueness constraints. "
            "Non-endpoint tokens are merged into ``properties`` during "
            ":meth:`finish_init` if not already declared (same idea as vertex identity)."
        ),
    )
    properties: list[Field] = PydanticField(
        default_factory=list,
        description=(
            "Edge property names/types (relationship properties). "
            "Vertex-derived bindings belong in ingestion (:class:`~graflo.architecture.contract."
            "runtime.edge_derivation.EdgeDerivationRegistry`)."
        ),
    )

    type: EdgeType = PydanticField(
        default=EdgeType.DIRECT,
        description="Edge type: DIRECT (created during ingestion) or INDIRECT (pre-existing collection).",
    )

    by: str | None = PydanticField(
        default=None,
        description="For INDIRECT edges: vertex type name used to define the edge.",
    )

    @field_validator("properties", mode="before")
    @classmethod
    def normalize_properties(cls, v: Any) -> Any:
        if not isinstance(v, list):
            return v
        return [_normalize_direct_item(item) for item in v]

    @field_validator("identities", mode="before")
    @classmethod
    def normalize_identities(cls, v: Any) -> Any:
        if v is None:
            return []
        if isinstance(v, list):
            # identities can be provided as [["source", "target"], ["source", "target", "pub_id"]]
            if all(isinstance(item, str) for item in v):
                return [list(v)]
            normalized: list[list[str]] = []
            for item in v:
                if isinstance(item, tuple):
                    item = list(item)
                if not isinstance(item, list) or not all(
                    isinstance(token, str) for token in item
                ):
                    raise ValueError("edge identities must be list[list[str]]")
                normalized.append(cast(list[str], item))
            return normalized
        raise ValueError("edge identities must be list[list[str]]")

    @model_validator(mode="after")
    def fold_duplicate_properties(self) -> Edge:
        """Fold properties declared twice, refusing an incompatible redeclaration.

        ``_normalize_direct_item`` maps each authored entry independently, so
        ``properties: ["tags", {name: tags, type: LIST, item_type: STRING}]``
        yields two fields of one name and nothing downstream collapses them --
        they reach DDL emission as two attributes. Vertices have folded
        duplicates since they gained typed properties; edges are brought to the
        same rule here, with the same merge and the same refusals.
        """
        object.__setattr__(
            self,
            "properties",
            union_field_lists(
                self.properties,
                owner=f"edge ({self.source!r}, {self.target!r}, {self.relation!r})",
            ),
        )
        return self

    @model_validator(mode="after")
    def normalize_identity_keys(self) -> Edge:
        deduped_keys: list[list[str]] = []
        seen_keys: set[tuple[str, ...]] = set()
        for key in self.identities:
            deduped_tokens: list[str] = []
            for token in key:
                if token not in deduped_tokens:
                    deduped_tokens.append(token)
            key_tuple = tuple(deduped_tokens)
            if key_tuple and key_tuple not in seen_keys:
                seen_keys.add(key_tuple)
                deduped_keys.append(deduped_tokens)
        object.__setattr__(self, "identities", deduped_keys)
        return self

    def finish_init(self, vertex_config: VertexConfig):
        """Complete logical edge initialization with vertex configuration."""
        _ = vertex_config
        self._merge_identity_fields_into_properties()
        self._validate_identity_tokens()

    def _merge_identity_fields_into_properties(self) -> None:
        """Append :class:`Field` entries for identity tokens not already declared.

        Endpoint tokens ``source`` and ``target`` are not edge properties; every
        other token (including ``relation``) is materialized like vertex identity.
        """
        endpoint_tokens = frozenset({"source", "target"})
        seen_names = {f.name for f in self.properties}
        augmented = list(self.properties)
        for key in self.identities:
            for token in key:
                if token in endpoint_tokens:
                    continue
                if token not in seen_names:
                    augmented.append(Field(name=token, type=None))
                    seen_names.add(token)
        object.__setattr__(self, "properties", augmented)

    def _validate_identity_tokens(self) -> None:
        """Validate edge identity keys against reserved tokens and declared edge fields."""
        reserved = {"source", "target", "relation"}
        direct_weight_fields = set(self.property_names)
        # Identity token "relation" maps to the default TigerGraph attribute name
        # when physical fields are declared (see EdgeConfigDBAware.effective_weights).
        logical_relation_attr = {DEFAULT_TIGERGRAPH_RELATION_WEIGHTNAME}
        allowed_fields = reserved | direct_weight_fields | logical_relation_attr
        unknown_by_key = [
            [token for token in key if token not in allowed_fields]
            for key in self.identities
        ]
        unknown_by_key = [u for u in unknown_by_key if u]
        if unknown_by_key:
            raise ValueError(
                "Edge identity key fields must use reserved tokens "
                "('source', 'target', 'relation') or declared edge property / relation fields. "
                f"Edge ({self.source}, {self.target}, {self.relation}) has unknown identity fields: {unknown_by_key}"
            )

    @property
    def edge_name_dyad(self):
        """Get the edge name as a dyad (source, target).

        Returns:
            tuple[str, str]: Source and target vertex names
        """
        return self.source, self.target

    @property
    def edge_id(self) -> EdgeId:
        """Alias for edge_id."""
        return self.source, self.target, self.relation

    @property
    def property_names(self) -> list[str]:
        """Declared materialized edge property names."""
        return [f.name for f in self.properties]


class EdgeInverse(ConfigBaseModel):
    """Declared inverse pair: two relation names reading one fact from its two endpoints.

    For every *directed* edge ``(S, T, a)`` the inverse is ``(T, S, b)``, and for
    every ``(S, T, b)`` it is ``(T, S, a)``: the pair is unordered, so
    ``{relation: a, inverse: b}`` and ``{relation: b, inverse: a}`` state the same
    thing, and the table stores each pair once, its names in sorted order. A
    relation that is its own inverse is *symmetric* and is declared in
    ``edge_config.symmetric`` instead.

    The declaration is purely logical and creates nothing. It is realized in one
    of two ways, never both for one relation:

    - **inverse edges**: explicit logical edges ``(T, S, b)`` in
      ``edge_config.edges``, portable to every backend (``add_inverse_edges``);
    - a **native inverse**: ``db_profile.native_inverses``, where the database
      maintains the paired type itself (TigerGraph only).
    """

    relation: str = PydanticField(
        ..., min_length=1, description="One relation of the pair."
    )
    inverse: str = PydanticField(
        ...,
        min_length=1,
        description="The other relation: the same fact read from the other endpoint.",
    )

    @model_validator(mode="after")
    def _reject_self_inverse(self) -> EdgeInverse:
        if self.relation == self.inverse:
            raise ValueError(
                f"relation {self.relation!r} cannot be paired with itself; declare "
                "a relation that is its own inverse in `symmetric`"
            )
        return self


def normalize_inverse_table(
    pairs: Iterable[tuple[str, str]], symmetric: Iterable[str], *, kind: str
) -> tuple[list[EdgeInverse], list[str]]:
    """The one consistency rule for declared inverses, and their canonical form.

    Pairs and symmetric names together define ``inv`` over relation names, which
    must be a function: every name has at most one inverse. Restating a pair in
    either order, or a symmetric name twice, is the same statement and is kept
    once. A chain (``a-b`` with ``b-c``), a name both paired and symmetric, or a
    name paired with itself is refused.

    Returns:
        Pairs sorted, each with its two names in sorted order, and the sorted
        symmetric names -- so two tables stating the same map compare equal.

    Raises:
        ValueError: naming every relation that would get two inverses.
    """
    inv: dict[str, str] = {}
    conflicts: set[str] = set()
    self_paired: set[str] = set()

    def _bind(name: str, other: str) -> None:
        bound = inv.setdefault(name, other)
        if bound != other:
            conflicts.add(f"{name!r}: {bound!r} vs {other!r}")

    for relation, inverse in pairs:
        if relation == inverse:
            self_paired.add(relation)
            continue
        _bind(relation, inverse)
        _bind(inverse, relation)
    for name in symmetric:
        _bind(name, name)

    if self_paired:
        raise ValueError(
            f"{kind}: relations cannot be paired with themselves: "
            f"{sorted(self_paired)}; declare them as symmetric instead"
        )
    if conflicts:
        raise ValueError(
            f"{kind}: each relation has at most one inverse (a symmetric relation "
            f"is its own); conflicting: {'; '.join(sorted(conflicts))}"
        )
    out_pairs = sorted(
        {
            (min(name, other), max(name, other))
            for name, other in inv.items()
            if name != other
        }
    )
    return (
        [EdgeInverse(relation=a, inverse=b) for a, b in out_pairs],
        sorted(name for name, other in inv.items() if name == other),
    )


def inverse_map(
    inverses: Iterable[EdgeInverse], symmetric: Iterable[str] = ()
) -> dict[str, str]:
    """``inv`` as a dict: both directions of every pair, and ``name -> name`` if symmetric."""
    out: dict[str, str] = {}
    for pair in inverses:
        out[pair.relation] = pair.inverse
        out[pair.inverse] = pair.relation
    for name in symmetric:
        out[name] = name
    return out


def remap_inverses(
    inverses: list[EdgeInverse],
    symmetric: list[str],
    relation_map: dict[str, str],
    *,
    kind: str,
) -> tuple[list[EdgeInverse], list[str]]:
    """Carry the declared inverses through a relation rename or merge.

    A merge (non-injective map) can collapse a pair onto one name. That is
    refused rather than read as "now symmetric": merging a relation with its
    inverse discards which way each edge was read, it does not make the
    relationship symmetric. A merge that gives one name two inverses is refused
    by :func:`normalize_inverse_table`.

    Raises:
        ValueError: naming the pairs the remap would corrupt.
    """
    collapsed = sorted(
        (pair.relation, pair.inverse)
        for pair in inverses
        if relation_map.get(pair.relation, pair.relation)
        == relation_map.get(pair.inverse, pair.inverse)
    )
    if collapsed:
        raise ValueError(
            f"{kind}: declared inverse pairs would collapse onto one relation: "
            f"{collapsed}; retract the pairs first"
        )
    return normalize_inverse_table(
        [
            (
                relation_map.get(pair.relation, pair.relation),
                relation_map.get(pair.inverse, pair.inverse),
            )
            for pair in inverses
        ],
        [relation_map.get(name, name) for name in symmetric],
        kind=kind,
    )


def union_inverses(
    left: EdgeConfig, right: EdgeConfig
) -> tuple[list[EdgeInverse], list[str]]:
    """Union two declared inverse tables, refusing a relation with two inverses.

    Raises:
        ValueError: when the sides declare different inverses for one relation.
    """
    return normalize_inverse_table(
        [(p.relation, p.inverse) for p in (*left.inverses, *right.inverses)],
        [*left.symmetric, *right.symmetric],
        kind="conflicting declared inverses",
    )


class EdgeConfig(ConfigBaseModel):
    """Configuration for managing collections of edges.

    This class manages a collection of edges, providing methods for accessing
    and manipulating edge configurations.

    Attributes:
        edges: List of edge configurations
        inverses: Declared inverse relation pairs (see :class:`EdgeInverse`)
        symmetric: Declared symmetric relations (each its own inverse)
    """

    edges: list[Edge] = PydanticField(
        default_factory=list,
        description="List of edge definitions (source, target, identities, properties, relation, etc.).",
    )
    inverses: list[EdgeInverse] = PydanticField(
        default_factory=list,
        description=(
            "Declared inverse relation pairs, unordered. Logical only: a pair is "
            "realized either by explicit inverse edges or by a native inverse on "
            "the db_profile."
        ),
    )
    symmetric: list[str] = PydanticField(
        default_factory=list,
        description=(
            "Declared symmetric relations: each is its own inverse. Logical only: "
            "every edge naming one must be `directed: false`, which is its "
            "realization."
        ),
    )
    _edges_map: dict[EdgeId, Edge] = PrivateAttr()

    @model_validator(mode="after")
    def _normalize_inverse_table(self) -> EdgeConfig:
        # Every consumer of the table assumes "the inverse of b" has one answer;
        # normalizing here also makes two orderings of one table hash alike.
        pairs, symmetric = normalize_inverse_table(
            [(pair.relation, pair.inverse) for pair in self.inverses],
            self.symmetric,
            kind="edge_config",
        )
        object.__setattr__(self, "inverses", pairs)
        object.__setattr__(self, "symmetric", symmetric)
        return self

    def inverse_of(self, relation: str | None) -> str | None:
        """Declared inverse of ``relation``: its pair partner, itself if symmetric, else ``None``."""
        if relation is None:
            return None
        return inverse_map(self.inverses, self.symmetric).get(relation)

    def is_symmetric(self, relation: str | None) -> bool:
        """Whether ``relation`` is declared its own inverse."""
        return relation is not None and relation in self.symmetric

    def with_edges(self, edges: list[Edge]) -> EdgeConfig:
        """A config over ``edges`` that keeps the declared inverses.

        Pairs whose two relations both vanished, and symmetric relations that
        vanished, are dropped: the declaration is about relations, and one
        naming nothing is dangling. Every rebuild of an edge config inside the
        evolution machinery must go through here, or the table is silently lost.
        """
        relations = {e.relation for e in edges if e.relation is not None}
        inverses = [
            pair.model_copy(deep=True)
            for pair in self.inverses
            if pair.relation in relations or pair.inverse in relations
        ]
        symmetric = [name for name in self.symmetric if name in relations]
        return EdgeConfig(edges=edges, inverses=inverses, symmetric=symmetric)

    def validate_inverses(self) -> None:
        """Check the declared inverses against the declared edges.

        Raises:
            ValueError: when a declaration names no edge at all, when a paired
                relation names an undirected edge (an undirected relationship
                already reads both ways), or when a symmetric relation names a
                directed edge (symmetric means both orientations are one fact).
        """
        relations = {e.relation for e in self.edges if e.relation is not None}
        # A relation-less template edge admits relations named only at ingest
        # time (``relation_field``), so a declaration can legitimately name none yet.
        has_template = any(e.relation is None for e in self.edges)
        dangling = sorted(
            [
                str((pair.relation, pair.inverse))
                for pair in self.inverses
                if pair.relation not in relations and pair.inverse not in relations
            ]
            + [repr(name) for name in self.symmetric if name not in relations]
        )
        if dangling and not has_template:
            raise ValueError(
                f"edge_config: declared inverses name no declared edge: {dangling}"
            )
        paired = inverse_map(self.inverses)
        undirected = sorted(
            str(e.edge_id)
            for e in self.edges
            if not e.directed and e.relation is not None and e.relation in paired
        )
        if undirected:
            raise ValueError(
                "edge_config.inverses: an undirected edge has no inverse pair "
                "(it already reads both ways; declare its relation symmetric "
                f"instead): {undirected}"
            )
        directed = sorted(
            str(e.edge_id)
            for e in self.edges
            if e.directed and e.relation in self.symmetric
        )
        if directed:
            raise ValueError(
                "edge_config.symmetric: an edge naming a symmetric relation must "
                f"be undirected (set_edge_directed with directed=false): {directed}"
            )

    def inverse_advisories(self) -> list[str]:
        """Non-fatal findings about how declared pairs are realized as explicit edges.

        Realizing a pair is optional, so nothing here is an error. What is worth
        reporting is a realization that no longer matches itself: an inverse
        edge whose properties or identity keys drifted from its forward edge, a
        relation realized for some endpoint pairs but not others, and a pair
        read from the same side (``(S, T, a)`` next to ``(S, T, b)``), which
        usually means one of the two was modeled backwards.
        """
        paired = inverse_map(self.inverses)
        by_id = {edge.edge_id: edge for edge in self.edges}
        findings: list[str] = []

        def _keys(edge: Edge) -> set[frozenset[str]]:
            # Endpoints are always part of an edge key; only the rest can drift.
            return {
                frozenset(key) - {"source", "target", "relation"}
                for key in edge.identities
            }

        realized: dict[str, list[bool]] = {}
        for edge in self.edges:
            relation = edge.relation
            inverse = paired.get(relation) if relation is not None else None
            if relation is None or inverse is None or not edge.directed:
                continue
            other = by_id.get((edge.target, edge.source, inverse))
            realized.setdefault(relation, []).append(other is not None)
            if (
                edge.source != edge.target
                and (edge.source, edge.target, inverse) in by_id
                and relation < inverse
            ):
                findings.append(
                    f"{edge.edge_id} and {(edge.source, edge.target, inverse)} read "
                    "a declared pair from the same side; one of them is probably "
                    "reversed"
                )
            if other is None or relation > inverse:
                continue
            if set(edge.property_names) != set(other.property_names):
                findings.append(
                    f"inverse edges {edge.edge_id} and {other.edge_id} declare "
                    f"different properties: {sorted(edge.property_names)} vs "
                    f"{sorted(other.property_names)}"
                )
            if _keys(edge) != _keys(other):
                findings.append(
                    f"inverse edges {edge.edge_id} and {other.edge_id} declare "
                    "different identity keys"
                )
        for relation, flags in sorted(realized.items()):
            if any(flags) and not all(flags):
                findings.append(
                    f"relation {relation!r} has explicit inverse edges for some "
                    "endpoint pairs but not all; add_inverse_edges completes it"
                )
        return findings

    @model_validator(mode="after")
    def _build_edges_map(self) -> EdgeConfig:
        """Build internal mapping of edge IDs to edge configurations."""
        # See VertexConfig.build_vertices_map: the list is the serialized truth and the
        # map the lookup truth, so a duplicate edge_id lets them disagree silently.
        duplicates = sorted(
            str(edge_id)
            for edge_id, count in Counter(e.edge_id for e in self.edges).items()
            if count > 1
        )
        if duplicates:
            raise ValueError(f"duplicate edge definitions: {duplicates}")
        object.__setattr__(self, "_edges_map", {e.edge_id: e for e in self.edges})
        return self

    @staticmethod
    def _map_key(edge: Edge) -> EdgeId:
        return edge.edge_id

    def finish_init(self, vc: VertexConfig):
        """Complete initialization of all logical edges."""
        for e in self.edges:
            e.finish_init(vertex_config=vc)
        self.validate_inverses()

    def values(self) -> Iterator[Edge]:
        """Iterate over edge configurations."""
        return iter(self._edges_map.values())

    def items(self) -> Iterator[tuple[EdgeId, Edge]]:
        """Iterate over ``(edge_id, edge)`` pairs."""
        return iter(self._edges_map.items())

    def __contains__(self, item: EdgeId | Edge):
        """Check if edge exists in configuration.

        Args:
            item: Edge ID or Edge instance to check

        Returns:
            bool: True if edge exists, False otherwise
        """
        if isinstance(item, Edge):
            return self._map_key(item) in self._edges_map
        if isinstance(item, tuple) and len(item) == 3:
            return item in self._edges_map
        return False

    def update_edges(
        self,
        edge: Edge,
        vertex_config: VertexConfig,
    ):
        """Update edge configuration.

        Args:
            edge: Edge configuration to update
            vertex_config: Vertex configuration
        """
        edge_key = self._map_key(edge)
        if edge_key in self._edges_map:
            self._edges_map[edge_key].update(edge)
        else:
            self._edges_map[edge_key] = edge
            self.edges.append(edge)

        self._edges_map[edge_key].finish_init(
            vertex_config=vertex_config,
        )

    def edge_for(self, edge_id: EdgeId) -> Edge:
        """Return the config-owned :class:`Edge` instance for ``edge_id`` after merges.

        Pipeline actors may construct a partial :class:`Edge` that is merged into the
        schema edge via :meth:`update_edges`. Callers that need properties, identities,
        etc. must use this object (same reference as in :meth:`items`), not the
        pre-merge actor copy.
        """
        return self._edges_map[edge_id]

    @property
    def vertices(self):
        """Get set of vertex names involved in edges.

        Returns:
            set[str]: Set of vertex names
        """
        return {e.source for e in self.edges} | {e.target for e in self.edges}
