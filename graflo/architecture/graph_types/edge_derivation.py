"""Edge-derivation wiring shared by contract (authoring) and pipeline (runtime).

:class:`EdgeDerivation` is the declarative model set on edge pipeline steps
(:class:`~graflo.architecture.contract.ingestion.steps.models.EdgeActorConfig`):
how an edge step binds to extracted locations and documents. These fields do
**not** belong in schema ``edge_config`` / :class:`~graflo.architecture.schema.edge.Edge`.

:class:`EdgeDerivationRegistry` is the mutable runtime store for ingestion-time
edge behavior keyed by :class:`~graflo.architecture.graph_types.identifiers.EdgeId`
(typically one instance per :class:`~graflo.architecture.pipeline.runtime.resource.ResourceRuntime`).
When :attr:`EdgeDerivation.relation_from_key` is true, the registry records the
edge id so :class:`~graflo.architecture.schema.db_aware.EdgeConfigDBAware` (with
overlay) can align TigerGraph DDL with runtime.

An endpoint selector names the identity an edge endpoint is matched on. It is
*plain* -- ``None`` / ``"identity"`` for the primary, a secondary identity's
name, or a field list -- or *per class*: ``{Class: plain}``, for an endpoint a
``vertex_router`` fills with rows of several classes. A class the mapping does
not name is matched on its primary identity. :func:`selector_for` reduces
either form to the plain selector one concrete class uses.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, TypeAlias

from pydantic import Field

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.graph_types.identifiers import EdgeId
from graflo.architecture.graph_types.index_config import Weight
from graflo.onto import (
    PRIMARY_IDENTITY_SELECTOR,
    EndpointAmbiguityPolicy,
)

PlainSelector: TypeAlias = str | list[str]
"""A secondary identity's name, a field list, ``"identity"`` or ``"secondary"``."""

EndpointSelector: TypeAlias = PlainSelector | dict[str, PlainSelector]
"""A plain selector, or one per endpoint class (``{Class: plain}``)."""


def selector_for(
    selector: EndpointSelector | None, vertex: str
) -> PlainSelector | None:
    """The plain selector *selector* applies to endpoint class *vertex*.

    A per-class mapping that does not name *vertex* leaves it on its primary
    identity (``None``).
    """
    if isinstance(selector, dict):
        return selector.get(vertex)
    return selector


def _is_primary(selector: EndpointSelector | None) -> bool:
    if isinstance(selector, dict):
        return all(_is_primary(value) for value in selector.values())
    return selector in (None, PRIMARY_IDENTITY_SELECTOR)


class EdgeDerivation(ConfigBaseModel):
    """How this edge step selects vertex locations and reads per-document relation from data."""

    match_source: str | None = Field(
        default=None,
        description="Require this path segment in source vertex locations.",
    )
    match_target: str | None = Field(
        default=None,
        description="Require this path segment in target vertex locations.",
    )
    exclude_source: str | None = Field(
        default=None,
        description="Exclude source locations containing this path segment.",
    )
    exclude_target: str | None = Field(
        default=None,
        description="Exclude target locations containing this path segment.",
    )
    match: str | None = Field(
        default=None,
        description="Require this segment in both source and target locations.",
    )
    relation_field: str | None = Field(
        default=None,
        description="Document/ctx field name for per-document relationship label when schema relation is unset.",
    )
    relation_from_key: bool = Field(
        default=False,
        description="If True, derive the per-document relation label from the location key during assembly.",
    )
    source_match: EndpointSelector | None = Field(
        default=None,
        description=(
            "Identity selector for the source endpoint: None/'identity' for the "
            "primary identity, a secondary identity name / field list, or a "
            "mapping of endpoint class to one of those."
        ),
    )
    target_match: EndpointSelector | None = Field(
        default=None,
        description="Identity selector for the target endpoint; see source_match.",
    )
    on_ambiguous: EndpointAmbiguityPolicy | None = Field(
        default=None,
        description=(
            "Per-step override of ingestion_model.endpoints_on_ambiguous when a "
            "secondary identity matches several vertices."
        ),
    )

    emit_inverse: bool = Field(
        default=False,
        description=(
            "If True, assembly also writes the declared inverse of every edge this "
            "step writes, when that inverse edge is declared."
        ),
    )

    def uses_secondary_identity(self) -> bool:
        """True when either endpoint is matched on something other than the primary identity."""
        return not (_is_primary(self.source_match) and _is_primary(self.target_match))

    def is_empty(self) -> bool:
        if self.relation_from_key or self.emit_inverse:
            return False
        return all(
            getattr(self, name) is None
            for name in (
                "match_source",
                "match_target",
                "exclude_source",
                "exclude_target",
                "match",
                "relation_field",
                "source_match",
                "target_match",
                "on_ambiguous",
            )
        )


@dataclass(frozen=True)
class EndpointMatch:
    """Which identity each endpoint of an edge is matched on at write time.

    ``None`` selectors mean the vertex's primary identity, which is the default
    and leaves the write path exactly as it has always been.
    """

    source: str | list[str] | None = None
    target: str | list[str] | None = None
    on_ambiguous: EndpointAmbiguityPolicy | None = None

    def is_default(self) -> bool:
        return (
            self.source in (None, PRIMARY_IDENTITY_SELECTOR)
            and self.target in (None, PRIMARY_IDENTITY_SELECTOR)
            and self.on_ambiguous is None
        )


@dataclass(frozen=True)
class EndpointRule:
    """Endpoint selectors for edges an edge step names only per document.

    A step whose endpoint comes from a router role, or whose relation comes
    from the data, has no single edge id to register at load time. The rule
    stands for every edge it can write: ``None`` in :attr:`source`,
    :attr:`target` or :attr:`relation` is whatever the document resolves, and
    a per-class selector is reduced to the concrete class when the rule is
    applied.
    """

    source: str | None
    target: str | None
    relation: str | None
    source_match: EndpointSelector | None = None
    target_match: EndpointSelector | None = None
    on_ambiguous: EndpointAmbiguityPolicy | None = None

    def applies_to(self, edge_id: EdgeId) -> bool:
        return all(
            fixed is None or fixed == actual
            for fixed, actual in zip((self.source, self.target, self.relation), edge_id)
        )

    def resolve(self, edge_id: EdgeId) -> EndpointMatch:
        source, target, _relation = edge_id
        return EndpointMatch(
            source=selector_for(self.source_match, source),
            target=selector_for(self.target_match, target),
            on_ambiguous=self.on_ambiguous,
        )


class EdgeDerivationRegistry:
    """Mutable store for ingestion-time edge behavior keyed by :class:`EdgeId`.

    Lives under the ingestion layer (typically one instance per :class:`ResourceRuntime`),
    not on :class:`~graflo.architecture.schema.core.CoreSchema`.
    """

    def __init__(self) -> None:
        self._relation_from_key: dict[EdgeId, bool] = {}
        self._vertex_weights: dict[EdgeId, list[Weight]] = {}
        self._endpoint_match: dict[EdgeId, EndpointMatch] = {}
        self._endpoint_rules: list[EndpointRule] = []

    def mark_relation_from_key(self, edge_id: EdgeId) -> None:
        self._relation_from_key[edge_id] = True

    def uses_relation_from_key(self, edge_id: EdgeId) -> bool:
        return self._relation_from_key.get(edge_id, False)

    def set_endpoint_match(self, edge_id: EdgeId, match: EndpointMatch) -> None:
        """Record how *edge_id* locates its endpoints, for the write stage.

        Only non-default selections are stored, so an edge matching on primary
        identity — the overwhelming majority — leaves no entry and costs nothing.
        """
        if match.is_default():
            return
        self._endpoint_match[edge_id] = match

    def add_endpoint_rule(self, rule: EndpointRule) -> None:
        """Record selectors for the edges a data-driven edge step can write.

        A later rule covering the same edge wins, as a later
        :meth:`set_endpoint_match` does.
        """
        if rule in self._endpoint_rules:
            return
        self._endpoint_rules.append(rule)

    def endpoint_match_for(self, edge_id: EdgeId) -> EndpointMatch | None:
        """How *edge_id* locates its endpoints, or ``None`` for the primary identity.

        An edge registered by id wins; otherwise the last rule covering it
        decides, reduced to the edge's concrete classes.
        """
        match = self._endpoint_match.get(edge_id)
        if match is not None:
            return match
        for rule in reversed(self._endpoint_rules):
            if rule.applies_to(edge_id):
                resolved = rule.resolve(edge_id)
                return None if resolved.is_default() else resolved
        return None

    def has_endpoint_matches(self) -> bool:
        """Whether any edge locates its endpoints by a secondary identity.

        Such edges are resolved against database state at write time, which
        makes cross-batch write ordering semantic for the resource.
        """
        return bool(self._endpoint_match or self._endpoint_rules)

    def merge_vertex_weights(self, edge_id: EdgeId, rules: list[Weight]) -> None:
        """Append vertex weight rules for *edge_id*, deduplicating by stable fingerprint."""
        if not rules:
            return
        bucket = self._vertex_weights.setdefault(edge_id, [])
        seen = {_weight_fingerprint(w) for w in bucket}
        for w in rules:
            fp = _weight_fingerprint(w)
            if fp in seen:
                continue
            seen.add(fp)
            bucket.append(w)

    def vertex_weights_for(self, edge_id: EdgeId) -> list[Weight]:
        return list(self._vertex_weights.get(edge_id, ()))

    def copy(self) -> EdgeDerivationRegistry:
        out = EdgeDerivationRegistry()
        out._relation_from_key = dict(self._relation_from_key)
        out._vertex_weights = {
            k: [w.model_copy(deep=True) for w in v]
            for k, v in self._vertex_weights.items()
        }
        out._endpoint_match = dict(self._endpoint_match)
        out._endpoint_rules = list(self._endpoint_rules)
        return out

    def merge_from(self, other: EdgeDerivationRegistry) -> None:
        for eid, flag in other._relation_from_key.items():
            if flag:
                self.mark_relation_from_key(eid)
        for eid, weights in other._vertex_weights.items():
            self.merge_vertex_weights(eid, weights)
        for eid, match in other._endpoint_match.items():
            self.set_endpoint_match(eid, match)
        for rule in other._endpoint_rules:
            self.add_endpoint_rule(rule)


def _weight_fingerprint(w: Weight) -> str:
    """JSON-stable fingerprint for deduplication."""
    payload: dict[str, Any] = w.model_dump(mode="json")
    return json.dumps(payload, sort_keys=True, default=str)
