"""Resolve edge endpoints declared by a secondary identity.

An edge-only source references its endpoints by an alternate key. Before the
edge can be written, that key has to be mapped back to the vertex's primary
identity — which is what every backend's edge write already expects.

Doing the mapping here rather than pushing the predicate into each backend's
edge query keeps one semantic across all of them, and is the only approach that
works for backends addressing endpoints by key (PostgreSQL foreign keys,
NebulaGraph VIDs, TigerGraph ``PRIMARY_ID``).
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from graflo.db.resolve import key_tuple
from graflo.onto import EndpointAmbiguityPolicy

logger = logging.getLogger(__name__)


class AmbiguousEndpointError(RuntimeError):
    """A secondary identity matched several vertices under the ``error`` policy."""


@dataclass
class EndpointResolutionStats:
    """What happened while resolving one edge batch.

    Unmatched and ambiguous endpoints are ordinary data conditions rather than
    failures, so they are counted and reported instead of raising.
    """

    rows: int = 0
    unresolvable: int = 0
    """Rows whose key was absent or incomplete, so no lookup was possible."""
    unmatched: int = 0
    """Rows whose key matched no vertex."""
    ambiguous: int = 0
    """Rows whose key matched more than one vertex."""
    dropped: int = 0
    """Rows that produced no edge."""
    written: int = 0
    """Edge documents produced, which exceeds rows when fanning out."""
    endpoints: list[str] = field(default_factory=list)

    def has_findings(self) -> bool:
        return bool(self.unresolvable or self.unmatched or self.ambiguous)

    def summary(self) -> str:
        return (
            f"endpoints={'+'.join(self.endpoints) or 'none'} rows={self.rows} "
            f"written={self.written} dropped={self.dropped} "
            f"unresolvable={self.unresolvable} unmatched={self.unmatched} "
            f"ambiguous={self.ambiguous}"
        )


def _sorted_candidates(
    matches: list[dict[str, Any]], identity_fields: Sequence[str]
) -> list[dict[str, Any]]:
    """Order matches by primary identity so ``first`` is reproducible."""
    return sorted(
        matches,
        key=lambda doc: tuple(str(doc.get(f, "")) for f in identity_fields),
    )


def resolve_edge_endpoints(
    db: Any,
    docs: list[Any],
    *,
    source_class: str,
    target_class: str,
    source_match_fields: Sequence[str],
    target_match_fields: Sequence[str],
    source_identity_fields: Sequence[str],
    target_identity_fields: Sequence[str],
    resolve_source: bool,
    resolve_target: bool,
    policy: EndpointAmbiguityPolicy,
) -> tuple[list[Any], EndpointResolutionStats]:
    """Rewrite endpoint projections from secondary keys to primary identities.

    Only the endpoints flagged for resolution are looked up; the other side is
    passed through untouched, which is what makes asymmetric selection work.

    Args:
        docs: ``(source_projection, target_projection, weight)`` triples
        resolve_source: True when the source endpoint uses a secondary identity
        resolve_target: Same, for the target endpoint
        policy: What to do when a key matches several vertices

    Returns:
        tuple: rewritten edge documents, and what happened while resolving.

    Raises:
        AmbiguousEndpointError: on multiple matches under the ``error`` policy.
    """
    stats = EndpointResolutionStats(rows=len(docs))
    if resolve_source:
        stats.endpoints.append("source")
    if resolve_target:
        stats.endpoints.append("target")

    source_matches: dict[int, list[dict[str, Any]]] = {}
    target_matches: dict[int, list[dict[str, Any]]] = {}
    if resolve_source:
        source_matches = db.resolve_vertices(
            source_class,
            [doc[0] for doc in docs],
            tuple(source_match_fields),
            tuple(source_identity_fields),
        )
    if resolve_target:
        target_matches = db.resolve_vertices(
            target_class,
            [doc[1] for doc in docs],
            tuple(target_match_fields),
            tuple(target_identity_fields),
        )

    resolved: list[Any] = []
    for position, doc in enumerate(docs):
        source_doc, target_doc = doc[0], doc[1]
        rest = tuple(doc[2:])

        source_options = _candidates_for(
            position=position,
            projection=source_doc,
            matches=source_matches,
            match_fields=source_match_fields,
            identity_fields=source_identity_fields,
            resolve=resolve_source,
            stats=stats,
            side="source",
            source_class=source_class,
            policy=policy,
        )
        target_options = _candidates_for(
            position=position,
            projection=target_doc,
            matches=target_matches,
            match_fields=target_match_fields,
            identity_fields=target_identity_fields,
            resolve=resolve_target,
            stats=stats,
            side="target",
            source_class=target_class,
            policy=policy,
        )

        if not source_options or not target_options:
            stats.dropped += 1
            continue

        for resolved_source in source_options:
            for resolved_target in target_options:
                resolved.append((resolved_source, resolved_target, *rest))
                stats.written += 1

    return resolved, stats


def _candidates_for(
    *,
    position: int,
    projection: dict[str, Any],
    matches: dict[int, list[dict[str, Any]]],
    match_fields: Sequence[str],
    identity_fields: Sequence[str],
    resolve: bool,
    stats: EndpointResolutionStats,
    side: str,
    source_class: str,
    policy: EndpointAmbiguityPolicy,
) -> list[dict[str, Any]]:
    """Endpoint documents to attach for one row, after applying *policy*."""
    if not resolve:
        return [projection]

    if key_tuple(projection, match_fields) is None:
        # A partial composite key must never be partially matched.
        stats.unresolvable += 1
        return []

    found = matches.get(position) or []
    if not found:
        stats.unmatched += 1
        return []

    if len(found) > 1:
        stats.ambiguous += 1
        if policy == "error":
            raise AmbiguousEndpointError(
                f"{side} endpoint of '{source_class}' matched {len(found)} vertices "
                f"on {list(match_fields)}={key_tuple(projection, match_fields)}. "
                "Set endpoints_on_ambiguous to all, first or skip to tolerate this."
            )
        if policy == "skip":
            return []
        if policy == "first":
            found = _sorted_candidates(found, identity_fields)[:1]

    return [
        {field_name: doc.get(field_name) for field_name in identity_fields}
        for doc in found
    ]
