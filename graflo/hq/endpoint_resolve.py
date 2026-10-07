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
import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from graflo.architecture.graph_types.identifiers import EdgeId
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

    documents: int = 0
    unresolvable: int = 0
    """Documents whose key was absent or incomplete, so no lookup was possible."""
    unmatched: int = 0
    """Documents whose key matched no vertex."""
    ambiguous: int = 0
    """Documents whose key matched more than one vertex."""
    dropped: int = 0
    """Documents that produced no edge."""
    written: int = 0
    """Edge documents produced, which exceeds ``documents`` when fanning out."""
    endpoints: list[str] = field(default_factory=list)

    def has_findings(self) -> bool:
        return bool(self.unresolvable or self.unmatched or self.ambiguous)

    def summary(self) -> str:
        return (
            f"endpoints={'+'.join(self.endpoints) or 'none'} "
            f"documents={self.documents} "
            f"written={self.written} dropped={self.dropped} "
            f"unresolvable={self.unresolvable} unmatched={self.unmatched} "
            f"ambiguous={self.ambiguous}"
        )

    def merge(self, other: EndpointResolutionStats) -> None:
        """Add *other*'s counts to these."""
        self.documents += other.documents
        self.unresolvable += other.unresolvable
        self.unmatched += other.unmatched
        self.ambiguous += other.ambiguous
        self.dropped += other.dropped
        self.written += other.written
        for endpoint in other.endpoints:
            if endpoint not in self.endpoints:
                self.endpoints.append(endpoint)


@dataclass
class AttachStats:
    """What happened while attaching records of one class to existing vertices.

    An attached record never creates a vertex, so a record whose key finds none
    is counted and not written.
    """

    documents: int = 0
    attached: int = 0
    """Records written onto at least one vertex."""
    unmatched: int = 0
    """Records whose key matched no vertex."""
    unresolvable: int = 0
    """Records whose key was absent or incomplete, so no lookup was possible."""
    ambiguous: int = 0
    """Records whose key matched more than one vertex."""
    written: int = 0
    """Rows upserted, one per vertex written onto: above ``attached`` when
    fanning out, below it when several records land on one vertex."""

    def has_findings(self) -> bool:
        return bool(self.unresolvable or self.unmatched or self.ambiguous)

    def summary(self) -> str:
        return (
            f"documents={self.documents} attached={self.attached} "
            f"written={self.written} unresolvable={self.unresolvable} "
            f"unmatched={self.unmatched} ambiguous={self.ambiguous}"
        )

    def merge(self, other: AttachStats) -> None:
        """Add *other*'s counts to these."""
        self.documents += other.documents
        self.attached += other.attached
        self.unmatched += other.unmatched
        self.unresolvable += other.unresolvable
        self.ambiguous += other.ambiguous
        self.written += other.written


@dataclass
class WriteStats:
    """Resolution counts accumulated by one writer over a run.

    Safe to add to from the worker threads concurrent writes run on.
    """

    attached: dict[str, AttachStats] = field(default_factory=dict)
    """By vertex class."""
    endpoints: dict[EdgeId, EndpointResolutionStats] = field(default_factory=dict)
    """By edge, for edges whose endpoints were resolved by a secondary identity."""
    _lock: threading.Lock = field(
        default_factory=threading.Lock, repr=False, compare=False
    )

    def add_attached(self, vertex: str, stats: AttachStats) -> None:
        with self._lock:
            self.attached.setdefault(vertex, AttachStats()).merge(stats)

    def add_endpoints(self, edge_id: EdgeId, stats: EndpointResolutionStats) -> None:
        with self._lock:
            self.endpoints.setdefault(edge_id, EndpointResolutionStats()).merge(stats)

    def is_empty(self) -> bool:
        return not self.attached and not self.endpoints

    def summary(self) -> str:
        """One line per attached class and per resolved edge."""
        with self._lock:
            lines = [
                f"attached {vertex}: {stats.summary()}"
                for vertex, stats in self.attached.items()
            ]
            lines.extend(
                f"edge {edge_id}: {stats.summary()}"
                for edge_id, stats in self.endpoints.items()
            )
        return "\n".join(lines)


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
    stats = EndpointResolutionStats(documents=len(docs))
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
    """Endpoint documents to attach for one document, after applying *policy*."""
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

    return select_matches(
        found,
        policy=policy,
        identity_fields=identity_fields,
        describe=lambda count: (
            f"{side} endpoint of '{source_class}' matched {count} vertices "
            f"on {list(match_fields)}={key_tuple(projection, match_fields)}."
        ),
    )


def select_matches(
    found: list[dict[str, Any]],
    *,
    policy: EndpointAmbiguityPolicy,
    identity_fields: Sequence[str],
    describe: Callable[[int], str],
) -> list[dict[str, Any]]:
    """The vertices a key's matches resolve to under *policy*.

    Several matches are kept (``all``), reduced to the lowest primary identity
    (``first``), dropped (``skip``) or refused (``error``); a single match is
    always kept. Callers check for a partial key before looking it up.

    Args:
        found: Every vertex the key matched, carrying *identity_fields*
        policy: What to do when there are several matches
        identity_fields: Primary identity fields to project the matches onto
        describe: Given the match count, the sentence naming what matched,
            for the ``error`` message

    Returns:
        list: The selected matches, projected onto *identity_fields*.

    Raises:
        AmbiguousEndpointError: on several matches under the ``error`` policy.
    """
    if len(found) > 1:
        if policy == "error":
            raise AmbiguousEndpointError(
                f"{describe(len(found))} "
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
