"""Bipartite equivalence clusters over compose-time vertex mappings.

A :class:`~graflo.architecture.evolution.ops.VertexEquivalence` is an edge
between a left-manifest class and a right-manifest class that collapses onto
one ``into`` label. Several such edges form a bipartite graph; its connected
components are the natural unit of consistency:

* every edge in a component must agree on one ``into``;
* a :class:`~graflo.architecture.evolution.canonical.CanonicalMap` label on any
  member must agree with that ``into``;
* members without an explicit canonical label inherit the resolved one
  (completion).

Nodes are ``(side, name)`` pairs so a class named ``Org`` on the left is never
confused with ``Org`` on the right.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Literal

from .ops import VertexEquivalence

Side = Literal["left", "right"]
Node = tuple[Side, str]


class ClusterConflictError(ValueError):
    """A connected equivalence component disagrees on its target label."""


@dataclass(frozen=True)
class LabelContribution:
    """One source of a candidate cluster label, with provenance."""

    node: Node | None
    label: str
    source: Literal["into", "canonical"]


@dataclass
class Cluster:
    """One connected component of the bipartite equivalence graph."""

    members: frozenset[Node]
    edges: list[VertexEquivalence] = field(default_factory=list)

    @property
    def left_names(self) -> frozenset[str]:
        return frozenset(name for side, name in self.members if side == "left")

    @property
    def right_names(self) -> frozenset[str]:
        return frozenset(name for side, name in self.members if side == "right")


@dataclass(frozen=True)
class ResolvedCluster:
    """A cluster whose edges and canonical labels agree on one target."""

    cluster: Cluster
    label: str
    unmapped: frozenset[Node]
    """Members that inherit ``label`` by completion (no explicit map entry)."""


class _UnionFind:
    def __init__(self) -> None:
        self._parent: dict[Node, Node] = {}

    def add(self, node: Node) -> None:
        self._parent.setdefault(node, node)

    def find(self, node: Node) -> Node:
        parent = self._parent.setdefault(node, node)
        if parent != node:
            self._parent[node] = self.find(parent)
        return self._parent[node]

    def union(self, a: Node, b: Node) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self._parent[rb] = ra


def build_clusters(vertices: Sequence[VertexEquivalence]) -> list[Cluster]:
    """Connected components of the bipartite graph induced by *vertices*."""
    uf = _UnionFind()
    edges_by_root: dict[Node, list[VertexEquivalence]] = {}
    members_by_root: dict[Node, set[Node]] = {}

    for veq in vertices:
        left: Node = ("left", veq.left)
        right: Node = ("right", veq.right)
        uf.add(left)
        uf.add(right)
        uf.union(left, right)

    for veq in vertices:
        left: Node = ("left", veq.left)
        right: Node = ("right", veq.right)
        root = uf.find(left)
        members_by_root.setdefault(root, set()).update((left, right))
        edges_by_root.setdefault(root, []).append(veq)

    clusters = [
        Cluster(
            members=frozenset(members),
            edges=list(edges_by_root[root]),
        )
        for root, members in sorted(
            members_by_root.items(),
            key=lambda item: sorted(item[1]),
        )
    ]
    return clusters


def _format_node(node: Node) -> str:
    side, name = node
    return f"{side}:{name}"


def _format_contribution(c: LabelContribution) -> str:
    if c.node is None:
        return f"{c.label!r} (declared into)"
    return f"{_format_node(c.node)}→{c.label!r} ({c.source})"


def resolve_cluster_labels(
    clusters: Sequence[Cluster],
    canonical_labels: Mapping[Node, str] | None = None,
) -> list[ResolvedCluster]:
    """Resolve each cluster to one label, or raise on disagreement.

    Labels are collected from every edge's ``into`` and from *canonical_labels*
    keyed by ``(side, name)``. A cluster with one distinct label is resolved;
    members absent from *canonical_labels* are listed in
    :attr:`ResolvedCluster.unmapped` for completion. Two or more distinct
    labels raise :class:`ClusterConflictError` with full provenance.
    """
    label_map = dict(canonical_labels or ())
    resolved: list[ResolvedCluster] = []

    for cluster in clusters:
        contributions: list[LabelContribution] = []
        for veq in cluster.edges:
            contributions.append(
                LabelContribution(node=None, label=veq.into, source="into")
            )
        for node in sorted(cluster.members):
            if node in label_map:
                contributions.append(
                    LabelContribution(
                        node=node, label=label_map[node], source="canonical"
                    )
                )

        distinct = sorted({c.label for c in contributions})
        if len(distinct) == 0:
            # Unreachable while ``into`` is required on every VertexEquivalence.
            raise ClusterConflictError(
                f"equivalence cluster {{{', '.join(_format_node(n) for n in sorted(cluster.members))}}} "
                "has no declared into and no canonical label"
            )
        if len(distinct) > 1:
            members = ", ".join(_format_node(n) for n in sorted(cluster.members))
            detail = ", ".join(_format_contribution(c) for c in contributions)
            raise ClusterConflictError(
                f"equivalence cluster {{{members}}} disagrees on target label: "
                f"{detail}. Pick one `into`, or fix the canonical map."
            )

        label = distinct[0]
        unmapped = frozenset(node for node in cluster.members if node not in label_map)
        # Edges whose into already equals the resolved label still leave their
        # endpoints "unmapped" for completion when the canonical map has no
        # entry — that is intentional: completion fills the map, not the edges.
        resolved.append(
            ResolvedCluster(cluster=cluster, label=label, unmapped=unmapped)
        )

    return resolved


def vertex_rename_maps(
    resolved: Sequence[ResolvedCluster],
) -> tuple[dict[str, str], dict[str, str]]:
    """Per-side ``{source: into}`` maps from resolved clusters.

    Unlike a plain dict built from the raw equivalence list, every source that
    participates in a cluster is routed to the *same* resolved label — so a
    class that appears in two edges can never silently take the last write.
    """
    left: dict[str, str] = {}
    right: dict[str, str] = {}
    for item in resolved:
        for side, name in item.cluster.members:
            if name == item.label:
                continue
            target_map = left if side == "left" else right
            existing = target_map.get(name)
            if existing is not None and existing != item.label:
                # Defensive: resolve_cluster_labels already forbids this.
                raise ClusterConflictError(
                    f"{side}:{name} would rename to both {existing!r} and "
                    f"{item.label!r}"
                )
            target_map[name] = item.label
    return left, right
