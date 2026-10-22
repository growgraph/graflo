"""One merge preview, narrowed to what a single equivalence or attribute is about.

A :class:`~graflo.architecture.evolution.preview.MergePreview` describes the
whole merge; on a real schema that is too much to read at once. A *focus
target* names one part of it, and :func:`focus` returns the preview of that
part alone, every edge closed over the nodes it keeps:

* a cluster id (``vertex-cluster-0``): the merged class, its members, their
  attributes, the resources its identity reads and the findings on any of them;
* a merged attribute (``merged:Machine.match_key``): every edge into it, with
  the rows and classes those edges leave from;
* ``rest``: the findings no other target holds, with the classes they name.

Selection is by node id, never by ``declared_by``: that field is a cluster id
for some edge kinds, a merged name, a side or a key space for others.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal

from .preview import MergeFinding, MergePreview, PreviewCluster, PreviewEdge

#: The target holding every finding no cluster or attribute holds.
REST = "rest"

#: ``nontrivial``: only attributes something other than a same-name fusion
#: decides; ``all``: every merged attribute.
AttributeScope = Literal["nontrivial", "all"]

_IDENTITY_EDGES = frozenset({"derivation", "local_key"})
_RELATION_EDGES = frozenset({"member", "map", "suggested"})


@dataclass(frozen=True)
class _Selection:
    nodes: frozenset[str]
    edges: tuple[PreviewEdge, ...]
    findings: tuple[int, ...]
    cluster: PreviewCluster | None


def focus_targets(
    preview: MergePreview, attributes: AttributeScope = "nontrivial"
) -> list[str]:
    """Every target of *preview*, in the order a reader should take them.

    Each vertex cluster is followed by its merged attributes, then come the
    relation clusters, then ``rest`` when some finding is held by no other
    target. Every finding lands in at least one target.

    Args:
        preview: The whole preview.
        attributes: Which merged attributes become targets of their own.
    """
    out: list[str] = []
    for cluster in _ordered_clusters(preview):
        out.append(cluster.id)
        if cluster.kind == "vertex":
            out.extend(
                node_id
                for node_id in _merged_attributes(preview, cluster)
                if attributes == "all" or _nontrivial(preview, cluster, node_id)
            )
    if _rest_findings(preview):
        out.append(REST)
    return out


def focus(preview: MergePreview, target: str) -> MergePreview:
    """The part of *preview* that *target* is about.

    Args:
        preview: The whole preview.
        target: A cluster id, a merged attribute node id, or ``rest``.

    Returns:
        A preview holding only the target's nodes, the edges between them,
        their findings in the original order, and the cluster they belong to.

    Raises:
        KeyError: *target* is none of these; the message lists the valid ones.
    """
    selection = _select(preview, target)
    if selection is None:
        valid = focus_targets(preview, attributes="all")
        raise KeyError(f"no focus target {target!r}; valid targets: {valid}")
    into = selection.cluster.into if selection.cluster else None
    return preview.model_copy(
        update={
            "nodes": [n for n in preview.nodes if n.id in selection.nodes],
            "edges": list(selection.edges),
            "clusters": [selection.cluster] if selection.cluster else [],
            "findings": [preview.findings[i] for i in selection.findings],
            "routing": [r for r in preview.routing if into and r.vertex == into],
        }
    )


def finding_positions(preview: MergePreview, target: str) -> list[int]:
    """Positions in ``preview.findings`` of the findings *target* holds."""
    selection = _select(preview, target)
    if selection is None:
        raise KeyError(f"no focus target {target!r}")
    return list(selection.findings)


# ── selection ───────────────────────────────────────────────────────────────


def _select(preview: MergePreview, target: str) -> _Selection | None:
    if target == REST:
        return _rest(preview)
    cluster = next((c for c in preview.clusters if c.id == target), None)
    if cluster is not None:
        return _cluster(preview, cluster)
    node = preview.node(target)
    if node is not None and node.kind == "attribute" and node.owner is not None:
        owner = preview.node(node.owner)
        if owner is not None and owner.kind == "merged":
            return _attribute(preview, target)
    return None


def _cluster(preview: MergePreview, cluster: PreviewCluster) -> _Selection:
    core = _core(cluster)
    if cluster.kind == "relation":
        edges = tuple(
            e
            for e in preview.edges
            if e.kind in _RELATION_EDGES and e.source in core and e.target in core
        )
        return _Selection(
            nodes=frozenset(core),
            edges=edges,
            findings=_findings_on(preview, core, edges),
            cluster=cluster,
        )
    nodes = set(core)
    nodes.update(n.id for n in preview.nodes if n.owner in core)
    for edge in preview.edges:
        if edge.kind in _IDENTITY_EDGES and edge.declared_by == cluster.id:
            nodes.add(edge.source)
            source = preview.node(edge.source)
            if source is not None and source.owner is not None:
                nodes.add(source.owner)
    edges = _closed(preview.edges, nodes)
    return _Selection(
        nodes=frozenset(nodes),
        edges=edges,
        findings=_findings_on(preview, nodes, edges),
        cluster=cluster,
    )


def _attribute(preview: MergePreview, attribute: str) -> _Selection:
    owner = preview.node(attribute)
    assert owner is not None and owner.owner is not None
    into = owner.owner
    incoming = [e for e in preview.edges if e.target == attribute]
    rows = {attribute, *(e.source for e in incoming)}
    nodes = set(rows)
    for row in rows:
        node = preview.node(row)
        if node is not None and node.owner is not None:
            nodes.add(node.owner)
    edges = _closed(preview.edges, nodes)
    cluster = next(
        (
            c
            for c in preview.clusters
            if c.kind == "vertex" and c.into is not None and f"merged:{c.into}" == into
        ),
        None,
    )
    return _Selection(
        nodes=frozenset(nodes),
        edges=edges,
        findings=_findings_on(preview, rows, tuple(incoming)),
        cluster=cluster,
    )


def _rest(preview: MergePreview) -> _Selection | None:
    positions = _rest_findings(preview)
    if not positions:
        return None
    named = {n for i in positions for n in preview.findings[i].nodes}
    nodes = set(named)
    for node_id in named:
        node = preview.node(node_id)
        if node is not None and node.owner is not None:
            nodes.add(node.owner)
    nodes.update(n.id for n in preview.nodes if n.owner in nodes)
    return _Selection(
        nodes=frozenset(n for n in nodes if preview.node(n) is not None),
        edges=_closed(preview.edges, nodes),
        findings=tuple(positions),
        cluster=None,
    )


def _rest_findings(preview: MergePreview) -> list[int]:
    held: set[int] = set()
    for cluster in preview.clusters:
        held.update(_cluster(preview, cluster).findings)
        if cluster.kind == "vertex":
            for node_id in _merged_attributes(preview, cluster):
                held.update(_attribute(preview, node_id).findings)
    return [i for i in range(len(preview.findings)) if i not in held]


# ── helpers ─────────────────────────────────────────────────────────────────


def _core(cluster: PreviewCluster) -> set[str]:
    core = {f"left:{m}" for m in cluster.left} | {f"right:{m}" for m in cluster.right}
    if cluster.into is not None:
        core.add(f"merged:{cluster.into}")
    return core


def _ordered_clusters(preview: MergePreview) -> list[PreviewCluster]:
    return [c for c in preview.clusters if c.kind == "vertex"] + [
        c for c in preview.clusters if c.kind == "relation"
    ]


def _merged_attributes(preview: MergePreview, cluster: PreviewCluster) -> list[str]:
    if cluster.into is None:
        return []
    return [n.id for n in preview.attributes_of(f"merged:{cluster.into}")]


def _nontrivial(preview: MergePreview, cluster: PreviewCluster, attribute: str) -> bool:
    """Whether anything but a same-name fusion decides *attribute*."""
    incoming = [e for e in preview.edges if e.target == attribute]
    if any(e.kind != "fusion" and e.effective for e in incoming):
        return True
    name = attribute.rsplit(".", 1)[-1]
    identity = cluster.identity
    if identity is not None and (
        name == identity.digest_field
        or any(name in b.fields for b in (*identity.branches, *identity.derive))
    ):
        return True
    sides = set()
    for edge in incoming:
        source = preview.node(edge.source)
        if source is not None and source.side is not None:
            sides.add(source.side)
    if len(sides) > 1:
        return True
    return any(attribute in f.nodes for f in preview.findings)


def _closed(edges: Iterable[PreviewEdge], nodes: set[str]) -> tuple[PreviewEdge, ...]:
    return tuple(e for e in edges if e.source in nodes and e.target in nodes)


def _findings_on(
    preview: MergePreview, nodes: Iterable[str], edges: Iterable[PreviewEdge]
) -> tuple[int, ...]:
    node_set = set(nodes)
    edge_ids = {e.id for e in edges}
    return tuple(
        i
        for i, finding in enumerate(preview.findings)
        if _touches(finding, node_set, edge_ids)
    )


def _touches(finding: MergeFinding, nodes: set[str], edges: set[str]) -> bool:
    return bool(set(finding.nodes) & nodes or set(finding.edges) & edges)


__all__ = ["REST", "AttributeScope", "finding_positions", "focus", "focus_targets"]
