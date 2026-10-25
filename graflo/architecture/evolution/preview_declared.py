"""A merge preview of what an op declares, built without the manifests.

:func:`declared_preview` reads only the equivalences: each member is a class
node whose rows are the properties the declaration names, the merged class
carries the ``into`` names, branch attributes and digest, and each identity
source is a resource node with the raw fields it reads. Nothing the manifests
settle is shown -- no other attributes, no resource sides, no vocabulary,
no demoted keys, no findings -- so the outcome stays ``not_attempted``.

The result is an ordinary
:class:`~graflo.architecture.evolution.preview.MergePreview`, so
:meth:`~graflo.architecture.evolution.preview.MergePreview.focus` and the
plots take it as they take a full one.
"""

from __future__ import annotations

from .equivalence import Side, SubjectScope, subject
from .ops import MergeManifestsOp, RelationEquivalence, VertexEquivalence
from .preview import (
    EdgeKind,
    MergePreview,
    NodeKind,
    PreviewCluster,
    PreviewEdge,
    PreviewNode,
)
from .preview_identity import PreviewBranch, declared_identity

_SIDES: tuple[Side, ...] = ("left", "right")


class _Graph:
    def __init__(self) -> None:
        self.nodes: dict[str, PreviewNode] = {}
        self.edges: dict[str, PreviewEdge] = {}

    def node(self, node: PreviewNode) -> str:
        self.nodes.setdefault(node.id, node)
        return node.id

    def attribute(
        self,
        scope: SubjectScope,
        owner: str,
        name: str,
        *,
        side: Side | None,
        identity: bool,
    ) -> str:
        node_id = subject(scope, owner, name)
        existing = self.nodes.get(node_id)
        if existing is not None:
            if identity and not existing.identity:
                self.nodes[node_id] = existing.model_copy(update={"identity": True})
            return node_id
        return self.node(
            PreviewNode(
                id=node_id,
                kind="field" if scope == "resource" else "attribute",
                name=name,
                side=side,
                owner=subject(scope, owner),
                identity=identity,
            )
        )

    def edge(
        self,
        source: str,
        target: str,
        kind: EdgeKind,
        *,
        label: str = "",
        declared_by: str = "",
        qualifier: str = "",
    ) -> None:
        edge_id = f"{kind}:{source}->{target}" + (f"#{qualifier}" if qualifier else "")
        self.edges.setdefault(
            edge_id,
            PreviewEdge(
                id=edge_id,
                source=source,
                target=target,
                kind=kind,
                label=label,
                declared_by=declared_by,
                layer="equivalence" if kind == "property_equivalence" else None,
            ),
        )


def declared_preview(
    op: MergeManifestsOp | VertexEquivalence | RelationEquivalence,
) -> MergePreview:
    """The declaration graph of *op* alone.

    Args:
        op: A merge op, or one equivalence (treated as position 0 of an op).

    Returns:
        A preview with one cluster per equivalence, ids and positions as
        :func:`~graflo.architecture.evolution.preview.preview_merge` numbers
        them for an op whose every declaration forms its own group.
    """
    if isinstance(op, VertexEquivalence):
        op = MergeManifestsOp(vertex_equivalences=[op])
    elif isinstance(op, RelationEquivalence):
        op = MergeManifestsOp(relation_equivalences=[op])
    graph = _Graph()
    clusters = [
        _vertex(graph, op, position, declaration)
        for position, declaration in enumerate(op.vertex_equivalences)
    ]
    clusters += [
        _relation(graph, op, position, declaration)
        for position, declaration in enumerate(op.relation_equivalences)
    ]
    return MergePreview(
        name_conflict=op.name_conflict,
        nodes=list(graph.nodes.values()),
        edges=list(graph.edges.values()),
        clusters=clusters,
    )


def _merged_name(
    op: MergeManifestsOp, declaration: VertexEquivalence | RelationEquivalence
) -> str:
    """``into``, else the first canonical name the op's maps give a member."""
    if declaration.into:
        return declaration.into
    for side in _SIDES:
        for scope in (side, "both"):
            vocabulary = op.canonical_maps.get(scope)
            if vocabulary is None:
                continue
            names = (
                vocabulary.vertices
                if isinstance(declaration, VertexEquivalence)
                else vocabulary.relations
            )
            for member in declaration.members(side):
                if member in names:
                    return names[member]
    return " ~ ".join("+".join(declaration.members(side)) for side in _SIDES)


def _members(
    graph: _Graph,
    declaration: VertexEquivalence | RelationEquivalence,
    merged: str,
    cluster_id: str,
    kind: NodeKind,
) -> None:
    for side in _SIDES:
        for member in declaration.members(side):
            member_id = graph.node(
                PreviewNode(id=subject(side, member), kind=kind, name=member, side=side)
            )
            graph.edge(member_id, merged, "member", declared_by=cluster_id)


def _vertex(
    graph: _Graph, op: MergeManifestsOp, position: int, declaration: VertexEquivalence
) -> PreviewCluster:
    cluster_id = f"vertex-cluster-{position}"
    into = _merged_name(op, declaration)
    merged = graph.node(
        PreviewNode(id=subject("merged", into), kind="merged", name=into)
    )
    _members(graph, declaration, merged, cluster_id, "class")

    for equivalence in declaration.properties:
        target = graph.attribute(
            "merged", into, equivalence.into, side=None, identity=False
        )
        for side in _SIDES:
            written = getattr(equivalence, side)
            if written is None:
                continue
            per_member = (
                written
                if isinstance(written, dict)
                else {m: written for m in declaration.members(side)}
            )
            for member, field in per_member.items():
                source = graph.attribute(side, member, field, side=side, identity=False)
                graph.edge(source, target, "property_equivalence", declared_by=into)

    identity = declared_identity(declaration)
    for branch in (*identity.derive, *identity.branches):
        for field in branch.fields:
            graph.attribute("merged", into, field, side=None, identity=True)
        if branch.kind != "raw":
            _sources(graph, branch, into, cluster_id)
    if identity.digest_field is not None:
        graph.attribute("merged", into, identity.digest_field, side=None, identity=True)

    return PreviewCluster(
        id=cluster_id,
        kind="vertex",
        into=into,
        declared_into=declaration.into,
        left=declaration.left_members,
        right=declaration.right_members,
        declarations=[position],
        declared_left=declaration.left_members,
        declared_right=declaration.right_members,
        allow=list(declaration.allow),
        declared_identity=declaration.identity is not None,
        derived_identity=declaration.has_derivation,
        identity=identity,
    )


def _sources(graph: _Graph, branch: PreviewBranch, into: str, cluster_id: str) -> None:
    target = subject("merged", into, branch.id)
    kind: EdgeKind = "derivation" if branch.kind == "derived" else "local_key"
    for source in branch.sources:
        if source.resource is None:
            continue
        graph.node(
            PreviewNode(
                id=subject("resource", source.resource),
                kind="resource",
                name=source.resource,
            )
        )
        fields = source.input if branch.kind == "derived" else [source.field or ""]
        for name in fields:
            if not name:
                continue
            field_id = graph.attribute(
                "resource", source.resource, name, side=None, identity=False
            )
            graph.edge(
                field_id,
                target,
                kind,
                label=source.label,
                declared_by=cluster_id,
                qualifier=source.member or "",
            )


def _relation(
    graph: _Graph, op: MergeManifestsOp, position: int, declaration: RelationEquivalence
) -> PreviewCluster:
    cluster_id = f"relation-cluster-{position}"
    into = _merged_name(op, declaration)
    merged = graph.node(
        PreviewNode(id=subject("merged", into), kind="merged", name=into)
    )
    _members(graph, declaration, merged, cluster_id, "relation")
    return PreviewCluster(
        id=cluster_id,
        kind="relation",
        into=into,
        declared_into=declaration.into,
        left=declaration.left_members,
        right=declaration.right_members,
        declarations=[position],
        declared_left=declaration.left_members,
        declared_right=declaration.right_members,
    )


__all__ = ["declared_preview"]
