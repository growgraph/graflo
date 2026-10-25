"""Draw one equivalence as four small graphs, each about one thing.

An equivalence says several things at once, and one picture of all of them is
a tangle of arrows. Each *view* here draws one of them, as a graph whose
partitions read left to right:

* ``members`` -- which types become the merged type: left members, the merged
  type, right members. A member a vocabulary joins, rather than one the
  equivalence names, has a dashed edge.
* ``attributes`` -- where each member attribute lands on the merged type: a
  dark edge for a declared equivalence, a blue one for a vocabulary rename, a
  thin grey one for a name kept as it is, a dotted one for a mapping a later
  declaration overrode.
* ``identity`` -- how a record is keyed: raw fields, the branches a record
  tries in order (``#1``, ``#2``, ...), and the key they feed. An edge into a
  branch is labelled with the transform or the tag it reads with.
* ``lookups`` -- each member's own key, kept on the merged type as a lookup;
  the edge is labelled with the key space.

Attributes and raw fields are small nodes inside a box named after their type
or resource; boxes are filled by side. A figure carries no title, legend or
note, so it can be embedded anywhere: :func:`plot_equivalences` writes what
they would say -- the declaration, the identity in words, the findings, a
caption per view -- to ``summary.md``, and all of it to ``index.json``.
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from fnmatch import fnmatch
from itertools import pairwise
from pathlib import Path
from typing import Any, Literal

import networkx as nx

from graflo.architecture.evolution.preview import (
    MergeFinding,
    MergePreview,
    PreviewCluster,
    PreviewNode,
)
from graflo.architecture.evolution.preview_focus import resolve_target
from graflo.architecture.evolution.preview_identity import (
    PreviewBranch,
    PreviewDemotion,
)
from graflo.plot.render import draw, sanitize_id, to_dot

#: What an equivalence is drawn as, one figure each.
View = Literal["members", "attributes", "identity", "lookups"]
VIEWS: tuple[View, ...] = ("members", "attributes", "identity", "lookups")

#: Version of the ``index.json`` layout :func:`plot_equivalences` writes.
INDEX_VERSION = 2

#: Box fill per side; light, so a figure prints and sits in other documents.
SIDE_FILL: dict[str, str] = {
    "left": "#E6EFF5",
    "right": "#E7F3EA",
    "merged": "#FFF1DB",
    "": "#F0F0F0",
}
_BRANCH_FILL = "#FFF8E8"
_INK = "#2C3E50"

#: Edge styles; each view uses at most three.
STYLE: dict[str, dict[str, str]] = {
    "declared": {"color": _INK, "penwidth": "1.3"},
    "vocabulary": {"color": "#1F5FBF", "penwidth": "1.3"},
    "joined": {"color": "#1F5FBF", "penwidth": "1.3", "style": "dashed"},
    "kept": {"color": "#A8A8A8", "penwidth": "0.7"},
    "overridden": {"color": "#B0B0B0", "penwidth": "0.9", "style": "dotted"},
    "source": {"color": "#B9770E", "penwidth": "1.1"},
    "key": {"color": _INK, "penwidth": "1.1"},
    "lookup": {"color": "#8E5A2B", "penwidth": "1.1"},
}

#: What each view shows, for a reader of ``summary.md``.
CAPTION: dict[View, str] = {
    "members": (
        "Members. The types that become the merged type; a dashed edge marks a "
        "member a vocabulary joins rather than one the equivalence names."
    ),
    "attributes": (
        "Attributes. Where each member attribute lands on the merged type: dark, "
        "a declared equivalence; blue, a vocabulary rename; thin grey, the name "
        "kept as it is; dotted, a mapping a later declaration overrode."
    ),
    "identity": (
        "Identity. The raw fields each branch reads, labelled with the transform "
        "or the tag; a record tries the branches in order, #1 first, and the "
        "first it completes keys it."
    ),
    "lookups": (
        "Lookups. Each member's own key, kept on the merged type as a lookup "
        "in the key space the edge names."
    ),
}


def _plain(text: str) -> str:
    """*text* for a plain (non-record) DOT label, where only a backslash is special."""
    return text.replace("\\", "\\\\")


_FIELD_NODE: dict[str, str] = {
    "shape": "box",
    "style": "rounded,filled",
    "fillcolor": "white",
    "color": "#9AA5B1",
    "height": "0.3",
    "fontsize": "10",
}


@dataclass
class _Figure:
    """A view under construction: nodes, their boxes, and edges."""

    graph: nx.MultiDiGraph
    boxes: dict[str, tuple[str, str, list[str]]] = field(default_factory=dict)
    ranks: list[list[str]] = field(default_factory=list)
    taken: dict[str, str] = field(default_factory=dict)

    def node(self, node_id: str, label: str, **attrs: Any) -> str:
        name = self.taken.get(node_id) or sanitize_id(node_id, self.taken)
        if name not in self.graph:
            self.graph.add_node(name, label=label, preview_id=node_id, **attrs)
        return name

    def boxed(
        self, node_id: str, label: str, box: str, box_label: str, side: str
    ) -> str:
        name = self.node(node_id, _plain(label), **_FIELD_NODE)
        _label, _side, members = self.boxes.setdefault(box, (box_label, side, []))
        if name not in members:
            members.append(name)
        return name

    def edge(
        self, tail: str, head: str, style: str, *, back: bool = False, **attrs: Any
    ) -> None:
        """An edge; *back* draws it head-first so a right-side source stays on the right."""
        attrs = {**STYLE[style], "kind": style, **attrs}
        if back:
            self.graph.add_edge(head, tail, dir="back", **attrs)
        else:
            self.graph.add_edge(tail, head, **attrs)


def _new_figure() -> _Figure:
    graph = nx.MultiDiGraph()
    graph.graph["graph"] = {
        "rankdir": "LR",
        "splines": "spline",
        "fontname": "Helvetica",
        "nodesep": "0.15",
        "ranksep": "0.9",
    }
    graph.graph["node"] = {"fontname": "Helvetica", "fontsize": "11"}
    graph.graph["edge"] = {"fontname": "Helvetica", "fontsize": "9", "arrowsize": "0.7"}
    return _Figure(graph=graph)


# ── the views ───────────────────────────────────────────────────────────────


def _cluster(preview: MergePreview) -> PreviewCluster:
    if len(preview.clusters) != 1:
        raise ValueError("focus the preview on one equivalence before drawing it")
    return preview.clusters[0]


def _side_of(node: PreviewNode | None) -> str:
    if node is None or node.side is None:
        return "merged"
    return node.side


def _attribute(fig: _Figure, nodes: dict[str, PreviewNode], node_id: str) -> str:
    node = nodes[node_id]
    owner = nodes.get(node.owner or "")
    return fig.boxed(
        node_id,
        node.name,
        box=node.owner or node_id,
        box_label=owner.name if owner else "",
        side=_side_of(node) if node.kind != "field" else (node.side or ""),
    )


def _members(preview: MergePreview, cluster: PreviewCluster) -> _Figure | None:
    fig = _new_figure()
    nodes = {n.id: n for n in preview.nodes}
    merged_id = f"merged:{cluster.into}"
    merged = fig.node(
        merged_id,
        _plain(cluster.into or ""),
        shape="box",
        style="rounded,filled",
        fillcolor=SIDE_FILL["merged"],
        color=_INK,
    )
    declared = {
        "left": set(cluster.declared_left),
        "right": set(cluster.declared_right),
    }
    for side, members in (("left", cluster.left), ("right", cluster.right)):
        for member in members:
            node_id = f"{side}:{member}"
            node = nodes.get(node_id)
            ghost = node is not None and node.kind == "ghost"
            tail = fig.node(
                node_id,
                _plain(member),
                shape="box",
                style="rounded,filled,dashed" if ghost else "rounded,filled",
                fillcolor=SIDE_FILL[side],
                color=_INK,
            )
            style = (
                "declared"
                if member in declared[side] or not declared[side]
                else "joined"
            )
            fig.edge(tail, merged, style, back=side == "right")
    return fig


def _attributes(preview: MergePreview, cluster: PreviewCluster) -> _Figure | None:
    nodes = {n.id: n for n in preview.nodes}
    merged_id = f"merged:{cluster.into}"
    edges = [
        e
        for e in preview.edges
        if e.kind in ("property_equivalence", "property_map", "fusion")
        and nodes.get(e.target) is not None
        and nodes[e.target].owner == merged_id
        and e.source in nodes
    ]
    if not edges:
        return None
    fig = _new_figure()
    # Merged attributes first, so the merged box lists them in schema order.
    for edge in edges:
        _attribute(fig, nodes, edge.target)
    for edge in edges:
        source = nodes[edge.source]
        tail = _attribute(fig, nodes, edge.source)
        head = _attribute(fig, nodes, edge.target)
        if not edge.effective:
            style = "overridden"
        elif edge.kind == "fusion":
            style = "kept"
        elif edge.kind == "property_map":
            style = "vocabulary"
        else:
            style = "declared"
        fig.edge(tail, head, style, back=source.side == "right")
    return fig


def _own_name(preview: MergePreview, owner: str, merged_attribute: str) -> str | None:
    """The attribute of *owner* that lands on *merged_attribute*, if any."""
    return next(
        (
            e.source.rsplit(".", 1)[-1]
            for e in preview.edges
            if e.target == merged_attribute and e.source.startswith(owner + ".")
        ),
        None,
    )


def _identity(preview: MergePreview, cluster: PreviewCluster) -> _Figure | None:
    identity = cluster.identity
    if identity is None or identity.mode == "carried":
        return None
    nodes = {n.id: n for n in preview.nodes}
    merged_id = f"merged:{cluster.into}"
    fig = _new_figure()

    branch_node: dict[str, str] = {}
    derive_node: dict[str, str] = {}
    for branch in (*identity.derive, *identity.branches):
        node = fig.node(
            f"{cluster.id}#branch:{branch.position}:{branch.id}",
            _branch_label(branch),
            shape="box",
            style="rounded,filled",
            fillcolor=_BRANCH_FILL,
            color="#B9770E",
        )
        (derive_node if branch.position == 0 else branch_node)[branch.id] = node
    # One rank, chained by invisible edges, so the branches read #1 first.
    fig.ranks.append(list(branch_node.values()))
    order = list(branch_node.values())
    for upper, lower in pairwise(order):
        fig.graph.add_edge(upper, lower, style="invis", kind="order")

    # Raw fields into derived and local-key branches, in branch order so each
    # resource lists its fields the way the branches read them.
    position = {b.id: b.position for b in (*identity.derive, *identity.branches)}
    sources = sorted(
        (
            e
            for e in preview.edges
            if e.kind in ("derivation", "local_key") and e.declared_by == cluster.id
        ),
        key=lambda e: position.get(e.target.rsplit(".", 1)[-1], 0),
    )
    for edge in sources:
        target = nodes.get(edge.target)
        if target is None:
            continue
        head = derive_node.get(target.name) or branch_node.get(target.name)
        if head is None:
            continue
        tail = _attribute(fig, nodes, edge.source)
        member = edge.id.partition("#")[2]
        label = f"{member}: {edge.label}" if member else edge.label
        fig.edge(tail, head, "source", label=_plain(label))

    # Property branches read member attributes; a derived attribute feeds the
    # property branch keying on it.
    for branch in identity.branches:
        if branch.kind != "raw":
            continue
        for name in branch.fields:
            if name in derive_node:
                fig.edge(derive_node[name], branch_node[branch.id], "key")
                continue
            for source in branch.sources:
                owner = f"{source.side}:{source.member}"
                own = _own_name(preview, owner, f"{merged_id}.{name}")
                if own is None or f"{owner}.{own}" not in nodes:
                    continue
                tail = _attribute(fig, nodes, f"{owner}.{own}")
                fig.edge(tail, branch_node[branch.id], "source")

    # Branches into the key: the digest of a funnel, or the attribute itself.
    keys: list[tuple[PreviewBranch, list[str]]] = []
    if identity.digest_field is not None:
        keys = [(b, [identity.digest_field]) for b in identity.branches]
    else:
        keys = [(b, b.fields) for b in identity.branches]
    for branch, names in keys:
        for name in names:
            key_id = f"{merged_id}.{name}"
            if key_id not in nodes:
                continue
            fig.edge(branch_node[branch.id], _attribute(fig, nodes, key_id), "key")
    return fig


def _branch_label(branch: PreviewBranch) -> str:
    """The branch's DOT label: its position and id, then its value format if any."""
    head = (
        f"derive {branch.id}"
        if branch.position == 0
        else f"#{branch.position} {branch.id}"
    )
    lines = [head]
    if branch.kind == "local_key" and branch.sep is not None:
        lines.append(f"tag{branch.sep}value")
    elif branch.kind == "raw" and branch.fields != [branch.id]:
        lines.append(", ".join(branch.fields))
    return "\\n".join(_plain(line) for line in lines)


def _demoted(
    preview: MergePreview, cluster: PreviewCluster
) -> list[tuple[PreviewDemotion, str, str]]:
    """``(demotion, member's own field, merged field)`` for every demoted key field."""
    identity = cluster.identity
    if identity is None:
        return []
    out: list[tuple[PreviewDemotion, str, str]] = []
    for demotion in identity.demoted:
        renamed = dict(zip(demotion.fields, demotion.merged_fields, strict=False))
        owner = f"{demotion.side}:{demotion.member}"
        for field_name in demotion.fields:
            merged = renamed.get(field_name, field_name)
            own = _own_name(preview, owner, f"merged:{cluster.into}.{merged}")
            out.append((demotion, own or field_name, merged))
    return out


def _lookups(preview: MergePreview, cluster: PreviewCluster) -> _Figure | None:
    demoted = _demoted(preview, cluster)
    nodes = {n.id: n for n in preview.nodes}
    if not demoted:
        return None
    fig = _new_figure()
    merged_id = f"merged:{cluster.into}"
    for _demotion, _own, merged in demoted:
        if f"{merged_id}.{merged}" in nodes:
            _attribute(fig, nodes, f"{merged_id}.{merged}")
    for demotion, own, merged in demoted:
        source_id = f"{demotion.side}:{demotion.member}.{own}"
        target_id = f"{merged_id}.{merged}"
        if source_id not in nodes or target_id not in nodes:
            continue
        fig.edge(
            _attribute(fig, nodes, source_id),
            _attribute(fig, nodes, target_id),
            "lookup",
            label=_plain(demotion.key_space),
            back=demotion.side == "right",
        )
    return fig


_BUILDERS = {
    "members": _members,
    "attributes": _attributes,
    "identity": _identity,
    "lookups": _lookups,
}


def _build(preview: MergePreview, target: str | int, view: View) -> _Figure | None:
    focused = preview.focus(target)
    cluster = _cluster(focused)
    if cluster.kind == "relation" and view != "members":
        return None
    return _BUILDERS[view](focused, cluster)


def build_equivalence_view(
    preview: MergePreview, target: str | int, view: View
) -> nx.MultiDiGraph | None:
    """One view of one equivalence as a networkx graph, or ``None`` when it shows nothing.

    Every node carries the preview id it stands for as ``preview_id``, and
    every edge its style as ``kind``. The ``boxes`` graph attribute maps a
    box id to ``(label, side, [node, ...])``; ``ranks`` lists node groups
    drawn in one rank, in order.

    Args:
        preview: The whole preview, from ``preview_merge`` or ``declared_preview``.
        target: A cluster id, or a position in the op's ``vertex_equivalences``.
        view: One of :data:`VIEWS`.

    Raises:
        KeyError: *target* names no equivalence.
    """
    fig = _build(preview, target, view)
    if fig is None:
        return None
    fig.graph.graph["boxes"] = fig.boxes
    fig.graph.graph["ranks"] = fig.ranks
    return fig.graph


def _groups(boxes: dict[str, Any], ranks: list[list[str]], dot: Any) -> None:
    for number, members in enumerate(ranks):
        dot.add_subgraph(members, name=f"rank_{number}", rank="same")
    for box, (label, side, members) in boxes.items():
        dot.add_subgraph(
            members,
            name=f"cluster_{sanitize_id(box)}",
            label=_plain(label),
            style="rounded,filled",
            fillcolor=SIDE_FILL.get(side, SIDE_FILL[""]),
            color="#9AA5B1",
            fontsize="11",
        )


def _draw(
    graph: nx.MultiDiGraph,
    path: Path,
    *,
    output_format: str | None,
    prog: str,
    dpi: int | None,
) -> Path:
    # Boxes and ranks are drawing data, not Graphviz attributes: keep them out of DOT.
    boxes = graph.graph.pop("boxes", {})
    ranks = graph.graph.pop("ranks", [])
    dot = to_dot(graph)
    graph.graph.update(boxes=boxes, ranks=ranks)
    _groups(boxes, ranks, dot)
    return draw(dot, path, output_format=output_format, prog=prog, dpi=dpi)


# ── targets, files, index and summary ───────────────────────────────────────


def _clusters(preview: MergePreview) -> list[PreviewCluster]:
    return [c for c in preview.clusters if c.kind == "vertex"] + [
        c for c in preview.clusters if c.kind == "relation"
    ]


def select_targets(preview: MergePreview, patterns: Sequence[str]) -> list[str]:
    """The cluster ids *patterns* select, in reading order; all of them when none.

    A pattern is a merged name, a position in ``vertex_equivalences``, or an
    fnmatch pattern on the cluster id.
    """
    clusters = _clusters(preview)
    if not patterns:
        return [c.id for c in clusters]
    chosen: set[str] = set()
    for pattern in patterns:
        resolved = resolve_target(preview, pattern) if pattern.isdigit() else None
        for cluster in clusters:
            if (
                cluster.into == pattern
                or cluster.id == resolved
                or fnmatch(cluster.id, pattern)
            ):
                chosen.add(cluster.id)
    return [c.id for c in clusters if c.id in chosen]


def _slug(stem: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", stem).strip("_") or "equivalence"


def plot_equivalence(
    preview: MergePreview,
    target: str | int,
    directory: str | os.PathLike[str],
    *,
    views: Sequence[View] = VIEWS,
    output_format: str = "svg",
    stem: str | None = None,
    prog: str = "dot",
    dpi: int | None = None,
) -> dict[View, Path]:
    """Draw *views* of one equivalence into *directory*; a view with nothing to show is skipped.

    Files are ``<stem>.<view>.<format>``; *stem* defaults to
    ``<kind>-<merged name>``, e.g. ``vertex-Machine.identity.svg``.

    Args:
        preview: The whole preview, from ``preview_merge`` or ``declared_preview``.
        target: A cluster id, or a position in the op's ``vertex_equivalences``.
        directory: Where to write; created if missing.
        views: Which views to draw.
        output_format: ``svg``, ``png``, ``pdf`` or ``dot``.
        stem: File name stem.
        prog: Graphviz layout program.
        dpi: Raster resolution, for ``png``.

    Returns:
        The path written per view.

    Raises:
        KeyError: *target* names no equivalence.
        RuntimeError: Rendering is unavailable or failed; the message says why.
    """
    out = Path(directory)
    focused = preview.focus(target)
    cluster = _cluster(focused)
    stem = stem or _slug(f"{cluster.kind}-{cluster.into or cluster.id}")
    written: dict[View, Path] = {}
    for view in views:
        graph = build_equivalence_view(preview, target, view)
        if graph is None:
            continue
        written[view] = _draw(
            graph,
            out / f"{stem}.{view}.{output_format}",
            output_format=output_format,
            prog=prog,
            dpi=dpi,
        )
    return written


def plot_equivalences(
    preview: MergePreview,
    directory: str | os.PathLike[str],
    *,
    select: Sequence[str] = (),
    views: Sequence[View] = VIEWS,
    output_format: str = "svg",
    prog: str = "dot",
    dpi: int | None = None,
) -> Path:
    """Draw every selected equivalence into *directory*, with ``index.json`` and ``summary.md``.

    Args:
        preview: The whole preview, after its outcome is folded in, so the
            finding positions in the index match ``preview.findings``.
        directory: Where to write; created if missing.
        select: Patterns restricting the equivalences (see :func:`select_targets`).
        views: Which views to draw.
        output_format: ``svg``, ``png``, ``pdf`` or ``dot``.
        prog: Graphviz layout program.
        dpi: Raster resolution, for ``png``.

    Returns:
        The path of ``index.json``.
    """
    from graflo.architecture.evolution.preview_focus import finding_positions

    out = Path(directory)
    out.mkdir(parents=True, exist_ok=True)
    taken: set[str] = set()
    entries: list[dict[str, Any]] = []
    held: set[int] = set()
    for cluster in _clusters(preview):
        held.update(finding_positions(preview, cluster.id))
    for target in select_targets(preview, select):
        cluster = next(c for c in preview.clusters if c.id == target)
        stem, n = _slug(f"{cluster.kind}-{cluster.into or cluster.id}"), 2
        unique = stem
        while unique.lower() in taken:
            unique, n = f"{stem}-{n}", n + 1
        taken.add(unique.lower())
        files = plot_equivalence(
            preview,
            target,
            out,
            views=views,
            output_format=output_format,
            stem=unique,
            prog=prog,
            dpi=dpi,
        )
        entries.append(
            {
                **_describe(preview, cluster),
                "views": {view: path.name for view, path in files.items()},
                "findings": [
                    _finding(preview, i) for i in finding_positions(preview, target)
                ],
            }
        )
    rest = [_finding(preview, i) for i in range(len(preview.findings)) if i not in held]
    index = out / "index.json"
    index.write_text(
        json.dumps(
            {
                "version": INDEX_VERSION,
                "left": preview.left_name,
                "right": preview.right_name,
                "outcome": preview.outcome.model_dump(mode="json"),
                "format": output_format,
                "summary": "summary.md",
                "equivalences": entries,
                "other_findings": rest,
            },
            indent=2,
        )
        + "\n"
    )
    (out / "summary.md").write_text(_summary(preview, entries, rest))
    return index


def _finding(preview: MergePreview, position: int) -> dict[str, Any]:
    finding: MergeFinding = preview.findings[position]
    return {
        "index": position,
        "severity": finding.severity,
        "kind": finding.kind,
        "message": finding.message,
    }


def _describe(preview: MergePreview, cluster: PreviewCluster) -> dict[str, Any]:
    """Everything a figure of *cluster* does not draw, as data."""
    declared_left = cluster.declared_left or cluster.left
    declared_right = cluster.declared_right or cluster.right
    out: dict[str, Any] = {
        "target": cluster.id,
        "kind": cluster.kind,
        "into": cluster.into,
        "declared_into": cluster.declared_into,
        "declarations": [
            f"{cluster.kind}_equivalences[{p}]" for p in cluster.declarations
        ],
        "synthesized": cluster.synthesized,
        "members": {"left": cluster.left, "right": cluster.right},
        "declared": {"left": declared_left, "right": declared_right},
        "joined_by_vocabulary": [m for m in cluster.left if m not in declared_left]
        + [m for m in cluster.right if m not in declared_right],
        "allow": cluster.allow,
        "identity": _identity_text(cluster),
        "lookups": [
            {
                "member": f"{d.side}:{d.member}",
                "field": own,
                "lookup": merged,
                "key_space": d.key_space,
                "tags": d.tags,
            }
            for d, own, merged in _demoted(preview, cluster)
        ],
    }
    if cluster.identity is not None and cluster.identity.derive_at:
        out["derive_at"] = cluster.identity.derive_at
    return out


def _identity_text(cluster: PreviewCluster) -> str | None:
    identity = cluster.identity
    if identity is None:
        return None
    if identity.mode == "carried":
        return (
            "No identity declared: the merged type keeps the members' key, "
            "which they must agree on."
        )
    if identity.mode == "funnel":
        text = (
            f"Keyed by a funnel: a record tries {len(identity.branches)} branches "
            f"in order and the first it completes is digested into "
            f"`{identity.digest_field}`."
        )
    else:
        text = f"Keyed by `{', '.join(identity.branches[0].fields)}`."
    if identity.retire == "demote":
        text += " Each member's own key becomes a lookup."
    elif identity.retire == "keep":
        text += " Each member's own key is kept."
    return text


def _summary(
    preview: MergePreview, entries: list[dict[str, Any]], rest: list[dict[str, Any]]
) -> str:
    outcome = preview.outcome
    verdict = {
        "merged": "goes through",
        "refused": f"is refused ({outcome.error_type})",
        "not_attempted": "not attempted",
    }[outcome.status]
    lines = [
        f"# {preview.left_name} + {preview.right_name}",
        "",
        f"The merge {verdict}.",
    ]
    if outcome.status == "not_attempted" and not preview.findings:
        lines.append("The declarations were not checked against the manifests.")
    for entry in entries:
        left = " + ".join(entry["members"]["left"])
        right = " + ".join(entry["members"]["right"])
        lines += ["", f"## {entry['into']}: {left} ~ {right}", ""]
        if entry["declarations"]:
            lines.append(f"- Declared by `{'`, `'.join(entry['declarations'])}`.")
        elif entry["synthesized"]:
            lines.append("- Declared by merge itself (`union_right`).")
        if entry["joined_by_vocabulary"]:
            lines.append(
                f"- Joined by a vocabulary: {', '.join(entry['joined_by_vocabulary'])}."
            )
        if entry["declared_into"] and entry["declared_into"] != entry["into"]:
            lines.append(
                f"- `into: {entry['declared_into']}` resolves to `{entry['into']}`."
            )
        if entry["allow"]:
            lines.append(f"- Allows: {', '.join(entry['allow'])}.")
        if entry["identity"]:
            lines.append(f"- {entry['identity']}")
        for resource, path in entry.get("derive_at", {}).items():
            lines.append(f"- `{resource}` derives at step {path}.")
        for lookup in entry["lookups"]:
            lines.append(
                f"- `{lookup['member'].split(':', 1)[1]}.{lookup['field']}` is kept as "
                f"`{lookup['lookup']}`, a lookup in key space `{lookup['key_space']}`."
            )
        for view in VIEWS:
            if view in entry["views"]:
                lines += [
                    "",
                    f"![{view}]({entry['views'][view]})",
                    "",
                    f"*{CAPTION[view]}*",
                ]
        lines += _findings_md(entry["findings"])
    if rest:
        lines += ["", "## Findings no equivalence holds"]
        lines += _findings_md(rest)
    return "\n".join(lines) + "\n"


def _findings_md(findings: list[dict[str, Any]]) -> list[str]:
    if not findings:
        return []
    order = {"refusal": 0, "possible": 1, "note": 2}
    ranked = sorted(findings, key=lambda f: (order[f["severity"]], f["kind"]))
    return ["", "Findings:", ""] + [
        f"{n}. **{f['severity']}** `{f['kind']}`: {' '.join(f['message'].split())}"
        for n, f in enumerate(ranked, start=1)
    ]


__all__ = [
    "CAPTION",
    "INDEX_VERSION",
    "SIDE_FILL",
    "STYLE",
    "VIEWS",
    "View",
    "build_equivalence_view",
    "plot_equivalence",
    "plot_equivalences",
    "select_targets",
]
