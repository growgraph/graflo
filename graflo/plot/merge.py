"""Draw a merge preview: the declaration graph, with its conflicts marked.

The picture a
:class:`~graflo.architecture.evolution.preview.MergePreview` is asking to be:
each side's classes down its own column, the merged names between them, and
every declaration as an edge. Classes are drawn as records — one field per
attribute, each its own Graphviz port — so a property rename lands on the row
it renames rather than somewhere on the box. That is the whole reason to draw
this at all: ``class_A - attr_a - attr_b - class_B`` is a statement about
attributes, and a diagram that only draws classes cannot show it.

Findings colour the border of what they name and are numbered into a legend,
the number repeated on the class or row it concerns, so the picture says
*where* as well as *what*. A refusal — the one merge actually raised —
is red; everything the structural pass found on its own is amber; the
declarations a completion suggests are dashed green, because a refusal that
carries its own fix should look like one.

:func:`build_preview_graph` returns a plain :mod:`networkx` graph and needs no
Graphviz, so what is drawn can be asserted directly;
:func:`plot_merge_preview` is the half that writes a file.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

import networkx as nx

from graflo.architecture.evolution.preview import (
    MergeFinding,
    MergePreview,
    PreviewNode,
)
from graflo.plot.render import draw, header_band, record_escape, sanitize_id, to_dot

if TYPE_CHECKING:  # pragma: no cover - typing only
    from collections.abc import Sequence

    from graflo.plot.dot import DotGraph

#: Header colour per column, from the palette the schema plots already use.
HEADER_COLOR: dict[str, str] = {
    "left": "#B7D1DF",
    "right": "#BEDFC8",
    "merged": "#FFE5B4",
    "canonical": "#DDD0E5",
    "ghost": "#E4E1D7",
    "relation": "#FFFFFF",
}

#: How a flagged element is drawn. A refusal is what merge raised; a
#: possible finding is what the preview found on its own and merge has not
#: reached yet.
SEVERITY_COLOR: dict[str, str] = {
    "refusal": "#C0392B",
    "possible": "#E67E22",
    "note": "#7F8C8D",
}

#: Per declaration kind. ``suggested`` is not a declaration that exists -- it
#: is the one a completion says would settle a refusal.
EDGE_STYLE: dict[str, dict[str, str]] = {
    "member": {"style": "solid", "color": "#2C3E50", "penwidth": "1.4"},
    "map": {"style": "dashed", "color": "#1F5FBF"},
    "property_map": {"style": "dashed", "color": "#1F5FBF", "penwidth": "0.7"},
    "property_equivalence": {"style": "dotted", "color": "#6B4C9A", "penwidth": "0.9"},
    "suggested": {"style": "dashed", "color": "#1E8449", "penwidth": "1.2"},
}

#: Point size of the class records; the header band is computed from it.
FONT_SIZE = 11

_PORT_PREFIX = "p_"


def _port(attribute: str) -> str:
    """The port name for an attribute row."""
    return _PORT_PREFIX + sanitize_id(attribute)


def _marks(badges: Sequence[int]) -> str:
    """Finding numbers as `` [1] [3]``, or nothing."""
    return "".join(f" [{index}]" for index in badges)


def _row(node: PreviewNode, *, typed: bool, badges: Sequence[int]) -> str:
    """One attribute field: its port, its name, whether it keys the class, its type.

    *typed* says whether this class shows types at all -- a schema that
    declares no field types would otherwise get a dangling separator on every
    row.
    """
    text = node.name
    if node.identity:
        text += " (key)"
    if typed and node.field_type:
        text += f": {node.field_type}"
    text += _marks(badges)
    return f"<{_port(node.name)}> {record_escape(text)}\\l"


def _class_node(
    node: PreviewNode,
    rows: Sequence[PreviewNode],
    *,
    badges: dict[str, list[int]],
    flagged: str | None,
    row_flags: dict[str, str],
    max_rows: int,
) -> dict[str, str]:
    """A class as a record: a header field, then one field per attribute.

    Rows an edge or a finding touches are always kept; the rest are trimmed to
    *max_rows* and summarised, because a class with sixty attributes drawn in
    full makes the whole diagram unreadable and none of those rows is what the
    reader came for. ``rankdir=LR`` stacks a record's top-level fields, so the
    fields read top to bottom.
    """
    kept, hidden = _trim(rows, row_flags=row_flags, max_rows=max_rows)
    typed = any(row.field_type for row in kept)
    header = record_escape(node.name + _marks(badges.get(node.id, [])))
    header_lines = 1
    if node.identity_mode and node.kind == "class":
        header += "\\n" + record_escape(node.identity_mode)
        header_lines = 2
    fields = [f"<head> {header}"]
    fields += [_row(row, typed=typed, badges=badges.get(row.id, [])) for row in kept]
    if hidden:
        fields.append(f"+{hidden} more\\l")
    style = "rounded,filled,dashed" if node.kind == "ghost" else "rounded,filled"
    colour = HEADER_COLOR.get(node.kind if node.side is None else node.side, "#FFFFFF")
    return {
        "shape": "record",
        "label": "|".join(fields),
        "style": style,
        **header_band(
            colour, header_lines=header_lines, rows=len(fields) - 1, fontsize=FONT_SIZE
        ),
        "color": SEVERITY_COLOR[flagged] if flagged else "#2C3E50",
        "penwidth": "2" if flagged else "1",
    }


def _trim(
    rows: Sequence[PreviewNode],
    *,
    row_flags: dict[str, str],
    max_rows: int,
) -> tuple[list[PreviewNode], int]:
    """Rows to draw, and how many were left out.

    An attribute that takes part in the declarations -- flagged, or keying its
    class -- survives whatever the budget, since dropping one would hide the
    thing being explained.
    """
    if max_rows <= 0 or len(rows) <= max_rows:
        return list(rows), 0
    must = [row for row in rows if row.id in row_flags or row.identity]
    rest = [row for row in rows if row not in must]
    budget = max(max_rows - len(must), 0)
    kept = must + rest[:budget]
    keep_ids = {row.id for row in kept}
    ordered = [row for row in rows if row.id in keep_ids]
    return ordered, len(rows) - len(ordered)


def _flags(
    preview: MergePreview,
) -> tuple[dict[str, str], dict[str, str], dict[str, list[int]]]:
    """``(node severity, edge severity, node badges)`` over every finding.

    A node touched by two findings takes the more serious of them, so a
    refusal is never drawn as though it were only a possibility.
    """
    rank = {"note": 0, "possible": 1, "refusal": 2}
    nodes: dict[str, str] = {}
    edges: dict[str, str] = {}
    badges: dict[str, list[int]] = {}
    for number, finding in enumerate(_numbered(preview), start=1):
        for node_id in finding.nodes:
            if rank[finding.severity] >= rank.get(nodes.get(node_id, "note"), -1):
                nodes[node_id] = finding.severity
            badges.setdefault(node_id, []).append(number)
        for edge_id in finding.edges:
            if rank[finding.severity] >= rank.get(edges.get(edge_id, "note"), -1):
                edges[edge_id] = finding.severity
    return nodes, edges, badges


def _numbered(preview: MergePreview) -> list[MergeFinding]:
    """Findings in the order the legend numbers them: refusal first."""
    order = {"refusal": 0, "possible": 1, "note": 2}
    return sorted(preview.findings, key=lambda f: (order[f.severity], f.kind))


def _legend(preview: MergePreview) -> dict[str, str]:
    """The findings, numbered, as one record: a header, then a field per finding."""
    fields = ["findings"]
    for number, finding in enumerate(_numbered(preview), start=1):
        line = (
            f"[{number}] {finding.severity}  {finding.kind}: {_wrap(finding.message)}"
        )
        fields.append(f"{record_escape(line)}\\l")
    if len(fields) == 1:
        fields.append("no conflicts found\\l")
    return {
        "shape": "record",
        "label": "|".join(fields),
        "style": "rounded",
        "color": "#B0B0B0",
    }


def _wrap(message: str, width: int = 88) -> str:
    """*message* on one line, elided rather than wrapped."""
    flat = " ".join(message.split())
    return flat if len(flat) <= width else f"{flat[: width - 1]}…"


def build_preview_graph(
    preview: MergePreview, *, max_rows: int = 12, legend: bool = True
) -> nx.MultiDiGraph:
    """The preview as a networkx graph, styled but not yet drawn.

    Only classes, relations and merged names become nodes: attributes are
    rows on their owner, addressed by port. Every node carries the preview id
    it came from as ``preview_id``, so a caller can map back.

    Args:
        preview: What :func:`~graflo.architecture.evolution.preview.preview_merge` returned.
        max_rows: Attribute rows to draw per class before summarising the rest.
        legend: Whether to include the findings legend.

    Returns:
        A graph whose node and edge attributes are Graphviz attributes.
    """
    graph = nx.MultiDiGraph()
    graph.graph["graph"] = {
        "rankdir": "LR",
        "splines": "spline",
        "fontname": "Helvetica",
        "labelloc": "t",
        "label": _title(preview),
    }
    graph.graph["node"] = {"fontname": "Helvetica", "fontsize": str(FONT_SIZE)}
    graph.graph["edge"] = {"fontname": "Helvetica", "fontsize": "9"}

    node_flags, edge_flags, badges = _flags(preview)
    taken: dict[str, str] = {}
    attributes: dict[str, list[PreviewNode]] = {}
    for node in preview.nodes:
        if node.owner is not None:
            attributes.setdefault(node.owner, []).append(node)

    row_flags_by_owner: dict[str, dict[str, str]] = {}
    for node in preview.nodes:
        if node.owner is not None and node.id in node_flags:
            row_flags_by_owner.setdefault(node.owner, {})[node.id] = node_flags[node.id]

    for node in preview.nodes:
        if node.owner is not None:
            continue  # an attribute: a row on its owner
        safe = sanitize_id(node.id, taken)
        graph.add_node(
            safe,
            **_class_node(
                node,
                attributes.get(node.id, []),
                badges=badges,
                flagged=node_flags.get(node.id),
                row_flags=row_flags_by_owner.get(node.id, {}),
                max_rows=max_rows,
            ),
            preview_id=node.id,
            kind=node.kind,
            side=node.side or "",
        )

    for edge in preview.edges:
        source = preview.node(edge.source)
        target = preview.node(edge.target)
        if source is None or target is None:
            continue
        tail = source.owner or source.id
        head = target.owner or target.id
        if tail not in taken or head not in taken:
            continue
        attrs: dict[str, Any] = {
            **EDGE_STYLE.get(edge.kind, {}),
            "preview_id": edge.id,
            "kind": edge.kind,
        }
        if source.owner is not None:
            attrs["tailport"] = _port(source.name)
        if target.owner is not None:
            attrs["headport"] = _port(target.name)
        if edge.label:
            attrs["label"] = edge.label
        severity = edge_flags.get(edge.id)
        if severity:
            attrs["color"] = SEVERITY_COLOR[severity]
            attrs["penwidth"] = "2"
        if source.side == "right":
            # rankdir=LR puts a tail on the left. Drawing the right column's
            # edges backwards keeps that column on the right where it belongs
            # -- which means swapping the ports along with the endpoints, or
            # each lands on a row the other node does not have.
            attrs["dir"] = "back"
            attrs["tailport"], attrs["headport"] = (
                attrs.pop("headport", None),
                attrs.pop("tailport", None),
            )
            attrs = {k: v for k, v in attrs.items() if v is not None}
            graph.add_edge(taken[head], taken[tail], **attrs)
        else:
            graph.add_edge(taken[tail], taken[head], **attrs)

    if legend:
        graph.add_node("legend", **_legend(preview), kind="legend", preview_id="legend")
    return graph


def _title(preview: MergePreview) -> str:
    outcome = preview.outcome
    verdict = {
        "merged": "merges",
        "refused": f"refused: {outcome.error_type}",
        "not_attempted": "not attempted",
    }[outcome.status]
    return f"{preview.left_name} + {preview.right_name} — {verdict}"


def _columns(graph: nx.MultiDiGraph, dot: DotGraph) -> None:
    """Put each side in its own cluster, so the picture reads left to right."""
    groups: dict[str, list[str]] = {}
    for name, data in graph.nodes(data=True):
        if data.get("kind") == "legend":
            continue
        side = data.get("side") or ""
        key = side or ("merged" if data.get("kind") == "merged" else "")
        if key:
            groups.setdefault(key, []).append(name)
    for key, label in (("left", "left"), ("merged", "merged"), ("right", "right")):
        members = groups.get(key)
        if not members:
            continue
        dot.add_subgraph(
            members,
            name=f"cluster_{key}",
            label=label,
            style="rounded",
            color="#B0B0B0",
            fontsize="10",
        )


def plot_merge_preview(
    preview: MergePreview,
    path: str | os.PathLike[str],
    *,
    output_format: str | None = None,
    prog: str = "dot",
    dpi: int | None = None,
    max_rows: int = 12,
    legend: bool = True,
) -> Path:
    """Draw *preview* to *path*.

    Args:
        preview: What ``preview_merge`` returned.
        path: Where to write; the suffix picks the format (svg/pdf/png/dot).
        output_format: Override the format the suffix implies.
        prog: Graphviz layout program.
        dpi: Raster resolution, for ``png``.
        max_rows: Attribute rows per class before the rest are summarised.
        legend: Whether to draw the findings legend.

    Returns:
        The path written.

    Raises:
        RuntimeError: Rendering is unavailable or failed; the message says why.
        ValueError: The format is not one this can write.
    """
    graph = build_preview_graph(preview, max_rows=max_rows, legend=legend)
    dot = to_dot(graph)
    _columns(graph, dot)
    return draw(dot, path, output_format=output_format, prog=prog, dpi=dpi)


__all__ = [
    "EDGE_STYLE",
    "HEADER_COLOR",
    "SEVERITY_COLOR",
    "build_preview_graph",
    "plot_merge_preview",
]
