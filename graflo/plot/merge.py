"""Draw a three-way merge: where two branches met, and the lineage they met on.

Two pictures, because a merge raises two questions.

:func:`plot_merge_preview` answers *where in the model did they collide* — the
slot tree, with the contested slots carrying what each branch did and what the
ancestor had. Slots are paths that contain one another, so the tree is already
there in the data; drawing it puts a rename of a vertex above the field edits
that live inside it, which is the relationship a flat list of conflicts loses.

:func:`plot_history` answers *which two commits, and from where* — the commit
DAG, with the merge base marked. First-parent edges are solid because that is
the line a commit's ops are a diff along; every other parent is dashed.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

import networkx as nx

from graflo.architecture.evolution.preview import MergePreview, SlotNode
from graflo.plot.render import draw, escape, sanitize_id, to_agraph

if TYPE_CHECKING:  # pragma: no cover - typing only
    from collections.abc import Collection

#: A contested slot against a settled one. The third is the ancestor's state,
#: which is context rather than a verdict.
SLOT_COLOR: dict[str, str] = {
    "contested": "#C0392B",
    "clean": "#7F8C8D",
    "base": "#5D6D7E",
}

#: Per side, matching the compose preview so the two figures read alike.
SIDE_COLOR: dict[str, str] = {"left": "#B7D1DF", "right": "#BEDFC8"}

#: Commit kinds that get their own outline in the lineage picture.
COMMIT_SHAPE: dict[str, str] = {
    "merge": "doubleoctagon",
    "compose": "doubleoctagon",
    "revert": "octagon",
    "edit": "box",
}


def _ops_row(label: str, ops: Collection[str], colour: str) -> str:
    """One branch's ops on a contested slot, as a row."""
    if not ops:
        return ""
    listed = escape(", ".join(sorted(set(ops))))
    return (
        f'<TR><TD ALIGN="RIGHT"><FONT POINT-SIZE="9" COLOR="{colour}">'
        f'{escape(label)}</FONT></TD><TD ALIGN="LEFT">'
        f'<FONT POINT-SIZE="9">{listed}</FONT></TD></TR>'
    )


def _slot_table(node: SlotNode) -> str:
    """A slot as a table: its segment, and what happened to it.

    A settled slot shows only what the merge applied; a contested one shows
    both branches and the ancestor's keys, because "what did this look like
    before either change" is the question a decision actually turns on and the
    one a two-way diff cannot answer.
    """
    colour = SLOT_COLOR["contested" if node.contested else "clean"]
    header = escape(node.segment)
    if node.contested:
        header = f"<B>{header}</B>"
    rows = ""
    if node.contested:
        rows += _ops_row("left", node.left_ops, SIDE_COLOR["left"])
        rows += _ops_row("right", node.right_ops, SIDE_COLOR["right"])
        if node.base_excerpt:
            keys = escape(", ".join(sorted(node.base_excerpt)))
            rows += (
                f'<TR><TD ALIGN="RIGHT"><FONT POINT-SIZE="9" '
                f'COLOR="{SLOT_COLOR["base"]}">base</FONT></TD>'
                f'<TD ALIGN="LEFT"><FONT POINT-SIZE="9">{keys}</FONT></TD></TR>'
            )
    else:
        rows += _ops_row("applied", node.clean_ops, SLOT_COLOR["clean"])
    span = "2" if rows else "1"
    fill = "#F5B7B1" if node.contested else "#FFFFFF"
    width = "2" if node.contested else "1"
    return (
        f'<<TABLE BORDER="{width}" CELLBORDER="0" CELLSPACING="0" CELLPADDING="3" '
        f'COLOR="{colour}">'
        f'<TR><TD COLSPAN="{span}" BGCOLOR="{fill}">{header}</TD></TR>'
        f"{rows}</TABLE>>"
    )


def build_merge_graph(preview: MergePreview) -> nx.DiGraph:
    """The slot tree as a networkx graph, styled but not drawn.

    Args:
        preview: What
            :func:`~graflo.architecture.evolution.preview.build_merge_preview`
            returned.

    Returns:
        A graph whose attributes are Graphviz attributes; every node carries
        its slot path as ``slot``.
    """
    graph = nx.DiGraph()
    verdict = "clean" if preview.clean else f"{preview.conflicts} conflict(s) to decide"
    graph.graph["graph"] = {
        "rankdir": "LR",
        "fontname": "Helvetica",
        "labelloc": "t",
        "label": f"three-way merge — {verdict}",
    }
    graph.graph["node"] = {"fontname": "Helvetica", "shape": "plain"}
    graph.graph["edge"] = {"fontname": "Helvetica", "color": "#95A5A6"}

    taken: dict[str, str] = {}
    for node in preview.nodes:
        graph.add_node(
            sanitize_id(node.id, taken),
            label=_slot_table(node),
            slot=node.id,
            contested=str(node.contested).lower(),
        )
    for node in preview.nodes:
        if node.parent is None or node.parent not in taken:
            continue
        graph.add_edge(
            taken[node.parent],
            taken[node.id],
            penwidth="1.6" if node.contested else "1.0",
            color=SLOT_COLOR["contested"] if node.contested else "#95A5A6",
        )
    return graph


def plot_merge_preview(
    preview: MergePreview,
    path: str | os.PathLike[str],
    *,
    output_format: str | None = None,
    prog: str = "dot",
    dpi: int | None = None,
) -> Path:
    """Draw the slot tree of *preview* to *path*.

    Args:
        preview: What ``build_merge_preview`` returned.
        path: Where to write; the suffix picks the format (svg/pdf/png/dot).
        output_format: Override the format the suffix implies.
        prog: Graphviz layout program.
        dpi: Raster resolution, for ``png``.

    Returns:
        The path written.
    """
    graph = build_merge_graph(preview)
    return draw(to_agraph(graph), path, output_format=output_format, prog=prog, dpi=dpi)


def build_history_graph(
    history: Any,
    *,
    heads: Collection[str] = (),
    merge_base: str | None = None,
) -> nx.DiGraph:
    """The commit DAG as a networkx graph.

    Args:
        history: A :class:`~graflo.architecture.evolution.history.History`.
        heads: Commit ids to mark as the sides being reconciled.
        merge_base: The common ancestor, marked as such.

    Returns:
        A graph with one node per commit, edges pointing parent to child.
    """
    graph = nx.DiGraph()
    graph.graph["graph"] = {
        "rankdir": "LR",
        "fontname": "Helvetica",
        "labelloc": "t",
        "label": "commit history",
    }
    graph.graph["node"] = {"fontname": "Helvetica", "style": "filled"}
    graph.graph["edge"] = {"fontname": "Helvetica"}

    head_ids = set(heads)
    for commit in history.commits:
        role = (
            "base"
            if commit.id == merge_base
            else "head"
            if commit.id in head_ids
            else commit.kind
        )
        fill = {
            "base": "#FFE5B4",
            "head": "#B7D1DF",
        }.get(role, "#FFFFFF")
        label = commit.id[:8]
        if commit.label:
            label = f"{label}\\n{commit.label}"
        if commit.id == merge_base:
            label = f"{label}\\n(merge base)"
        graph.add_node(
            sanitize_id(commit.id),
            label=label,
            shape=COMMIT_SHAPE.get(commit.kind, "box"),
            fillcolor=fill,
            commit=commit.id,
            role=role,
        )
    for commit in history.commits:
        for position, parent in enumerate(commit.parents):
            graph.add_edge(
                sanitize_id(parent),
                sanitize_id(commit.id),
                # A commit's ops are a diff from its *first* parent; the other
                # parents are lineage, not derivation.
                style="solid" if position == 0 else "dashed",
                color="#2C3E50" if position == 0 else "#95A5A6",
            )
    return graph


def plot_history(
    history: Any,
    path: str | os.PathLike[str],
    *,
    heads: Collection[str] = (),
    merge_base: str | None = None,
    output_format: str | None = None,
    prog: str = "dot",
    dpi: int | None = None,
) -> Path:
    """Draw the commit DAG of *history* to *path*.

    Args:
        history: A :class:`~graflo.architecture.evolution.history.History`.
        path: Where to write; the suffix picks the format.
        heads: Commit ids to mark as the sides being reconciled.
        merge_base: The common ancestor, marked as such.
        output_format: Override the format the suffix implies.
        prog: Graphviz layout program.
        dpi: Raster resolution, for ``png``.

    Returns:
        The path written.
    """
    graph = build_history_graph(history, heads=heads, merge_base=merge_base)
    return draw(to_agraph(graph), path, output_format=output_format, prog=prog, dpi=dpi)


__all__ = [
    "COMMIT_SHAPE",
    "SIDE_COLOR",
    "SLOT_COLOR",
    "build_history_graph",
    "build_merge_graph",
    "plot_history",
    "plot_merge_preview",
]
