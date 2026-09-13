"""Turning a networkx graph into a file, and the Graphviz details that needs.

Kept apart from the graphs themselves so every plot in this package builds a
plain :mod:`networkx` graph — which any test can assert against — and only this
module knows about pygraphviz. Nothing here is specific to what is being drawn.
"""

from __future__ import annotations

import html
import os
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - typing only
    import networkx as nx

#: What :func:`draw` can write. ``dot`` is the source, not a rendering, so it
#: is the one format that needs no Graphviz layout run.
OUTPUT_FORMATS: tuple[str, ...] = ("svg", "pdf", "png", "dot")

_UNSAFE = re.compile(r"[^A-Za-z0-9_]")

#: Pointing at a missing extra is more use than the ImportError networkx
#: raises from three frames down.
_NO_PYGRAPHVIZ = (
    "drawing needs pygraphviz and a system Graphviz: install the extra with "
    "`uv add 'graflo[plot]'` (or `pip install 'graflo[plot]'`) and the "
    "binaries with e.g. `apt install graphviz graphviz-dev`"
)


def sanitize_id(node_id: str, taken: dict[str, str] | None = None) -> str:
    """A Graphviz-safe node id, one-to-one with *node_id*.

    ``:`` separates a node from its port and ``.`` a record field, so an id
    like ``left:Firm.firm_id`` cannot be written as it stands. *taken* carries
    the mapping across a whole graph so two different ids that sanitize alike
    are still told apart.

    Args:
        node_id: The id to convert.
        taken: Accumulating ``{node_id: safe_id}``; pass one per graph.

    Returns:
        The safe id, stable for the same *node_id* within one *taken*.
    """
    if taken is not None and node_id in taken:
        return taken[node_id]
    safe = _UNSAFE.sub("_", node_id) or "n"
    if safe[0].isdigit():
        safe = f"n{safe}"
    if taken is not None:
        used = set(taken.values())
        if safe in used:
            ordinal = 2
            while f"{safe}_{ordinal}" in used:
                ordinal += 1
            safe = f"{safe}_{ordinal}"
        taken[node_id] = safe
    return safe


def escape(text: Any) -> str:
    """*text* as an HTML-label payload, with ``& < >`` and quotes escaped."""
    return html.escape(str(text), quote=True)


def resolve_format(path: Path, output_format: str | None) -> str:
    """The format to write, from *output_format* or the path's own suffix."""
    chosen = (output_format or path.suffix.lstrip(".")).lower()
    if chosen not in OUTPUT_FORMATS:
        raise ValueError(
            f"unsupported output format {chosen!r}; expected one of "
            f"{', '.join(OUTPUT_FORMATS)}"
        )
    return chosen


def to_agraph(graph: nx.Graph) -> Any:
    """*graph* as a pygraphviz ``AGraph``, or an actionable error."""
    import networkx as nx_mod

    try:
        return nx_mod.nx_agraph.to_agraph(graph)
    except ImportError as exc:  # pragma: no cover - depends on the environment
        raise RuntimeError(_NO_PYGRAPHVIZ) from exc


def draw(
    agraph: Any,
    path: str | os.PathLike[str],
    *,
    output_format: str | None = None,
    prog: str = "dot",
    dpi: int | None = None,
) -> Path:
    """Write *agraph* to *path*, creating the directory if it is missing.

    Args:
        agraph: The pygraphviz graph to write.
        path: Where to write it; its suffix chooses the format by default.
        output_format: Override the format the suffix implies.
        prog: Graphviz layout program.
        dpi: Raster resolution, for ``png``.

    Returns:
        The path written.
    """
    out = Path(path)
    chosen = resolve_format(out, output_format)
    out.parent.mkdir(parents=True, exist_ok=True)
    if chosen == "png" and dpi is not None:
        agraph.graph_attr["dpi"] = str(dpi)
    if chosen == "dot":
        # The source, not a rendering: no layout to run.
        agraph.write(str(out))
    else:
        agraph.draw(str(out), chosen, prog=prog)
    return out


__all__ = [
    "OUTPUT_FORMATS",
    "draw",
    "escape",
    "resolve_format",
    "sanitize_id",
    "to_agraph",
]
