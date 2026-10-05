"""Turning a networkx graph into a file, and the Graphviz details that needs.

Kept apart from the graphs themselves so every plot in this package builds a
plain :mod:`networkx` graph — which any test can assert against — and only this
module renders. Nothing here is specific to what is being drawn.

Rendering needs no system packages: Graphviz runs as WebAssembly
(``wasi-graphviz`` on ``wasmtime``) and writes SVG; PDF is converted from the
SVG (``svglib`` + ``reportlab``) and PNG rasterised from the PDF
(``pypdfium2``). PDF and PNG text uses the Liberation fonts shipped in
``graflo/plot/fonts``, which are metric-compatible with the Helvetica, Times
and Courier metrics Graphviz sizes nodes by, so text fits the boxes drawn for it.
"""

from __future__ import annotations

import io
import os
import re
import sys
from functools import cache
from importlib import resources
from pathlib import Path
from typing import TYPE_CHECKING, Any

from graflo.plot.dot import DotGraph

if TYPE_CHECKING:  # pragma: no cover - typing only
    import networkx as nx

#: What :func:`draw` can write. ``dot`` is the source, not a rendering, so it
#: is the one format that needs no Graphviz layout run.
OUTPUT_FORMATS: tuple[str, ...] = ("svg", "pdf", "png", "dot")

#: Raster resolution when the caller gives none.
DEFAULT_DPI = 150

_UNSAFE = re.compile(r"[^A-Za-z0-9_]")

_MISSING_EXTRA = (
    "drawing needs the `plot` extra, which is not installed for {python} "
    "(missing module {module!r}): install it into that environment with "
    "`uv add 'graflo[plot]'` (or `pip install 'graflo[plot]'`)"
)
_WASMTIME_UNLOADABLE = (
    "the WebAssembly runtime cannot load: {cause}. On Alpine install `libgcc` "
    "(`apk add libgcc`)"
)

#: Liberation faces shipped with graflo, by the family Graphviz writes.
_FONT_FILES: dict[str, dict[str, str]] = {
    "LiberationSans": {
        "normal": "LiberationSans-Regular.ttf",
        "bold": "LiberationSans-Bold.ttf",
    },
    "LiberationSerif": {
        "normal": "LiberationSerif-Regular.ttf",
        "bold": "LiberationSerif-Bold.ttf",
    },
    "LiberationMono": {
        "normal": "LiberationMono-Regular.ttf",
        "bold": "LiberationMono-Regular.ttf",
    },
}
_SERIF = {"times", "times-roman", "times new roman", "serif"}
_MONO = {"courier", "courier new", "monospace"}
_FONT_FAMILY = re.compile(r'font-family="([^"]*)"')


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


def record_escape(text: Any) -> str:
    """*text* as a record-label field: ``{ } | < >``, ``\\`` and newlines escaped."""
    escaped = re.sub(r"([{}|<>\\])", r"\\\1", str(text))
    return escaped.replace("\n", "\\n")


def header_band(
    colour: str, *, header_lines: int, rows: int, fontsize: float
) -> dict[str, str]:
    """Fill attributes that colour only a record's first field, the rest white.

    A record cannot fill one field, but a two-stop gradient whose weight is
    the header's share of the node height draws the same picture. Graphviz
    sizes a field as ``lines * fontsize * 1.2`` plus 8 points of padding, so
    the share is exact for single-line rows stacked under ``rankdir=LR``.

    Args:
        colour: The header colour.
        header_lines: Text lines in the header field.
        rows: Single-line fields below the header.
        fontsize: The node's font size.

    Returns:
        ``fillcolor`` and ``gradientangle``, to merge into the node's attributes.
    """
    header = header_lines * fontsize * 1.2 + 8
    share = header / (header + rows * (fontsize * 1.2 + 8))
    if share >= 1:
        return {"fillcolor": colour}
    return {"fillcolor": f"{colour};{share:.4f}:white", "gradientangle": "270"}


def resolve_format(path: Path, output_format: str | None) -> str:
    """The format to write, from *output_format* or the path's own suffix."""
    chosen = (output_format or path.suffix.lstrip(".")).lower()
    if chosen not in OUTPUT_FORMATS:
        raise ValueError(
            f"unsupported output format {chosen!r}; expected one of "
            f"{', '.join(OUTPUT_FORMATS)}"
        )
    return chosen


def to_dot(graph: nx.Graph) -> DotGraph:
    """*graph* as a :class:`~graflo.plot.dot.DotGraph`, ready to group and draw."""
    return DotGraph.from_networkx(graph)


def render_svg(source: str, *, prog: str = "dot") -> bytes:
    """Lay out and draw DOT *source* as SVG.

    Raises:
        RuntimeError: The ``plot`` extra is missing, the WebAssembly runtime
            cannot load, or Graphviz rejected the graph.
    """
    try:
        from wasi_graphviz import RenderError, render
    except ImportError as exc:
        raise RuntimeError(_missing(exc)) from exc
    try:
        return render(source, format="svg", engine=prog, backend="wasmtime")
    except ImportError as exc:
        raise RuntimeError(_missing(exc)) from exc
    except OSError as exc:
        raise RuntimeError(_WASMTIME_UNLOADABLE.format(cause=exc)) from exc
    except RenderError as exc:
        raise RuntimeError(f"Graphviz could not draw the graph: {exc}") from exc


def svg_to_pdf(svg: bytes) -> bytes:
    """*svg* as a one-page PDF whose text uses the shipped Liberation fonts."""
    try:
        from reportlab.graphics import renderPDF
        from svglib.svglib import svg2rlg
    except ImportError as exc:
        raise RuntimeError(_missing(exc)) from exc
    drawing = svg2rlg(io.BytesIO(_use_shipped_fonts(svg)), font_map=_font_map())
    if drawing is None:  # pragma: no cover - svglib only fails on unparsable XML
        raise RuntimeError("the SVG Graphviz wrote could not be read back")
    out = io.BytesIO()
    # invariant: no timestamps or random ids, so the same figure is the same bytes
    renderPDF.drawToFile(drawing, out, canvasKwds={"invariant": 1})
    return out.getvalue()


def pdf_to_png(pdf: bytes, *, dpi: int = DEFAULT_DPI) -> bytes:
    """The first page of *pdf* rasterised at *dpi* on a white background."""
    try:
        import pypdfium2 as pdfium
    except ImportError as exc:
        raise RuntimeError(_missing(exc)) from exc
    document = pdfium.PdfDocument(pdf)
    try:
        image = document[0].render(scale=dpi / 72).to_pil()
        out = io.BytesIO()
        image.save(out, format="PNG")
        return out.getvalue()
    finally:
        document.close()


def draw(
    dot: DotGraph,
    path: str | os.PathLike[str],
    *,
    output_format: str | None = None,
    prog: str = "dot",
    dpi: int | None = None,
) -> Path:
    """Write *dot* to *path*, creating the directory if it is missing.

    Args:
        dot: The graph to write.
        path: Where to write it; its suffix chooses the format by default.
        output_format: Override the format the suffix implies.
        prog: Graphviz layout program.
        dpi: Raster resolution, for ``png``; :data:`DEFAULT_DPI` if omitted.

    Returns:
        The path written.

    Raises:
        RuntimeError: Rendering is unavailable or failed; the message says why.
        ValueError: The format is not one this can write.
    """
    out = Path(path)
    chosen = resolve_format(out, output_format)
    out.parent.mkdir(parents=True, exist_ok=True)
    source = dot.to_string()
    if chosen == "dot":
        # The source, not a rendering: no layout to run.
        out.write_text(source, encoding="utf-8")
        return out
    svg = render_svg(source, prog=prog)
    if chosen == "svg":
        out.write_bytes(svg)
        return out
    pdf = svg_to_pdf(svg)
    if chosen == "pdf":
        out.write_bytes(pdf)
    else:
        out.write_bytes(pdf_to_png(pdf, dpi=dpi or DEFAULT_DPI))
    return out


def _missing(exc: ImportError) -> str:
    return _MISSING_EXTRA.format(python=sys.executable, module=exc.name or str(exc))


def _shipped_family(families: str) -> str:
    """The shipped face for an SVG ``font-family`` list, by its first family."""
    first = families.split(",")[0].strip().strip("'\"").lower()
    if first in _SERIF:
        return "LiberationSerif"
    if first in _MONO:
        return "LiberationMono"
    return "LiberationSans"


def _use_shipped_fonts(svg: bytes) -> bytes:
    """*svg* with every ``font-family`` pointing at a shipped face.

    Without this svglib maps Helvetica and Times onto the PDF base-14 fonts,
    which cover Latin-1 only, and asks the host's fontconfig for anything else.
    """
    text = svg.decode("utf-8")
    text = _FONT_FAMILY.sub(
        lambda m: f'font-family="{_shipped_family(m.group(1))}"', text
    )
    return text.encode("utf-8")


@cache
def _font_map() -> Any:
    """svglib's font map holding only the shipped faces, built once."""
    from svglib.fonts import FontMap

    fonts = resources.files("graflo.plot").joinpath("fonts")
    font_map = FontMap()
    for family, faces in _FONT_FILES.items():
        for weight, filename in faces.items():
            path = str(fonts.joinpath(filename))
            for style in ("normal", "italic"):
                font_map.register_font(family, path, weight=weight, style=style)
    return font_map


__all__ = [
    "DEFAULT_DPI",
    "OUTPUT_FORMATS",
    "draw",
    "header_band",
    "pdf_to_png",
    "record_escape",
    "render_svg",
    "resolve_format",
    "sanitize_id",
    "svg_to_pdf",
    "to_dot",
]
