"""Rendered figures: text survives every format, fits its box, and looks as before.

Each fixture figure is drawn through the whole chain — DOT → SVG (Graphviz as
WebAssembly) → PDF → PNG — and checked for what a reader would notice:

* every character is one the shipped fonts can draw (no tofu boxes);
* every text of the SVG is in the PDF (a dropped label is the failure mode of
  an unsupported construct, e.g. an HTML-like label);
* text fits inside the node drawn for it, and nodes do not overlap;
* the PDF and PNG have the SVG's size;
* the PNG matches the committed golden image.

Regenerate the golden images after a deliberate change of look with
``GRAFLO_WRITE_PLOT_GOLDENS=1 uv run pytest test/plot/test_render_quality.py``
and review them before committing.
"""

from __future__ import annotations

import io
import json
import os
import re
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from importlib import metadata, resources
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

pytest.importorskip("wasi_graphviz")
pytest.importorskip("svglib")
pytest.importorskip("pypdfium2")

import pypdfium2 as pdfium
from PIL import Image, ImageChops
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfbase.ttfonts import TTFont

from graflo.architecture.evolution.ops import MergeManifestsOp
from graflo.architecture.evolution.preview import (
    MergeFinding,
    preview_merge,
)
from graflo.cli.io import load_manifest, load_mapping
from graflo.plot import ManifestPlotter
from graflo.plot.merge import plot_merge_preview
from graflo.plot.merge3 import plot_history, plot_merge3_preview
from graflo.plot.render import (
    _FONT_FILES,
    _shipped_family,
    pdf_to_png,
    render_svg,
    svg_to_pdf,
)

sys.path.insert(0, str(Path(__file__).parent))
import test_conflict_plots as conflict

EXAMPLES = Path(__file__).parents[2] / "examples"
GOLDEN = Path(__file__).parent / "golden"
GOLDEN_DPI = 96
#: A pixel counts as changed past this per-channel difference (anti-aliasing
#: and hinting move edges by a level or two); a figure fails past this share.
PIXEL_THRESHOLD = 32
CHANGED_SHARE = 0.005
#: Points of slack for text against its box: Graphviz pads labels by more.
FIT_SLACK = 1.0
_SVG = "{http://www.w3.org/2000/svg}"


@dataclass
class Rendered:
    name: str
    svg: bytes
    pdf: bytes
    png: bytes


def _dot_sources(out: Path) -> dict[str, str]:
    """Every fixture figure as DOT source, written by graflo's own plotting code."""
    out.mkdir(parents=True, exist_ok=True)
    plotter = ManifestPlotter(
        str(EXAMPLES / "06-vertex-roles-edge-links" / "manifest.yaml"),
        str(out),
        output_format="dot",
    )
    plotter.plot_vc2vc()
    plotter.plot_vc2fields()
    plotter.plot_resources()
    plotter.plot_source2vc()

    example = EXAMPLES / "21-router-union-alignment"
    preview = preview_merge(
        load_manifest(example / "manifest_maintenance.yaml"),
        load_manifest(example / "manifest_sensors.yaml"),
        MergeManifestsOp.model_validate(load_mapping(example / "merge.yaml")),
    )
    plot_merge_preview(preview, out / "merge_example.dot")
    findings = conflict._preview(
        findings=[
            MergeFinding(
                kind="identity_disagreement",
                severity="refusal",
                message="the members disagree on what identifies a Firm",
                source="merge",
                nodes=["left:Firm"],
            ),
            MergeFinding(
                kind="type_conflict",
                severity="possible",
                message="note is STRING on the left and INT on the right",
                source="structure",
                nodes=["left:Firm.note"],
            ),
        ]
    )
    plot_merge_preview(findings, out / "merge_findings.dot")
    plot_merge3_preview(conflict._merge_preview(), out / "merge3.dot")
    history = SimpleNamespace(
        commits=[
            SimpleNamespace(id="a" * 12, parents=[], kind="edit", label="root"),
            SimpleNamespace(id="b" * 12, parents=["a" * 12], kind="edit", label="left"),
            SimpleNamespace(
                id="c" * 12, parents=["a" * 12], kind="edit", label="right"
            ),
            SimpleNamespace(
                id="d" * 12, parents=["b" * 12, "c" * 12], kind="merge3", label="merged"
            ),
        ]
    )
    plot_history(
        history, out / "history.dot", heads=["b" * 12, "c" * 12], merge_base="a" * 12
    )
    (out / "scripts.dot").write_text(
        'digraph { node [fontname="Helvetica"]; '
        'a [label="Компания — café"]; b [shape=box, label="Ελληνικά"]; '
        'c [fontname="Times", label="Times serif"]; '
        'd [fontname="Courier", label="mono_space()"]; a -> b; a -> c; c -> d }',
        encoding="utf-8",
    )
    return {
        path.stem: path.read_text(encoding="utf-8")
        for path in sorted(out.glob("*.dot"))
    }


@pytest.fixture(scope="module")
def rendered(tmp_path_factory) -> list[Rendered]:
    sources = _dot_sources(tmp_path_factory.mktemp("dot"))
    figures = []
    for name, source in sources.items():
        svg = render_svg(source)
        pdf = svg_to_pdf(svg)
        figures.append(Rendered(name, svg, pdf, pdf_to_png(pdf, dpi=GOLDEN_DPI)))
    return figures


def _ids(figures: list[Rendered]) -> list[str]:
    return [figure.name for figure in figures]


# ── SVG parsing ─────────────────────────────────────────────────────────────


@dataclass
class Text:
    content: str
    x: float
    anchor: str
    family: str
    weight: str
    size: float


def _texts(element: ET.Element) -> list[Text]:
    found = []
    for node in element.iter(f"{_SVG}text"):
        content = "".join(node.itertext())
        if not content.strip():
            continue
        found.append(
            Text(
                content=content,
                x=float(node.get("x", "0")),
                anchor=node.get("text-anchor", "start"),
                family=_shipped_family(node.get("font-family", "Times,serif")),
                weight=node.get("font-weight", "normal"),
                size=float(node.get("font-size", "14")),
            )
        )
    return found


_NUMBER = re.compile(r"-?\d+(?:\.\d+)?")


def _bbox(group: ET.Element) -> tuple[float, float, float, float] | None:
    """``(x0, y0, x1, y1)`` of every shape in *group*, text excluded."""
    xs: list[float] = []
    ys: list[float] = []
    for shape in group:
        tag = shape.tag.removeprefix(_SVG)
        if tag == "polygon" or tag == "polyline":
            numbers = [float(n) for n in _NUMBER.findall(shape.get("points", ""))]
            xs += numbers[0::2]
            ys += numbers[1::2]
        elif tag == "ellipse":
            cx, cy = float(shape.get("cx", 0)), float(shape.get("cy", 0))
            rx, ry = float(shape.get("rx", 0)), float(shape.get("ry", 0))
            xs += [cx - rx, cx + rx]
            ys += [cy - ry, cy + ry]
        elif tag == "path":
            numbers = [float(n) for n in _NUMBER.findall(shape.get("d", ""))]
            xs += numbers[0::2]
            ys += numbers[1::2]
    if not xs:
        return None
    return min(xs), min(ys), max(xs), max(ys)


def _groups(svg: bytes, kind: str) -> list[ET.Element]:
    root = ET.fromstring(svg)
    return [g for g in root.iter(f"{_SVG}g") if g.get("class") == kind]


_fonts: dict[tuple[str, str], str] = {}


def _font_name(family: str, weight: str) -> str:
    """A reportlab font registered from the shipped file, for measuring."""
    key = (family, "bold" if weight == "bold" else "normal")
    if key not in _fonts:
        filename = _FONT_FILES[family][key[1]]
        path = resources.files("graflo.plot").joinpath("fonts", filename)
        name = f"quality-{family}-{key[1]}"
        from reportlab.pdfbase import pdfmetrics

        pdfmetrics.registerFont(TTFont(name, str(path)))
        _fonts[key] = name
    return _fonts[key]


def _width(text: Text) -> float:
    return stringWidth(text.content, _font_name(text.family, text.weight), text.size)


# ── the checks ──────────────────────────────────────────────────────────────


def test_every_character_is_in_the_shipped_fonts(rendered):
    covered: dict[str, set[int]] = {}
    for family, faces in _FONT_FILES.items():
        path = resources.files("graflo.plot").joinpath("fonts", faces["normal"])
        glyphs = cast(dict[int, int], TTFont(family, str(path)).face.charToGlyph)
        covered[family] = set(glyphs)
    for figure in rendered:
        for text in _texts(ET.fromstring(figure.svg)):
            missing = {ch for ch in text.content if ord(ch) not in covered[text.family]}
            assert not missing, (
                f"{figure.name}: {text.content!r} has no glyph for {missing}"
            )


def test_every_svg_text_reaches_the_pdf(rendered):
    for figure in rendered:
        document = pdfium.PdfDocument(figure.pdf)
        try:
            extracted = " ".join(document[0].get_textpage().get_text_range().split())
        finally:
            document.close()
        for text in _texts(ET.fromstring(figure.svg)):
            assert " ".join(text.content.split()) in extracted, (
                f"{figure.name}: {text.content!r} is missing from the PDF"
            )


def test_figures_carry_their_labels(rendered):
    """Spot labels per figure: a construct Graphviz cannot parse drops them."""
    expected = {
        "merge_findings": ["Firm [1]", "firm_id (key): STRING", "note [2]", "findings"],
        "merge_example": ["Machine", "(key)", "serial_number"],
        "merge3": ["identity", "left: replace_identity", "base: identity, name"],
        "history": ["(merge base)", "merged"],
        "scripts": ["Компания — café", "Ελληνικά"],
    }
    by_name = {figure.name: figure for figure in rendered}
    for name, labels in expected.items():
        texts = " ".join(t.content for t in _texts(ET.fromstring(by_name[name].svg)))
        for label in labels:
            assert label in texts, f"{name}: {label!r} not drawn"


def test_text_fits_the_node_drawn_for_it(rendered):
    for figure in rendered:
        for group in _groups(figure.svg, "node") + _groups(figure.svg, "cluster"):
            box = _bbox(group)
            if box is None:
                continue
            x0, _, x1, _ = box
            for text in _texts(group):
                width = _width(text)
                left = {
                    "middle": text.x - width / 2,
                    "end": text.x - width,
                }.get(text.anchor, text.x)
                assert left >= x0 - FIT_SLACK and left + width <= x1 + FIT_SLACK, (
                    f"{figure.name}: {text.content!r} ({width:.1f}pt) overflows "
                    f"its box [{x0:.1f}, {x1:.1f}]"
                )


def test_nodes_do_not_overlap(rendered):
    for figure in rendered:
        boxes = [b for g in _groups(figure.svg, "node") if (b := _bbox(g)) is not None]
        for i, a in enumerate(boxes):
            for b in boxes[i + 1 :]:
                overlap_x = min(a[2], b[2]) - max(a[0], b[0])
                overlap_y = min(a[3], b[3]) - max(a[1], b[1])
                assert overlap_x <= 0.5 or overlap_y <= 0.5, (
                    f"{figure.name}: {a} overlaps {b}"
                )


def test_pdf_and_png_keep_the_svg_size(rendered):
    for figure in rendered:
        root = ET.fromstring(figure.svg)
        width = float(root.get("width", "0").removesuffix("pt"))
        height = float(root.get("height", "0").removesuffix("pt"))
        document = pdfium.PdfDocument(figure.pdf)
        try:
            page_width, page_height = document[0].get_size()
        finally:
            document.close()
        assert abs(page_width - width) <= 1 and abs(page_height - height) <= 1, (
            figure.name
        )
        image = Image.open(io.BytesIO(figure.png))
        scale = GOLDEN_DPI / 72
        assert abs(image.width - width * scale) <= 2, figure.name
        assert abs(image.height - height * scale) <= 2, figure.name
        darkest, _ = cast(tuple[int, int], image.convert("L").getextrema())
        assert darkest < 128, f"{figure.name}: blank image"


# ── golden images ───────────────────────────────────────────────────────────


def _versions() -> dict[str, str]:
    return {
        package: metadata.version(package)
        for package in ("wasi-graphviz", "svglib", "reportlab", "pypdfium2")
    }


def test_images_match_the_golden_ones(rendered, tmp_path):
    manifest_path = GOLDEN / "golden.json"
    if os.environ.get("GRAFLO_WRITE_PLOT_GOLDENS") == "1":
        GOLDEN.mkdir(exist_ok=True)
        for figure in rendered:
            (GOLDEN / f"{figure.name}.png").write_bytes(figure.png)
        manifest_path.write_text(
            json.dumps(
                {"dpi": GOLDEN_DPI, "versions": _versions(), "figures": _ids(rendered)},
                indent=4,
            )
            + "\n"
        )
        pytest.skip("golden images written; review them before committing")

    recorded = json.loads(manifest_path.read_text())
    assert recorded["figures"] == _ids(rendered), "fixture figures changed; regenerate"
    drift = {
        k: (v, recorded["versions"].get(k))
        for k, v in _versions().items()
        if recorded["versions"].get(k) != v
    }
    failures = []
    for figure in rendered:
        expected = Image.open(GOLDEN / f"{figure.name}.png").convert("RGB")
        actual = Image.open(io.BytesIO(figure.png)).convert("RGB")
        if expected.size != actual.size:
            failures.append(f"{figure.name}: size {actual.size} != {expected.size}")
            share = 1.0
        else:
            diff = ImageChops.difference(expected, actual).convert("L")
            changed = sum(
                diff.point(lambda v: 255 if v > PIXEL_THRESHOLD else 0).histogram()[
                    255:
                ]
            )
            share = changed / (actual.width * actual.height)
            if share > CHANGED_SHARE:
                failures.append(f"{figure.name}: {share:.2%} of pixels changed")
        if share > CHANGED_SHARE:
            actual.save(tmp_path / f"{figure.name}.actual.png")
    hint = f" (versions differ from the goldens: {drift})" if drift else ""
    assert not failures, (
        f"{'; '.join(failures)}{hint}; actual images in {tmp_path}. If the change is "
        "intended, regenerate with GRAFLO_WRITE_PLOT_GOLDENS=1 and review."
    )
