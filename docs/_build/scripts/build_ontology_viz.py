#!/usr/bin/env python
"""Generate interactive HTML visualization for the GraFlo OWL ontology."""

from __future__ import annotations

import importlib.util
import shutil
import sys
from pathlib import Path

from graflo.rdf.namespace import GF_ONTOLOGY_IRI, GF_VERSION

REPO_ROOT = Path(__file__).resolve().parents[3]
OUTPUT_DIR = REPO_ROOT / "docs" / "assets" / "graflo-ontology-viz"
VIZ_DIR = Path(__file__).resolve().parent / "ontology_viz"

ASSET_FILES = (
    "graph-view.css",
    "graph-view.js",
)


def _load_extract_module():
    spec = importlib.util.spec_from_file_location(
        "graflo_ontology_viz_extract",
        VIZ_DIR / "extract.py",
    )
    if spec is None or spec.loader is None:
        msg = "Could not load ontology graph extractor"
        raise RuntimeError(msg)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _render_template(template_name: str, *, title: str, graph_json: str) -> str:
    extract = _load_extract_module()
    template = (VIZ_DIR / template_name).read_text(encoding="utf-8")
    return template.replace("{{TITLE}}", title).replace(
        "{{GRAPH_JSON}}", extract.escape_json_for_html(graph_json)
    )


def build_ontology_viz(*, output_dir: Path = OUTPUT_DIR) -> str:
    """Build ontology visualization HTML. Returns the viz kind identifier."""
    extract = _load_extract_module()
    graph_data = extract.extract_ontology_graph()
    graph_json = extract.graph_to_json(graph_data)
    title = f"GraFlo Ontology (v{GF_VERSION})"

    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)

    for name in ASSET_FILES:
        shutil.copy2(VIZ_DIR / name, output_dir / name)

    (output_dir / "graph-data.json").write_text(graph_json + "\n", encoding="utf-8")
    (output_dir / "index.html").write_text(
        _render_template("page.template.html", title=title, graph_json=graph_json),
        encoding="utf-8",
    )
    (output_dir / "embed.html").write_text(
        _render_template("embed.template.html", title=title, graph_json=graph_json),
        encoding="utf-8",
    )
    return "hierarchical-graph"


def main() -> int:
    try:
        viz_id = build_ontology_viz()
    except Exception as exc:  # noqa: BLE001 — CLI entrypoint
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(f"Built {OUTPUT_DIR} using GraFlo viz '{viz_id}'")
    print(f"Ontology: {GF_ONTOLOGY_IRI} (v{GF_VERSION})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
