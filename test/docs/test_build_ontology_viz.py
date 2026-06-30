"""Smoke tests for the GraFlo ontology visualization build script."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import OWL, RDF, RDFS, SKOS

REPO_ROOT = Path(__file__).resolve().parents[2]
BUILD_SCRIPT = REPO_ROOT / "docs" / "_build" / "scripts" / "build_ontology_viz.py"
EXTRACT_SCRIPT = (
    REPO_ROOT / "docs" / "_build" / "scripts" / "ontology_viz" / "extract.py"
)
OUTPUT_DIR = REPO_ROOT / "docs" / "assets" / "graflo-ontology-viz"
INDEX_HTML = OUTPUT_DIR / "index.html"
EMBED_HTML = OUTPUT_DIR / "embed.html"
GRAPH_JSON = OUTPUT_DIR / "graph-data.json"
ONTOLOGY_IRI = "https://ontology.growgraph.dev/graflo"


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_committed_ontology_viz_assets_exist() -> None:
    assert INDEX_HTML.is_file(), (
        "Run docs/_build/scripts/build_ontology_viz.py and commit assets"
    )
    assert EMBED_HTML.is_file()
    assert GRAPH_JSON.is_file()
    assert (OUTPUT_DIR / "graph-view.js").is_file()


def test_committed_ontology_viz_contains_metadata() -> None:
    html = INDEX_HTML.read_text(encoding="utf-8")
    assert "GraFlo Ontology" in html
    assert "GRAFLO_ONTOLOGY_GRAPH" in html
    payload = json.loads(GRAPH_JSON.read_text(encoding="utf-8"))
    assert payload["ontology"] == ONTOLOGY_IRI
    assert payload["nodes"]
    assert any(edge["kind"] == "subClassOf" for edge in payload["edges"])


def test_build_ontology_viz_script_runs() -> None:
    module = _load_module(BUILD_SCRIPT, "build_ontology_viz")
    viz_id = module.build_ontology_viz()
    assert viz_id == "hierarchical-graph"
    assert INDEX_HTML.is_file()
    assert EMBED_HTML.is_file()


def test_extract_graph_has_subclass_and_property_edges() -> None:
    extract = _load_module(EXTRACT_SCRIPT, "ontology_viz_extract")
    payload = extract.extract_ontology_graph()
    kinds = {edge["kind"] for edge in payload["edges"]}
    assert "subClassOf" in kinds
    assert "objectProperty" in kinds or "datatypeProperty" in kinds
    assert payload["nodeWidth"] > 0
    assert payload["nodeHeight"] > 0


def test_extract_prefers_skos_pref_label_for_nodes() -> None:
    extract = _load_module(EXTRACT_SCRIPT, "ontology_viz_extract")
    graph = Graph()
    node_uri = URIRef("https://ontology.growgraph.dev/graflo/LabelNode")
    graph.add((node_uri, RDF.type, OWL.Class))
    graph.add((node_uri, RDFS.label, Literal("Technical Label")))
    graph.add((node_uri, SKOS.prefLabel, Literal("User Label")))

    payload = extract.extract_ontology_graph(graph)
    node = next(item for item in payload["nodes"] if item["id"] == str(node_uri))
    assert node["label"] == "User Label"


def test_extract_label_fallback_is_rdfs_then_local_name() -> None:
    extract = _load_module(EXTRACT_SCRIPT, "ontology_viz_extract")
    graph = Graph()
    rdfs_node = URIRef("https://ontology.growgraph.dev/graflo/RdfsNode")
    local_node = URIRef("https://ontology.growgraph.dev/graflo/LocalNode")
    graph.add((rdfs_node, RDF.type, OWL.Class))
    graph.add((rdfs_node, RDFS.label, Literal("From RDFS")))
    graph.add((local_node, RDF.type, OWL.Class))

    payload = extract.extract_ontology_graph(graph)
    labels = {item["id"]: item["label"] for item in payload["nodes"]}
    assert labels[str(rdfs_node)] == "From RDFS"
    assert labels[str(local_node)] == "LocalNode"
