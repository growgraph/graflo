"""Smoke tests for the GraFlo ontology visualization build script.

The "derivation guard" tests below are what make the ontology -> viz pipeline
safe to leave unattended. The viewer's hierarchy is computed from the graph
(universal root, entry class, group seeds) rather than hardcoded, and each rule
must keep yielding a *unique* answer. If the ontology changes shape so that one
no longer does, ``extract.py`` degrades to neutral behaviour and these tests say
which rule broke.
"""

from __future__ import annotations

import importlib.util
import json
import re
from collections import deque
from pathlib import Path

import pytest
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import OWL, RDF, RDFS, SKOS

from graflo.rdf.namespace import GF_BASE, GF_VERSION
from graflo.rdf.utils import load_ontology_graph

REPO_ROOT = Path(__file__).resolve().parents[2]
BUILD_SCRIPT = REPO_ROOT / "docs" / "_build" / "scripts" / "build_ontology_viz.py"
EXTRACT_SCRIPT = (
    REPO_ROOT / "docs" / "_build" / "scripts" / "ontology_viz" / "extract.py"
)
OUTPUT_DIR = REPO_ROOT / "docs" / "assets" / "graflo-ontology-viz"
ONTOLOGY_DOC = REPO_ROOT / "docs" / "concepts" / "schema" / "ontology.md"
INDEX_HTML = OUTPUT_DIR / "index.html"
EMBED_HTML = OUTPUT_DIR / "embed.html"
GRAPH_JSON = OUTPUT_DIR / "graph-data.json"
ONTOLOGY_IRI = "https://ontology.growgraph.dev/graflo"

GRAFLO_ARTIFACT = f"{GF_BASE}GrafloArtifact"
GRAPH_MANIFEST = f"{GF_BASE}GraphManifest"


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def extract():
    return _load_module(EXTRACT_SCRIPT, "ontology_viz_extract")


@pytest.fixture(scope="module")
def payload(extract) -> dict:
    return extract.extract_ontology_graph()


@pytest.fixture(scope="module")
def ontology() -> Graph:
    return load_ontology_graph()


def _local(uri: str) -> str:
    return uri[len(GF_BASE) :] if uri.startswith(GF_BASE) else uri.rsplit("#", 1)[-1]


def _node(payload: dict, local: str) -> dict:
    return next(item for item in payload["nodes"] if item["local"] == local)


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


# --------------------------------------------------------------- derivation guards


def test_universal_root_is_unique(extract, ontology) -> None:
    """One class must dominate the taxonomy, else the viewer suppresses nothing."""
    class_uris = {
        str(subject)
        for subject in ontology.subjects(RDF.type, OWL.Class)
        if extract._include_class_uri(str(subject))
    }
    parents = extract._gf_parents(ontology, class_uris)
    assert extract.find_universal_root(parents) == GRAFLO_ARTIFACT


def test_entry_class_is_unique(extract, ontology) -> None:
    """Being a domain-but-never-a-range matches five classes on its own; only
    ``GraphManifest`` also has the universal root as its sole own superclass."""
    class_uris = {
        str(subject)
        for subject in ontology.subjects(RDF.type, OWL.Class)
        if extract._include_class_uri(str(subject))
    }
    parents = extract._gf_parents(ontology, class_uris)
    links = extract._object_property_links(ontology, class_uris)
    entry = extract.find_entry_class(links, parents, GRAFLO_ARTIFACT)
    assert entry == GRAPH_MANIFEST


def test_group_seeds_come_from_the_entry_class(payload) -> None:
    seeds = {
        group["label"]
        for group in payload["groups"]
        if group["id"] not in {"core", "external"}
    }
    assert seeds == {"Schema", "IngestionModel", "Bindings"}


def test_every_class_lands_in_a_derived_block(payload) -> None:
    """A newly added class must never vanish silently into the ``core`` bucket:
    only the universal root and the entry class itself belong there."""
    core = {item["id"] for item in payload["nodes"] if item["group"] == "core"}
    assert core == {GRAFLO_ARTIFACT, GRAPH_MANIFEST}
    declared = {group["id"] for group in payload["groups"]}
    assert all(item["group"] in declared for item in payload["nodes"])


def test_extract_is_deterministic(extract) -> None:
    """CI runs `git diff --exit-code` on the committed payload."""
    first = extract.graph_to_json(extract.extract_ontology_graph())
    second = extract.graph_to_json(extract.extract_ontology_graph())
    assert first == second


# ------------------------------------------------------------------ payload content


def test_enum_classes_are_detected_structurally(payload) -> None:
    """The old `endswith Type|Mode|Policy` heuristic missed these three."""
    for local in ("BoundSourceKind", "TransformTarget", "TransformStrategy"):
        assert _node(payload, local)["kind"] == "enum", local
    enums = {item["local"] for item in payload["nodes"] if item["kind"] == "enum"}
    assert "DBType" in enums and "FieldType" in enums


def test_enum_values_are_exposed(payload) -> None:
    values = {item["value"] for item in _node(payload, "DBType")["enumValues"]}
    assert {"arango", "neo4j", "postgres"} <= values


def test_datatype_properties_attach_to_their_domain(payload) -> None:
    labels = {
        item["label"]
        for item in _node(payload, "DatabaseProfile")["datatypeProperties"]
    }
    assert labels
    assert all(item["range"] for item in _node(payload, "Vertex")["datatypeProperties"])


def test_no_datatype_property_is_dropped(payload, ontology) -> None:
    """They used to be emitted as edges, which silently discarded all of them."""
    declared = {
        str(prop)
        for prop in ontology.subjects(RDF.type, OWL.DatatypeProperty)
        if str(prop).startswith(GF_BASE)
    }
    seen = {item["label"] for item in payload["sharedProperties"]}
    for node in payload["nodes"]:
        seen.update(item["label"] for item in node.get("datatypeProperties", []))
    assert payload["sharedProperties"]
    assert {_local(uri) for uri in declared} <= seen
    assert not any(edge["kind"] == "datatypeProperty" for edge in payload["edges"])


def test_universal_root_edges_stay_in_the_payload(payload) -> None:
    """Suppression is a rendering decision; the data stays a faithful projection."""
    assert payload["universalRoot"] == GRAFLO_ARTIFACT
    to_root = [
        edge
        for edge in payload["edges"]
        if edge["kind"] == "subClassOf" and edge["target"] == GRAFLO_ARTIFACT
    ]
    assert len(to_root) > 20
    assert any(item["id"] == GRAFLO_ARTIFACT for item in payload["nodes"])


# ------------------------------------------------------------------- band ranking
#
# The viewer ranks each band's columns in JS. These guards restate the same rule
# in Python and check it against the payload, so a divergence — or an ontology
# change that flattens a block — fails here rather than quietly turning a band
# back into an arbitrary grid.


def _band_depths(payload: dict, group_id: str) -> dict[str, int]:
    """Column index per class, by the rule ``graph-view.js`` documents.

    Shortest distance from the band's roots over ``subClassOf`` (child to
    superclass) plus composition (an object property's range to its domain),
    then relaxed so a subclass is strictly right of its superclass.
    """
    members = {item["id"] for item in payload["nodes"] if item["group"] == group_id}
    root = payload["universalRoot"]
    taxonomy = [
        (edge["source"], edge["target"])
        for edge in payload["edges"]
        if edge["kind"] == "subClassOf"
        and edge["target"] != root
        and {edge["source"], edge["target"]} <= members
    ]
    rank = set(taxonomy)
    rank.update(
        (edge["target"], edge["source"])
        for edge in payload["edges"]
        if edge["kind"] == "objectProperty"
        and {edge["source"], edge["target"]} <= members
        and edge["source"] != edge["target"]
    )

    parents: dict[str, set[str]] = {uri: set() for uri in members}
    children: dict[str, set[str]] = {uri: set() for uri in members}
    for child, parent in rank:
        parents[child].add(parent)
        children[parent].add(child)

    depth = {uri: 0 for uri in sorted(members) if not parents[uri]}
    queue = deque(sorted(depth))
    while queue:
        current = queue.popleft()
        for child in sorted(children[current]):
            if child not in depth:
                depth[child] = depth[current] + 1
                queue.append(child)
    for uri in members:
        depth.setdefault(uri, 0)

    for _ in range(len(members)):
        changed = False
        for child, parent in taxonomy:
            if depth[child] <= depth[parent]:
                depth[child] = depth[parent] + 1
                changed = True
        if not changed:
            break
    return depth


def test_every_block_has_a_single_root_column(payload) -> None:
    """A block whose root is not unique would read as several parallel trees."""
    for group in payload["groups"]:
        if group["id"] in {"core", "external"}:
            continue
        depths = _band_depths(payload, group["id"])
        roots = [uri for uri, value in depths.items() if value == 0]
        assert len(roots) == 1, (group["id"], sorted(roots))
        assert _local(roots[0]) == group["label"]


def test_specialisation_runs_left_to_right_in_every_band(payload) -> None:
    groups = {item["id"]: item["group"] for item in payload["nodes"]}
    root = payload["universalRoot"]
    checked = 0
    for group in payload["groups"]:
        depths = _band_depths(payload, group["id"])
        for edge in payload["edges"]:
            if edge["kind"] != "subClassOf" or edge["target"] == root:
                continue
            if groups.get(edge["source"]) != group["id"]:
                continue
            if groups.get(edge["target"]) != group["id"]:
                continue
            assert depths[edge["source"]] > depths[edge["target"]], edge
            checked += 1
    assert checked >= 8


def test_composition_ranks_the_schema_block(payload) -> None:
    """The regression this rule exists for: the Schema block has no internal
    ``subClassOf`` at all, so before composition ranked it these 17 classes were
    packed into an arbitrary grid with no general-to-specific reading."""
    depths = _band_depths(payload, "schema")
    by_local = {_local(uri): value for uri, value in depths.items()}
    chain = ["Schema", "CoreSchema", "VertexConfig", "Vertex", "Field", "FieldType"]
    assert [by_local[name] for name in chain] == sorted(by_local[n] for n in chain)
    assert len({by_local[name] for name in chain}) == len(chain)
    # Peers stay peers: the `edgeSource` reference between them must not push
    # `Vertex` a column behind `Edge`.
    assert by_local["Vertex"] == by_local["Edge"]


def test_every_connector_shares_one_column(payload) -> None:
    depths = _band_depths(payload, "bindings")
    by_local = {_local(uri): value for uri, value in depths.items()}
    connectors = [
        "FileConnector",
        "TableConnector",
        "SparqlConnector",
        "APIConnector",
        "KafkaConnector",
    ]
    assert {by_local[name] for name in connectors} == {by_local["BoundConnector"] + 1}


# ------------------------------------------------------------------- doc drift


def test_ontology_doc_states_the_current_version() -> None:
    assert GF_VERSION in ONTOLOGY_DOC.read_text(encoding="utf-8")


def test_ontology_doc_only_names_terms_that_exist(ontology) -> None:
    """The prose block list is hand-written against the same ontology the viz
    bands are derived from; keep the two from diverging."""
    text = ONTOLOGY_DOC.read_text(encoding="utf-8")
    section = text.split("## What the vocabulary covers", 1)[1].split("\n## ", 1)[0]
    known = {
        str(subject)
        for subject in ontology.subjects(unique=True)
        if str(subject).startswith(GF_BASE)
    }
    known_locals = {_local(uri) for uri in known}
    mentioned = set(re.findall(r"gf:([A-Za-z_][A-Za-z0-9_]*)", section))
    assert mentioned
    assert mentioned <= known_locals, sorted(mentioned - known_locals)
