"""Extract class and property graph data from the GraFlo OWL ontology."""

from __future__ import annotations

import json
from typing import Any

from rdflib import Graph, URIRef
from rdflib.namespace import OWL, RDF, RDFS, SKOS

from graflo.rdf.namespace import GF_BASE, GF_ONTOLOGY_IRI, GF_VERSION
from graflo.rdf.utils import load_ontology_graph

PROV = URIRef("http://www.w3.org/ns/prov#")
EXTERNAL_PREFIXES = (
    "http://www.w3.org/ns/prov#",
    "http://www.w3.org/2002/07/owl#",
    "http://www.w3.org/2000/01/rdf-schema#",
    "http://www.w3.org/2001/XMLSchema#",
)

NODE_W = 168
NODE_H = 40


def local_name(uri: str) -> str:
    if uri.startswith(GF_BASE):
        return uri[len(GF_BASE) :]
    if "#" in uri:
        return uri.rsplit("#", 1)[-1]
    return uri.rsplit("/", 1)[-1]


def _label(graph: Graph, uri: URIRef) -> str:
    for candidate in graph.objects(uri, SKOS.prefLabel):
        return str(candidate)
    for candidate in graph.objects(uri, RDFS.label):
        return str(candidate)
    return local_name(str(uri))


def _comment(graph: Graph, uri: URIRef) -> str | None:
    for candidate in graph.objects(uri, RDFS.comment):
        return str(candidate)
    return None


def _node_kind(uri: str) -> str:
    if uri.startswith(GF_BASE):
        name = local_name(uri)
        if name.endswith("Type") or name.endswith("Mode") or name.endswith("Policy"):
            return "enum"
        return "gf"
    if uri.startswith(str(PROV)):
        return "external"
    return "external"


def _include_class_uri(uri: str) -> bool:
    if uri.startswith(GF_BASE):
        return True
    return uri.startswith(EXTERNAL_PREFIXES)


def extract_ontology_graph(graph: Graph | None = None) -> dict[str, Any]:
    """Build nodes and edges for the ontology viewer (layout runs in the browser)."""
    g = graph or load_ontology_graph()
    class_uris: set[str] = {
        str(subject)
        for subject in g.subjects(RDF.type, OWL.Class)
        if _include_class_uri(str(subject))
    }

    for child in list(class_uris):
        for parent in g.objects(URIRef(child), RDFS.subClassOf):
            if not isinstance(parent, URIRef):
                continue
            parent_uri = str(parent)
            if parent_uri.startswith(GF_BASE) or parent_uri.startswith(str(PROV)):
                class_uris.add(parent_uri)

    nodes: dict[str, dict[str, Any]] = {}
    for uri in sorted(class_uris):
        ref = URIRef(uri)
        nodes[uri] = {
            "id": uri,
            "label": _label(g, ref),
            "local": local_name(uri),
            "kind": _node_kind(uri),
            "comment": _comment(g, ref),
        }

    edges: list[dict[str, str]] = []
    for child in sorted(class_uris):
        child_ref = URIRef(child)
        for parent in g.objects(child_ref, RDFS.subClassOf):
            if not isinstance(parent, URIRef):
                continue
            parent_uri = str(parent)
            if parent_uri not in class_uris:
                continue
            edges.append(
                {
                    "id": f"sub:{child}->{parent_uri}",
                    "source": child,
                    "target": parent_uri,
                    "kind": "subClassOf",
                    "label": "subClassOf",
                }
            )

    # Explicit equivalent class relations (both directions represented once here;
    # renderer can visualize with directional styles as needed).
    seen_equiv: set[tuple[str, str]] = set()
    for left in sorted(class_uris):
        left_ref = URIRef(left)
        for right in g.objects(left_ref, OWL.equivalentClass):
            if not isinstance(right, URIRef):
                continue
            right_uri = str(right)
            if right_uri not in class_uris:
                continue
            key: tuple[str, str]
            if left < right_uri:
                key = (left, right_uri)
            else:
                key = (right_uri, left)
            if left == right_uri or key in seen_equiv:
                continue
            seen_equiv.add(key)
            edges.append(
                {
                    "id": f"equiv:{left}<->{right_uri}",
                    "source": left,
                    "target": right_uri,
                    "kind": "equivalentClass",
                    "label": "equivalentClass",
                }
            )

    for prop_type in (OWL.ObjectProperty, OWL.DatatypeProperty):
        for prop in g.subjects(RDF.type, prop_type):
            if not isinstance(prop, URIRef):
                continue
            prop_uri = str(prop)
            if not prop_uri.startswith(GF_BASE):
                continue
            prop_label = _label(g, prop)
            domain = g.value(prop, RDFS.domain)
            range_ = g.value(prop, RDFS.range)
            if not isinstance(domain, URIRef) or not isinstance(range_, URIRef):
                continue
            domain_uri = str(domain)
            range_uri = str(range_)
            if domain_uri not in nodes or range_uri not in nodes:
                continue
            kind = (
                "objectProperty"
                if prop_type == OWL.ObjectProperty
                else "datatypeProperty"
            )
            edges.append(
                {
                    "id": f"prop:{prop_uri}",
                    "source": domain_uri,
                    "target": range_uri,
                    "kind": kind,
                    "label": prop_label,
                }
            )

    return {
        "ontology": GF_ONTOLOGY_IRI,
        "version": GF_VERSION,
        "nodeWidth": NODE_W,
        "nodeHeight": NODE_H,
        "nodes": list(nodes.values()),
        "edges": edges,
    }


def graph_to_json(graph_data: dict[str, Any]) -> str:
    return json.dumps(graph_data, indent=2, sort_keys=True)


def escape_json_for_html(json_text: str) -> str:
    return json_text.replace("</", "<\\/")
