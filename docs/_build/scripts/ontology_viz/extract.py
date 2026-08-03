"""Extract class and property graph data from the GraFlo OWL ontology.

Everything the viewer needs to draw a hierarchy is *derived* from the graph here:
the universal root, the manifest entry class, the conceptual group seeds and group
membership. Nothing about a specific class is hardcoded, so the pipeline
``GraFlo model -> graflo.ttl -> viz`` stays automatic. Each derivation returns a
unique answer or falls back to a neutral one (no root, no grouping); tests in
``test/docs/test_build_ontology_viz.py`` assert uniqueness so a shape change in the
ontology fails loudly instead of degrading the picture silently.
"""

from __future__ import annotations

import json
from collections import deque
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

CORE_GROUP = "core"
EXTERNAL_GROUP = "external"


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


def _is_gf(uri: str) -> bool:
    return uri.startswith(GF_BASE)


def _include_class_uri(uri: str) -> bool:
    if _is_gf(uri):
        return True
    return uri.startswith(EXTERNAL_PREFIXES)


def _is_enum_class(graph: Graph, uri: str) -> bool:
    """A class is an enumeration iff the ontology declares individuals of it.

    Structural, unlike a name-suffix heuristic: ``gf:BoundSourceKind`` and
    ``gf:TransformTarget`` are enums despite not ending in Type/Mode/Policy.
    """
    return next(graph.subjects(RDF.type, URIRef(uri)), None) is not None


def _node_kind(graph: Graph, uri: str) -> str:
    if _is_gf(uri):
        return "enum" if _is_enum_class(graph, uri) else "gf"
    return "external"


def _gf_parents(graph: Graph, class_uris: set[str]) -> dict[str, list[str]]:
    """subClassOf parents restricted to classes we actually render."""
    return {
        child: sorted(
            str(parent)
            for parent in graph.objects(URIRef(child), RDFS.subClassOf)
            if isinstance(parent, URIRef) and str(parent) in class_uris
        )
        for child in sorted(class_uris)
    }


def _ancestors(child: str, parents: dict[str, list[str]]) -> set[str]:
    """Ancestor closure of ``child``, including itself; cycle-safe."""
    seen: set[str] = set()
    stack = [child]
    while stack:
        current = stack.pop()
        if current in seen:
            continue
        seen.add(current)
        stack.extend(parents.get(current, ()))
    return seen


def find_universal_root(parents: dict[str, list[str]]) -> str | None:
    """The single class every taxonomy participant descends from, or ``None``.

    Computed by intersecting ancestor closures rather than by counting children,
    so there is no threshold to tune. Returns ``None`` when the answer is not
    unique, in which case the viewer suppresses nothing.
    """
    closures = [_ancestors(child, parents) for child, links in parents.items() if links]
    if not closures:
        return None
    common = set.intersection(*closures)
    if len(common) != 1:
        return None
    return next(iter(common))


def _object_property_links(
    graph: Graph, class_uris: set[str]
) -> list[tuple[str, str, URIRef]]:
    """(domain, range, property) triples where both ends are rendered classes."""
    links: list[tuple[str, str, URIRef]] = []
    for prop in graph.subjects(RDF.type, OWL.ObjectProperty):
        if not isinstance(prop, URIRef) or not _is_gf(str(prop)):
            continue
        domain = graph.value(prop, RDFS.domain)
        range_ = graph.value(prop, RDFS.range)
        if not isinstance(domain, URIRef) or not isinstance(range_, URIRef):
            continue
        if str(domain) not in class_uris or str(range_) not in class_uris:
            continue
        links.append((str(domain), str(range_), prop))
    return sorted(links, key=lambda item: (item[0], item[1], str(item[2])))


def find_entry_class(
    links: list[tuple[str, str, URIRef]],
    parents: dict[str, list[str]],
    universal_root: str | None,
) -> str | None:
    """The manifest root: composes other blocks but is composed by nothing.

    Being a domain-but-never-a-range is not enough on its own (five classes
    qualify); requiring the universal root to be the only *own* superclass narrows
    it to one. Imported superclasses (``prov:Entity`` and friends) are ignored —
    they say how the class is published, not where it sits in the meta-model.
    Returns ``None`` if the answer is not unique.
    """
    if universal_root is None:
        return None
    domains = {domain for domain, _, _ in links}
    ranges = {range_ for _, range_, _ in links}
    candidates = sorted(
        uri
        for uri in domains - ranges
        if [parent for parent in parents.get(uri, ()) if _is_gf(parent)]
        == [universal_root]
    )
    if len(candidates) != 1:
        return None
    return candidates[0]


def _assign_groups(
    graph: Graph,
    class_uris: set[str],
    links: list[tuple[str, str, URIRef]],
    parents: dict[str, list[str]],
    entry_class: str | None,
) -> tuple[dict[str, str], list[dict[str, str]]]:
    """Partition classes into conceptual blocks seeded by the entry class.

    The seeds are whatever the entry class composes (``hasSchema``,
    ``hasIngestionModel``, ``hasBindings``, ...), so adding a block to the
    ontology adds a band to the viz with no code change. Falls back to a single
    ``core`` group when no entry class could be derived.
    """
    groups: dict[str, str] = {
        uri: (CORE_GROUP if _is_gf(uri) else EXTERNAL_GROUP) for uri in class_uris
    }
    if entry_class is None:
        return groups, [{"id": CORE_GROUP, "label": "Core"}]

    seeds = sorted({range_ for domain, range_, _ in links if domain == entry_class})
    seed_ids = {seed: local_name(seed).lower() for seed in seeds}

    children: dict[str, list[str]] = {}
    for child, links_ in parents.items():
        for parent in links_:
            children.setdefault(parent, []).append(child)

    adjacency: dict[str, list[str]] = {}
    for domain, range_, _ in links:
        adjacency.setdefault(domain, []).append(range_)
    for parent, kids in children.items():
        adjacency.setdefault(parent, []).extend(kids)
    for key, values in adjacency.items():
        adjacency[key] = sorted(set(values))

    assigned: dict[str, str] = {}
    queue: deque[str] = deque()
    for seed in seeds:
        assigned[seed] = seed_ids[seed]
        queue.append(seed)
    while queue:
        current = queue.popleft()
        for neighbour in adjacency.get(current, ()):
            if neighbour in assigned or not _is_gf(neighbour):
                continue
            assigned[neighbour] = assigned[current]
            queue.append(neighbour)

    groups.update(assigned)

    counts: dict[str, int] = {}
    for group_id in groups.values():
        counts[group_id] = counts.get(group_id, 0) + 1
    seed_order = sorted(
        seeds, key=lambda seed: (-counts.get(seed_ids[seed], 0), local_name(seed))
    )

    declared: list[dict[str, str]] = [{"id": CORE_GROUP, "label": "Core"}]
    declared.extend(
        {"id": seed_ids[seed], "label": _label(graph, URIRef(seed))}
        for seed in seed_order
    )
    if EXTERNAL_GROUP in counts:
        declared.append({"id": EXTERNAL_GROUP, "label": "External"})
    return groups, declared


def _enum_values(graph: Graph, uri: str) -> list[dict[str, str]]:
    values: list[dict[str, str]] = []
    for individual in graph.subjects(RDF.type, URIRef(uri)):
        if not isinstance(individual, URIRef):
            continue
        raw = graph.value(individual, URIRef(f"{GF_BASE}enumValue"))
        values.append(
            {
                "label": _label(graph, individual),
                "value": str(raw) if raw is not None else local_name(str(individual)),
            }
        )
    return sorted(values, key=lambda item: (item["value"], item["label"]))


def _datatype_properties(
    graph: Graph, class_uris: set[str]
) -> tuple[dict[str, list[dict[str, str]]], list[dict[str, str]]]:
    """Datatype properties keyed by domain, plus the domain-less shared ones.

    These never survived as edges (every range is ``xsd:*``, so the range is
    never a node); attaching them to their domain class keeps the information.
    """
    by_domain: dict[str, list[dict[str, str]]] = {}
    shared: list[dict[str, str]] = []
    for prop in graph.subjects(RDF.type, OWL.DatatypeProperty):
        if not isinstance(prop, URIRef) or not _is_gf(str(prop)):
            continue
        range_ = graph.value(prop, RDFS.range)
        entry = {
            "label": _label(graph, prop),
            "range": local_name(str(range_)) if isinstance(range_, URIRef) else "",
        }
        domain = graph.value(prop, RDFS.domain)
        if isinstance(domain, URIRef) and str(domain) in class_uris:
            by_domain.setdefault(str(domain), []).append(entry)
        else:
            shared.append(entry)
    for entries in by_domain.values():
        entries.sort(key=lambda item: item["label"])
    shared.sort(key=lambda item: item["label"])
    return by_domain, shared


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
            if parent_uri.startswith((GF_BASE, str(PROV))):
                class_uris.add(parent_uri)

    parents = _gf_parents(g, class_uris)
    links = _object_property_links(g, class_uris)
    universal_root = find_universal_root(parents)
    entry_class = find_entry_class(links, parents, universal_root)
    groups, declared_groups = _assign_groups(g, class_uris, links, parents, entry_class)
    datatype_by_domain, shared_properties = _datatype_properties(g, class_uris)

    nodes: dict[str, dict[str, Any]] = {}
    for uri in sorted(class_uris):
        ref = URIRef(uri)
        kind = _node_kind(g, uri)
        node: dict[str, Any] = {
            "id": uri,
            "label": _label(g, ref),
            "local": local_name(uri),
            "kind": kind,
            "group": groups.get(uri, CORE_GROUP),
            "comment": _comment(g, ref),
            "datatypeProperties": datatype_by_domain.get(uri, []),
        }
        if kind == "enum":
            node["enumValues"] = _enum_values(g, uri)
        nodes[uri] = node

    edges: list[dict[str, str]] = []
    for child in sorted(class_uris):
        for parent_uri in parents.get(child, ()):
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
    # renderer can visualize with directional styles as needed). None exist today,
    # but unlike datatype-property edges these are semantically reachable.
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

    for domain_uri, range_uri, prop in links:
        edges.append(
            {
                "id": f"prop:{prop}",
                "source": domain_uri,
                "target": range_uri,
                "kind": "objectProperty",
                "label": _label(g, prop),
            }
        )

    return {
        "ontology": GF_ONTOLOGY_IRI,
        "version": GF_VERSION,
        "nodeWidth": NODE_W,
        "nodeHeight": NODE_H,
        "universalRoot": universal_root,
        "entryClass": entry_class,
        "groups": declared_groups,
        "sharedProperties": shared_properties,
        "nodes": list(nodes.values()),
        "edges": edges,
    }


def graph_to_json(graph_data: dict[str, Any]) -> str:
    return json.dumps(graph_data, indent=2, sort_keys=True)


def escape_json_for_html(json_text: str) -> str:
    return json_text.replace("</", "<\\/")
