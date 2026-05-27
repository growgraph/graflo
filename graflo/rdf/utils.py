"""Shared helpers for GraFlo RDF serialization."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import quote

from rdflib import BNode, Graph, Literal, RDF, URIRef
from rdflib.namespace import XSD

from graflo.architecture.pipeline.runtime.actor.config.normalize import (
    normalize_actor_step,
)
from graflo.rdf import namespace as ns


def ontology_path() -> Path:
    """Return the packaged GraFlo meta-ontology Turtle file path."""
    return Path(__file__).resolve().parent / "ontology" / "graflo.ttl"


def load_ontology_graph() -> Graph:
    """Load the GraFlo meta-ontology into an rdflib graph."""
    graph = Graph()
    graph.parse(str(ontology_path()), format="turtle")
    return graph


def slug_token(value: str) -> str:
    """Normalize arbitrary text into a URI path segment."""
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip())
    return quote(cleaned.strip("-") or "item", safe="-._~")


def join_uri(base_uri: str, *parts: str) -> str:
    """Join base URI with path segments."""
    base = base_uri.rstrip("/") + "/"
    tail = "/".join(slug_token(part) for part in parts if part)
    return base + tail if tail else base.rstrip("/")


def json_literal(value: Any) -> Literal:
    """Encode a Python value as an xsd:string JSON literal."""
    return Literal(json.dumps(value, sort_keys=True), datatype=XSD.string)


def parse_json_literal(value: Literal | str | None) -> Any:
    """Decode a JSON literal back to Python."""
    if value is None:
        return None
    text = str(value)
    if not text:
        return None
    return json.loads(text)


def add_literal(
    graph: Graph,
    subject: URIRef | BNode,
    predicate: URIRef,
    value: Any,
) -> None:
    """Add a literal triple when value is not None."""
    if value is None:
        return
    if isinstance(value, bool):
        graph.add((subject, predicate, Literal(value, datatype=XSD.boolean)))
    elif isinstance(value, int):
        graph.add((subject, predicate, Literal(value, datatype=XSD.integer)))
    elif isinstance(value, float):
        graph.add((subject, predicate, Literal(value, datatype=XSD.decimal)))
    else:
        graph.add((subject, predicate, Literal(str(value))))


def add_enum_individual(
    graph: Graph,
    subject: URIRef | BNode,
    predicate: URIRef,
    value: str | None,
    mapping: dict[str, object],
) -> None:
    """Link subject to a named enumeration individual."""
    if value is None:
        return
    individual = mapping.get(str(value))
    if individual is not None:
        graph.add((subject, predicate, URIRef(str(individual))))


def add_rdf_list(
    graph: Graph,
    values: list[str],
) -> BNode | None:
    """Build an RDF collection for string values."""
    if not values:
        return None
    head = BNode()
    current = head
    for index, item in enumerate(values):
        graph.add((current, RDF.first, Literal(item)))
        if index == len(values) - 1:
            graph.add((current, RDF.rest, RDF.nil))
        else:
            nxt = BNode()
            graph.add((current, RDF.rest, nxt))
            current = nxt
    return head


def actor_step_type(step: dict[str, Any]) -> str:
    """Return normalized actor step type."""
    normalized = normalize_actor_step(dict(step))
    step_type = normalized.get("type")
    if not isinstance(step_type, str):
        raise ValueError(f"Unsupported pipeline step: {step!r}")
    return step_type


def actor_step_class(step_type: str) -> object:
    """Map actor step type to ontology class."""
    cls = ns.ACTOR_STEP_CLASSES.get(step_type)
    if cls is None:
        raise ValueError(f"Unknown actor step type: {step_type!r}")
    return cls


def reverse_enum(mapping: dict[str, object], individual: URIRef | BNode) -> str | None:
    """Resolve named individual IRI back to enum string."""
    individual_str = str(individual)
    for value, term in mapping.items():
        if str(term) == individual_str:
            return value
    return None
