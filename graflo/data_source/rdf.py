"""RDF data source hierarchy.

Provides two concrete data sources that share a common abstract parent:

* :class:`RdfFileDataSource` – reads local RDF files (Turtle, RDF/XML, N3,
  JSON-LD, …) via *rdflib*.
* :class:`SparqlEndpointDataSource` – queries a remote SPARQL endpoint
  (e.g. Apache Fuseki) via *SPARQLWrapper*.

Both convert RDF triples into flat dictionaries grouped by subject URI, one
dict per ``rdf:Class`` instance.  Each document has ``_uri`` (full subject URI)
and ``_key`` (URI local name — the fragment or last path segment).  All other
identity decisions (e.g. using a domain-specific literal as the storage key)
belong in the schema layer, not here.

Two kinds of subject have no IRI of their own to key on, and both sources
treat them alike:

* A blank node is keyed on its content: ``_uri`` is ``_:<key>`` and ``_key``
  the key, the same on every read. A reference to it carries that ``_uri``.
* IRIs joined by ``owl:sameAs`` are one document under the smallest IRI; the
  others are listed in ``_same_as`` and references to them are rewritten.
  ``same_as="keep"`` reads the statements as an ordinary property instead.

Uses ``rdflib`` and ``SPARQLWrapper``, which are **core** dependencies of
``graflo`` (see ``pyproject.toml``).
"""

from __future__ import annotations

import abc
import logging
from collections.abc import Callable, Iterable, Iterator
from itertools import groupby
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from pydantic import Field

from graflo.architecture.base import ConfigBaseModel
from graflo.data_source.base import AbstractDataSource, DataSourceType
from graflo.data_source.rdf_identity import (
    BLANK_PREFIX,
    OWL_SAME_AS,
    BlankNodeKeys,
    SameAs,
    Term,
    term_text,
)

if TYPE_CHECKING:
    from rdflib import Graph

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------ #
# Shared helpers                                                      #
# ------------------------------------------------------------------ #

# rdflib extension -> format mapping
_EXT_FORMAT: dict[str, str] = {
    ".ttl": "turtle",
    ".turtle": "turtle",
    ".rdf": "xml",
    ".xml": "xml",
    ".n3": "n3",
    ".nt": "nt",
    ".nq": "nquads",
    ".jsonld": "json-ld",
    ".json": "json-ld",
    ".trig": "trig",
}


def _local_name(uri: str) -> str:
    """Extract the local name (fragment or last path segment) from a URI."""
    if "#" in uri:
        return uri.rsplit("#", 1)[-1]
    return uri.rsplit("/", 1)[-1]


class _Identity:
    """The ``_uri`` a subject, or a reference to one, is keyed on."""

    def __init__(self, same_as: SameAs, blank_keys: BlankNodeKeys) -> None:
        self.same_as = same_as
        self.blank_keys = blank_keys

    def uri(self, term: Term) -> str:
        if term.kind == "bnode":
            return BLANK_PREFIX + self.blank_keys.key(term.value)
        return self.same_as.canonical(term.value)

    def text(self, term: Term) -> str:
        """A spelling of *term* to order values by, stable across reads."""
        if term.kind == "bnode":
            return self.uri(term)
        return term_text(term)


def _new_doc(uri: str) -> dict[str, Any]:
    key = uri.removeprefix(BLANK_PREFIX) if uri.startswith(BLANK_PREFIX) else None
    return {"_uri": uri, "_key": key if key is not None else _local_name(uri)}


def _add_value(doc: dict[str, Any], name: str, value: Any) -> None:
    """Set *name*, or add to it: a property with several values holds a list."""
    if name not in doc:
        doc[name] = value
        return
    existing = doc[name]
    if isinstance(existing, list):
        if value not in existing:
            existing.append(value)
    elif existing != value:
        doc[name] = [existing, value]


#: The IRI of ``rdf:type``.
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"


def _values(value: Any) -> list[Any]:
    """A property's values as a list, whether it holds one or several."""
    if value is None:
        return []
    return list(value) if isinstance(value, list) else [value]


def split_objects_by_type(
    docs: list[dict[str, Any]],
    properties: Iterable[str],
    types_of: Callable[[list[str]], dict[str, set[str]]],
) -> None:
    """Add ``<property>@<Class>`` beside each listed property, in place.

    The new field holds the property's objects typed ``<Class>`` (the class
    IRI's local name); an object of several classes is under each, one of none
    under no field. *types_of* maps IRIs to their ``rdf:type`` IRIs.
    """
    wanted = list(dict.fromkeys(properties))
    if not wanted:
        return
    objects = sorted(
        {
            value
            for doc in docs
            for name in wanted
            for value in _values(doc.get(name))
            if isinstance(value, str) and not value.startswith(BLANK_PREFIX)
        }
    )
    if not objects:
        return
    types = types_of(objects)
    for doc in docs:
        for name in wanted:
            for value in _values(doc.get(name)):
                for cls in sorted(types.get(value, ())):
                    _add_value(doc, f"{name}@{_local_name(cls)}", value)


def _node_term(node: Any) -> Term:
    """An rdflib node as a :class:`Term`."""
    from rdflib.term import BNode, Literal

    if isinstance(node, Literal):
        datatype = str(node.datatype) if node.datatype is not None else None
        return Term("literal", str(node), datatype, node.language)
    return Term("bnode" if isinstance(node, BNode) else "iri", str(node))


def _triples_to_docs(
    graph: Graph,
    rdf_class: str | None = None,
    *,
    same_as: Literal["collapse", "keep"] = "collapse",
    typed_objects: Iterable[str] = (),
) -> list[dict]:
    """Convert triples from *graph* into flat dictionaries grouped by subject.

    When *rdf_class* is given only subjects that are ``a <rdf_class>`` are
    returned.  Otherwise all subjects are included. The objects of each
    property in *typed_objects* are also split by class
    (:func:`split_objects_by_type`).

    Each dict has ``_uri`` and ``_key`` plus one key per predicate local-name.
    Documents are in ``_uri`` order, and the values of a property in a fixed one.
    """
    from rdflib import OWL, RDF, URIRef
    from rdflib.term import BNode, Literal

    collapse = same_as == "collapse"
    components = SameAs(
        (str(left), str(right))
        for left, right in graph.subject_objects(OWL.sameAs)
        if collapse and isinstance(left, URIRef) and isinstance(right, URIRef)
    )
    identity = _Identity(
        components,
        BlankNodeKeys(
            lambda label: (
                (str(pred), _node_term(obj))
                for pred, obj in graph.predicate_objects(BNode(label))
            )
        ),
    )

    selected = (
        graph.subjects(RDF.type, URIRef(rdf_class)) if rdf_class else graph.subjects()
    )
    # One document per `_uri`: the members of a sameAs component, or the blank
    # nodes that share a content.
    members_by_uri: dict[str, set[Any]] = {}
    for subj in selected:
        if isinstance(subj, URIRef):
            members = {URIRef(iri) for iri in components.members(str(subj))}
        elif isinstance(subj, BNode):
            members = {subj}
        else:
            continue
        members_by_uri.setdefault(identity.uri(_node_term(subj)), set()).update(members)

    docs: list[dict] = []
    for uri in sorted(members_by_uri):
        doc = _new_doc(uri)
        for member in sorted(members_by_uri[uri], key=str):
            pairs = sorted(
                (str(pred), identity.text(_node_term(obj)), obj)
                for pred, obj in graph.predicate_objects(member)
            )
            for pred_iri, _, obj in pairs:
                pred_name = _local_name(pred_iri)
                if pred_name == "type" or (collapse and pred_iri == OWL_SAME_AS):
                    continue
                value = (
                    obj.toPython()
                    if isinstance(obj, Literal)
                    else identity.uri(_node_term(obj))
                )
                _add_value(doc, pred_name, value)
        aliases = sorted(
            str(member)
            for member in members_by_uri[uri]
            if isinstance(member, URIRef) and str(member) != uri
        )
        if aliases:
            doc["_same_as"] = aliases
        docs.append(doc)

    def types_of(iris: list[str]) -> dict[str, set[str]]:
        return {
            iri: {
                str(cls)
                for member in components.members(iri)
                for cls in graph.objects(URIRef(member), RDF.type)
                if isinstance(cls, URIRef)
            }
            for iri in iris
        }

    split_objects_by_type(docs, typed_objects, types_of)
    return docs


def _sparql_object_binding_to_value(o_binding: dict[str, Any]) -> Any:
    """Decode a SPARQL JSON result binding for ``?o`` into a Python value."""
    if o_binding["type"] == "literal":
        raw = o_binding["value"]
        datatype = o_binding.get("datatype", "")
        if "integer" in datatype:
            return int(raw)
        if "float" in datatype or "double" in datatype or "decimal" in datatype:
            return float(raw)
        if "boolean" in datatype:
            return raw.lower() in ("true", "1")
        return raw
    return o_binding["value"]


def _binding_term(binding: dict[str, Any]) -> Term:
    """One variable of a SPARQL JSON result row as a :class:`Term`."""
    kind = binding["type"]
    if kind == "uri":
        return Term("iri", binding["value"])
    if kind == "bnode":
        return Term("bnode", binding["value"])
    return Term(
        "literal", binding["value"], binding.get("datatype"), binding.get("xml:lang")
    )


def _merge_sparql_binding_into_doc(
    doc: dict[str, Any],
    binding: dict[str, Any],
    identity: _Identity,
    *,
    collapse: bool,
) -> None:
    """Merge one ``?s ?p ?o`` binding into an existing subject document dict."""
    p_val = binding["p"]["value"]
    p_name = _local_name(p_val)
    if p_name == "type" or (collapse and p_val == OWL_SAME_AS):
        return

    term = _binding_term(binding["o"])
    value = (
        _sparql_object_binding_to_value(binding["o"])
        if term.kind == "literal"
        else identity.uri(term)
    )
    _add_value(doc, p_name, value)


# ------------------------------------------------------------------ #
# Abstract parent                                                     #
# ------------------------------------------------------------------ #


class RdfDataSource(AbstractDataSource, abc.ABC):
    """Abstract base for RDF data sources (file and endpoint).

    Captures the fields and batch-yielding logic shared by both
    :class:`RdfFileDataSource` and :class:`SparqlEndpointDataSource`.

    Attributes:
        rdf_class: Optional URI of the ``rdf:Class`` to filter subjects by.
        same_as: ``collapse`` (default) reads IRIs joined by ``owl:sameAs`` as
            one document; ``keep`` reads the statements as a property.
    """

    source_type: DataSourceType = DataSourceType.SPARQL
    rdf_class: str | None = Field(
        default=None, description="URI of the rdf:Class to filter by"
    )
    same_as: Literal["collapse", "keep"] = Field(
        default="collapse",
        description="How owl:sameAs statements between IRIs are read.",
    )
    typed_objects: list[str] = Field(
        default_factory=list,
        description=(
            "Properties (local names) whose objects are also read by class, as "
            "``<property>@<Class>``: for a property with several ranges."
        ),
    )

    @staticmethod
    def _yield_batches(
        docs: list[dict], batch_size: int, limit: int | None
    ) -> Iterator[list[dict]]:
        """Apply *limit*, then yield *docs* in chunks of *batch_size*."""
        if limit is not None:
            docs = docs[:limit]
        for i in range(0, max(len(docs), 1), batch_size):
            batch = docs[i : i + batch_size]
            if batch:
                yield batch


# ------------------------------------------------------------------ #
# File transport                                                      #
# ------------------------------------------------------------------ #


class RdfFileDataSource(RdfDataSource):
    """Data source for local RDF files.

    Parses RDF files using *rdflib* and yields flat dictionaries grouped by
    subject URI.  Optionally filters by ``rdf_class`` so that only instances
    of a specific class are returned.

    Attributes:
        path: Path to the RDF file.
        rdf_format: Explicit rdflib format string (e.g. ``"turtle"``).
            When ``None`` the format is guessed from the file extension.
    """

    path: Path
    rdf_format: str | None = Field(
        default=None, description="rdflib serialization format"
    )

    def _resolve_format(self) -> str:
        """Return the rdflib format string, guessing from extension if needed."""
        if self.rdf_format:
            return self.rdf_format
        ext = self.path.suffix.lower()
        fmt = _EXT_FORMAT.get(ext)
        if fmt is None:
            raise ValueError(
                f"Cannot determine RDF format for extension '{ext}'. "
                f"Set rdf_format explicitly. Known: {list(_EXT_FORMAT.keys())}"
            )
        return fmt

    def iter_batches(
        self, batch_size: int = 1000, limit: int | None = None
    ) -> Iterator[list[dict]]:
        """Parse the RDF file and yield batches of flat dictionaries."""
        try:
            from rdflib import Graph
        except ImportError as exc:
            raise ImportError(
                "rdflib is required for RDF data sources. "
                "It is a core dependency of graflo; reinstall with "
                "`pip install --force-reinstall graflo` or install rdflib manually."
            ) from exc

        g = Graph()
        g.parse(str(self.path), format=self._resolve_format())
        logger.info(
            "Parsed %d triples from %s (format=%s)",
            len(g),
            self.path,
            self._resolve_format(),
        )

        docs = _triples_to_docs(
            g,
            rdf_class=self.rdf_class,
            same_as=self.same_as,
            typed_objects=self.typed_objects,
        )
        yield from self._yield_batches(docs, batch_size, limit)


# ------------------------------------------------------------------ #
# Endpoint transport                                                  #
# ------------------------------------------------------------------ #


class SparqlSourceConfig(ConfigBaseModel):
    """Configuration for a SPARQL endpoint data source.

    Attributes:
        endpoint_url: Full SPARQL query endpoint URL
            (e.g. ``http://localhost:3030/dataset/sparql``)
        rdf_class: URI of the rdf:Class whose instances to fetch
        graph_uri: Named graph to restrict the query to (optional)
        sparql_query: Custom SPARQL query override (optional)
        username: HTTP basic-auth username (optional)
        password: HTTP basic-auth password (optional)
        page_size: Number of results per SPARQL LIMIT/OFFSET page
    """

    endpoint_url: str
    rdf_class: str | None = None
    graph_uri: str | None = None
    sparql_query: str | None = None
    username: str | None = None
    password: str | None = None
    page_size: int = Field(default=10_000, description="SPARQL pagination page size")

    def _select(self, pattern: str) -> str:
        """``SELECT ?s ?p ?o`` over *pattern*, inside the named graph when one is set."""
        if self.graph_uri:
            pattern = f"GRAPH <{self.graph_uri}> {{ {pattern} }}"
        return f"SELECT ?s ?p ?o WHERE {{ {pattern} }}"

    def _page(self, base: str, offset: int, limit: int | None = None) -> str:
        effective_limit = limit if limit is not None else self.page_size
        return f"{base} LIMIT {effective_limit} OFFSET {offset}"

    def build_query(self, offset: int = 0, limit: int | None = None) -> str:
        """Build a SPARQL SELECT query.

        If *sparql_query* is set it is returned with LIMIT/OFFSET appended.
        Otherwise generates::

            SELECT ?s ?p ?o WHERE { ?s a <rdf_class> . ?s ?p ?o . }
        """
        if self.sparql_query:
            base = self.sparql_query.rstrip().rstrip(";")
        else:
            class_filter = f"?s a <{self.rdf_class}> . " if self.rdf_class else ""
            base = self._select(f"{class_filter}?s ?p ?o .")

        # Group bindings by subject during streaming pagination; requires all
        # triple rows for one ?s to appear contiguously in the result.
        order_clause = "" if "ORDER BY" in base.upper() else " ORDER BY ?s"
        return self._page(f"{base}{order_clause}", offset, limit)

    def same_as_query(self, offset: int = 0) -> str:
        """The ``owl:sameAs`` statements between IRIs, as ``?s`` and ``?o``."""
        base = self._select(f"?s <{OWL_SAME_AS}> ?o . FILTER(isIRI(?s) && isIRI(?o))")
        return self._page(f"{base} ORDER BY ?s ?o", offset)

    def blank_node_query(self, offset: int = 0) -> str:
        """Every triple whose subject is a blank node."""
        base = self._select("?s ?p ?o . FILTER(isBlank(?s))")
        return self._page(f"{base} ORDER BY ?s ?p ?o", offset)

    def types_query(self, iris: list[str], offset: int = 0) -> str:
        """The ``rdf:type`` of each of *iris*, as ``?s`` and ``?o``."""
        values = " ".join(f"<{iri}>" for iri in iris)
        base = self._select(
            f"VALUES ?s {{ {values} }} ?s ?p ?o . FILTER(?p = <{RDF_TYPE}>)"
        )
        return self._page(f"{base} ORDER BY ?s ?o", offset)

    def subjects_query(self, iris: list[str], offset: int = 0) -> str:
        """Every triple of the subjects *iris*."""
        values = " ".join(f"<{iri}>" for iri in iris)
        base = self._select(f"VALUES ?s {{ {values} }} ?s ?p ?o .")
        return self._page(f"{base} ORDER BY ?s", offset)


#: How many subjects one ``VALUES`` query names.
_SUBJECTS_PER_QUERY = 50


class _BlankNodeTriples:
    """The endpoint's blank-node triples, read the first time one is needed."""

    def __init__(self, read: Callable[[], Iterable[dict[str, Any]]]) -> None:
        self._read = read
        self._by_label: dict[str, list[tuple[str, Term]]] | None = None

    def _loaded(self) -> dict[str, list[tuple[str, Term]]]:
        if self._by_label is None:
            self._by_label = {}
            for binding in self._read():
                self._by_label.setdefault(binding["s"]["value"], []).append(
                    (binding["p"]["value"], _binding_term(binding["o"]))
                )
        return self._by_label

    def __contains__(self, label: str) -> bool:
        return label in self._loaded()

    def outgoing(self, label: str) -> list[tuple[str, Term]]:
        return self._loaded().get(label, [])


class SparqlEndpointDataSource(RdfDataSource):
    """Data source that reads from a SPARQL endpoint.

    Uses ``SPARQLWrapper`` to query an endpoint and returns flat dictionaries
    grouped by subject.

    Attributes:
        config: SPARQL source configuration.
    """

    config: SparqlSourceConfig

    def _create_wrapper(self) -> Any:
        """Create a configured ``SPARQLWrapper`` instance."""
        try:
            from SPARQLWrapper import JSON, SPARQLWrapper
        except ImportError as exc:
            raise ImportError(
                "SPARQLWrapper is required for SPARQL endpoint data sources. "
                "It is a core dependency of graflo; reinstall with "
                "`pip install --force-reinstall graflo` or install SPARQLWrapper manually."
            ) from exc

        sparql = SPARQLWrapper(self.config.endpoint_url)
        sparql.setReturnFormat(JSON)
        if self.config.username and self.config.password:
            sparql.setCredentials(self.config.username, self.config.password)
        return sparql

    def _bindings(
        self, wrapper: Any, query_at: Callable[[int], str]
    ) -> Iterator[dict[str, Any]]:
        """The rows of a paged query, fetched a page at a time as they are read."""
        page_size = self.config.page_size
        offset = 0
        while True:
            query = query_at(offset)
            wrapper.setQuery(query)
            logger.debug("SPARQL query (offset=%d): %s", offset, query)
            bindings = wrapper.queryAndConvert().get("results", {}).get("bindings", [])
            yield from bindings
            if len(bindings) < page_size:
                return
            offset += page_size

    def iter_batches(
        self, batch_size: int = 1000, limit: int | None = None
    ) -> Iterator[list[dict]]:
        """Query the SPARQL endpoint and yield batches of flat dictionaries.

        Paginates with SPARQL LIMIT/OFFSET on **bindings** (triple rows), merges
        rows into subject documents in a streaming fashion, and stops fetching
        once *limit* subjects have been yielded (when set).

        Two more reads happen when the graph calls for them. The ``owl:sameAs``
        statements are read first (unless ``same_as`` is ``keep``); the members
        of a component are held back and yielded last, as one document. The
        graph's blank-node triples are read when the first blank node is met,
        which keys blank nodes on their content; this relies on the endpoint
        giving a blank node the same label in every query of the read.
        """
        wrapper = self._create_wrapper()
        config = self.config
        collapse = self.same_as == "collapse"
        components = SameAs(
            (binding["s"]["value"], binding["o"]["value"])
            for binding in (
                self._bindings(wrapper, config.same_as_query) if collapse else ()
            )
        )
        blank_triples = _BlankNodeTriples(
            lambda: self._bindings(wrapper, config.blank_node_query)
        )
        identity = _Identity(components, BlankNodeKeys(blank_triples.outgoing))

        def fill(doc: dict[str, Any], rows: Iterable[dict[str, Any]]) -> None:
            for binding in rows:
                _merge_sparql_binding_into_doc(
                    doc, binding, identity, collapse=collapse
                )

        batch: list[dict] = []
        emitted = 0
        # Documents of sameAs components, by canonical IRI, and the members
        # the query returned for each.
        held: dict[str, dict[str, Any]] = {}
        returned: dict[str, set[str]] = {}

        def full() -> bool:
            return limit is not None and emitted >= limit

        def types_of(iris: list[str]) -> dict[str, set[str]]:
            canonical = {
                member: iri for iri in iris for member in components.members(iri)
            }
            members = sorted(canonical)
            found: dict[str, set[str]] = {}
            for start in range(0, len(members), _SUBJECTS_PER_QUERY):
                chunk = members[start : start + _SUBJECTS_PER_QUERY]
                for binding in self._bindings(
                    wrapper,
                    lambda offset, chunk=chunk: config.types_query(chunk, offset),
                ):
                    if binding["o"]["type"] == "uri":
                        found.setdefault(canonical[binding["s"]["value"]], set()).add(
                            binding["o"]["value"]
                        )
            return found

        def out(docs: list[dict]) -> list[dict]:
            split_objects_by_type(docs, self.typed_objects, types_of)
            return docs

        rows_by_subject = groupby(
            self._bindings(wrapper, lambda offset: config.build_query(offset=offset)),
            key=lambda binding: (binding["s"]["type"], binding["s"]["value"]),
        )
        for _, rows in rows_by_subject:
            if full():
                break
            first = next(rows)
            subject = _binding_term(first["s"])
            if subject.kind == "bnode" and subject.value not in blank_triples:
                raise ValueError(
                    f"{config.endpoint_url} returned a blank node under a label "
                    "its blank-node triples do not carry. Blank nodes are keyed "
                    "on their triples, which needs blank-node labels that stay "
                    "the same across the queries of one read."
                )
            uri = identity.uri(subject)
            if subject.kind == "iri" and len(components.members(uri)) > 1:
                returned.setdefault(uri, set()).add(subject.value)
                doc = held.setdefault(uri, _new_doc(uri))
                fill(doc, [first, *rows])
                continue
            doc = _new_doc(uri)
            fill(doc, [first, *rows])
            batch.append(doc)
            emitted += 1
            if len(batch) >= batch_size:
                yield out(batch)
                batch = []

        for uri in sorted(held):
            if full():
                break
            members = components.members(uri)
            absent = [iri for iri in members if iri not in returned[uri]]
            for start in range(0, len(absent), _SUBJECTS_PER_QUERY):
                chunk = absent[start : start + _SUBJECTS_PER_QUERY]
                fill(
                    held[uri],
                    self._bindings(
                        wrapper,
                        lambda offset, chunk=chunk: config.subjects_query(
                            chunk, offset
                        ),
                    ),
                )
            held[uri]["_same_as"] = [iri for iri in members if iri != uri]
            batch.append(held[uri])
            emitted += 1
            if len(batch) >= batch_size:
                yield out(batch)
                batch = []

        if batch:
            yield out(batch)


# Backward-compatible alias
SparqlDataSource = SparqlEndpointDataSource
