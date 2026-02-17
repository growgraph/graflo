"""RDF data source hierarchy.

Provides two concrete data sources that share a common abstract parent:

* :class:`RdfFileDataSource` – reads local RDF files (Turtle, RDF/XML, N3,
  JSON-LD, …) via *rdflib*.
* :class:`SparqlEndpointDataSource` – queries a remote SPARQL endpoint
  (e.g. Apache Fuseki) via *SPARQLWrapper*.

Both convert RDF triples into flat dictionaries grouped by subject URI, one
dict per ``rdf:Class`` instance.

Requires the ``sparql`` extra::

    pip install graflo[sparql]
"""

from __future__ import annotations

import abc
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator

from pydantic import Field

from graflo.architecture.base import ConfigBaseModel
from graflo.data_source.base import AbstractDataSource, DataSourceType

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


def _triples_to_docs(
    graph: Graph,
    rdf_class: str | None = None,
) -> list[dict]:
    """Convert triples from *graph* into flat dictionaries grouped by subject.

    When *rdf_class* is given only subjects that are ``a <rdf_class>`` are
    returned.  Otherwise all subjects are included.

    Each dict has an ``_uri`` key with the full subject URI plus one key per
    predicate local-name.
    """
    from rdflib import RDF, URIRef
    from rdflib.term import BNode, Literal

    if rdf_class:
        subjects = {
            s
            for s in graph.subjects(RDF.type, URIRef(rdf_class))
            if isinstance(s, (URIRef, BNode))
        }
    else:
        subjects = {s for s in graph.subjects() if isinstance(s, (URIRef, BNode))}

    docs: list[dict] = []
    for subj in subjects:
        doc: dict = {"_uri": str(subj), "_key": _local_name(str(subj))}
        for pred, obj in graph.predicate_objects(subj):
            pred_name = _local_name(str(pred))
            if pred_name == "type":
                continue
            value = obj.toPython() if isinstance(obj, Literal) else str(obj)
            if pred_name in doc:
                existing = doc[pred_name]
                if isinstance(existing, list):
                    existing.append(value)
                else:
                    doc[pred_name] = [existing, value]
            else:
                doc[pred_name] = value
        docs.append(doc)
    return docs


def _sparql_results_to_docs(results: dict[str, Any]) -> list[dict]:
    """Convert a SPARQL ``SELECT ?s ?p ?o`` result set to flat dicts.

    Groups bindings by subject and converts predicate/object pairs to
    ``{predicate_local_name: value}`` dictionaries.
    """
    subjects: dict[str, dict] = {}
    for binding in results.get("results", {}).get("bindings", []):
        s_val = binding["s"]["value"]
        p_val = binding["p"]["value"]
        o_binding = binding["o"]

        p_name = _local_name(p_val)
        if p_name == "type":
            continue

        value: Any
        if o_binding["type"] == "literal":
            raw = o_binding["value"]
            datatype = o_binding.get("datatype", "")
            if "integer" in datatype:
                value = int(raw)
            elif "float" in datatype or "double" in datatype or "decimal" in datatype:
                value = float(raw)
            elif "boolean" in datatype:
                value = raw.lower() in ("true", "1")
            else:
                value = raw
        else:
            value = o_binding["value"]

        if s_val not in subjects:
            subjects[s_val] = {"_uri": s_val, "_key": _local_name(s_val)}

        doc = subjects[s_val]
        if p_name in doc:
            existing = doc[p_name]
            if isinstance(existing, list):
                existing.append(value)
            else:
                doc[p_name] = [existing, value]
        else:
            doc[p_name] = value

    return list(subjects.values())


# ------------------------------------------------------------------ #
# Abstract parent                                                     #
# ------------------------------------------------------------------ #


class RdfDataSource(AbstractDataSource, abc.ABC):
    """Abstract base for RDF data sources (file and endpoint).

    Captures the fields and batch-yielding logic shared by both
    :class:`RdfFileDataSource` and :class:`SparqlEndpointDataSource`.

    Attributes:
        rdf_class: Optional URI of the ``rdf:Class`` to filter subjects by.
    """

    source_type: DataSourceType = DataSourceType.SPARQL
    rdf_class: str | None = Field(
        default=None, description="URI of the rdf:Class to filter by"
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
                "Install it with: pip install graflo[sparql]"
            ) from exc

        g = Graph()
        g.parse(str(self.path), format=self._resolve_format())
        logger.info(
            "Parsed %d triples from %s (format=%s)",
            len(g),
            self.path,
            self._resolve_format(),
        )

        docs = _triples_to_docs(g, rdf_class=self.rdf_class)
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

    def build_query(self, offset: int = 0, limit: int | None = None) -> str:
        """Build a SPARQL SELECT query.

        If *sparql_query* is set it is returned with LIMIT/OFFSET appended.
        Otherwise generates::

            SELECT ?s ?p ?o WHERE { ?s a <rdf_class> . ?s ?p ?o . }
        """
        if self.sparql_query:
            base = self.sparql_query.rstrip().rstrip(";")
        else:
            graph_open = f"GRAPH <{self.graph_uri}> {{" if self.graph_uri else ""
            graph_close = "}" if self.graph_uri else ""
            class_filter = f"?s a <{self.rdf_class}> . " if self.rdf_class else ""
            base = (
                f"SELECT ?s ?p ?o WHERE {{ "
                f"{graph_open} "
                f"{class_filter}"
                f"?s ?p ?o . "
                f"{graph_close} "
                f"}}"
            )

        effective_limit = limit if limit is not None else self.page_size
        return f"{base} LIMIT {effective_limit} OFFSET {offset}"


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
                "Install it with: pip install graflo[sparql]"
            ) from exc

        sparql = SPARQLWrapper(self.config.endpoint_url)
        sparql.setReturnFormat(JSON)
        if self.config.username and self.config.password:
            sparql.setCredentials(self.config.username, self.config.password)
        return sparql

    def iter_batches(
        self, batch_size: int = 1000, limit: int | None = None
    ) -> Iterator[list[dict]]:
        """Query the SPARQL endpoint and yield batches of flat dictionaries.

        Paginates using SPARQL LIMIT/OFFSET.
        """
        wrapper = self._create_wrapper()
        offset = 0
        all_docs: list[dict] = []

        while True:
            query = self.config.build_query(offset=offset, limit=self.config.page_size)
            wrapper.setQuery(query)

            logger.debug("SPARQL query (offset=%d): %s", offset, query)
            results = wrapper.queryAndConvert()

            bindings = results.get("results", {}).get("bindings", [])
            if not bindings:
                break

            page_docs = _sparql_results_to_docs(results)
            all_docs.extend(page_docs)

            if len(bindings) < self.config.page_size:
                break

            offset += self.config.page_size

        yield from self._yield_batches(all_docs, batch_size, limit)


# Backward-compatible alias
SparqlDataSource = SparqlEndpointDataSource
