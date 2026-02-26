"""RDF / OWL ontology inference manager.

Reads the TBox (class & property definitions) from an RDF source and
produces a graflo :class:`Schema` with vertices, edges, resources, and
:class:`Patterns`.

The mapping follows these conventions:

- ``owl:Class`` / ``rdfs:Class`` -> **Vertex**
- ``owl:DatatypeProperty`` (``rdfs:domain``) -> **Field** on the domain vertex
- ``owl:ObjectProperty`` (``rdfs:domain``, ``rdfs:range``) -> **Edge**
  (source = domain class, target = range class)
- Subject URI local name -> ``_key``

Requires the ``sparql`` extra::

    pip install graflo[sparql]
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from graflo.architecture.edge import Edge, EdgeConfig
from graflo.architecture.database_features import DatabaseFeatures
from graflo.architecture.resource import Resource
from graflo.architecture.schema import Schema, SchemaMetadata
from graflo.architecture.vertex import Field as VertexField, Vertex, VertexConfig
from graflo.onto import DBType
from graflo.util.onto import Patterns, SparqlPattern

logger = logging.getLogger(__name__)


def _local_name(uri: str) -> str:
    """Extract the local name (fragment or last path segment) from a URI."""
    if "#" in uri:
        return uri.rsplit("#", 1)[-1]
    return uri.rsplit("/", 1)[-1]


def _load_graph(
    source: str | Path,
    *,
    endpoint_url: str | None = None,
    graph_uri: str | None = None,
) -> Any:
    """Load an rdflib Graph from a file or SPARQL endpoint.

    Args:
        source: Path to an RDF file **or** a SPARQL endpoint URL.
        endpoint_url: If provided, used as SPARQL endpoint (overrides *source*).
        graph_uri: Named graph to query from the endpoint.

    Returns:
        An ``rdflib.Graph`` instance.
    """
    from rdflib import Graph

    g = Graph()

    if endpoint_url:
        from SPARQLWrapper import N3, SPARQLWrapper

        sparql = SPARQLWrapper(endpoint_url)
        sparql.setReturnFormat(N3)

        query = "CONSTRUCT { ?s ?p ?o } WHERE { "
        if graph_uri:
            query += f"GRAPH <{graph_uri}> {{ ?s ?p ?o }} "
        else:
            query += "?s ?p ?o "
        query += "}"

        sparql.setQuery(query)
        raw: bytes = sparql.query().convert()  # type: ignore[assignment]
        g.parse(data=raw, format="n3")
    else:
        g.parse(str(source))

    logger.info("Loaded %d triples from %s", len(g), source or endpoint_url)
    return g


class RdfInferenceManager:
    """Infer a graflo :class:`Schema` from an RDF / OWL ontology.

    The manager reads the TBox (class and property declarations) from an
    rdflib ``Graph`` and constructs the corresponding graflo artefacts.

    Attributes:
        target_db_flavor: Target graph-database flavour for downstream
            schema sanitisation.
    """

    def __init__(self, target_db_flavor: DBType = DBType.ARANGO):
        self.target_db_flavor = target_db_flavor

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def infer_schema(
        self,
        source: str | Path,
        *,
        endpoint_url: str | None = None,
        graph_uri: str | None = None,
        schema_name: str | None = None,
    ) -> Schema:
        """Infer a complete graflo Schema from an RDF/OWL ontology.

        Args:
            source: Path to an RDF file or a base URL (when using endpoint).
            endpoint_url: SPARQL endpoint to CONSTRUCT the ontology from.
            graph_uri: Named graph containing the ontology.
            schema_name: Name for the resulting schema.

        Returns:
            A fully initialised :class:`Schema`.
        """
        from rdflib import OWL, RDF, RDFS

        g = _load_graph(source, endpoint_url=endpoint_url, graph_uri=graph_uri)

        # -- Discover classes -------------------------------------------------
        classes: dict[str, str] = {}  # local_name -> full URI
        for cls_uri in set(g.subjects(RDF.type, OWL.Class)) | set(
            g.subjects(RDF.type, RDFS.Class)
        ):
            uri_str = str(cls_uri)
            name = _local_name(uri_str)
            if (
                name
                and not uri_str.startswith(str(OWL))
                and not uri_str.startswith(str(RDFS))
            ):
                classes[name] = uri_str

        logger.info("Discovered %d classes: %s", len(classes), list(classes.keys()))

        # -- Discover datatype properties -> vertex fields --------------------
        fields_by_class: dict[str, list[str]] = {c: ["_key", "_uri"] for c in classes}

        for dp in g.subjects(RDF.type, OWL.DatatypeProperty):
            dp_name = _local_name(str(dp))
            for domain in g.objects(dp, RDFS.domain):
                domain_name = _local_name(str(domain))
                if domain_name in fields_by_class:
                    fields_by_class[domain_name].append(dp_name)

        # -- Discover object properties -> edges ------------------------------
        edges: list[dict[str, str]] = []
        for op in g.subjects(RDF.type, OWL.ObjectProperty):
            op_name = _local_name(str(op))
            domains = [_local_name(str(d)) for d in g.objects(op, RDFS.domain)]
            ranges = [_local_name(str(r)) for r in g.objects(op, RDFS.range)]

            for src in domains:
                for tgt in ranges:
                    if src in classes and tgt in classes:
                        edges.append(
                            {"source": src, "target": tgt, "relation": op_name}
                        )

        logger.info("Discovered %d edges", len(edges))

        # -- Build Schema artefacts -------------------------------------------
        vertices = []
        for cls_name, fields in fields_by_class.items():
            vertex_fields = [VertexField(name=f) for f in fields]
            vertices.append(Vertex(name=cls_name, fields=vertex_fields))

        vertex_config = VertexConfig(vertices=vertices)

        edge_objects = [
            Edge(
                source=e["source"],
                target=e["target"],
                relation=e.get("relation"),
            )
            for e in edges
        ]
        edge_config = EdgeConfig(edges=edge_objects)

        # -- Build Resources (one per class) ----------------------------------
        resources: list[Resource] = []
        for cls_name in classes:
            pipeline: list[dict[str, Any]] = [{"vertex": cls_name}]
            for edge_def in edges:
                if edge_def["source"] == cls_name:
                    pipeline.append(
                        {
                            "source": edge_def["source"],
                            "target": edge_def["target"],
                            "relation": edge_def.get("relation"),
                        }
                    )
            resources.append(Resource(resource_name=cls_name, pipeline=pipeline))

        effective_name = schema_name or "rdf_schema"
        schema = Schema(
            general=SchemaMetadata(name=effective_name),
            vertex_config=vertex_config,
            edge_config=edge_config,
            database_features=DatabaseFeatures(db_flavor=self.target_db_flavor),
            resources=resources,
        )
        schema.finish_init()
        return schema

    def create_patterns(
        self,
        source: str | Path,
        *,
        endpoint_url: str | None = None,
        graph_uri: str | None = None,
    ) -> Patterns:
        """Create :class:`Patterns` from an RDF ontology.

        One :class:`SparqlPattern` is created per ``owl:Class`` / ``rdfs:Class``.
        The ontology is always loaded from *source* (a local file).  The
        *endpoint_url* is attached to each pattern for runtime data queries
        but is **not** used to load the ontology itself.

        Args:
            source: Path to an RDF file containing the ontology.
            endpoint_url: SPARQL endpoint for the data (ABox) at runtime.
            graph_uri: Named graph containing the data.

        Returns:
            Patterns with one SparqlPattern per class.
        """
        from rdflib import OWL, RDF, RDFS

        # Always load the ontology from the local file, not from the endpoint.
        g = _load_graph(source)

        classes: dict[str, str] = {}
        for cls_uri in set(g.subjects(RDF.type, OWL.Class)) | set(
            g.subjects(RDF.type, RDFS.Class)
        ):
            uri_str = str(cls_uri)
            name = _local_name(uri_str)
            if (
                name
                and not uri_str.startswith(str(OWL))
                and not uri_str.startswith(str(RDFS))
            ):
                classes[name] = uri_str

        patterns = Patterns()
        for cls_name, cls_uri in classes.items():
            sp = SparqlPattern(
                rdf_class=cls_uri,
                endpoint_url=endpoint_url,
                graph_uri=graph_uri,
                rdf_file=Path(source) if not endpoint_url else None,
                resource_name=cls_name,
            )
            patterns.add_sparql_pattern(cls_name, sp)

        logger.info(
            "Created %d SPARQL patterns from ontology", len(patterns.sparql_patterns)
        )
        return patterns
