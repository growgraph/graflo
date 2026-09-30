"""RDF / OWL ontology inference manager.

Reads the TBox (class & property definitions) from an RDF source and
produces a graflo :class:`Schema` with vertices, edges, resources, and
:class:`Bindings`.

The mapping follows these conventions:

- ``owl:Class`` / ``rdfs:Class`` -> **Vertex**
- ``owl:DatatypeProperty`` (``rdfs:domain``) -> **Field** on the domain vertex
- ``owl:ObjectProperty`` (``rdfs:domain``, ``rdfs:range``) -> **Edge**
  (source = domain class, target = range class)
- Subject URI local name -> ``_key``
- Inferred vertex **identity** is ``["_uri"]`` so ABox ingestion keys on stable RDF subject URIs.
- **Object properties** (edges) become per-resource pipeline steps on the domain class that
  materialize the range vertex from the object-property field (URI reference) and emit the edge,
  including when domain and range are the same class (e.g. ``Publication`` -> ``Publication`` for ``cites``).
  The subject and the objects of each property are kept in their own role, so an
  edge joins the subject to the objects of its own property and to nothing else.
  A property with several ranges reads each range from ``<property>@<Class>``,
  the objects the source found typed with that class (``typed_objects`` on the
  connector).

Classes, fields, edges and resources are ordered by IRI.

Requires ``rdflib`` (a **core** dependency of ``graflo``).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, cast

from graflo.architecture.contract.bindings import (
    Bindings,
    SparqlConnector,
)
from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.contract.ingestion.resource import Resource
from graflo.architecture.contract.ingestion.steps.references import (
    Reference,
    subject_with_references,
)
from graflo.architecture.schema import (
    CoreSchema,
    GraphMetadata,
    Schema,
)
from graflo.architecture.schema.database_features import DatabaseProfile
from graflo.architecture.schema.edge import Edge, EdgeConfig, EdgeInverse
from graflo.architecture.schema.vertex import Field as VertexField
from graflo.architecture.schema.vertex import Vertex, VertexConfig
from graflo.onto import DBType

logger = logging.getLogger(__name__)


def _local_name(uri: str) -> str:
    """Extract the local name (fragment or last path segment) from a URI."""
    if "#" in uri:
        return uri.rsplit("#", 1)[-1]
    return uri.rsplit("/", 1)[-1]


def _ontology_classes(g: Any) -> dict[str, str]:
    """Named classes the ontology declares, as ``{local name: IRI}`` in IRI order.

    Anonymous classes (restrictions, unions) are not vertices. Two classes with
    one local name would share a vertex name, so they are refused.
    """
    from rdflib import OWL, RDF, RDFS, URIRef

    declared = {
        str(cls)
        for kind in (OWL.Class, RDFS.Class)
        for cls in g.subjects(RDF.type, kind)
        if isinstance(cls, URIRef)
    }
    classes: dict[str, str] = {}
    for uri in sorted(declared):
        name = _local_name(uri)
        if not name or uri.startswith((str(OWL), str(RDFS))):
            continue
        if name in classes:
            raise ValueError(
                f"Classes <{classes[name]}> and <{uri}> share the local name "
                f"'{name}', which is the vertex name; rename one of them."
            )
        classes[name] = uri
    return classes


def _object_edges(g: Any, classes: dict[str, str]) -> list[dict[str, str]]:
    """``{source, target, relation}`` per object property, domain and range, in IRI order."""
    from rdflib import OWL, RDF, RDFS

    class_names = {uri: name for name, uri in classes.items()}

    def named(prop: Any, predicate: Any) -> list[str]:
        return [
            class_names[uri]
            for uri in sorted({str(cls) for cls in g.objects(prop, predicate)})
            if uri in class_names
        ]

    edges: list[dict[str, str]] = []
    for op in sorted(set(g.subjects(RDF.type, OWL.ObjectProperty)), key=str):
        if op == OWL.sameAs:
            # An identity statement, settled when the data is read.
            continue
        op_name = _local_name(str(op))
        for src in named(op, RDFS.domain):
            for tgt in named(op, RDFS.range):
                edge_def = {"source": src, "target": tgt, "relation": op_name}
                if edge_def not in edges:
                    edges.append(edge_def)
    return edges


def _typed_properties(edges: list[dict[str, str]]) -> dict[str, list[str]]:
    """Per domain class, the properties with several ranges, in IRI order.

    A record carries no class for the objects of such a property, so they are
    read split by type.
    """
    ranges: dict[tuple[str, str], int] = {}
    for edge_def in edges:
        key = (edge_def["source"], edge_def["relation"])
        ranges[key] = ranges.get(key, 0) + 1
    typed: dict[str, list[str]] = {}
    for (source, relation), count in ranges.items():
        if count > 1:
            typed.setdefault(source, []).append(relation)
    return typed


def _declared_inverses(
    g: Any, relations: set[str]
) -> tuple[list[EdgeInverse], list[str]]:
    """``owl:inverseOf`` and ``owl:SymmetricProperty`` as GraFlo declarations.

    Only what the ontology *states* is carried over, restricted to relations
    that label an inferred edge. A property that is its own ``owl:inverseOf``
    is symmetric. Declarations are logical: nothing here decides whether an
    inverse is stored.

    An ontology can say more than the inverse table admits -- a property with
    two inverses, or one both symmetric and paired. Those statements are dropped
    with a log line rather than failing the inference: the edges are still
    right, and the author can declare what they meant.
    """
    from rdflib import OWL, RDF

    symmetric = {
        name
        for prop in g.subjects(RDF.type, OWL.SymmetricProperty)
        if (name := _local_name(str(prop))) in relations
    }
    partners: dict[str, set[str]] = {}
    for prop, _predicate, other in g.triples((None, OWL.inverseOf, None)):
        left, right = _local_name(str(prop)), _local_name(str(other))
        if left == right:
            if left in relations:
                symmetric.add(left)
            continue
        if left not in relations and right not in relations:
            continue
        partners.setdefault(left, set()).add(right)
        partners.setdefault(right, set()).add(left)

    ambiguous = {
        name
        for name, others in partners.items()
        if len(others) > 1 or name in symmetric
    }
    pairs: set[tuple[str, str]] = set()
    for name, others in partners.items():
        for other in others:
            if name in ambiguous or other in ambiguous:
                continue
            pairs.add((min(name, other), max(name, other)))
    if ambiguous:
        logger.warning(
            "owl:inverseOf statements not carried over -- each relation has at "
            "most one inverse, and a symmetric relation is its own: %s",
            sorted(ambiguous),
        )
    return (
        [EdgeInverse(relation=a, inverse=b) for a, b in sorted(pairs)],
        sorted(symmetric),
    )


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
        raw = cast(bytes, sparql.query().convert())
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
    ) -> tuple[Schema, IngestionModel]:
        """Infer a complete graflo Schema from an RDF/OWL ontology.

        Vertices use ``identity: ["_uri"]``. For each ``owl:ObjectProperty``, each
        source-class resource gets pipeline steps that (1) extract the range vertex
        from the predicate field via ``from: {_uri: <relation>}`` and (2) emit the
        edge, including when domain and range are the same class.

        Args:
            source: Path to an RDF file or a base URL (when using endpoint).
            endpoint_url: SPARQL endpoint to CONSTRUCT the ontology from.
            graph_uri: Named graph containing the ontology.
            schema_name: Name for the resulting schema.

        Returns:
            tuple[Schema, IngestionModel]: fully initialised schema and ingestion model.
        """
        from rdflib import OWL, RDF, RDFS

        g = _load_graph(source, endpoint_url=endpoint_url, graph_uri=graph_uri)

        # -- Discover classes -------------------------------------------------
        classes = _ontology_classes(g)  # local name -> IRI
        class_names = {uri: name for name, uri in classes.items()}

        logger.info("Discovered %d classes: %s", len(classes), list(classes.keys()))

        def named(prop: Any, predicate: Any) -> list[str]:
            """Vertex names of the classes *prop* points at, in IRI order."""
            return [
                class_names[uri]
                for uri in sorted({str(cls) for cls in g.objects(prop, predicate)})
                if uri in class_names
            ]

        # -- Discover datatype properties -> vertex fields --------------------
        fields_by_class: dict[str, list[str]] = {c: ["_key", "_uri"] for c in classes}

        for dp in sorted(set(g.subjects(RDF.type, OWL.DatatypeProperty)), key=str):
            dp_name = _local_name(str(dp))
            for domain_name in named(dp, RDFS.domain):
                if dp_name not in fields_by_class[domain_name]:
                    fields_by_class[domain_name].append(dp_name)

        # -- Discover object properties -> edges ------------------------------
        edges = _object_edges(g, classes)
        typed = _typed_properties(edges)

        logger.info("Discovered %d edges", len(edges))

        # -- Build Schema artefacts -------------------------------------------
        vertices = []
        for cls_name, fields in fields_by_class.items():
            vertex_fields = [VertexField(name=f) for f in fields]
            vertices.append(
                Vertex(name=cls_name, properties=vertex_fields, identity=["_uri"])
            )

        vertex_config = VertexConfig(vertices=vertices)

        inverses, symmetric = _declared_inverses(
            g, {e["relation"] for e in edges if e.get("relation")}
        )
        edge_objects = [
            Edge(
                source=e["source"],
                target=e["target"],
                relation=e.get("relation"),
                # A symmetric relation is realized by its edges being undirected.
                directed=e.get("relation") not in symmetric,
            )
            for e in edges
        ]
        edge_config = EdgeConfig(
            edges=edge_objects, inverses=inverses, symmetric=symmetric
        )
        logger.info(
            "Declared %d inverse pair(s) and %d symmetric relation(s)",
            len(inverses),
            len(symmetric),
        )

        # -- Build Resources (one per class) ----------------------------------
        edge_defs_by_source: dict[str, list[dict[str, str]]] = {}
        for edge_def in edges:
            edge_defs_by_source.setdefault(edge_def["source"], []).append(edge_def)

        resources: list[Resource] = []
        for cls_name in classes:
            # The object of a property is the target's `_uri`; a property with
            # several ranges reads each from the objects typed with it.
            references: list[Reference] = []
            for edge_def in edge_defs_by_source.get(cls_name, []):
                relation = field = edge_def["relation"]
                if relation in typed.get(cls_name, []):
                    field = f"{relation}@{edge_def['target']}"
                references.append(
                    (edge_def["target"], relation, {"_uri": field}, field)
                )
            resources.append(
                Resource(
                    name=cls_name,
                    pipeline=subject_with_references(cls_name, references),
                    infer_edges=not references,
                )
            )

        effective_name = schema_name or "rdf_schema"
        schema = Schema(
            metadata=GraphMetadata(name=effective_name),
            core_schema=CoreSchema(
                vertex_config=vertex_config, edge_config=edge_config
            ),
            db_profile=DatabaseProfile(db_flavor=self.target_db_flavor),
        )
        ingestion_model = IngestionModel(resources=resources)
        ingestion_model.finish_init(schema.core_schema)
        return schema, ingestion_model

    def create_bindings(
        self,
        source: str | Path,
        *,
        endpoint_url: str | None = None,
        graph_uri: str | None = None,
    ) -> Bindings:
        """Create :class:`Bindings` from an RDF ontology.

        One :class:`SparqlConnector` is created per ``owl:Class`` / ``rdfs:Class``.
        The ontology is always loaded from *source* (a local file).  The
        *endpoint_url* is attached to each connector for runtime data queries
        but is **not** used to load the ontology itself.

        Args:
            source: Path to an RDF file containing the ontology.
            endpoint_url: SPARQL endpoint for the data (ABox) at runtime.
            graph_uri: Named graph containing the data.

        Returns:
            Bindings with one SparqlConnector per class.
        """
        # Always load the ontology from the local file, not from the endpoint.
        g = _load_graph(source)
        classes = _ontology_classes(g)
        typed = _typed_properties(_object_edges(g, classes))

        bindings = Bindings()
        for cls_name, cls_uri in classes.items():
            connector = SparqlConnector(
                rdf_class=cls_uri,
                endpoint_url=endpoint_url,
                graph_uri=graph_uri,
                rdf_file=Path(source) if not endpoint_url else None,
                typed_objects=typed.get(cls_name, []),
            )
            bindings.add_connector(connector)
            bindings.bind_resource(cls_name, connector)

        logger.info(
            "Created %d SPARQL connectors from ontology",
            len(classes),
        )
        return bindings
