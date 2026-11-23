"""A schema as a domain ontology: OWL 2 RL TBox and SHACL shapes.

Unlike :mod:`graflo.rdf.serializer`, which describes the manifest itself in the
``gf:`` meta-ontology, these functions describe the *domain* a schema models,
so an RDF tool can read it without graflo:

========================================  =====================================
Schema                                    OWL
========================================  =====================================
vertex                                    ``owl:Class``
``subclass_of``                           ``rdfs:subClassOf``
field                                     ``owl:DatatypeProperty``, one domain,
                                          XSD range; ``owl:FunctionalProperty``
                                          unless a ``LIST``
``identity``, ``secondary_identities``    ``owl:hasKey``, one axiom each
edge                                      ``owl:ObjectProperty``, domain x range
``subproperty_of``                        ``rdfs:subPropertyOf``
``edge_config.inverses`` / ``symmetric``  ``owl:inverseOf`` / ``owl:SymmetricProperty``
``semantics``                             ``rdfs:label``, ``skos:*``
========================================  =====================================

Edge attributes OWL has no construct for (properties, identities, undirected,
indirect) travel as the ``gf:edgeExtension`` annotation, and every exported term
carries its manifest name as ``gf:elementName``.

:func:`schema_to_shacl` emits the shapes a user's instance data must satisfy:
one ``sh:NodeShape`` per class, a property shape per field (identity fields
exactly once) and per edge. Keys are therefore stated twice, as ``owl:hasKey``
and as SHACL cardinalities, because ``owl:hasKey`` neither requires a value nor
enforces uniqueness on its own.

IRIs come from :mod:`graflo.rdf.iri`.
"""

from __future__ import annotations

from rdflib import RDF, RDFS, BNode, Graph, Literal, URIRef
from rdflib.namespace import OWL, SH, XSD
from suthing import slugify

from graflo.architecture.graph_types.enums import EdgeType
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.semantics import Semantics
from graflo.architecture.schema.vertex import Field, FieldType, field_type_value
from graflo.rdf import namespace as ns
from graflo.rdf.iri import _Minter
from graflo.rdf.utils import json_literal

#: The XSD datatype each scalar field type is exported as.
XSD_BY_FIELD_TYPE: dict[str, URIRef] = {
    FieldType.INT.value: XSD.integer,
    FieldType.UINT.value: XSD.nonNegativeInteger,
    FieldType.FLOAT.value: XSD.float,
    FieldType.DOUBLE.value: XSD.double,
    FieldType.BOOL.value: XSD.boolean,
    FieldType.STRING.value: XSD.string,
    FieldType.DATETIME.value: XSD.dateTime,
    FieldType.UUID.value: XSD.string,
}


def _datatype(field: Field) -> URIRef | None:
    type_value = field_type_value(field.type)
    if type_value == FieldType.LIST.value:
        type_value = field_type_value(field.item_type)
    return None if type_value is None else XSD_BY_FIELD_TYPE[type_value]


def _is_list(field: Field) -> bool:
    return field_type_value(field.type) == FieldType.LIST.value


def _new_graph(schema: Schema) -> Graph:
    graph = Graph()
    graph.bind("owl", OWL)
    graph.bind("skos", ns.SKOS)
    graph.bind("gf", ns.GF)
    for prefix, iri in sorted(schema.metadata.namespaces.items()):
        graph.bind(prefix, iri)
    return graph


def _add_semantics(graph: Graph, subject: URIRef, semantics: Semantics | None) -> None:
    if semantics is None:
        return
    if semantics.label:
        graph.add((subject, RDFS.label, Literal(semantics.label)))
    for synonym in semantics.synonyms:
        graph.add((subject, ns.altLabel, Literal(synonym)))
    for iri in semantics.exact_match:
        graph.add((subject, ns.exactMatch, URIRef(iri)))
    for iri in semantics.close_match:
        graph.add((subject, ns.closeMatch, URIRef(iri)))
    for iri in semantics.broader:
        graph.add((subject, ns.broader, URIRef(iri)))


def _add_term(
    graph: Graph,
    subject: URIRef,
    kind: URIRef,
    name: str,
    description: str | None,
    semantics: Semantics | None,
) -> None:
    graph.add((subject, RDF.type, kind))
    graph.add((subject, ns.elementName, Literal(name)))
    if semantics is None or not semantics.label:
        graph.add((subject, RDFS.label, Literal(name)))
    if description:
        graph.add((subject, RDFS.comment, Literal(description)))
    _add_semantics(graph, subject, semantics)


def _edge_extension(edge: Edge) -> dict[str, object]:
    extension: dict[str, object] = {}
    if edge.properties:
        extension["properties"] = [
            field.model_dump(mode="json", exclude_defaults=True)
            for field in edge.properties
        ]
    if edge.identities:
        extension["identities"] = edge.identities
    if not edge.directed:
        extension["directed"] = False
    if edge.type == EdgeType.INDIRECT:
        extension["type"] = EdgeType.INDIRECT.value
        extension["by"] = edge.by
    return extension


def _add_key(graph: Graph, cls: URIRef, label: str, properties: list[URIRef]) -> None:
    # Cells get ids derived from the key, so a serialization is reproducible:
    # random blank-node ids reorder a class's keys between runs.
    cells = [BNode(f"key_{label}_{index}") for index in range(len(properties))]
    graph.add((cls, OWL.hasKey, cells[0]))
    for index, (cell, prop) in enumerate(zip(cells, properties)):
        graph.add((cell, RDF.first, prop))
        rest = cells[index + 1] if index + 1 < len(cells) else RDF.nil
        graph.add((cell, RDF.rest, rest))


def schema_to_owl(schema: Schema, base_iri: str | None = None) -> Graph:
    """The domain ontology *schema* declares, as an OWL 2 RL TBox.

    Args:
        schema: An initialised schema.
        base_iri: Overrides ``schema.metadata.base_iri`` for minted IRIs.

    Returns:
        A new graph; serialize it with ``graph.serialize(format="turtle")``.
    """
    mint = _Minter(schema, base_iri)
    graph = _new_graph(schema)
    ontology = URIRef(mint.ontology)
    graph.add((ontology, RDF.type, OWL.Ontology))
    if schema.metadata.version:
        graph.add((ontology, OWL.versionInfo, Literal(schema.metadata.version)))
    if schema.metadata.description:
        graph.add((ontology, RDFS.comment, Literal(schema.metadata.description)))

    vertices = schema.core_schema.vertex_config.vertices
    for vertex in vertices:
        cls = URIRef(mint.vertex(vertex))
        _add_term(
            graph, cls, OWL.Class, vertex.name, vertex.description, vertex.semantics
        )
        for parent in vertex.subclass_of:
            target = (
                mint.vertex_by_name(parent.vertex)
                if parent.vertex is not None
                else parent.iri
            )
            graph.add((cls, RDFS.subClassOf, URIRef(str(target))))

        field_iris: dict[str, URIRef] = {}
        for field in vertex.properties:
            prop = URIRef(mint.field(vertex.name, field))
            field_iris[field.name] = prop
            _add_term(
                graph,
                prop,
                OWL.DatatypeProperty,
                field.name,
                field.description,
                field.semantics,
            )
            graph.add((prop, RDFS.domain, cls))
            datatype = _datatype(field)
            if datatype is not None:
                graph.add((prop, RDFS.range, datatype))
            if not _is_list(field):
                graph.add((prop, RDF.type, OWL.FunctionalProperty))

        keys = [vertex.identity] + [s.fields for s in vertex.secondary_identities]
        for number, key in enumerate(keys):
            if key and all(name in field_iris for name in key):
                label = f"{slugify(vertex.name, fallback='vertex')}_{number}"
                _add_key(graph, cls, label, [field_iris[name] for name in key])

    edge_config = schema.core_schema.edge_config
    for edge in edge_config.edges:
        prop = URIRef(mint.edge(edge))
        _add_term(
            graph,
            prop,
            OWL.ObjectProperty,
            edge.relation or f"{edge.source}_{edge.target}",
            edge.description,
            edge.semantics,
        )
        graph.add((prop, RDFS.domain, URIRef(mint.vertex_by_name(edge.source))))
        graph.add((prop, RDFS.range, URIRef(mint.vertex_by_name(edge.target))))
        if mint.is_shared(edge) and (
            edge.semantics is None or edge.semantics.iri is None
        ):
            parent = URIRef(mint.relation(edge.relation or ""))
            graph.add((parent, RDF.type, OWL.ObjectProperty))
            graph.add((parent, ns.elementName, Literal(edge.relation or "")))
            graph.add((prop, RDFS.subPropertyOf, parent))
        for parent_ref in edge.subproperty_of:
            if parent_ref.edge is not None:
                parent_edge = edge_config.edge_for(parent_ref.edge.edge_id())
                target = mint.edge(parent_edge)
            else:
                target = str(parent_ref.iri)
            graph.add((prop, RDFS.subPropertyOf, URIRef(target)))
        extension = _edge_extension(edge)
        if extension:
            graph.add((prop, ns.edgeExtension, json_literal(extension)))

    for pair in edge_config.inverses:
        graph.add(
            (
                URIRef(mint.relation(pair.relation)),
                OWL.inverseOf,
                URIRef(mint.relation(pair.inverse)),
            )
        )
    for relation in edge_config.symmetric:
        graph.add((URIRef(mint.relation(relation)), RDF.type, OWL.SymmetricProperty))
    return graph


def schema_to_shacl(schema: Schema, base_iri: str | None = None) -> Graph:
    """SHACL shapes for instance data of *schema* (SHACL-D).

    Args:
        schema: An initialised schema.
        base_iri: Overrides ``schema.metadata.base_iri`` for minted IRIs.

    Returns:
        A new graph of ``sh:NodeShape`` resources, one per vertex type.
    """
    mint = _Minter(schema, base_iri)
    graph = _new_graph(schema)
    graph.bind("sh", SH)
    edges_by_source: dict[str, list[Edge]] = {}
    for edge in schema.core_schema.edge_config.edges:
        edges_by_source.setdefault(edge.source, []).append(edge)

    for vertex in schema.core_schema.vertex_config.vertices:
        cls = URIRef(mint.vertex(vertex))
        shape = URIRef(f"{mint.base}{vertex.name}Shape")
        graph.add((shape, RDF.type, SH.NodeShape))
        graph.add((shape, SH.targetClass, cls))
        graph.add((shape, ns.elementName, Literal(vertex.name)))
        identity = set(vertex.identity)
        for field in vertex.properties:
            prop_shape = URIRef(f"{shape}_{field.name}")
            graph.add((shape, SH.property, prop_shape))
            graph.add((prop_shape, SH.path, URIRef(mint.field(vertex.name, field))))
            datatype = _datatype(field)
            if datatype is not None:
                graph.add((prop_shape, SH.datatype, datatype))
            if field.name in identity:
                graph.add((prop_shape, SH.minCount, Literal(1)))
            if field.name in identity or not _is_list(field):
                graph.add((prop_shape, SH.maxCount, Literal(1)))
        for edge in edges_by_source.get(vertex.name, []):
            relation = edge.relation or f"{edge.source}_{edge.target}"
            prop_shape = URIRef(f"{shape}_{relation}_{edge.target}")
            graph.add((shape, SH.property, prop_shape))
            graph.add((prop_shape, SH.path, URIRef(mint.edge(edge))))
            graph.add(
                (prop_shape, SH["class"], URIRef(mint.vertex_by_name(edge.target)))
            )
    return graph
