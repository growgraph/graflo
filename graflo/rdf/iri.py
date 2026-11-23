"""IRIs for the domain terms a schema exports: classes, fields, relations.

**Provisional.** Core has one rule for vertex IRIs
(:func:`~graflo.architecture.schema.hierarchy.vertex_iri`) and none yet for
fields or edges. This module is the interim rule the outbound exporters share
(:mod:`graflo.rdf.tbox`, :mod:`graflo.interop.ossie`); it is private so it can
be replaced by a schema-level IRI policy without a deprecation:

- base: the caller's ``base_iri``, else ``metadata.base_iri``, else
  ``urn:graflo:<schema name>:``;
- vertex: ``semantics.iri``, else ``{base}{name}``;
- field: ``semantics.iri``, else ``{base}{Vertex}_{field}`` -- always qualified
  by its vertex, so a field's IRI does not change when another vertex gains a
  field of the same name;
- edge: ``semantics.iri``, else ``{base}{relation}`` when one (source, target)
  pair uses the relation, else ``{base}{source}_{relation}_{target}``, a
  subproperty of the domain-less ``{base}{relation}``; a relation-less edge is
  named ``{source}_{target}``.
"""

from __future__ import annotations

from collections import Counter

from suthing import slugify

from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.vertex import Field, Vertex


def _base(schema: Schema, base_iri: str | None) -> str:
    if base_iri:
        return base_iri
    if schema.metadata.base_iri:
        return schema.metadata.base_iri
    name = slugify(schema.metadata.name or "", fallback="manifest")
    return f"urn:graflo:{name}:"


def _ontology_iri(base: str) -> str:
    """The ontology's own IRI: the base without its trailing separator."""
    return base.rstrip("/#:") if base.endswith(("/", "#", ":")) else base


def _relation_name(edge: Edge) -> str:
    return edge.relation or f"{edge.source}_{edge.target}"


class _Minter:
    """IRIs for one schema's vertices, fields and edges."""

    def __init__(self, schema: Schema, base_iri: str | None = None) -> None:
        self.schema = schema
        self.base = _base(schema, base_iri)
        pairs = Counter(
            _relation_name(edge) for edge in schema.core_schema.edge_config.edges
        )
        self._shared = {name for name, count in pairs.items() if count > 1}
        self._vertices = {
            vertex.name: vertex for vertex in schema.core_schema.vertex_config.vertices
        }

    @property
    def ontology(self) -> str:
        return _ontology_iri(self.base)

    def vertex(self, vertex: Vertex) -> str:
        if vertex.semantics is not None and vertex.semantics.iri is not None:
            return vertex.semantics.iri
        return f"{self.base}{vertex.name}"

    def vertex_by_name(self, name: str) -> str:
        return self.vertex(self._vertices[name])

    def field(self, owner: str, field: Field) -> str:
        if field.semantics is not None and field.semantics.iri is not None:
            return field.semantics.iri
        return f"{self.base}{owner}_{field.name}"

    def relation(self, name: str) -> str:
        """The relation's own IRI: the edge's, or the parent of a shared one."""
        return f"{self.base}{name}"

    def is_shared(self, edge: Edge) -> bool:
        return _relation_name(edge) in self._shared

    def edge(self, edge: Edge) -> str:
        if edge.semantics is not None and edge.semantics.iri is not None:
            return edge.semantics.iri
        name = _relation_name(edge)
        if name in self._shared:
            return f"{self.base}{edge.source}_{name}_{edge.target}"
        return self.relation(name)
