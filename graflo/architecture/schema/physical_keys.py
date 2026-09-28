"""Translate document keys between logical and stored property names.

The ingestion pipeline produces documents keyed by logical property names. A
database may store some of those properties under other names, recorded in
``DatabaseProfile.vertex_property_names`` and ``edge_specs[].property_names``.
:class:`PhysicalKeys` is the one place those maps are applied to documents: the
writer translates every document and field list it hands a backend, and the
schema-aware reads (``Connection.graph_neighbors`` / ``traverse``) translate
their arguments in and their results back out.

With no stored names on the profile every method returns its input unchanged
(the same object), so the translation costs nothing for the common case.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from graflo.architecture.graph_types import EdgeId, GraphContainer
from graflo.architecture.schema.database_features import DatabaseProfile
from graflo.architecture.schema.edge import EdgeConfig
from graflo.filter.onto import FilterExpression


def _invert(names: dict[str, str]) -> dict[str, str]:
    return {stored: logical for logical, stored in names.items()}


def _rekey(doc: dict[str, Any], names: dict[str, str]) -> dict[str, Any]:
    """*doc* with the keys in *names* renamed; *doc* itself when none occur.

    A C-level copy plus one move per renamed key: maps are short and documents
    wide, so this beats rebuilding every key. Renamed keys move to the end,
    which no database write or read depends on.
    """
    present = [key for key in names if key in doc]
    if not present:
        return doc
    out = dict(doc)
    values = {key: out.pop(key) for key in present}
    for key, value in values.items():
        out[names[key]] = value
    return out


class PhysicalKeys:
    """Logical <-> stored name translation for one resolved profile."""

    def __init__(self, profile: DatabaseProfile):
        self._vertex = {
            vertex: dict(names)
            for vertex, names in profile.vertex_property_names.items()
            if names
        }
        self._vertex_back = {
            vertex: _invert(names) for vertex, names in self._vertex.items()
        }
        self._edge: dict[EdgeId, dict[str, str]] = {
            spec.edge_id: dict(spec.property_names)
            for spec in profile.edge_specs
            if spec.purpose is None and spec.property_names
        }

    @property
    def active(self) -> bool:
        """Whether any property is stored under a different name."""
        return bool(self._vertex) or bool(self._edge)

    # -- vertices ---------------------------------------------------------

    def vertex_fields(self, vertex: str, fields: Iterable[str]) -> list[str]:
        """Stored names for logical *fields* of *vertex*."""
        names = self._vertex.get(vertex, {})
        return [names.get(field, field) for field in fields]

    def vertex_doc(self, vertex: str, doc: dict[str, Any]) -> dict[str, Any]:
        """*doc* keyed by stored names; *doc* itself when nothing is renamed."""
        return _rekey(doc, self._vertex.get(vertex, {}))

    def vertex_docs(
        self, vertex: str, docs: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        if vertex not in self._vertex:
            return docs
        return [self.vertex_doc(vertex, doc) for doc in docs]

    def logical_vertex_doc(self, vertex: str, doc: dict[str, Any]) -> dict[str, Any]:
        """A document read back from the database, keyed by logical names."""
        return _rekey(doc, self._vertex_back.get(vertex, {}))

    # -- edges ------------------------------------------------------------

    def edge_fields(self, edge_id: EdgeId, fields: Iterable[str]) -> list[str]:
        """Stored names for logical property *fields* of the schema edge *edge_id*."""
        names = self._edge.get(edge_id, {})
        return [names.get(field, field) for field in fields]

    def edge_triples(
        self,
        edge_id: EdgeId,
        source: str,
        target: str,
        triples: list[Any],
    ) -> list[Any]:
        """``(source_doc, target_doc, weights)`` triples keyed by stored names.

        *edge_id* is the schema edge the triples belong to, which differs from
        the container key for an edge whose relation is extracted per document.
        """
        edge_names = self._edge.get(edge_id, {})
        if not edge_names and source not in self._vertex and target not in self._vertex:
            return triples
        out: list[Any] = []
        for triple in triples:
            source_doc, target_doc, *rest = triple
            weights = rest[0] if rest else {}
            out.append(
                (
                    self.vertex_doc(source, source_doc),
                    self.vertex_doc(target, target_doc),
                    _rekey(weights, edge_names),
                    *rest[1:],
                )
            )
        return out

    # -- whole containers -------------------------------------------------

    def container(self, gc: GraphContainer, edge_config: EdgeConfig) -> GraphContainer:
        """*gc* with every vertex and edge document keyed by stored names.

        For backends that take a whole container (native bulk load). Edge keys
        whose schema edge cannot be found are passed through: the writer skips
        them the same way.
        """
        if not self.active:
            return gc
        vertices = {
            vertex: self.vertex_docs(vertex, docs)
            for vertex, docs in gc.vertices.items()
        }
        edges: dict[EdgeId, list[Any]] = {}
        for edge_id, triples in gc.edges.items():
            source, target, _relation = edge_id
            schema_id = schema_edge_id(edge_config, edge_id)
            edges[edge_id] = (
                self.edge_triples(schema_id, source, target, triples)
                if schema_id is not None
                else triples
            )
        return gc.model_copy(update={"vertices": vertices, "edges": edges})

    # -- schema-aware reads -----------------------------------------------

    def anchor_key(
        self, vertex: str, key: str | dict[str, Any]
    ) -> str | dict[str, Any]:
        """An anchor given as a field mapping, keyed by stored names; a raw id as-is."""
        return self.vertex_doc(vertex, key) if isinstance(key, dict) else key

    def edge_filter(self, filters: Any, edge_ids: Iterable[EdgeId]) -> Any:
        """An edge filter over the edges *edge_ids*, naming stored attributes.

        One filter is applied to every edge a walk follows, so a property it
        names must be stored under one name across them; edges sharing a
        relation always are.

        Raises:
            ValueError: if a property the filter names is stored under
                different names on different edges.
        """
        if filters is None or not self._edge:
            return filters
        names: dict[str, str] = {}
        conflicts: set[str] = set()
        for edge_id in edge_ids:
            for logical, stored in self._edge.get(edge_id, {}).items():
                if names.setdefault(logical, stored) != stored:
                    conflicts.add(logical)
        if not names:
            return filters
        expression = (
            filters
            if isinstance(filters, FilterExpression)
            else FilterExpression.from_dict(filters)
        )
        named = _filter_fields(expression)
        clash = sorted(conflicts & named)
        if clash:
            raise ValueError(
                f"Edge filter on {clash}: stored under different names on the "
                "edges this walk follows; restrict edge_types to one relation"
            )
        return expression.rename_fields(names)

    def logical_container(
        self, gc: GraphContainer, edge_config: EdgeConfig
    ) -> GraphContainer:
        """A container read from the database, keyed by logical names.

        Vertex documents are translated per type. Edge rows are dicts of edge
        attributes (plus the endpoint keys, which are left alone); a row read
        through a declared inverse name is translated with its stored edge's
        names.
        """
        if not self.active:
            return gc
        vertices = {
            vertex: (
                [self.logical_vertex_doc(vertex, doc) for doc in docs]
                if vertex in self._vertex_back
                else docs
            )
            for vertex, docs in gc.vertices.items()
        }
        edges: dict[EdgeId, list[Any]] = {}
        for edge_id, rows in gc.edges.items():
            stored_id = _stored_edge_id(edge_config, edge_id)
            back = _invert(self._edge.get(stored_id, {})) if stored_id else {}
            edges[edge_id] = (
                [_rekey(row, back) if isinstance(row, dict) else row for row in rows]
                if back
                else rows
            )
        return gc.model_copy(update={"vertices": vertices, "edges": edges})


def _filter_fields(expression: FilterExpression) -> set[str]:
    if expression.kind == "leaf":
        return {expression.field} if expression.field is not None else set()
    return set().union(*(_filter_fields(dep) for dep in expression.deps))


def _stored_edge_id(edge_config: EdgeConfig, edge_id: EdgeId) -> EdgeId | None:
    """The declared edge a read container key came from.

    As :func:`schema_edge_id`, plus an edge read through its declared inverse
    name, which a walk reports as ``(target, source, inverse)``.
    """
    found = schema_edge_id(edge_config, edge_id)
    if found is not None:
        return found
    source, target, relation = edge_id
    forward = edge_config.inverse_of(relation)
    if forward is None:
        return None
    return schema_edge_id(edge_config, (target, source, forward))


def schema_edge_id(edge_config: EdgeConfig, edge_id: EdgeId) -> EdgeId | None:
    """The declared edge a container key belongs to.

    An exact match first, then the ``relation=None`` edge that carries
    per-document relations.
    """
    if edge_id in edge_config:
        return edge_id
    null_id = (edge_id[0], edge_id[1], None)
    if null_id in edge_config:
        return null_id
    return None
