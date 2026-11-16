"""Statements stored as the provenance aspect's ``Assertion`` vertices, and read back.

One ``Assertion`` is one :class:`~graflo.link.statements.Statement`, one triple
(``rdf:Statement``):

- ``Assertion -subject-> <Type>``: the entity the statement is about;
- ``predicate``: the property or the relation stated;
- for a property, ``value``: the stated value as text, read back as the
  property's declared type; for a relation, ``Assertion -object-> <Type>``: the
  target;
- ``polarity``, ``modality``, ``valid_from`` / ``valid_to``, ``granularity``,
  ``quote``, ``span_start`` / ``span_end`` and ``basis``, as on the statement;
- ``Assertion -wasDerivedFrom-> SourceRecord``: the record it was read from, whose
  ``written_at`` is when it was said.

Whether a statement states a property or a relation follows from whether it has
an ``object`` edge; the entity's type, from its ``subject`` edge.
:func:`statement_container` writes statements in this shape and
:func:`read_statements` reads them back, about one entity or all of them;
:func:`graph_time` is the newest time a record was written.
"""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from typing import Any, cast

from pydantic import Field

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.evolution.aspects.provenance import vocabulary as vocab
from graflo.architecture.evolution.sanitize import with_physical_names
from graflo.architecture.graph_types.container import GraphContainer
from graflo.architecture.graph_types.enums import EdgeDirection
from graflo.architecture.query.models import TraverseQuery
from graflo.architecture.schema.coerce import CoercionError, coerce_value
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.physical_keys import PhysicalKeys
from graflo.architecture.schema.vertex import FieldType
from graflo.db.conn import Connection
from graflo.filter.onto import FilterExpression
from graflo.link.statements import Basis, Statement, entity_key
from graflo.onto import AggregationType

ENDS = (vocab.SUBJECT, vocab.OBJECT)
"""The relations an ``Assertion`` names its subject and its object by."""


class Evidence(ConfigBaseModel):
    """Statements read from a graph, with the records they were read from."""

    statements: list[Statement] = Field(default_factory=list)
    records: dict[str, dict[str, Any]] = Field(
        default_factory=dict,
        description="``SourceRecord`` documents by ``source_record_id``.",
    )
    reserved: dict[str, dict[str, Any]] = Field(
        default_factory=dict,
        description=(
            "Per statement id, the reserved fields its ``Assertion`` carries, "
            "such as the access zones it was written in."
        ),
    )


def stated_types(schema: Schema) -> frozenset[str]:
    """The types *schema* lets a statement be about: each ``Assertion -subject->`` target."""
    return frozenset(
        edge.target
        for edge in schema.core_schema.edge_config.values()
        if edge.source == vocab.ASSERTION and edge.relation == vocab.SUBJECT
    )


def _identity(schema: Schema, vertex_type: str) -> list[str]:
    return list(schema.core_schema.vertex_config.identity_fields(vertex_type))


def _key_doc(text: str) -> dict[str, Any]:
    return json.loads(text)


def _text(value: Any) -> str:
    """A stated value as the text ``value`` stores: strings as they are, else JSON."""
    return value if isinstance(value, str) else json.dumps(value, default=str)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def assertion_doc(statement: Statement) -> dict[str, Any]:
    """The ``Assertion`` document for one statement, without its edges."""
    doc: dict[str, Any] = {
        vocab.ASSERTION_ID: statement.id,
        vocab.PREDICATE: statement.slot,
        vocab.VALUE: None if statement.kind == "relation" else _text(statement.value),
        vocab.POLARITY: statement.polarity,
        vocab.MODALITY: statement.modality,
        vocab.VALID_FROM: _iso(statement.valid_from),
        vocab.VALID_TO: _iso(statement.valid_to),
        vocab.GRANULARITY: statement.granularity,
        vocab.QUOTE: statement.quote,
        vocab.SPAN_START: statement.span_start,
        vocab.SPAN_END: statement.span_end,
        vocab.BASIS: Basis(statement.basis).name.lower(),
    }
    return {k: v for k, v in doc.items() if v is not None}


def statement_container(
    schema: Schema, statements: Iterable[Statement]
) -> GraphContainer:
    """The ``Assertion`` documents and their edges for *statements*, ready to write.

    Each statement gives one ``Assertion``, its ``subject`` edge, its ``object``
    edge for a relation, and its ``wasDerivedFrom`` edge when it names a record.
    A statement given twice is written once.

    Raises:
        ValueError: *schema* has no ``Assertion``, or a statement is about a
            type it declares no ``subject`` (or ``object``) edge to.
    """
    core = schema.core_schema
    statements = list(statements)
    if not statements:
        return GraphContainer()
    if vocab.ASSERTION not in core.vertex_config.vertex_set:
        raise ValueError("the schema has no Assertion type to store statements in")
    declared = {
        (edge.target, edge.relation)
        for edge in core.edge_config.values()
        if edge.source == vocab.ASSERTION
    }
    docs: dict[str, dict[str, Any]] = {}
    edges: dict[tuple[str, str, str | None], list[Any]] = defaultdict(list)
    for statement in statements:
        if statement.id in docs:
            continue
        ends = [(vocab.SUBJECT, statement.vertex_type, statement.key)]
        if statement.kind == "relation":
            if statement.target_type is None:
                raise ValueError(f"relation statement {statement.id} names no target")
            ends.append((vocab.OBJECT, statement.target_type, str(statement.value)))
        for relation, vertex_type, _ in ends:
            if (vertex_type, relation) not in declared:
                raise ValueError(
                    f"the schema declares no Assertion -{relation}-> {vertex_type}"
                )
        docs[statement.id] = assertion_doc(statement)
        ref = {vocab.ASSERTION_ID: statement.id}
        for relation, vertex_type, key in ends:
            edges[vocab.ASSERTION, vertex_type, relation].append(
                (ref, _key_doc(key), {})
            )
        if statement.record is not None:
            edges[vocab.ASSERTION, vocab.SOURCE_RECORD, vocab.DERIVED_FROM].append(
                (ref, {vocab.SOURCE_RECORD_ID: statement.record}, {})
            )
    return GraphContainer(
        vertices={vocab.ASSERTION: list(docs.values())} if docs else {},
        edges=dict(edges),
    )


def _time(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        moment = value
    else:
        try:
            moment = datetime.fromisoformat(str(value))
        except ValueError:
            return None
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)


def _basis(value: Any) -> Basis:
    try:
        return Basis[str(value).upper()]
    except KeyError:
        return Basis.EXTRACTED


class _Decoder:
    """Assertion documents and their ends, by assertion id, into statements."""

    def __init__(self, schema: Schema, thread_field: str | None) -> None:
        core = schema.core_schema
        self._schema = schema
        self._thread = thread_field
        self._fields = {
            vertex.name: {f.name: f for f in vertex.properties}
            for vertex in core.vertex_config.vertices
        }
        self._reserved = [
            f.name for f in core.vertex_config.properties(vocab.ASSERTION) if f.reserved
        ]
        self.assertions: dict[str, dict[str, Any]] = {}
        self.ends: dict[str, dict[str, tuple[str, dict[str, Any]]]] = defaultdict(dict)
        self.records: dict[str, dict[str, Any]] = {}
        self.record_of: dict[str, str] = {}

    def _value(self, vertex_type: str, slot: str, text: Any) -> Any:
        field = self._fields.get(vertex_type, {}).get(slot)
        if field is None or not isinstance(text, str):
            return text
        raw: Any = text
        if field.type == FieldType.LIST:
            try:
                raw = json.loads(text)
            except json.JSONDecodeError:
                raw = text
        try:
            return coerce_value(raw, field)
        except CoercionError:
            return text

    def evidence(self) -> Evidence:
        """Every assertion read with its subject, as statements."""
        found = Evidence()
        for aid, doc in sorted(self.assertions.items()):
            ends = self.ends.get(aid, {})
            if vocab.SUBJECT not in ends or not doc.get(vocab.PREDICATE):
                continue
            s_type, s_doc = ends[vocab.SUBJECT]
            s_key = entity_key(s_doc, _identity(self._schema, s_type))
            slot = str(doc[vocab.PREDICATE])
            if vocab.OBJECT in ends:
                o_type, o_doc = ends[vocab.OBJECT]
                value: Any = entity_key(o_doc, _identity(self._schema, o_type))
                kind, target = "relation", o_type
            else:
                kind, target = "property", None
                value = self._value(s_type, slot, doc.get(vocab.VALUE))
            record = self.records.get(self.record_of.get(aid, ""), {})
            thread = record.get(self._thread) if self._thread else None
            found.statements.append(
                Statement(
                    id=aid,
                    vertex_type=s_type,
                    key=s_key,
                    slot=slot,
                    kind=kind,
                    value=value,
                    target_type=target,
                    polarity=doc.get(vocab.POLARITY) or "stated",
                    modality=doc.get(vocab.MODALITY) or "actual",
                    valid_from=_time(doc.get(vocab.VALID_FROM)),
                    valid_to=_time(doc.get(vocab.VALID_TO)),
                    granularity=doc.get(vocab.GRANULARITY) or None,
                    said_at=_time(record.get(vocab.WRITTEN_AT)),
                    basis=_basis(doc.get(vocab.BASIS)),
                    record=self.record_of.get(aid),
                    thread=None if thread is None else str(thread),
                    quote=doc.get(vocab.QUOTE),
                    span_start=doc.get(vocab.SPAN_START),
                    span_end=doc.get(vocab.SPAN_END),
                )
            )
            rid = self.record_of.get(aid)
            if rid is not None and rid in self.records:
                found.records[rid] = self.records[rid]
            reserved = {f: doc[f] for f in self._reserved if f in doc}
            if reserved:
                found.reserved[aid] = reserved
        return found


def _assertion_edges(schema: Schema) -> list[Edge]:
    return [
        edge
        for edge in schema.core_schema.edge_config.values()
        if edge.source == vocab.ASSERTION
        and (
            edge.relation in ENDS
            or (
                edge.relation == vocab.DERIVED_FROM
                and edge.target == vocab.SOURCE_RECORD
            )
        )
    ]


class _Stored:
    """Reads of whole types and edges in logical names, through one connection."""

    def __init__(
        self, conn: Connection, schema: Schema, guard: FilterExpression | None
    ) -> None:
        named = with_physical_names(schema, conn.flavor)
        self._conn = conn
        self._schema = schema
        self._keys = PhysicalKeys(named.db_profile)
        self._db_aware = named.resolve_db_aware(conn.flavor)
        self._guard = guard

    def docs(self, vertex_type: str) -> list[dict[str, Any]]:
        storage = self._db_aware.vertex_config.vertex_dbname(vertex_type)
        rows = (
            self._conn.fetch_all_docs(storage)
            if self._guard is None
            else self._conn.fetch_docs(storage, filters=cast(Any, self._guard))
        )
        return [self._keys.logical_vertex_doc(vertex_type, d) for d in rows or []]

    def edges(self, edge: Edge) -> list[tuple[dict[str, Any], dict[str, Any]]]:
        """``(source, target)`` identity documents of every stored *edge*."""
        vc = self._db_aware.vertex_config
        ec = self._db_aware.edge_config
        source, target = edge.source, edge.target
        rows = self._conn.fetch_all_edges(
            vc.vertex_dbname(source),
            vc.vertex_dbname(target),
            ec.relation_dbname(edge),
            match_keys_source=tuple(
                self._keys.vertex_fields(source, _identity(self._schema, source))
            ),
            match_keys_target=tuple(
                self._keys.vertex_fields(target, _identity(self._schema, target))
            ),
            collection_name=ec.runtime(edge).storage_name(),
        )
        guard = self._guard
        return [
            (
                self._keys.logical_vertex_doc(source, row[0]),
                self._keys.logical_vertex_doc(target, row[1]),
            )
            for row in rows or []
            if guard is None or guard.matches(row[2] if len(row) > 2 else {})
        ]

    def newest(
        self, vertex_type: str, field: str, *, timeout_s: float | None = None
    ) -> Any:
        storage = self._db_aware.vertex_config.vertex_dbname(vertex_type)
        (stored,) = self._keys.vertex_fields(vertex_type, [field])
        kwargs: dict[str, Any] = {} if timeout_s is None else {"timeout_s": timeout_s}
        return self._conn.aggregate(
            storage,
            AggregationType.MAX,
            aggregated_field=stored,
            filters=self._guard,
            **kwargs,
        )


def stored_edges(
    conn: Connection,
    schema: Schema,
    edge: Edge,
    *,
    guard: FilterExpression | None = None,
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    """Every stored *edge* as ``(source, target)`` identity documents, in logical names."""
    return _Stored(conn, schema, guard).edges(edge)


def read_statements(
    conn: Connection,
    schema: Schema,
    *,
    about: tuple[str, Mapping[str, Any]] | None = None,
    guard: FilterExpression | None = None,
    thread_field: str | None = None,
    limit: int = 1000,
    timeout_s: float | None = None,
) -> Evidence:
    """The statements stored in *conn*, with the records they were read from.

    Args:
        conn: An open connection.
        schema: The graph's logical schema, lifted with ``Assertion``.
        about: ``(vertex_type, key)``: only the statements whose subject or
            object is this entity; *key* is a complete key, as a traversal
            accepts. ``None`` reads every statement.
        guard: A trusted read guard, as :meth:`Connection.traverse` takes:
            only assertions, entities, records and edges it admits are read.
        thread_field: The ``SourceRecord`` property naming the conversation a
            record belongs to, where the source has one.
        limit: At most this many edges per traversal step of an *about* read.
        timeout_s: Seconds each traversal of an *about* read may run.

    Returns:
        Evidence: each statement with its quote, record id, said time and
        thread, the records, and the reserved fields of each assertion.
    """
    if vocab.ASSERTION not in schema.core_schema.vertex_config.vertex_set:
        return Evidence()
    decoder = _Decoder(schema, thread_field)
    if about is None:
        _read_all(conn, schema, guard, decoder)
        return decoder.evidence()
    vertex_type, key = about
    _read_about(conn, schema, vertex_type, dict(key), guard, decoder, limit, timeout_s)
    return decoder.evidence()


def _read_all(
    conn: Connection,
    schema: Schema,
    guard: FilterExpression | None,
    decoder: _Decoder,
) -> None:
    stored = _Stored(conn, schema, guard)
    decoder.assertions = {
        str(doc[vocab.ASSERTION_ID]): doc
        for doc in stored.docs(vocab.ASSERTION)
        if doc.get(vocab.ASSERTION_ID) is not None
    }
    decoder.records = {
        str(doc[vocab.SOURCE_RECORD_ID]): doc
        for doc in stored.docs(vocab.SOURCE_RECORD)
        if doc.get(vocab.SOURCE_RECORD_ID) is not None
    }
    admitted: dict[str, set[str]] = {}
    for edge in _assertion_edges(schema):
        if guard is not None and edge.target not in admitted:
            admitted[edge.target] = {
                entity_key(doc, _identity(schema, edge.target))
                for doc in stored.docs(edge.target)
            }
        for source, target in stored.edges(edge):
            aid = str(source.get(vocab.ASSERTION_ID))
            end = entity_key(target, _identity(schema, edge.target))
            if guard is not None and end not in admitted[edge.target]:
                continue
            if edge.relation == vocab.DERIVED_FROM:
                decoder.record_of[aid] = str(target.get(vocab.SOURCE_RECORD_ID))
            else:
                decoder.ends[aid][str(edge.relation)] = (edge.target, target)


def _read_about(
    conn: Connection,
    schema: Schema,
    vertex_type: str,
    key: dict[str, Any],
    guard: FilterExpression | None,
    decoder: _Decoder,
    limit: int,
    timeout_s: float | None,
) -> None:
    """The anchor's assertions, then each one's ends and record."""
    if vertex_type not in stated_types(schema):
        return
    traverse_kwargs: dict[str, Any] = {"limit": limit}
    if timeout_s is not None:
        traverse_kwargs["timeout_s"] = timeout_s
    # Step 1: the assertions about the anchor, as subject or object.
    first = conn.traverse(
        TraverseQuery(
            seeds=[{"vertex_type": vertex_type, "key": key}],
            max_hops=1,
            direction=EdgeDirection.IN,
            edge_relations=list(ENDS),
            **traverse_kwargs,
        ),
        schema=schema,
        guard=guard,
    )
    ids = sorted(
        {
            str(doc[vocab.ASSERTION_ID])
            for doc in first.vertices.get(vocab.ASSERTION, [])
            if doc.get(vocab.ASSERTION_ID) is not None
        }
    )
    if not ids:
        return
    # Step 2: from each assertion, its subject, object and record.
    second = conn.traverse(
        TraverseQuery(
            seeds=[
                {"vertex_type": vocab.ASSERTION, "key": {vocab.ASSERTION_ID: aid}}
                for aid in ids
            ],
            max_hops=1,
            direction=EdgeDirection.OUT,
            edge_relations=[*ENDS, vocab.DERIVED_FROM],
            **traverse_kwargs,
        ),
        schema=schema,
        guard=guard,
    )
    assertions = {
        str(doc[vocab.ASSERTION_ID]): doc
        for doc in first.vertices.get(vocab.ASSERTION, [])
        if doc.get(vocab.ASSERTION_ID) is not None
    }
    decoder.assertions = assertions
    # A traversal names an edge's ends by address, and leaves out its seeds.
    by_address: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for found in (first, second):
        for name, docs in found.vertices.items():
            fields = _identity(schema, name)
            for doc in docs:
                address = conn.vertex_address(doc, fields)
                if address is not None:
                    by_address[name][address] = doc
    for (source, target, relation), rows in second.edges.items():
        if source != vocab.ASSERTION:
            continue
        for row in rows:
            assertion = by_address[vocab.ASSERTION].get(str(row.get("source")))
            other = by_address[target].get(str(row.get("target")))
            if assertion is None or other is None:
                continue
            aid = str(assertion[vocab.ASSERTION_ID])
            if relation == vocab.DERIVED_FROM and target == vocab.SOURCE_RECORD:
                rid = str(other.get(vocab.SOURCE_RECORD_ID))
                decoder.record_of[aid] = rid
                decoder.records[rid] = other
            elif relation in ENDS:
                decoder.ends[aid][str(relation)] = (target, other)


def graph_time(
    conn: Connection,
    schema: Schema,
    *,
    guard: FilterExpression | None = None,
    timeout_s: float | None = None,
) -> datetime | None:
    """The graph's own time: the newest ``SourceRecord.written_at`` *guard* admits.

    When a record was *written* is the world's clock; when it was read
    (``retrieved_at``) is the reader's, so a replayed or historical world
    resolves at its own time. ``None`` when no record has a time.
    """
    if vocab.SOURCE_RECORD not in schema.core_schema.vertex_config.vertex_set:
        return None
    newest = _Stored(conn, schema, guard).newest(
        vocab.SOURCE_RECORD, vocab.WRITTEN_AT, timeout_s=timeout_s
    )
    return _time(newest)


__all__ = [
    "ENDS",
    "Evidence",
    "assertion_doc",
    "graph_time",
    "read_statements",
    "stated_types",
    "statement_container",
    "stored_edges",
]
