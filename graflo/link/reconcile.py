"""Keep a graph's stated relations to what its statements resolve to.

A producer writes each affirmed relation as an edge when it states it, including
one a later statement corrects and one stated only as planned or hedged.
:func:`reconcile` reads every relation statement
(:func:`~graflo.link.assertions.read_statements`), resolves them under the
schema's slot rules (:func:`~graflo.link.statements.resolve`) ``as_of`` a time,
by default the graph's own (:func:`~graflo.link.assertions.graph_time`), and
materialises each resolved relation slot:

- a stored edge that statements back but that does not hold is deleted;
- a value that holds and is not stored is written, through the writer, so a
  ``merge: replace`` edge replaces its source's other target. A planned value
  deleted while pending comes back once it is due.

An edge no statement backs is never deleted here; writing a replace edge may
still displace it, as the edge declares. On a connection that cannot delete
edges the stale ones stay and are counted as ``kept``, and the value that holds
in a single-valued slot is written again, so it is the slot's last.

An exclusive edge whose source already holds another target is refused by the
writer; the stored target stays and the refusal is reported, not raised.

A written edge carries the reserved fields its statements' assertions carry (a
list field takes the union), so it is visible where they are; a value whose
assertions do not all carry them is left unwritten and counted.
"""

from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from datetime import datetime
from typing import Any

from pydantic import Field

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.evolution.aspects.provenance.vocabulary import ASSERTION
from graflo.architecture.graph_types.container import GraphContainer
from graflo.architecture.refusal import WriteRefusal
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.vertex import FieldType
from graflo.connections.onto import DBConfig
from graflo.db.conn import ConnectionCapability
from graflo.db.manager import ConnectionManager
from graflo.hq.container_write import edge_label, write_container
from graflo.hq.db_writer import DBWriter
from graflo.link.assertions import (
    Evidence,
    graph_time,
    read_statements,
    stored_edges,
)
from graflo.link.statements import entity_key, resolve, slot_rules

Pair = tuple[str, str]
"""``(source key, target key)``, each as :func:`entity_key` gives it."""


class ReconcileReport(ConfigBaseModel):
    """What one reconcile weighed, removed and wrote, per relation."""

    as_of: datetime | None = None
    statements: int = 0
    conflicts: int = 0
    removed: dict[str, int] = Field(default_factory=dict)
    kept: dict[str, int] = Field(
        default_factory=dict,
        description="Stale edges left in place: the connection cannot delete edges.",
    )
    written: dict[str, int] = Field(
        default_factory=dict,
        description="Values sent to the writer; those it refused are under ``refused``.",
    )
    unwritten: dict[str, int] = Field(
        default_factory=dict,
        description="Values that hold but whose statements carry no reserved fields to write them with.",
    )
    refused: list[str] = Field(
        default_factory=list,
        description=(
            "Values of an exclusive relation the stored graph refused, one line "
            "each: the source holds another target, which stays."
        ),
    )


def _count(into: dict[str, int], relation: str, n: int) -> None:
    if n:
        into[relation] = into.get(relation, 0) + n


def _identity(schema: Schema, vertex_type: str) -> list[str]:
    return list(schema.core_schema.vertex_config.identity_fields(vertex_type))


def _stamp(edge: Edge, supports: list[str], found: Evidence) -> dict[str, Any] | None:
    """The reserved fields to write *edge* with, from its supports; None if unknown."""
    stamp: dict[str, Any] = {}
    for field in (f for f in edge.properties if f.reserved):
        values = [
            v
            for v in (found.reserved.get(sid, {}).get(field.name) for sid in supports)
            if v is not None
        ]
        if not values or len(values) != len(supports):
            return None
        if field.type == FieldType.LIST:
            stamp[field.name] = sorted({m for v in values for m in v})
        elif all(v == values[0] for v in values):
            stamp[field.name] = values[0]
        else:
            return None
    return stamp


class _Read(ConfigBaseModel):
    """What reconcile read in one connection: statements, stored pairs, capability."""

    found: Evidence
    when: datetime | None
    stated: dict[str, set[Pair]]
    stored: dict[str, set[Pair]]
    deletes: bool


def _edge_key(edge: Edge) -> str:
    return edge_label(edge.edge_id)


def _read(
    schema: Schema,
    conn_conf: DBConfig,
    edges: dict[tuple[str, str, str], Edge],
    as_of: datetime | None,
    thread_field: str | None,
) -> _Read:
    with ConnectionManager(connection_config=conn_conf) as conn:
        found = read_statements(conn, schema, thread_field=thread_field)
        when = as_of or graph_time(conn, schema)
        stated: dict[str, set[Pair]] = defaultdict(set)
        for s in found.statements:
            edge = edges.get((s.vertex_type, s.slot, s.target_type or ""))
            if s.kind == "relation" and edge is not None:
                stated[_edge_key(edge)].add((s.key, str(s.value)))
        by_key = {_edge_key(edge): edge for edge in edges.values()}
        stored = {
            key: {
                (
                    entity_key(source, _identity(schema, by_key[key].source)),
                    entity_key(target, _identity(schema, by_key[key].target)),
                )
                for source, target in stored_edges(conn, schema, by_key[key])
            }
            for key in stated
        }
        deletes = bool(getattr(conn, ConnectionCapability.INSTANCE_DELETE.value, False))
    return _Read(
        found=found, when=when, stated=dict(stated), stored=stored, deletes=deletes
    )


async def reconcile(
    schema: Schema,
    conn_conf: DBConfig,
    *,
    as_of: datetime | None = None,
    thread_field: str | None = None,
) -> ReconcileReport:
    """Delete stated edges that do not hold and write those that do but are missing.

    Args:
        schema: The graph's logical schema, lifted with ``Assertion``.
        conn_conf: The graph's database.
        as_of: When to resolve at; the graph's own time when left out.
        thread_field: As for :func:`~graflo.link.assertions.read_statements`.
    """
    if ASSERTION not in schema.core_schema.vertex_config.vertex_set:
        return ReconcileReport()
    edges = {
        (edge.source, str(edge.relation), edge.target): edge
        for edge in schema.core_schema.edge_config.values()
        if edge.relation is not None
    }
    by_key = {_edge_key(edge): edge for edge in edges.values()}
    rules = slot_rules(schema)
    read = await asyncio.to_thread(_read, schema, conn_conf, edges, as_of, thread_field)
    relations = [s for s in read.found.statements if s.kind == "relation"]
    result = resolve(relations, rules, as_of=read.when)
    report = ReconcileReport(
        as_of=read.when, statements=len(relations), conflicts=len(result.conflicts)
    )
    current: dict[str, dict[Pair, list[str]]] = defaultdict(dict)
    for fact in result.current:
        edge = edges.get((fact.vertex_type, fact.slot, fact.target_type or ""))
        if edge is not None:
            current[_edge_key(edge)][fact.key, str(fact.value)] = fact.supports

    deletes: list[tuple[Edge, list[Pair]]] = []
    to_write: dict[str, dict[Any, list[Any]]] = defaultdict(lambda: defaultdict(list))
    stamps: dict[str, dict[str, Any] | None] = {}
    for key, pairs in sorted(read.stated.items()):
        edge = by_key[key]
        relation = str(edge.relation)
        holds = current.get(key, {})
        stored = read.stored.get(key, set())
        stale = sorted((stored & pairs) - set(holds))
        if stale and read.deletes:
            deletes.append((edge, stale))
            _count(report.removed, relation, len(stale))
        else:
            _count(report.kept, relation, len(stale))
        rule = rules.get((edge.source, relation))
        # Where stale targets stay, the value that holds is written again, last.
        again = (
            {s for s, _ in stale}
            if not read.deletes and rule is not None and rule.single
            else set()
        )
        for (source, target), supports in sorted(holds.items()):
            if (source, target) in stored and source not in again:
                continue
            stamp = _stamp(edge, supports, read.found)
            if stamp is None:
                _count(report.unwritten, relation, 1)
                continue
            label = json.dumps(stamp, sort_keys=True, default=str)
            stamps[label] = stamp or None
            to_write[label][edge.edge_id].append(
                (json.loads(source), json.loads(target), {})
            )
            _count(report.written, relation, 1)

    if deletes:
        await asyncio.to_thread(_delete, schema, conn_conf, deletes)
    for label, rows in sorted(to_write.items()):
        try:
            await write_container(
                schema,
                GraphContainer(edges=dict(rows)),
                conn_conf,
                stamp=stamps[label],
            )
        except WriteRefusal as refusal:
            report.refused.extend(refusal.subjects)
    return report


def _delete(
    schema: Schema, conn_conf: DBConfig, deletes: list[tuple[Edge, list[Pair]]]
) -> None:
    """Delete the stale pairs through the writer, which stores names as the target does."""
    flavored = schema.model_copy(deep=True)
    flavored.db_profile.db_flavor = conn_conf.connection_type
    flavored.finish_init()
    ingestion = IngestionModel(resources=[])
    ingestion.finish_init(flavored.core_schema)
    writer = DBWriter(schema=flavored, ingestion_model=ingestion)
    try:
        for edge, stale in deletes:
            writer.delete_edges(
                conn_conf,
                edge.edge_id,
                [(json.loads(s), json.loads(t)) for s, t in stale],
            )
    finally:
        writer.close()


__all__ = ["ReconcileReport", "reconcile"]
