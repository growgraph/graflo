"""Resolve partial records to entities, then write what is resolved.

:func:`resolve_records` identifies each record of a batch of
:class:`~graflo.link.records.RecordGraph` by keys only, without a model. Every
keyed decision is :func:`~graflo.hq.identify.identify`; this module adds the
batch around it:

1. A complete primary key identifies the record. For a connector-owned type
   (see :func:`~graflo.architecture.contract.ownership.connector_owned_types`)
   the entity must already be in the graph; it is never created from a record.
2. A complete secondary identity looks the entity up: first among the batch's
   identified records, repeated until nothing new resolves, then in the graph.
   Exactly one entity resolves the record; several stage it as ``ambiguous``.
3. A type that takes no key from the data is minted when the record may create
   its entity and no secondary identity found it.

Every other record is staged with its reason, and a relation waits for its
staged endpoints. A partial key never identifies anything: nothing merges two
vertices later, so a guess would stay a duplicate. A relation between two
records whose identities turn out to be one entity is dropped, never written as
a self-loop (:func:`dropped_relations`).

:func:`promote` turns the resolution into a :class:`GraphContainer`, and
:func:`write_record_graphs` resolves against the target and writes it.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from graflo.architecture.graph_types.container import GraphContainer
from graflo.architecture.graph_types.identifiers import EdgeId
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.identity_uuid import mint_uuid
from graflo.architecture.schema.key_contract import KeyContract, KeySet, key_contract
from graflo.filter.onto import FilterExpression, conjoin
from graflo.hq.identify import Identity, identify_many
from graflo.link.records import (
    DroppedRelation,
    HeldRelation,
    PartialRecord,
    RecordGraph,
    RecordRelation,
    Resolution,
    Resolved,
    StagedRecord,
    StageReason,
)

if TYPE_CHECKING:
    from graflo.connections.onto import DBConfig
    from graflo.hq.container_write import ContainerWriteReport

_Key = tuple[str, str]  # (source_id, ref)


def _frozen(identity: dict[str, Any]) -> tuple[tuple[str, Any], ...]:
    return tuple(sorted(identity.items()))


_Entity = tuple[str, tuple[tuple[str, Any], ...]]  # (type, frozen identity)


def _entities(resolved: Sequence[Resolved]) -> dict[_Key, _Entity]:
    return {(r.source_id, r.ref): (r.type, _frozen(r.identity)) for r in resolved}


def _same_entity(
    entities: Mapping[_Key, _Entity], source_id: str, relation: RecordRelation
) -> bool:
    """Whether *relation* joins two records resolved to one entity."""
    if relation.source == relation.target:
        return False
    source = entities.get((source_id, relation.source))
    return source is not None and source == entities.get((source_id, relation.target))


def dropped_relations(resolution: Resolution) -> list[DroppedRelation]:
    """The relations of *resolution* that are never written, in graph order.

    A relation between two records states two entities; one whose records
    resolved to the same entity (type and identity) is dropped as
    ``same_entity``. A relation from a record to itself is kept. Call it again
    after changing ``resolution.resolved`` (a pick fixed by hand):
    :func:`promote` drops such relations whatever ``resolution.dropped`` says.
    """
    entities = _entities(resolution.resolved)
    return [
        DroppedRelation(
            source_id=graph.source_id, relation=relation, reason="same_entity"
        )
        for graph in resolution.graphs
        for relation in graph.relations
        if _same_entity(entities, graph.source_id, relation)
    ]


class _Batch:
    """Resolution state of one batch."""

    def __init__(
        self, schema: Schema, graphs: Sequence[RecordGraph], owned: frozenset[str]
    ):
        self.schema = schema
        self.vertex_config = schema.core_schema.vertex_config
        self.owned = owned
        self.contracts: dict[str, KeyContract] = {}
        self.records: dict[_Key, PartialRecord] = {}
        self.order: list[_Key] = []
        self.schema_refs: dict[str, str] = {}
        for graph in graphs:
            self.schema_refs[graph.source_id] = graph.schema_ref
            for record in graph.records:
                key = (graph.source_id, record.ref)
                self.records[key] = record
                self.order.append(key)
        self.resolved: dict[_Key, Resolved] = {}
        self.staged: dict[_Key, tuple[StageReason, int]] = {}
        # (type, secondary identity, values) -> identities of resolved records
        self.index: dict[tuple[str, str, tuple[Any, ...]], set[tuple]] = {}
        self.identities: dict[tuple, dict[str, Any]] = {}

    def contract(self, vertex_type: str) -> KeyContract:
        if vertex_type not in self.contracts:
            try:
                vertex = self.vertex_config[vertex_type]
            except KeyError as error:
                raise ValueError(f"unknown vertex type {vertex_type!r}") from error
            self.contracts[vertex_type] = key_contract(vertex)
        return self.contracts[vertex_type]

    def identify(
        self,
        keys: Sequence[_Key],
        *,
        db: Any | None = None,
        guard: FilterExpression | None = None,
    ) -> dict[_Key, Identity]:
        """``identify`` for each of *keys*, one call per vertex type."""
        by_type: dict[str, list[_Key]] = {}
        for key in keys:
            by_type.setdefault(self.records[key].type, []).append(key)
        out: dict[_Key, Identity] = {}
        for vertex_type, group in by_type.items():
            found = identify_many(
                self.schema,
                vertex_type,
                [self.records[k].values() for k in group],
                db=db,
                guard=guard,
            )
            out.update(zip(group, found))
        return out

    def resolve(self, key: _Key, identity: dict[str, Any], **fields: Any) -> None:
        record = self.records[key]
        self.resolved[key] = Resolved(
            source_id=key[0],
            ref=key[1],
            type=record.type,
            identity=identity,
            owned=record.type in self.owned,
            **fields,
        )
        frozen = _frozen(identity)
        self.identities[frozen] = identity
        values = record.values()
        for alternate in self.contract(record.type).alternates:
            if all(values.get(f) not in (None, "") for f in alternate.fields):
                entry = (
                    record.type,
                    alternate.name,
                    tuple(values[f] for f in alternate.fields),
                )
                self.index.setdefault(entry, set()).add(frozen)

    def mint(self, key: _Key) -> None:
        record = self.records[key]
        field = self.contract(record.type).identity_fields[0]
        minted = mint_uuid(self.schema_refs[key[0]], key[0], record.type, key[1])
        self.resolve(key, {field: minted}, by="minted")


def _values(record: PartialRecord, key: KeySet) -> tuple[Any, ...]:
    values = record.values()
    return tuple(values[f] for f in key.fields)


def resolve_records(
    schema: Schema,
    graphs: Sequence[RecordGraph],
    *,
    db: Any | None = None,
    owned: frozenset[str] = frozenset(),
    guard: FilterExpression | None = None,
    writable: Mapping[str, FilterExpression] | None = None,
) -> Resolution:
    """Identify the records of *graphs* by their keys, against *db* when given.

    Args:
        schema: Schema the graphs were read against.
        graphs: Record graphs, resolved together: a record may be found by
            another graph's record.
        db: An open connection to look secondary identities and connector-owned
            keys up in. Without one, only the batch is searched.
        owned: Connector-owned vertex types: linked, never created or written.
        guard: A trusted read guard every lookup in *db* must pass, so a record
            is never resolved to an entity the guard hides; refused by a backend
            without read guards. A stated key of a type that is not
            connector-owned is then checked against the graph as well: an entity
            that exists but the guard hides stages the record as ``hidden``
            (an existence check only; nothing from it is returned). The guard
            applies only to vertex types that declare every field it names;
            other types are looked up unguarded.
        writable: With a guard, the entities a write may change, one filter
            per vertex type: an entity found in the graph that the guard shows
            but its type's filter does not is resolved ``link_only`` -- an edge
            endpoint whose properties the write leaves alone, like a
            connector-owned one. So is such an entity of a guarded type the
            mapping leaves out, or of one that does not declare every field its
            filter names.

    Raises:
        ValueError: A record names an unknown vertex type.
    """
    batch = _Batch(schema, graphs, owned)
    guarded_fields = guard.fields() if guard is not None else set()

    def covered(vertex_type: str) -> bool:
        """Whether the guard applies to *vertex_type* (it declares its fields)."""
        if guard is None:
            return False
        names = {f.name for f in batch.vertex_config[vertex_type].properties}
        return guarded_fields <= names

    for key in batch.order:
        batch.contract(batch.records[key].type)  # unknown types fail first
    stated = batch.identify(batch.order)
    owned_keyed: list[_Key] = []
    guarded_keyed: list[_Key] = []
    for key in batch.order:
        record = batch.records[key]
        identity = stated[key]
        if identity.outcome == "identified":
            if record.type in owned:
                owned_keyed.append(key)
            elif db is not None and covered(record.type):
                guarded_keyed.append(key)
            else:
                batch.resolve(key, dict(identity.identity or {}), by="key")
        elif identity.state == "primary":
            batch.staged[key] = ("partial_key", 0)
        elif identity.alternates:
            continue  # looked up below
        elif identity.state == "keyless":
            if record.may_create and record.type not in owned:
                batch.mint(key)
            else:
                batch.staged[key] = ("keyless_reference", 0)
        else:
            batch.staged[key] = (
                "partial_key" if identity.state == "partial" else "no_key",
                0,
            )

    # Connector-owned keys must already be in the graph.
    if owned_keyed:
        present = (
            batch.identify(owned_keyed, db=db, guard=guard) if db is not None else {}
        )
        for key in owned_keyed:
            found = present.get(key)
            if found is not None and found.found:
                batch.resolve(
                    key, dict(found.identity or {}), by="key", found_in="graph"
                )
            else:
                batch.staged[key] = ("owned_unmatched", 0)

    # Under a guard, a stated key may name an entity the guard hides: that
    # record stages rather than writing onto, or linking to, what it cannot see.
    if guarded_keyed:
        seen = batch.identify(guarded_keyed, db=db, guard=guard)
        unseen = [k for k in guarded_keyed if not seen[k].found]
        exists = batch.identify(unseen, db=db) if unseen else {}
        for key in guarded_keyed:
            identity = dict(seen[key].identity or {})
            if seen[key].found:
                batch.resolve(key, identity, by="key", found_in="graph")
            elif exists[key].found:
                batch.staged[key] = ("hidden", 0)
            else:
                batch.resolve(key, identity, by="key")

    # Secondary identities: the batch first, to a fixpoint, then the graph.
    waiting = [
        k for k in batch.order if k not in batch.resolved and k not in batch.staged
    ]
    ambiguous: dict[_Key, int] = {}
    looked_up: set[_Key] = set()
    while waiting:
        progress = False
        for key in list(waiting):
            record = batch.records[key]
            contract = batch.contract(record.type)
            for alternate in contract.alternates:
                if alternate.name not in stated[key].alternates:
                    continue
                hits = batch.index.get(
                    (record.type, alternate.name, _values(record, alternate)), set()
                )
                if len(hits) == 1:
                    (frozen,) = hits
                    batch.resolve(
                        key,
                        batch.identities[frozen],
                        by="alternate",
                        alternate=alternate.name,
                        found_in="batch",
                    )
                    waiting.remove(key)
                    progress = True
                    break
                if len(hits) > 1:
                    ambiguous[key] = len(hits)
        if progress:
            continue
        pending = [k for k in waiting if k not in looked_up and k not in ambiguous]
        if db is None or not pending:
            break
        for key, found in batch.identify(pending, db=db, guard=guard).items():
            if found.outcome == "identified" and found.identity is not None:
                batch.resolve(
                    key,
                    found.identity,
                    by="alternate",
                    alternate=found.alternate,
                    found_in="graph",
                )
                waiting.remove(key)
                progress = True
            elif found.outcome == "ambiguous":
                ambiguous[key] = max(ambiguous.get(key, 0), len(found.identities))
        looked_up.update(pending)
        if not progress:
            break

    for key in waiting:
        record = batch.records[key]
        if key in ambiguous:
            batch.staged[key] = ("ambiguous", ambiguous[key])
        elif (
            stated[key].state == "keyless"
            and record.may_create
            and record.type not in owned
        ):
            batch.mint(key)
        elif record.type in owned:
            batch.staged[key] = ("owned_unmatched", 0)
        else:
            batch.staged[key] = ("unmatched", 0)

    resolution = Resolution(graphs=list(graphs))
    resolution.resolved = [
        batch.resolved[k] for k in batch.order if k in batch.resolved
    ]
    if db is not None and guard is not None and writable is not None:
        _mark_link_only(resolution.resolved, batch, db, guard, writable, covered)
    resolution.staged.records = [
        StagedRecord(
            source_id=k[0],
            record=batch.records[k],
            reason=batch.staged[k][0],
            matches=batch.staged[k][1],
        )
        for k in batch.order
        if k in batch.staged
    ]
    for graph in graphs:
        for relation in graph.relations:
            waiting_on = [
                ref
                for ref in (relation.source, relation.target)
                if (graph.source_id, ref) not in batch.resolved
            ]
            if waiting_on:
                resolution.staged.relations.append(
                    HeldRelation(
                        source_id=graph.source_id,
                        relation=relation,
                        waiting_on=list(dict.fromkeys(waiting_on)),
                    )
                )
    resolution.dropped = dropped_relations(resolution)
    return resolution


def _mark_link_only(
    resolved: list[Resolved],
    batch: _Batch,
    db: Any,
    guard: FilterExpression,
    writable: Mapping[str, FilterExpression],
    covered: Callable[[str], bool],
) -> None:
    """Mark entities found in the graph that the write may not change.

    Every record of such an entity becomes link-only, including records the
    batch resolved to it, so no other record's properties are written onto it.
    """
    checked = [
        (item.source_id, item.ref)
        for item in resolved
        if item.found_in == "graph" and not item.owned and covered(item.type)
    ]
    if not checked:
        return
    by_type: dict[str, list[_Key]] = {}
    for key in checked:
        by_type.setdefault(batch.records[key].type, []).append(key)
    found: dict[_Key, Identity] = {}
    for vertex_type, keys in by_type.items():
        allowed = writable.get(vertex_type)
        names = {f.name for f in batch.vertex_config[vertex_type].properties}
        if allowed is None or not allowed.fields() <= names:
            continue
        found.update(batch.identify(keys, db=db, guard=conjoin(guard, allowed)))
    frozen: set[tuple[str, tuple]] = set()
    for key in checked:
        item = batch.resolved[key]
        again = found.get(key)
        if (
            again is None
            or again.outcome != "identified"
            or not again.found
            or again.identity != item.identity
        ):
            frozen.add((item.type, _frozen(item.identity)))
    if not frozen:
        return
    for position, item in enumerate(resolved):
        if (item.type, _frozen(item.identity)) in frozen:
            resolved[position] = item.model_copy(update={"link_only": True})


def promote(schema: Schema, resolution: Resolution) -> GraphContainer:
    """The vertices and edges *resolution* identified, ready to write.

    A resolved record becomes its stated properties plus its identity, fused
    with other records of the same entity in order, later values winning. A
    connector-owned or link-only entity is only an edge endpoint: a record
    never writes onto it. A relation between two records resolved to one
    entity is not emitted (:func:`dropped_relations`).
    """
    records = {
        (graph.source_id, r.ref): r
        for graph in resolution.graphs
        for r in graph.records
    }
    identities: dict[_Key, dict[str, Any]] = {}
    vertices: dict[str, list[dict[str, Any]]] = {}
    positions: dict[tuple[str, tuple], int] = {}
    for item in resolution.resolved:
        key = (item.source_id, item.ref)
        identities[key] = item.identity
        if item.owned or item.link_only:
            continue
        doc = {**records[key].values(), **item.identity}
        docs = vertices.setdefault(item.type, [])
        at = positions.get((item.type, _frozen(item.identity)))
        if at is None:
            positions[(item.type, _frozen(item.identity))] = len(docs)
            docs.append(doc)
        else:
            docs[at].update(doc)
    entities = _entities(resolution.resolved)
    edges: dict[EdgeId, list[Any]] = {}
    for graph in resolution.graphs:
        for relation in graph.relations:
            if _same_entity(entities, graph.source_id, relation):
                continue
            source = identities.get((graph.source_id, relation.source))
            target = identities.get((graph.source_id, relation.target))
            if source is not None and target is not None:
                edges.setdefault(relation.edge_id, []).append(
                    (dict(source), dict(target), relation.values())
                )
    return GraphContainer(vertices=vertices, edges=edges)


async def write_record_graphs(
    schema: Schema,
    graphs: Sequence[RecordGraph],
    conn_conf: DBConfig,
    *,
    owned: frozenset[str] = frozenset(),
    recreate_schema: bool = False,
    stamp: Mapping[str, Any] | None = None,
    guard: FilterExpression | None = None,
    writable: Mapping[str, FilterExpression] | None = None,
) -> tuple[Resolution, ContainerWriteReport]:
    """Resolve *graphs* against the target, then write what resolved.

    The schema is defined first, so lookups never meet a missing collection.
    Staged records, held and dropped relations are returned, not written. *stamp* sets
    the schema's reserved fields on what is written (``write_container``);
    *guard* limits the lookups and *writable* what they may write onto
    (:func:`resolve_records`).
    """
    from graflo.db.manager import ConnectionManager
    from graflo.hq.container_write import define_schema, write_container

    schema = schema.model_copy(deep=True)
    defined = await asyncio.to_thread(define_schema, schema, conn_conf, recreate_schema)

    def _resolve() -> Resolution:
        with ConnectionManager(connection_config=conn_conf) as db:
            return resolve_records(
                schema, graphs, db=db, owned=owned, guard=guard, writable=writable
            )

    resolution = await asyncio.to_thread(_resolve)
    report = await write_container(
        schema, promote(schema, resolution), conn_conf, stamp=stamp
    )
    return resolution, report.model_copy(update={"schema_defined": defined})
