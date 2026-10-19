"""Resolve mentions to entities, then write what is resolved.

:func:`resolve_mentions` identifies each mention of a batch of
:class:`~graflo.architecture.graph_types.mention.MentionGraph` by keys only,
without a model:

1. A complete primary key identifies the mention. For a connector-owned type
   (see :func:`~graflo.architecture.contract.ownership.connector_owned_types`)
   the entity must already be in the graph; it is never created from a mention.
2. A complete secondary identity looks the entity up: first among the batch's
   identified mentions, repeated until nothing new resolves, then in the graph.
   Exactly one entity resolves the mention; several stage it as ``ambiguous``.
3. A type that takes no key from the data is minted when the text introduces
   the entity and no secondary identity found it.

Every other mention is staged with its reason, and a relation waits for its
staged endpoints. A partial key never identifies anything: nothing merges two
vertices later, so a guess would stay a duplicate.

:func:`promote` turns the resolution into a :class:`GraphContainer`, and
:func:`write_mention_graphs` resolves against the target and writes it.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from graflo.architecture.graph_types.container import GraphContainer
from graflo.architecture.graph_types.identifiers import EdgeId
from graflo.architecture.graph_types.mention import (
    HeldRelation,
    Mention,
    MentionGraph,
    Resolution,
    Resolved,
    StagedMention,
    StageReason,
)
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.identity_uuid import mint_uuid
from graflo.architecture.schema.key_contract import (
    KeyContract,
    KeySet,
    KeyStatus,
    key_contract,
    key_status,
    primary_identity,
)

if TYPE_CHECKING:
    from graflo.connections.onto import DBConfig
    from graflo.hq.container_write import ContainerWriteReport

_Key = tuple[str, str]  # (source_id, ref)


def _frozen(identity: dict[str, Any]) -> tuple[tuple[str, Any], ...]:
    return tuple(sorted(identity.items()))


class _Batch:
    """Resolution state of one batch."""

    def __init__(
        self, schema: Schema, graphs: Sequence[MentionGraph], owned: frozenset[str]
    ):
        self.schema = schema
        self.vertex_config = schema.core_schema.vertex_config
        self.owned = owned
        self.contracts: dict[str, KeyContract] = {}
        self.mentions: dict[_Key, Mention] = {}
        self.order: list[_Key] = []
        self.schema_refs: dict[str, str] = {}
        for graph in graphs:
            self.schema_refs[graph.source_id] = graph.schema_ref
            for mention in graph.mentions:
                key = (graph.source_id, mention.ref)
                self.mentions[key] = mention
                self.order.append(key)
        self.resolved: dict[_Key, Resolved] = {}
        self.staged: dict[_Key, tuple[StageReason, int]] = {}
        # (type, secondary identity, values) -> identities of resolved mentions
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

    def resolve(self, key: _Key, identity: dict[str, Any], **fields: Any) -> None:
        mention = self.mentions[key]
        self.resolved[key] = Resolved(
            source_id=key[0],
            ref=key[1],
            type=mention.type,
            identity=identity,
            owned=mention.type in self.owned,
            **fields,
        )
        frozen = _frozen(identity)
        self.identities[frozen] = identity
        values = mention.values()
        for alternate in self.contract(mention.type).alternates:
            if all(values.get(f) not in (None, "") for f in alternate.fields):
                entry = (
                    mention.type,
                    alternate.name,
                    tuple(values[f] for f in alternate.fields),
                )
                self.index.setdefault(entry, set()).add(frozen)

    def mint(self, key: _Key) -> None:
        mention = self.mentions[key]
        field = self.contract(mention.type).identity_fields[0]
        minted = mint_uuid(self.schema_refs[key[0]], key[0], mention.type, key[1])
        self.resolve(key, {field: minted}, by="minted")


def _values(mention: Mention, key: KeySet) -> tuple[Any, ...]:
    values = mention.values()
    return tuple(values[f] for f in key.fields)


def _graph_lookup(
    reader: Any,
    vertex_type: str,
    fields: list[str],
    docs: list[dict[str, Any]],
    identity_fields: list[str],
) -> dict[int, list[dict[str, Any]]]:
    found = reader.find_by(vertex_type, fields, docs, identity_fields)
    out: dict[int, list[dict[str, Any]]] = {}
    for position, matches in found.items():
        unique: dict[tuple, dict[str, Any]] = {}
        for match in matches:
            identity = {f: match.get(f) for f in identity_fields}
            if all(v not in (None, "") for v in identity.values()):
                unique.setdefault(_frozen(identity), identity)
        out[position] = list(unique.values())
    return out


def resolve_mentions(
    schema: Schema,
    graphs: Sequence[MentionGraph],
    *,
    db: Any | None = None,
    owned: frozenset[str] = frozenset(),
) -> Resolution:
    """Identify the mentions of *graphs* by their keys, against *db* when given.

    Args:
        schema: Schema the graphs were read against.
        graphs: Mention graphs, resolved together: a mention may be found by
            another graph's mention.
        db: An open connection to look secondary identities and connector-owned
            keys up in. Without one, only the batch is searched.
        owned: Connector-owned vertex types: linked, never created or written.

    Raises:
        ValueError: A mention names an unknown vertex type.
    """
    batch = _Batch(schema, graphs, owned)
    reader = None
    if db is not None:
        from graflo.hq.lookup import _Reader

        reader = _Reader(db, schema)

    statuses: dict[_Key, KeyStatus] = {}
    owned_keyed: list[tuple[_Key, dict[str, Any]]] = []
    for key in batch.order:
        mention = batch.mentions[key]
        contract = batch.contract(mention.type)
        values = mention.values()
        status = key_status(contract, values)
        statuses[key] = status
        if status.state == "primary":
            identity = primary_identity(
                batch.vertex_config[mention.type], contract, values
            )
            if identity is None:
                batch.staged[key] = ("partial_key", 0)
            elif mention.type in owned:
                owned_keyed.append((key, identity))
            else:
                batch.resolve(key, identity, by="key")
        elif status.alternates:
            continue  # looked up below
        elif status.state == "keyless":
            if mention.introduces and mention.type not in owned:
                batch.mint(key)
            else:
                batch.staged[key] = ("keyless_reference", 0)
        else:
            batch.staged[key] = (
                "partial_key" if status.state == "partial" else "no_key",
                0,
            )

    # Connector-owned keys must already be in the graph.
    if owned_keyed:
        by_type: dict[str, list[tuple[_Key, dict[str, Any]]]] = {}
        for key, identity in owned_keyed:
            by_type.setdefault(batch.mentions[key].type, []).append((key, identity))
        for vertex_type, items in by_type.items():
            fields = list(batch.contract(vertex_type).identity_fields)
            found = (
                _graph_lookup(
                    reader, vertex_type, fields, [i for _, i in items], fields
                )
                if reader is not None
                else {}
            )
            for position, (key, identity) in enumerate(items):
                if found.get(position):
                    batch.resolve(key, identity, by="key", found_in="graph")
                else:
                    batch.staged[key] = ("owned_unmatched", 0)

    # Secondary identities: the batch first, to a fixpoint, then the graph.
    waiting = [
        k for k in batch.order if k not in batch.resolved and k not in batch.staged
    ]
    ambiguous: dict[_Key, int] = {}
    looked_up: set[_Key] = set()
    while waiting:
        progress = False
        for key in list(waiting):
            mention = batch.mentions[key]
            for alternate in statuses[key].alternates:
                hits = batch.index.get(
                    (mention.type, alternate.name, _values(mention, alternate)), set()
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
        if reader is None or not pending:
            break
        # One graph lookup per (type, secondary identity).
        groups: dict[tuple[str, str], list[_Key]] = {}
        for key in pending:
            mention = batch.mentions[key]
            for alternate in statuses[key].alternates:
                groups.setdefault((mention.type, alternate.name), []).append(key)
        for (vertex_type, name), keys in groups.items():
            contract = batch.contract(vertex_type)
            alternate = next(a for a in contract.alternates if a.name == name)
            keys = [k for k in keys if k in waiting]
            docs = [
                dict(zip(alternate.fields, _values(batch.mentions[k], alternate)))
                for k in keys
            ]
            found = _graph_lookup(
                reader,
                vertex_type,
                list(alternate.fields),
                docs,
                list(contract.identity_fields),
            )
            for position, key in enumerate(keys):
                matches = found.get(position, [])
                if len(matches) == 1:
                    batch.resolve(
                        key,
                        matches[0],
                        by="alternate",
                        alternate=name,
                        found_in="graph",
                    )
                    waiting.remove(key)
                    progress = True
                elif len(matches) > 1:
                    ambiguous[key] = max(ambiguous.get(key, 0), len(matches))
        looked_up.update(pending)
        if not progress:
            break

    for key in waiting:
        mention = batch.mentions[key]
        if key in ambiguous:
            batch.staged[key] = ("ambiguous", ambiguous[key])
        elif (
            statuses[key].state == "keyless"
            and mention.introduces
            and mention.type not in owned
        ):
            batch.mint(key)
        elif mention.type in owned:
            batch.staged[key] = ("owned_unmatched", 0)
        else:
            batch.staged[key] = ("unmatched", 0)

    resolution = Resolution(graphs=list(graphs))
    resolution.resolved = [
        batch.resolved[k] for k in batch.order if k in batch.resolved
    ]
    resolution.staged.mentions = [
        StagedMention(
            source_id=k[0],
            mention=batch.mentions[k],
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
    return resolution


def promote(schema: Schema, resolution: Resolution) -> GraphContainer:
    """The vertices and edges *resolution* identified, ready to write.

    A resolved mention becomes its stated properties plus its identity, fused
    with other mentions of the same entity in order, later values winning. A
    connector-owned entity is only an edge endpoint: text never writes onto it.
    """
    mentions = {
        (graph.source_id, m.ref): m
        for graph in resolution.graphs
        for m in graph.mentions
    }
    identities: dict[_Key, dict[str, Any]] = {}
    vertices: dict[str, list[dict[str, Any]]] = {}
    positions: dict[tuple[str, tuple], int] = {}
    for item in resolution.resolved:
        key = (item.source_id, item.ref)
        identities[key] = item.identity
        if item.owned:
            continue
        doc = {**mentions[key].values(), **item.identity}
        docs = vertices.setdefault(item.type, [])
        at = positions.get((item.type, _frozen(item.identity)))
        if at is None:
            positions[(item.type, _frozen(item.identity))] = len(docs)
            docs.append(doc)
        else:
            docs[at].update(doc)
    edges: dict[EdgeId, list[Any]] = {}
    for graph in resolution.graphs:
        for relation in graph.relations:
            source = identities.get((graph.source_id, relation.source))
            target = identities.get((graph.source_id, relation.target))
            if source is not None and target is not None:
                edges.setdefault(relation.edge_id, []).append(
                    (dict(source), dict(target), relation.values())
                )
    return GraphContainer(vertices=vertices, edges=edges)


async def write_mention_graphs(
    schema: Schema,
    graphs: Sequence[MentionGraph],
    conn_conf: DBConfig,
    *,
    owned: frozenset[str] = frozenset(),
    recreate_schema: bool = False,
) -> tuple[Resolution, ContainerWriteReport]:
    """Resolve *graphs* against the target, then write what resolved.

    The schema is defined first, so lookups never meet a missing collection.
    Staged mentions and held relations are returned, not written.
    """
    from graflo.db.manager import ConnectionManager
    from graflo.hq.container_write import define_schema, write_container

    schema = schema.model_copy(deep=True)
    defined = await asyncio.to_thread(define_schema, schema, conn_conf, recreate_schema)

    def _resolve() -> Resolution:
        with ConnectionManager(connection_config=conn_conf) as db:
            return resolve_mentions(schema, graphs, db=db, owned=owned)

    resolution = await asyncio.to_thread(_resolve)
    report = await write_container(schema, promote(schema, resolution), conn_conf)
    return resolution, report.model_copy(update={"schema_defined": defined})
