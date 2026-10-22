"""Gather the entities a staged record may be, without a model.

:func:`gather_candidates` runs after :func:`~graflo.link.resolve.resolve_records`
(and, on a live graph, after its write) and lists, for each staged record, the
entities it may be, by three kinds of evidence:

- ``alternate``: the entities a stated secondary identity matched -- several,
  or the record would have resolved (``ambiguous``);
- ``exact`` / ``key`` / ``normalized`` / ``alias``:
  :func:`~graflo.hq.lookup.lookup_candidates` over the label fields the record
  states and the caller's lookup text for it;
- ``relation``: a relation stated between the record and a resolved one --
  "the customer" that RAISED a resolved HD-4411 gives the customers linked to
  HD-4411 by ``RAISED``.

Candidates never close a record: only keys do. A pick, a declared rule, an
agent or a person chooses among them. A candidate whose stored key or
secondary-identity values contradict what the record states is left out: a
record that states ``email`` is not an entity holding another email. Nor is
the entity a record of the same source, related to this one by a stated
relation, resolved to: the relation states two entities.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from graflo.architecture.graph_types.enums import EdgeDirection
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.key_contract import (
    KeyContract,
    key_contract,
    key_status,
)
from graflo.filter.onto import FilterExpression
from graflo.hq.lookup import LookupSpec, _Reader, label_fields, lookup_candidates
from graflo.link.records import (
    CandidateLane,
    EntityCandidate,
    PartialRecord,
    Resolution,
    Resolved,
    StagedCandidates,
    StagedRecord,
)

#: Words to look a staged record up by, besides its stated label values.
LookupText = Callable[[PartialRecord], str | None]

#: Lanes best first; the cap keeps the best.
LANE_ORDER: dict[CandidateLane, int] = {
    "alternate": 0,
    "exact": 1,
    "key": 2,
    "normalized": 3,
    "alias": 4,
    "relation": 5,
    "metadata": 6,
}


def _frozen(key: dict[str, Any]) -> tuple[tuple[str, str], ...]:
    return tuple(sorted((k, str(v)) for k, v in key.items()))


def _empty(value: Any) -> bool:
    return value is None or value == ""


def _same(a: Any, b: Any) -> bool:
    if a == b:
        return True
    return str(a).strip().casefold() == str(b).strip().casefold()


class _Gatherer:
    """Shared state of one :func:`gather_candidates` call."""

    def __init__(
        self,
        schema: Schema,
        resolution: Resolution,
        db: Any | None,
        guard: FilterExpression | None,
        spec: LookupSpec,
        lookup_text: LookupText | None,
    ) -> None:
        self.schema = schema
        self.db = db
        self.guard = guard
        self.spec = spec
        self.lookup_text = lookup_text
        self.vertex_config = schema.core_schema.vertex_config
        self.vertices = {v.name: v for v in self.vertex_config.vertices}
        self.contracts: dict[str, KeyContract] = {}
        self.records: dict[tuple[str, str], PartialRecord] = {
            (g.source_id, r.ref): r for g in resolution.graphs for r in g.records
        }
        self.resolved: dict[tuple[str, str], Resolved] = {
            (r.source_id, r.ref): r for r in resolution.resolved
        }
        self.graphs = resolution.graphs
        self.held = resolution.staged.relations
        self.guarded_fields = guard.fields() if guard is not None else set()
        self.reader: _Reader | None = None
        self.trusted: _Reader | None = None
        if db is not None:
            self.reader = _Reader(db, schema, guard)
            self.trusted = _Reader(db, schema) if guard is not None else self.reader

    def contract(self, vertex_type: str) -> KeyContract:
        if vertex_type not in self.contracts:
            self.contracts[vertex_type] = key_contract(self.vertex_config[vertex_type])
        return self.contracts[vertex_type]

    def covered(self, vertex_type: str) -> bool:
        """Whether the guard applies to *vertex_type* (it declares its fields)."""
        if self.guard is None:
            return False
        names = {f.name for f in self.vertices[vertex_type].properties}
        return self.guarded_fields <= names

    def guard_for(self, vertex_type: str) -> FilterExpression | None:
        return self.guard if self.covered(vertex_type) else None

    def reader_for(self, vertex_type: str) -> _Reader | None:
        return self.reader if self.covered(vertex_type) else self.trusted

    def labels(self, vertex_type: str) -> list[str]:
        if vertex_type in self.spec.label_fields:
            return list(self.spec.label_fields[vertex_type])
        return label_fields(self.vertices[vertex_type])

    def label_of(self, vertex_type: str, doc: dict[str, Any]) -> str | None:
        for name in self.labels(vertex_type):
            if not _empty(doc.get(name)):
                return str(doc[name])
        return None

    # -- lanes ---------------------------------------------------------------

    def alternate(self, staged: StagedRecord) -> list[EntityCandidate]:
        """The entities a stated secondary identity matched, batch and graph."""
        record = staged.record
        contract = self.contract(record.type)
        identity = list(contract.identity_fields)
        values = record.values()
        found: list[EntityCandidate] = []
        for alternate in key_status(contract, values).alternates:
            probe = {f: values[f] for f in alternate.fields}
            matched = ", ".join(f"{f}={values[f]}" for f in alternate.fields)
            for key, item in self.resolved.items():
                other = self.records.get(key)
                if item.type != record.type or other is None:
                    continue
                stated = other.values()
                if all(_same(stated.get(f), probe[f]) for f in alternate.fields):
                    found.append(
                        EntityCandidate(
                            vertex_type=record.type,
                            key=dict(item.identity),
                            label=self.label_of(record.type, stated),
                            lane="alternate",
                            matched=matched,
                        )
                    )
            reader = self.reader_for(record.type)
            if reader is None:
                continue
            returned = list(dict.fromkeys([*identity, *self.labels(record.type)]))
            for doc in reader.find_by(
                record.type, list(alternate.fields), [probe], returned
            ).get(0, []):
                found.append(
                    EntityCandidate(
                        vertex_type=record.type,
                        key={f: doc.get(f) for f in identity},
                        label=self.label_of(record.type, doc),
                        lane="alternate",
                        matched=matched,
                    )
                )
        return found

    def lookup(self, staged: StagedRecord, limit: int) -> list[EntityCandidate]:
        """``lookup_candidates`` over the stated labels and the lookup text."""
        if self.db is None:
            return []
        record = staged.record
        values = record.values()
        words = [self.lookup_text(record) or ""] if self.lookup_text else []
        words += [
            str(values[f])
            for f in self.labels(record.type)
            if not _empty(values.get(f)) and isinstance(values[f], str)
        ]
        text = " ".join(dict.fromkeys(w.strip() for w in words if w and w.strip()))
        if not text:
            return []
        spec = self.spec.model_copy(
            update={"vertex_types": [record.type], "max_seeds": limit}
        )
        found = lookup_candidates(
            self.db, self.schema, text, spec, guard=self.guard_for(record.type)
        )
        return [
            EntityCandidate(
                vertex_type=c.vertex_type,
                key=dict(c.key),
                label=c.label,
                lane=c.lane,
                matched=c.mention,
            )
            for c in found
            if c.vertex_type == record.type
        ]

    def relation(self, staged: StagedRecord) -> list[EntityCandidate]:
        """Entities linked to a resolved anchor by a relation the text states."""
        if self.db is None:
            return []
        record = staged.record
        ref = record.ref
        identity = list(self.contract(record.type).identity_fields)
        found: list[EntityCandidate] = []
        for held in self.held:
            relation = held.relation
            if held.source_id != staged.source_id:
                continue
            if relation.source == ref and relation.target != ref:
                anchor_ref, direction = relation.target, EdgeDirection.IN
            elif relation.target == ref and relation.source != ref:
                anchor_ref, direction = relation.source, EdgeDirection.OUT
            else:
                continue
            anchor = self.resolved.get((staged.source_id, anchor_ref))
            if anchor is None:
                continue
            name = relation.edge_id[2]
            reached = self.db.graph_neighbors(
                anchor.type,
                dict(anchor.identity),
                hops=1,
                direction=direction,
                edge_types=[name] if name is not None else None,
                schema=self.schema,
                guard=self.guard_for(record.type),
            )
            for doc in reached.vertices.get(record.type, []):
                key = {f: doc.get(f) for f in identity}
                found.append(
                    EntityCandidate(
                        vertex_type=record.type,
                        key=key,
                        label=self.label_of(record.type, doc),
                        lane="relation",
                        relation=name,
                        anchor=anchor_ref,
                    )
                )
        return found

    # -- checks --------------------------------------------------------------

    def related(self, staged: StagedRecord) -> set[tuple[str, tuple]]:
        """The entities the record is stated to be distinct from.

        A relation between two records states two entities, so the entity a
        record related to this one resolved to is never this one's candidate.
        """
        ref = staged.record.ref
        out: set[tuple[str, tuple]] = set()
        for graph in self.graphs:
            if graph.source_id != staged.source_id:
                continue
            for relation in graph.relations:
                if relation.source == relation.target:
                    continue
                if relation.source == ref:
                    other = relation.target
                elif relation.target == ref:
                    other = relation.source
                else:
                    continue
                anchor = self.resolved.get((staged.source_id, other))
                if anchor is None:
                    continue
                identity = self.contract(anchor.type).identity_fields
                key = {f: anchor.identity.get(f) for f in identity}
                out.add((anchor.type, _frozen(key)))
        return out

    def compatible(
        self, staged: StagedRecord, candidates: list[EntityCandidate]
    ) -> list[EntityCandidate]:
        """*candidates* whose stored keys agree with what the record states."""
        record = staged.record
        contract = self.contract(record.type)
        identity = list(contract.identity_fields)
        values = {k: v for k, v in record.values().items() if not _empty(v)}
        keyed = {f for key in contract.keys for f in key.fields}
        keyed |= {f for alt in contract.alternates for f in alt.fields}
        checked = [f for f in values if f in keyed and f not in identity]
        kept = [
            c
            for c in candidates
            if all(_same(c.key.get(f), values[f]) for f in identity if f in values)
        ]
        reader = self.reader_for(record.type)
        if not checked or not kept or reader is None:
            return kept
        stored = reader.find_by(
            record.type,
            identity,
            [c.key for c in kept],
            list(dict.fromkeys([*identity, *checked])),
        )
        out = []
        for position, candidate in enumerate(kept):
            docs = stored.get(position)
            if not docs:  # not in the graph (a batch entity): nothing to contradict
                out.append(candidate)
                continue
            doc = docs[0]
            if all(_empty(doc.get(f)) or _same(doc.get(f), values[f]) for f in checked):
                out.append(candidate)
        return out


def gather_candidates(
    schema: Schema,
    resolution: Resolution,
    db: Any | None = None,
    *,
    guard: FilterExpression | None = None,
    spec: LookupSpec | None = None,
    lookup_text: LookupText | None = None,
    max_candidates: int = 10,
) -> list[StagedCandidates]:
    """The entities each staged record of *resolution* may be, best first.

    Args:
        schema: Schema the records were resolved against.
        resolution: What :func:`~graflo.link.resolve.resolve_records`
            returned. Relation-path anchors are its resolved records, so run
            this after their write when they are new to the graph.
        db: An open connection. Without one, only the batch's resolved records
            are searched (the ``alternate`` lane).
        guard: A trusted read guard every graph read passes, as in
            ``resolve_records``: it applies to the reads of a type that
            declares every field it names, and on a relation path to every
            vertex and edge of the path. Refused by a backend without read
            guards.
        spec: Lookup spec for the label lanes (normalised fields, key
            patterns, alias hops, label field overrides). Its ``vertex_types``
            and ``max_seeds`` are set per record.
        lookup_text: Words to look each staged record up by besides the label
            values it states, such as the passage it was read from; typically
            taken from the caller's own ``evidence``.
        max_candidates: Most candidates per record; ``truncated`` says when
            more were found.

    Returns:
        One entry per staged record, in the resolution's order; an entry with
        no candidates when nothing was found.

    Raises:
        NotImplementedError: A *guard* on a backend that cannot apply one.
    """
    if max_candidates < 1:
        raise ValueError("max_candidates must be at least 1")
    if guard is not None and db is not None:
        db.require_read_guard()
    gatherer = _Gatherer(
        schema, resolution, db, guard, spec or LookupSpec(), lookup_text
    )
    out: list[StagedCandidates] = []
    for staged in resolution.staged.records:
        record = staged.record
        entry = StagedCandidates(
            source_id=staged.source_id,
            record=record,
            reason=staged.reason,
        )
        out.append(entry)
        if not gatherer.contract(record.type).identity_fields:
            continue
        found = [
            *gatherer.alternate(staged),
            *gatherer.lookup(staged, max_candidates + 1),
            *gatherer.relation(staged),
        ]
        distinct = gatherer.related(staged)
        found = [
            c
            for c in found
            if not any(_empty(v) for v in c.key.values())
            and (c.vertex_type, _frozen(c.key)) not in distinct
        ]
        found.sort(key=lambda c: LANE_ORDER[c.lane])
        best: dict[tuple[tuple[str, str], ...], EntityCandidate] = {}
        for candidate in found:
            best.setdefault(_frozen(candidate.key), candidate)
        kept = gatherer.compatible(staged, list(best.values()))
        entry.candidates = kept[:max_candidates]
        entry.truncated = len(kept) > max_candidates
    return out
