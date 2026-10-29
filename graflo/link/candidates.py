"""Gather the entities a staged record may be, without a model.

:func:`gather_candidates` runs after :func:`~graflo.link.resolve.resolve_records`
(and, on a live graph, after its write) and lists, for each staged record, the
entities it may be, by three kinds of evidence:

- ``alternate``: the entities a stated secondary identity matched -- several,
  or the record would have resolved (``ambiguous``);
- ``exact`` / ``key`` / ``normalized`` / ``alias`` / ``compatible``:
  :func:`~graflo.hq.lookup.lookup_candidates` over the label fields the record
  states, the values of fields others are derived from, and the caller's
  lookup text for it;
- ``relation``: a relation stated between the record and a resolved one --
  "the customer" that RAISED a resolved HD-4411 gives the customers linked to
  HD-4411 by ``RAISED``;
- ``context``: a neighbour of the record's type, one hop from an entity the
  same source resolved by key or a subject it is known to concern -- "the
  unit" of a ticket about one unit.

One candidate per entity carries every lane that found it, best first, and
the record's other stated properties graded against the entity's stored
values (:mod:`graflo.architecture.schema.compare`: ``exact``, ``compatible``,
``differs``, ``conflict``, ``unknown``); stored values are never returned.

Candidates never close a record: only keys do. A pick, a declared rule, an
agent or a person chooses among them, and :func:`support` says whether a
candidate's features identify it. A candidate whose stored key or
secondary-identity values conflict with what the record states is left out: a
record that states ``email`` is not an entity holding another email. Nor is
the entity a record of the same source, related to this one by a stated
relation, resolved to: the relation states two entities.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any, Literal, NamedTuple

from graflo.architecture.graph_types.enums import EdgeDirection
from graflo.architecture.schema.coerce import CoercionError, coerce_value
from graflo.architecture.schema.compare import (
    LEVEL_ORDER,
    MatchLevel,
    compare_value,
    is_exclusive,
)
from graflo.architecture.schema.derive import complete_derived
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.key_contract import (
    KeyContract,
    key_contract,
    key_status,
)
from graflo.architecture.schema.vertex import Field
from graflo.filter.onto import FilterExpression
from graflo.hq.lookup import LookupSpec, _Reader, label_fields, lookup_candidates
from graflo.link.records import (
    AttributeCheck,
    CandidateHit,
    CandidateLane,
    EntityCandidate,
    KnownSubject,
    PartialRecord,
    Resolution,
    Resolved,
    StagedCandidates,
    StagedRecord,
    SupportFeatures,
)

#: Words to look a staged record up by, besides its stated label values.
LookupText = Callable[[PartialRecord], str | None]

#: Whether a stated property ``(vertex type, field)`` may be read and compared.
Comparable = Callable[[str, str], bool]

#: Lanes best first; candidates are ordered by their best lane.
LANE_ORDER: dict[CandidateLane, int] = {
    "alternate": 0,
    "exact": 1,
    "key": 2,
    "normalized": 3,
    "alias": 4,
    "compatible": 5,
    "relation": 6,
    "metadata": 7,
    "context": 8,
}

#: Lanes whose hit alone identifies a candidate (``support``).
IDENTIFYING_LANES: frozenset[CandidateLane] = frozenset(
    {"key", "alternate", "exact", "normalized"}
)


def _frozen(key: dict[str, Any]) -> tuple[tuple[str, str], ...]:
    return tuple(sorted((k, str(v)) for k, v in key.items()))


def _empty(value: Any) -> bool:
    return value is None or value == ""


class _Hit(NamedTuple):
    """One lane's finding, before candidates are merged per entity."""

    key: dict[str, Any]
    label: str | None
    hit: CandidateHit


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
        comparable: Comparable | None,
        subjects: Mapping[str, Sequence[KnownSubject]],
    ) -> None:
        self.schema = schema
        self.subjects = subjects
        self.db = db
        self.guard = guard
        self.spec = spec
        self.lookup_text = lookup_text
        self.allowed = comparable
        self.reserved = schema.reserved_fields()
        self.vertex_config = schema.core_schema.vertex_config
        self.vertices = {v.name: v for v in self.vertex_config.vertices}
        self.fields: dict[str, dict[str, Field]] = {
            name: {f.name: f for f in vertex.properties}
            for name, vertex in self.vertices.items()
        }
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

    # -- values --------------------------------------------------------------

    def comparable(self, vertex_type: str, name: str) -> bool:
        """A declared, unreserved field of the type that the caller lets through."""
        if name not in self.fields[vertex_type] or name in self.reserved:
            return False
        return self.allowed is None or self.allowed(vertex_type, name)

    def values(self, record: PartialRecord) -> dict[str, Any]:
        """The record's stated values that may be read and compared.

        Derived fields are set from their sources first, so a key may name one.
        """
        stated = complete_derived(self.vertices[record.type], record.values())
        return {
            name: value
            for name, value in stated.items()
            if not _empty(value) and self.comparable(record.type, name)
        }

    def derived(self, vertex_type: str) -> set[str]:
        return {f.name for f in self.vertices[vertex_type].derived_fields}

    def sources(self, vertex_type: str) -> list[str]:
        """Fields some derived field of the type is computed from."""
        return list(
            dict.fromkeys(
                f.derived.source
                for f in self.vertices[vertex_type].derived_fields
                if f.derived is not None
            )
        )

    def equal(self, vertex_type: str, name: str, a: Any, b: Any) -> bool:
        """Whether *a* and *b* are one value of field *name*, as its declared type."""
        field = self.fields[vertex_type].get(name)
        if field is not None:
            try:
                return coerce_value(a, field) == coerce_value(b, field)
            except CoercionError:
                pass
        return a == b

    def freeze(self, vertex_type: str, key: dict[str, Any]) -> tuple:
        """*key* as a hashable value, its fields read as their declared types."""
        fields = self.fields[vertex_type]
        typed: dict[str, Any] = {}
        for name, value in key.items():
            try:
                typed[name] = coerce_value(value, fields[name])
            except (CoercionError, KeyError):
                typed[name] = value
        return _frozen(typed)

    # -- lanes ---------------------------------------------------------------

    def alternate(self, staged: StagedRecord) -> list[_Hit]:
        """The entities a stated secondary identity matched, batch and graph."""
        record = staged.record
        contract = self.contract(record.type)
        identity = list(contract.identity_fields)
        values = self.values(record)
        found: list[_Hit] = []
        for alternate in key_status(contract, values).alternates:
            probe = {f: values[f] for f in alternate.fields}
            hit = CandidateHit(
                lane="alternate",
                matched=", ".join(f"{f}={values[f]}" for f in alternate.fields),
            )
            for key, item in self.resolved.items():
                other = self.records.get(key)
                if item.type != record.type or other is None:
                    continue
                stated = other.values()
                if all(
                    self.equal(record.type, f, stated.get(f), probe[f])
                    for f in alternate.fields
                ):
                    found.append(
                        _Hit(
                            dict(item.identity), self.label_of(record.type, stated), hit
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
                    _Hit(
                        {f: doc.get(f) for f in identity},
                        self.label_of(record.type, doc),
                        hit,
                    )
                )
        return found

    def lookup(self, staged: StagedRecord, limit: int) -> list[_Hit]:
        """``lookup_candidates`` over the stated labels and the lookup text."""
        if self.db is None:
            return []
        record = staged.record
        values = self.values(record)
        words = [self.lookup_text(record) or ""] if self.lookup_text else []
        words += [
            values[f]
            for f in dict.fromkeys(
                [*self.labels(record.type), *self.sources(record.type)]
            )
            if isinstance(values.get(f), str)
        ]
        text = " ".join(dict.fromkeys(w.strip() for w in words if w and w.strip()))
        if not text:
            return []
        spec = self.spec.model_copy(
            update={"vertex_types": [record.type], "max_seeds": limit}
        )
        found = lookup_candidates(
            self.db,
            self.schema,
            text,
            spec,
            guard=self.guard_for(record.type),
            comparable=self.comparable,
        )
        return [
            _Hit(
                dict(c.key),
                c.label,
                CandidateHit(lane=hit.lane, matched=hit.matched, targets=hit.targets),
            )
            for c in found
            if c.vertex_type == record.type
            for hit in c.hits
        ]

    def relation(self, staged: StagedRecord) -> list[_Hit]:
        """Entities linked to a resolved anchor by a relation the text states."""
        if self.db is None:
            return []
        record = staged.record
        ref = record.ref
        identity = list(self.contract(record.type).identity_fields)
        found: list[_Hit] = []
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
                found.append(
                    _Hit(
                        {f: doc.get(f) for f in identity},
                        self.label_of(record.type, doc),
                        CandidateHit(lane="relation", relation=name, anchor=anchor_ref),
                    )
                )
        return found

    def context(self, staged: StagedRecord) -> list[_Hit]:
        """Neighbours of the record's type around what its source is known to be about.

        Anchors are the source's records resolved by key and its known subjects.
        One hop in both directions, under the guard, which the caller sets to
        what every reader of the source may see. A neighbour is ranked by its
        label against the record's stated one; a ``conflict`` drops it. The
        quote alone ranks nothing.
        """
        if self.db is None:
            return []
        record = staged.record
        identity = list(self.contract(record.type).identity_fields)
        anchors: list[tuple[str, dict[str, Any], CandidateHit]] = [
            (
                item.type,
                dict(item.identity),
                CandidateHit(lane="context", anchor=ref, level="unknown"),
            )
            for (source_id, ref), item in sorted(self.resolved.items())
            if source_id == staged.source_id and item.by == "key" and ref != record.ref
        ]
        anchors += [
            (
                subject.vertex_type,
                dict(subject.key),
                CandidateHit(lane="context", role="subject", level="unknown"),
            )
            for subject in self.subjects.get(staged.source_id, ())
        ]
        stated = {
            f: v
            for f, v in self.values(record).items()
            if f in self.labels(record.type)
        }
        found: list[_Hit] = []
        for anchor_type, key, hit in anchors:
            reached = self.db.graph_neighbors(
                anchor_type,
                key,
                hops=1,
                direction=EdgeDirection.ANY,
                schema=self.schema,
                guard=self.guard_for(record.type),
            )
            for doc in reached.vertices.get(record.type, []):
                entity = {f: doc.get(f) for f in identity}
                if anchor_type == record.type and self.freeze(
                    record.type, entity
                ) == self.freeze(record.type, key):
                    continue
                level = self.label_level(record.type, stated, doc)
                if level == "conflict":
                    continue
                found.append(
                    _Hit(
                        entity,
                        self.label_of(record.type, doc),
                        hit.model_copy(update={"level": level}),
                    )
                )
        return found

    def label_level(
        self, vertex_type: str, stated: dict[str, Any], doc: dict[str, Any]
    ) -> MatchLevel:
        """The best level of any stated label against any of the entity's labels."""
        levels: list[MatchLevel] = []
        for value in stated.values():
            for label in self.labels(vertex_type):
                field = self.fields[vertex_type].get(label)
                if field is not None:
                    levels.append(
                        compare_value(field, value, doc.get(label), keyed=False)
                    )
        return min(levels, key=LEVEL_ORDER.__getitem__, default="unknown")

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
                out.add((anchor.type, self.freeze(anchor.type, key)))
        return out

    def merge(self, staged: StagedRecord, hits: list[_Hit]) -> list[EntityCandidate]:
        """One candidate per entity, with every lane that found it, best first.

        The order is total and independent of the database's row order: by best
        lane, then key, and a candidate's lanes by rank, then content.
        """
        vertex_type = staged.record.type
        distinct = self.related(staged)
        hits = [
            h
            for h in hits
            if not any(_empty(v) for v in h.key.values())
            and (vertex_type, self.freeze(vertex_type, h.key)) not in distinct
        ]
        hits.sort(
            key=lambda h: (
                LANE_ORDER[h.hit.lane],
                LEVEL_ORDER[h.hit.level] if h.hit.level is not None else 0,
                _frozen(h.key),
                h.hit.model_dump_json(),
            )
        )
        merged: dict[tuple, EntityCandidate] = {}
        for item in hits:
            frozen = self.freeze(vertex_type, item.key)
            candidate = merged.get(frozen)
            if candidate is None:
                merged[frozen] = EntityCandidate(
                    vertex_type=vertex_type,
                    key=dict(item.key),
                    label=item.label,
                    lanes=[item.hit],
                )
            elif item.hit not in candidate.lanes:
                candidate.lanes.append(item.hit)
                if candidate.label is None:
                    candidate.label = item.label
        return list(merged.values())

    def check(
        self, staged: StagedRecord, candidates: list[EntityCandidate]
    ) -> list[EntityCandidate]:
        """*candidates* whose stored keys do not conflict with the record, graded.

        A key or secondary-identity field in ``conflict`` leaves the candidate
        out; every other stated field the caller lets through is graded and
        reported, and the candidate's support features are set.
        """
        record = staged.record
        contract = self.contract(record.type)
        identity = list(contract.identity_fields)
        values = self.values(record)
        keyed = {f for key in contract.keys for f in key.fields}
        keyed |= {f for alt in contract.alternates for f in alt.fields}
        keyed |= set(identity)
        fields = self.fields[record.type]
        kept = [
            c
            for c in candidates
            if all(
                self.level(record.type, f, values[f], c.key.get(f), keyed=True)
                != "conflict"
                for f in identity
                if f in values
            )
        ]
        derived = self.derived(record.type)
        compared = [f for f in values if f not in identity and f not in derived]
        reader = self.reader_for(record.type)
        stored: dict[int, list[dict[str, Any]]] = {}
        if compared and kept and reader is not None:
            stored = reader.find_by(
                record.type,
                identity,
                [c.key for c in kept],
                list(dict.fromkeys([*identity, *compared])),
            )
        out = []
        for position, candidate in enumerate(kept):
            docs = stored.get(position)
            doc = docs[0] if docs else {}  # a batch entity: nothing stored
            checks: list[AttributeCheck] = []
            for name in compared:
                level = self.level(
                    record.type, name, values[name], doc.get(name), keyed=name in keyed
                )
                if level == "conflict" and name in keyed:
                    break
                checks.append(
                    AttributeCheck(
                        field=name,
                        level=level,
                        exclusive=_exclusive(fields.get(name), keyed=name in keyed),
                    )
                )
            else:
                candidate.attributes = checks
                candidate.support = _features(candidate)
                out.append(candidate)
        return out

    def level(
        self, vertex_type: str, name: str, stated: Any, stored: Any, *, keyed: bool
    ) -> MatchLevel:
        field = self.fields[vertex_type].get(name)
        if field is None:
            if _empty(stated) or _empty(stored):
                return "unknown"
            return "exact" if stated == stored else ("conflict" if keyed else "differs")
        return compare_value(field, stated, stored, keyed=keyed)


def _exclusive(field: Field | None, *, keyed: bool) -> bool:
    return keyed if field is None else is_exclusive(field, keyed=keyed)


def _features(candidate: EntityCandidate) -> SupportFeatures:
    return SupportFeatures(
        identifying_lane=any(h.lane in IDENTIFYING_LANES for h in candidate.lanes),
        exclusive_agreement=any(
            a.exclusive and a.level == "exact" for a in candidate.attributes
        ),
        unique_alias=any(h.lane == "alias" and h.targets == 1 for h in candidate.lanes),
        conflict=any(a.level == "conflict" for a in candidate.attributes),
    )


def support(candidate: EntityCandidate) -> Literal["accept", "propose"]:
    """``accept`` when a feature identifies *candidate* and nothing conflicts.

    Identifying: a hit in an identifying lane (``key``, ``alternate``,
    ``exact``, ``normalized``), an exclusive field stated and stored alike, or
    an alias naming only it in the reader's view. ``compatible`` and
    ``context`` hits never identify on their own.
    """
    features = candidate.support
    identified = (
        features.identifying_lane
        or features.exclusive_agreement
        or features.unique_alias
    )
    return "accept" if identified and not features.conflict else "propose"


def gather_candidates(
    schema: Schema,
    resolution: Resolution,
    db: Any | None = None,
    *,
    guard: FilterExpression | None = None,
    spec: LookupSpec | None = None,
    lookup_text: LookupText | None = None,
    comparable: Comparable | None = None,
    subjects: Mapping[str, Sequence[KnownSubject]] | None = None,
    max_candidates: int = 10,
) -> list[StagedCandidates]:
    """The entities each staged record of *resolution* may be, best first.

    Args:
        schema: Schema the records were resolved against.
        resolution: What :func:`~graflo.link.resolve.resolve_records`
            returned. Relation-path anchors are its resolved records, so run
            this after their write when they are new to the graph.
        db: An open connection. Without one, only the batch's resolved records
            are searched (the ``alternate`` lane), and every attribute is
            ``unknown``.
        guard: A trusted read guard every graph read passes, as in
            ``resolve_records``: it applies to the reads of a type that
            declares every field it names, and on a relation path to every
            vertex and edge of the path. Refused by a backend without read
            guards.
        spec: Lookup spec for the label lanes (key patterns, alias hops,
            label field overrides). Its ``vertex_types`` and ``max_seeds`` are
            set per record. The ``normalized`` and ``compatible`` lanes read
            the schema's derived fields.
        lookup_text: Words to look each staged record up by besides the label
            values it states, such as the passage it was read from; typically
            taken from the caller's own ``evidence``.
        comparable: Which stated properties may be read and compared, as
            ``(vertex type, field) -> bool``; one it refuses is never probed,
            looked up by or checked, keys included. Declared, unreserved
            fields are the only ones ever considered.
        subjects: Per source id, the entities the source is known to concern
            (from its metadata), anchors of the ``context`` lane besides the
            records it resolved by key.
        max_candidates: Most candidates per record, counted as entities after
            lanes are merged and keys checked; ``truncated`` says when more
            were found.

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
    spec = spec or LookupSpec()
    gatherer = _Gatherer(
        schema, resolution, db, guard, spec, lookup_text, comparable, subjects or {}
    )
    limit = max(spec.max_seeds, max_candidates + 1)
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
        hits = [
            *gatherer.alternate(staged),
            *gatherer.lookup(staged, limit),
            *gatherer.relation(staged),
            *gatherer.context(staged),
        ]
        kept = gatherer.check(staged, gatherer.merge(staged, hits))
        entry.candidates = kept[:max_candidates]
        entry.truncated = len(kept) > max_candidates
    return out
