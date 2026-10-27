"""Which existing vertices a short text may name.

:func:`lookup_candidates` reads a graph for vertices a text mentions, by four
lanes, all exact or rule-based -- no model and no vectors:

- ``exact``: a run of the text's words equals a label field;
- ``key``: a declared pattern matches the text and its value equals a key field;
- ``normalized``: a run of words, in :func:`~graflo.util.transform.normalize_label`
  form, equals a stored normalised label field;
- ``alias``: a label lane finds a vertex of a declared alias type, and its
  alias edge leads to the target.

It returns keys and labels only, never other fields, so a caller can show the
candidates to a model without copying the graph's content into its prompt.
"""

from __future__ import annotations

import re
from typing import Any, Literal, NamedTuple

from pydantic import Field

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.evolution.sanitize import with_physical_names
from graflo.architecture.graph_types.enums import EdgeDirection
from graflo.architecture.schema import Schema
from graflo.architecture.schema.physical_keys import PhysicalKeys
from graflo.architecture.schema.vertex import Vertex
from graflo.filter.onto import FilterExpression
from graflo.util.transform import normalize_label

#: Field semantics IRIs that mark a property as a name a text may use.
LABEL_IRIS: frozenset[str] = frozenset(
    {
        "http://www.w3.org/2004/02/skos/core#prefLabel",
        "http://www.w3.org/2004/02/skos/core#altLabel",
        "http://www.w3.org/2000/01/rdf-schema#label",
        "https://schema.org/name",
        "http://schema.org/name",
    }
)

Lane = Literal["exact", "key", "normalized", "alias"]
_LANE_RANK: dict[str, int] = {"exact": 0, "key": 1, "normalized": 2, "alias": 3}
_EDGE_PUNCTUATION = "\"'`.,;:!?()[]{}<>«»“”‘’"


class KeyPattern(ConfigBaseModel):
    """A pattern whose match in a text is a key value of a vertex type.

    Attributes:
        vertex_type: Logical vertex type the value identifies.
        field: Logical field holding the value (an identity or secondary key).
        pattern: Regular expression; its first group, else the whole match,
            is the value.
    """

    vertex_type: str
    field: str
    pattern: str


class AliasHop(ConfigBaseModel):
    """A vertex type whose labels name vertices of another type.

    Attributes:
        alias_type: Type of the alias vertices (e.g. ``Alias``).
        relation: Edge relation from an alias to what it names.
        target_type: Type the relation leads to.
    """

    alias_type: str
    relation: str
    target_type: str


class LookupSpec(ConfigBaseModel):
    """What :func:`lookup_candidates` searches and how.

    Attributes:
        vertex_types: Types to return candidates of; default every type except
            the alias types.
        label_fields: Label fields per type, replacing those found by
            :func:`label_fields`.
        normalized_fields: Fields per type that hold a label in
            ``normalize_label`` form, for the ``normalized`` lane.
        key_patterns: Patterns for the ``key`` lane.
        aliases: Alias hops for the ``alias`` lane.
        max_seeds: Most candidates returned.
        max_words: Longest run of words tried as a label.
    """

    vertex_types: list[str] | None = None
    label_fields: dict[str, list[str]] = Field(default_factory=dict)
    normalized_fields: dict[str, list[str]] = Field(default_factory=dict)
    key_patterns: list[KeyPattern] = Field(default_factory=list)
    aliases: list[AliasHop] = Field(default_factory=list)
    max_seeds: int = Field(default=20, ge=1)
    max_words: int = Field(default=4, ge=1)


class LaneHit(ConfigBaseModel):
    """One way a text names a vertex.

    Attributes:
        lane: How it was found.
        matched: The words or key value of the text it was found from.
    """

    lane: Lane
    matched: str


class Candidate(ConfigBaseModel):
    """A vertex a text may name.

    Attributes:
        vertex_type: Logical type of the vertex.
        key: Its identity fields and values.
        label: The label of its best label hit, if a label lane found it.
        hits: The best hit of each lane that found it, best first.
    """

    vertex_type: str
    key: dict[str, Any]
    label: str | None = None
    hits: list[LaneHit] = Field(min_length=1)


class _Found(NamedTuple):
    vertex_type: str
    key: dict[str, Any]
    label: str | None
    hit: LaneHit


def _identity_key(key: dict[str, Any]) -> tuple[tuple[str, str], ...]:
    return tuple(sorted((k, str(v)) for k, v in key.items()))


def _rank(item: _Found) -> tuple[int, int, str, str, tuple[tuple[str, str], ...]]:
    return (
        _LANE_RANK[item.hit.lane],
        -len(item.hit.matched),
        item.hit.matched,
        item.vertex_type,
        _identity_key(item.key),
    )


def _merge(found: list[_Found]) -> list[Candidate]:
    """One candidate per vertex with its best hit in each lane, best vertex first.

    Ties break on matched text, so the candidates of one mention stay together,
    then on type and key; the order never follows the database's row order.
    """
    found = sorted(found, key=_rank)
    merged: dict[tuple[str, tuple[tuple[str, str], ...]], Candidate] = {}
    for item in found:
        identity_key = _identity_key(item.key)
        candidate = merged.get((item.vertex_type, identity_key))
        if candidate is None:
            merged[(item.vertex_type, identity_key)] = Candidate(
                vertex_type=item.vertex_type,
                key=dict(item.key),
                label=item.label,
                hits=[item.hit],
            )
        elif all(hit.lane != item.hit.lane for hit in candidate.hits):
            candidate.hits.append(item.hit)
            if candidate.label is None:
                candidate.label = item.label
    return list(merged.values())


def label_fields(vertex: Vertex) -> list[str]:
    """Fields of *vertex* whose semantics IRI is one of :data:`LABEL_IRIS`."""
    return [
        field.name
        for field in vertex.properties
        if field.semantics is not None and field.semantics.iri in LABEL_IRIS
    ]


def word_runs(text: str, max_words: int) -> list[str]:
    """Runs of up to *max_words* words of *text*, longest first, without repeats."""
    words = [word.strip(_EDGE_PUNCTUATION) for word in text.split()]
    words = [word for word in words if word]
    runs: dict[str, None] = {}
    for size in range(min(max_words, len(words)), 0, -1):
        for start in range(len(words) - size + 1):
            runs.setdefault(" ".join(words[start : start + size]))
    return list(runs)


class _Reader:
    """Exact field lookups by logical names on one connection."""

    def __init__(
        self, db: Any, schema: Schema, guard: FilterExpression | None = None
    ) -> None:
        self.db = db
        self.schema = schema
        self.guard = guard
        # Only passed when set, so a backend that predates guards is untouched.
        self.guarded: dict[str, Any] = {"guard": guard} if guard is not None else {}
        named = with_physical_names(schema, db.flavor)
        self.keys = PhysicalKeys(named.db_profile)
        self.vertices = named.resolve_db_aware(db.flavor).vertex_config

    def identity(self, vertex_type: str) -> list[str]:
        return list(self.vertices.identity_fields(vertex_type))

    def find(
        self,
        vertex_type: str,
        field: str,
        values: list[str],
        returned: list[str],
    ) -> dict[int, list[dict[str, Any]]]:
        """Matches of each value of *values* on *field*, by position."""
        return self.find_by(
            vertex_type, [field], [{field: v} for v in values], returned
        )

    def find_by(
        self,
        vertex_type: str,
        fields: list[str],
        docs: list[dict[str, Any]],
        returned: list[str],
    ) -> dict[int, list[dict[str, Any]]]:
        """Matches of each of *docs* on all of *fields*, by position."""
        if not docs:
            return {}
        stored_docs = self.keys.vertex_docs(vertex_type, docs)
        found = self.db.resolve_vertices(
            self.vertices.vertex_dbname(vertex_type),
            stored_docs,
            tuple(self.keys.vertex_fields(vertex_type, fields)),
            tuple(self.keys.vertex_fields(vertex_type, returned)),
            **self.guarded,
        )
        return {
            position: [self.keys.logical_vertex_doc(vertex_type, d) for d in docs]
            for position, docs in found.items()
        }

    def neighbours(
        self, vertex_type: str, key: dict[str, Any], relation: str, target: str
    ) -> list[dict[str, Any]]:
        reached = self.db.graph_neighbors(
            vertex_type,
            key,
            hops=1,
            direction=EdgeDirection.OUT,
            edge_types=[relation],
            schema=self.schema,
            **self.guarded,
        )
        return list(reached.vertices.get(target, []))


def _label_lanes(
    reader: _Reader,
    vertex_type: str,
    spec: LookupSpec,
    runs: list[str],
    labels: list[str],
) -> list[_Found]:
    """The ``exact`` and ``normalized`` lanes for one type."""
    identity = reader.identity(vertex_type)
    if not identity:
        return []
    found: list[_Found] = []
    normal = [normalize_label(run) for run in runs]
    lanes: list[tuple[Lane, list[str], list[str]]] = [
        ("exact", labels, runs),
        (
            "normalized",
            spec.normalized_fields.get(vertex_type, []),
            [n for n in normal if n],
        ),
    ]
    for lane, fields, values in lanes:
        mentions = runs if lane == "exact" else [r for r, n in zip(runs, normal) if n]
        for field in fields:
            returned = list(dict.fromkeys([*identity, *labels]))
            for position, docs in reader.find(
                vertex_type, field, values, returned
            ).items():
                for doc in docs:
                    found.append(
                        _Found(
                            vertex_type,
                            {f: doc.get(f) for f in identity},
                            next(
                                (doc[f] for f in labels if doc.get(f) is not None),
                                None,
                            ),
                            LaneHit(lane=lane, matched=mentions[position]),
                        )
                    )
    return found


def lookup_candidates(
    db: Any,
    schema: Schema,
    text: str,
    spec: LookupSpec | None = None,
    *,
    guard: FilterExpression | None = None,
) -> list[Candidate]:
    """Vertices of *db* that *text* may name, best first, at most ``max_seeds``.

    One candidate per vertex, carrying the best hit of every lane that found
    it, in the order ``exact``, ``key``, ``normalized``, ``alias``; within a
    lane the longer match wins. Vertices are ordered by their best hit.

    Args:
        db: An open graph connection.
        schema: The graph's logical schema.
        text: The text to look in.
        spec: What to search; a default spec searches the label fields of
            every type.
        guard: A trusted read guard (see ``Connection.graph_neighbors``); a
            vertex it hides is never a candidate, and an alias is followed
            only through elements it lets through.

    Returns:
        The candidates; empty when the text names nothing found.

    Raises:
        NotImplementedError: A *guard* on a backend that cannot apply one.
    """
    spec = spec or LookupSpec()
    if guard is not None:
        db.require_read_guard()
    reader = _Reader(db, schema, guard)
    vertex_by_name = {v.name: v for v in schema.core_schema.vertex_config.vertices}
    alias_types = {hop.alias_type for hop in spec.aliases}
    types = spec.vertex_types or [
        name for name in vertex_by_name if name not in alias_types
    ]
    runs = word_runs(text, spec.max_words)

    def labels_of(vertex_type: str) -> list[str]:
        if vertex_type in spec.label_fields:
            return list(spec.label_fields[vertex_type])
        vertex = vertex_by_name.get(vertex_type)
        return label_fields(vertex) if vertex is not None else []

    found: list[_Found] = []
    for vertex_type in types:
        found.extend(
            _label_lanes(reader, vertex_type, spec, runs, labels_of(vertex_type))
        )

    for pattern in spec.key_patterns:
        if pattern.vertex_type not in types:
            continue
        values = list(
            dict.fromkeys(
                match.group(1) if match.groups() else match.group(0)
                for match in re.finditer(pattern.pattern, text)
            )
        )
        identity = reader.identity(pattern.vertex_type)
        returned = list(dict.fromkeys([*identity, pattern.field]))
        for position, docs in reader.find(
            pattern.vertex_type, pattern.field, values, returned
        ).items():
            found.extend(
                _Found(
                    pattern.vertex_type,
                    {f: doc.get(f) for f in identity},
                    None,
                    LaneHit(lane="key", matched=values[position]),
                )
                for doc in docs
            )

    for hop in spec.aliases:
        if hop.target_type not in types:
            continue
        target_identity = reader.identity(hop.target_type)
        for alias in _label_lanes(
            reader, hop.alias_type, spec, runs, labels_of(hop.alias_type)
        ):
            for doc in reader.neighbours(
                hop.alias_type, alias.key, hop.relation, hop.target_type
            ):
                found.append(
                    _Found(
                        hop.target_type,
                        {f: doc.get(f) for f in target_identity},
                        alias.label,
                        LaneHit(lane="alias", matched=alias.hit.matched),
                    )
                )

    return _merge(found)[: spec.max_seeds]
