"""Which facts hold, given what was stated about them, when, and how.

A *statement* says that one entity's property has a value, or that it relates
to a target, as one source stated it: affirmed or denied (``polarity``),
reported as done, intended or uncertain (``modality``), with the time the
statement says it holds (``valid_from`` / ``valid_to``) and the time it was
said (``said_at``). A *slot* is what holds the values: one property, or one
relation from one entity.

:func:`resolve` turns statements into the facts that hold ``as_of`` a time:

1. **Withdrawals.** A denial withdraws the affirmations of the same value said
   no later than it, or said in the same record or thread when either time is
   unknown. A denial that cannot be ordered against an affirmation is a
   conflict.
2. **Ordering.** In a single-valued slot, of two values the one with the later
   stated time holds; failing stated times on both, the later said time;
   failing both, the stronger basis (how it was obtained), and the pair is a
   conflict. A
   missing time is unknown, never the oldest. Where no value outranks every
   other, the slot is a conflict and holds nothing.
3. **Modality.** A planned value counts once its ``valid_from`` is at or before
   ``as_of``; before that, and when ``as_of`` is not given, it is pending. A
   hedged statement never decides a slot; it is pending.
4. **Exclusive slots.** A second value is always a conflict; the value with the
   strongest basis stays current when one has it.
5. **History.** In a stateful slot, each value the current one replaced is kept
   with its interval, when both are ordered by time.

The result does not depend on the order of the statements. Which slots are
single-valued, exclusive or stateful is read from a schema by
:func:`slot_rules`. Statements are stored as the provenance aspect's
``Assertion`` vertices (:mod:`graflo.link.assertions`).
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from enum import IntEnum
from itertools import pairwise
from typing import Any, Literal

from pydantic import Field, model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.vertex import FieldType

Polarity = Literal["stated", "denied"]
Modality = Literal["actual", "planned", "hedged"]
Granularity = Literal["instant", "day", "week", "month", "year"]
SlotKind = Literal["property", "relation"]


class Basis(IntEnum):
    """How a statement was obtained, weakest first; it weighs when time does not decide."""

    EXTRACTED = 0
    """Read from text by a model."""
    CONFIRMED = 1
    """Confirmed by a person."""
    RECORDED = 2
    """Recorded by a system of record."""


def entity_key(doc: Mapping[str, Any], fields: Iterable[str]) -> str:
    """An entity's key as one string: its identity *fields*, sorted, as JSON."""
    return json.dumps({f: doc.get(f) for f in fields}, sort_keys=True, default=str)


class Statement(ConfigBaseModel):
    """One source's statement about one slot.

    ``id`` left empty is minted from the record, the quote's start, the entity,
    the slot and the value, so re-reading the same record gives the same ids.
    """

    id: str = Field(default="", description="Stable id: the stored assertion's key.")
    vertex_type: str
    key: str = Field(description="The entity's key, as :func:`entity_key` gives it.")
    slot: str = Field(description="The property, or the relation, stated.")
    kind: SlotKind = "property"
    value: Any = Field(
        description="The property value, or the relation's target key (entity_key)."
    )
    target_type: str | None = None
    polarity: Polarity = "stated"
    modality: Modality = "actual"
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    granularity: Granularity | None = None
    said_at: datetime | None = Field(
        default=None, description="When the source said it: its record's time."
    )
    basis: Basis = Basis.EXTRACTED
    record: str | None = Field(default=None, description="The record it was read from.")
    thread: str | None = Field(
        default=None, description="The conversation the record belongs to."
    )
    quote: str | None = Field(default=None, description="The words it was read from.")
    span_start: int | None = None
    span_end: int | None = None

    @model_validator(mode="after")
    def _times_in_utc_and_an_id(self) -> Statement:
        for name in ("valid_from", "valid_to", "said_at"):
            value = getattr(self, name)
            if value is not None and value.tzinfo is None:
                object.__setattr__(self, name, value.replace(tzinfo=UTC))
        if not self.id:
            object.__setattr__(self, "id", _mint(self))
        return self


def _mint(statement: Statement) -> str:
    material = json.dumps(
        [
            statement.record,
            statement.span_start,
            statement.vertex_type,
            statement.key,
            statement.slot,
            statement.kind,
            statement.target_type,
            statement.value,
        ],
        default=str,
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


class SlotRule(ConfigBaseModel):
    """How one slot holds values."""

    single: bool = False
    exclusive: bool = False
    stateful: bool = False


class Fact(ConfigBaseModel):
    """A value a slot holds, with the statements behind it."""

    vertex_type: str
    key: str
    slot: str
    kind: SlotKind
    value: Any
    target_type: str | None = None
    modality: Modality = "actual"
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    supports: list[str] = Field(default_factory=list)


class Conflict(ConfigBaseModel):
    """Values of one slot that nothing orders, or that an exclusive slot refuses."""

    vertex_type: str
    key: str
    slot: str
    values: list[Fact]
    reason: str


class Withdrawal(ConfigBaseModel):
    """An affirmation a denial withdrew."""

    statement: str
    by: str


class Resolution(ConfigBaseModel):
    """What holds ``as_of``, what is pending, what was replaced, withdrawn or disputed."""

    current: list[Fact] = Field(default_factory=list)
    pending: list[Fact] = Field(default_factory=list)
    history: list[Fact] = Field(default_factory=list)
    conflicts: list[Conflict] = Field(default_factory=list)
    withdrawn: list[Withdrawal] = Field(default_factory=list)


SlotId = tuple[str, str]
"""``(vertex_type, slot)``: which rule applies."""


def slot_rules(schema: Schema) -> dict[SlotId, SlotRule]:
    """The single-valued, exclusive and stateful slots *schema* declares.

    A property is single-valued unless it is a LIST that merges by union, and
    exclusive when its match declaration says so. A relation is single-valued
    with ``merge: replace`` and exclusive with ``exclusive``. Either is stateful
    when the type's ``<Type>State`` carries it. A slot left out is multi-valued.
    """
    from graflo.architecture.evolution.aspects.state.vocabulary import (
        state_type_name,
    )

    core = schema.core_schema
    vertices = {v.name: v for v in core.vertex_config.vertices}

    def stateful(vertex_type: str, slot: str) -> bool:
        state = vertices.get(state_type_name(vertex_type))
        return state is not None and slot in state.property_names

    rules: dict[SlotId, SlotRule] = {}
    for vertex in core.vertex_config.vertices:
        for field in vertex.properties:
            many = field.type == FieldType.LIST and field.merge == "union"
            if many or field.name in vertex.identity:
                continue
            rules[vertex.name, field.name] = SlotRule(
                single=True,
                exclusive=bool(field.match and field.match.exclusive),
                stateful=stateful(vertex.name, field.name),
            )
    for edge in core.edge_config.edges:
        if edge.relation is None or edge.merge != "replace":
            continue
        rules[edge.source, edge.relation] = SlotRule(
            single=True,
            exclusive=edge.exclusive,
            stateful=stateful(edge.source, edge.relation),
        )
    return rules


def resolve(
    statements: Iterable[Statement],
    rules: Mapping[SlotId, SlotRule],
    *,
    as_of: datetime | None = None,
) -> Resolution:
    """The facts that hold ``as_of``, from *statements* under *rules*.

    Args:
        statements: Every statement to weigh, in any order.
        rules: Per ``(vertex_type, slot)``, how the slot holds values; a slot
            left out is multi-valued.
        as_of: The time to resolve at, for planned values. ``None`` keeps every
            planned value pending.
    """
    if as_of is not None and as_of.tzinfo is None:
        as_of = as_of.replace(tzinfo=UTC)
    by_slot: dict[tuple[str, str, str], list[Statement]] = defaultdict(list)
    for statement in sorted(statements, key=lambda s: s.id):
        by_slot[statement.vertex_type, statement.key, statement.slot].append(statement)

    result = Resolution()
    for (vertex_type, key, slot), group in sorted(by_slot.items()):
        rule = rules.get((vertex_type, slot), SlotRule())
        _resolve_slot(group, rule, as_of, result)
    return result


def _resolve_slot(
    group: list[Statement],
    rule: SlotRule,
    as_of: datetime | None,
    result: Resolution,
) -> None:
    vertex_type, key, slot = group[0].vertex_type, group[0].key, group[0].slot
    denials = [s for s in group if s.polarity == "denied"]
    withdrawn: set[str] = set()
    for affirmation in (s for s in group if s.polarity == "stated"):
        for denial in denials:
            if denial.value != affirmation.value:
                continue
            verdict = _denial_withdraws(denial, affirmation)
            if verdict is True:
                withdrawn.add(affirmation.id)
                result.withdrawn.append(
                    Withdrawal(statement=affirmation.id, by=denial.id)
                )
                break
            if verdict is None:
                result.conflicts.append(
                    Conflict(
                        vertex_type=vertex_type,
                        key=key,
                        slot=slot,
                        values=[_fact([affirmation]), _fact([denial])],
                        reason="a denial that cannot be ordered against the statement",
                    )
                )

    standing = [s for s in group if s.polarity == "stated" and s.id not in withdrawn]
    deciding: list[Statement] = []
    for statement in standing:
        if statement.modality == "hedged" or not _in_force(statement, as_of):
            result.pending.append(_fact([statement]))
        else:
            deciding.append(statement)

    values = _by_value(deciding)
    if not values:
        return
    if not rule.single:
        result.current.extend(_fact(supports) for supports in values.values())
        return
    if len(values) == 1:
        result.current.append(_fact(next(iter(values.values()))))
        return

    facts = [_fact(supports) for supports in values.values()]
    if rule.exclusive:
        best = _strongest(values)
        result.conflicts.append(
            Conflict(
                vertex_type=vertex_type,
                key=key,
                slot=slot,
                values=facts,
                reason="an exclusive slot holds one value",
            )
        )
        if best is not None:
            result.current.append(_fact(values[best]))
        return

    winner, by_rank = _latest(values)
    if winner is None:
        result.conflicts.append(
            Conflict(
                vertex_type=vertex_type,
                key=key,
                slot=slot,
                values=facts,
                reason="no time or basis orders the values",
            )
        )
        return
    if by_rank:
        result.conflicts.append(
            Conflict(
                vertex_type=vertex_type,
                key=key,
                slot=slot,
                values=facts,
                reason="ordered by basis, not by time",
            )
        )
    result.current.append(_fact(values[winner]))
    if rule.stateful:
        result.history.extend(_history(values, winner))


def _denial_withdraws(denial: Statement, affirmation: Statement) -> bool | None:
    """True when *denial* withdraws *affirmation*, False when it came before it,
    None when nothing orders them."""
    if denial.said_at is not None and affirmation.said_at is not None:
        return affirmation.said_at <= denial.said_at
    if denial.record is not None and denial.record == affirmation.record:
        return True
    if denial.thread is not None and denial.thread == affirmation.thread:
        return True
    return None


def _in_force(statement: Statement, as_of: datetime | None) -> bool:
    if statement.modality != "planned":
        return True
    if as_of is None or statement.valid_from is None:
        return False
    return statement.valid_from <= as_of


def _by_value(statements: list[Statement]) -> dict[str, list[Statement]]:
    values: dict[str, list[Statement]] = {}
    for statement in statements:
        values.setdefault(_value_key(statement.value), []).append(statement)
    return values


def _value_key(value: Any) -> str:
    return repr(value)


def _when(supports: list[Statement]) -> tuple[datetime | None, datetime | None, int]:
    """A value's stated time, said time and basis: the latest and strongest of its supports."""
    stated = [s.valid_from for s in supports if s.valid_from is not None]
    said = [s.said_at for s in supports if s.said_at is not None]
    return (
        max(stated) if stated else None,
        max(said) if said else None,
        max(int(s.basis) for s in supports),
    )


def _compare(
    a: tuple[datetime | None, datetime | None, int],
    b: tuple[datetime | None, datetime | None, int],
) -> tuple[int, bool]:
    """``(order, by_rank)``: 1 when *a* is later, -1 when earlier, 0 when unordered."""
    if a[0] is not None and b[0] is not None and a[0] != b[0]:
        return (1 if a[0] > b[0] else -1), False
    if a[1] is not None and b[1] is not None and a[1] != b[1]:
        return (1 if a[1] > b[1] else -1), False
    if a[2] != b[2]:
        return (1 if a[2] > b[2] else -1), True
    return 0, False


def _latest(values: dict[str, list[Statement]]) -> tuple[str | None, bool]:
    """The value later than every other, and whether basis decided any pair."""
    times = {value: _when(supports) for value, supports in values.items()}
    for value, when in sorted(times.items()):
        by_rank = False
        for other, other_when in times.items():
            if other == value:
                continue
            order, ranked = _compare(when, other_when)
            if order != 1:
                break
            by_rank = by_rank or ranked
        else:
            return value, by_rank
    return None, False


def _strongest(values: dict[str, list[Statement]]) -> str | None:
    ranks = {
        value: max(int(s.basis) for s in supports) for value, supports in values.items()
    }
    top = max(ranks.values())
    best = [value for value, rank in ranks.items() if rank == top]
    return best[0] if len(best) == 1 else None


def _history(values: dict[str, list[Statement]], winner: str) -> list[Fact]:
    """The values *winner* replaced, each closed by the next, when time orders them."""
    timed: list[tuple[datetime, str]] = []
    for value, supports in values.items():
        stated, said, _ = _when(supports)
        start = stated or said
        if start is not None:
            timed.append((start, value))
    timed.sort()
    past: list[Fact] = []
    for (start, value), (end, _) in pairwise(timed):
        if value == winner:
            continue
        fact = _fact(values[value])
        past.append(fact.model_copy(update={"valid_from": start, "valid_to": end}))
    return past


def _fact(supports: list[Statement]) -> Fact:
    first = supports[0]
    stated = [s.valid_from for s in supports if s.valid_from is not None]
    ends = [s.valid_to for s in supports if s.valid_to is not None]
    modality: Modality = (
        "actual" if any(s.modality == "actual" for s in supports) else first.modality
    )
    return Fact(
        vertex_type=first.vertex_type,
        key=first.key,
        slot=first.slot,
        kind=first.kind,
        value=first.value,
        target_type=first.target_type,
        modality=modality,
        valid_from=min(stated) if stated else None,
        valid_to=max(ends) if ends else None,
        supports=sorted(s.id for s in supports),
    )


__all__ = [
    "Basis",
    "Conflict",
    "Fact",
    "Granularity",
    "Modality",
    "Polarity",
    "Resolution",
    "SlotKind",
    "SlotRule",
    "Statement",
    "Withdrawal",
    "entity_key",
    "resolve",
    "slot_rules",
]
