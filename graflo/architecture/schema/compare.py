"""Graded comparison of a stated value with a stored one, under a field's ``match``.

A comparison yields a level, never agree/contradict:

- ``exact``: equal after the field's normalizer (or ``coerce_value`` without one);
- ``compatible``: one is a less specific form of the other, as the field's
  ``match.parts`` defines it -- a leading run of parts (``general_first``), a
  trailing run (``specific_first``) or a subset (``unordered``);
- ``differs``: neither, and not evidence against;
- ``conflict``: neither, on an ``exclusive`` field -- the one level that vetoes;
- ``unknown``: a value is missing on either side.

Nothing here knows what a field means; the schema that declares it does. Pure,
and reads no database.
"""

from __future__ import annotations

from typing import Any, Literal

from graflo.architecture.schema.coerce import CoercionError, coerce_value
from graflo.architecture.schema.derive import normal_value, value_parts
from graflo.architecture.schema.vertex import Field, FieldType, field_type_value

MatchLevel = Literal["exact", "compatible", "differs", "conflict", "unknown"]

#: Best first; a list value takes its best member's level.
LEVEL_ORDER: dict[MatchLevel, int] = {
    "exact": 0,
    "compatible": 1,
    "differs": 2,
    "conflict": 3,
    "unknown": 4,
}


def is_exclusive(field: Field, *, keyed: bool) -> bool:
    """Whether a difference on *field* is a ``conflict``.

    The field's ``match.exclusive`` when declared; otherwise true exactly for a
    key or alternate-key field (*keyed*). A LIST field is never exclusive unless
    it says so.
    """
    if field.match is not None and field.match.exclusive is not None:
        return field.match.exclusive
    return keyed and field_type_value(field.type) != FieldType.LIST.value


def _scalar_field(field: Field) -> Field:
    """The field as one of its members: a LIST's item type, same ``match``."""
    if field_type_value(field.type) != FieldType.LIST.value:
        return field
    return field.model_copy(update={"type": field.item_type, "item_type": None})


def _normal(field: Field, value: Any) -> Any:
    if field.match is not None and field.match.normalizer is not None:
        return normal_value(field, value)
    try:
        return coerce_value(value, field)
    except CoercionError:
        return value


def _within(shorter: list[str], longer: list[str], order: str) -> bool:
    if len(shorter) > len(longer):
        return False
    if order == "general_first":
        return longer[: len(shorter)] == shorter
    if order == "specific_first":
        return longer[len(longer) - len(shorter) :] == shorter
    return set(shorter) <= set(longer)


def _compare_scalar(
    field: Field, stated: Any, stored: Any, exclusive: bool
) -> MatchLevel:
    left, right = _normal(field, stated), _normal(field, stored)
    if left is None or right is None:
        return "unknown"
    if left == right:
        return "exact"
    match = field.match
    if match is not None and match.parts is not None:
        a, b = value_parts(field, stated), value_parts(field, stored)
        if a and b:
            if a == b:
                return "exact"
            order = match.parts.order
            if _within(a, b, order) or _within(b, a, order):
                return "compatible"
    return "conflict" if exclusive else "differs"


def _members(value: Any) -> list[Any]:
    if isinstance(value, (list, tuple, set, frozenset)):
        return [member for member in value if member is not None and member != ""]
    return [] if value is None or value == "" else [value]


def compare_value(field: Field, stated: Any, stored: Any, *, keyed: bool) -> MatchLevel:
    """The level of *stated* against *stored* for *field*.

    A list on either side is compared member by member, and the best level wins.
    *keyed* says whether the field is part of a key or an alternate key, which
    makes it ``exclusive`` unless its ``match`` says otherwise.
    """
    exclusive = is_exclusive(field, keyed=keyed)
    scalar = _scalar_field(field)
    left, right = _members(stated), _members(stored)
    if not left or not right:
        return "unknown"
    levels = [_compare_scalar(scalar, a, b, exclusive) for a in left for b in right]
    return min(levels, key=LEVEL_ORDER.__getitem__)
