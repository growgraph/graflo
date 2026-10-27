"""Upsert semantics over append-only chunks: records folded by identity.

A file backend directory is written append-only, so one entity upserted twice
is stored twice. Reads fold those records into one per entity, in write order,
the way a database applies the same upserts: a later record's fields replace
earlier ones, except ``merge: union`` list fields, which take the union of
every record's members in first-seen order, nulls dropped. The folded entity
sits where its first record was.

A record whose identity is incomplete is not an upsert target and is kept as
written.
"""

from __future__ import annotations

from collections.abc import Collection, Iterable, Sequence
from typing import Any


def _members(value: Any) -> list[Any]:
    if value is None:
        return []
    return list(value) if isinstance(value, list) else [value]


def _union(stored: Any, written: Any) -> list[Any]:
    out: list[Any] = []
    for member in [*_members(stored), *_members(written)]:
        if member is not None and member not in out:
            out.append(member)
    return out


def merge_record(
    into: dict[str, Any], record: dict[str, Any], union: Collection[str]
) -> None:
    """Apply *record* to *into* as an upsert: replace, and union *union* fields."""
    for field, value in record.items():
        if field in union:
            into[field] = _union(into.get(field), value)
        else:
            into[field] = value


def _key(doc: Any, fields: Sequence[str]) -> tuple[Any, ...] | None:
    """The values of *fields* in *doc*, or ``None`` when any is missing."""
    if not fields or not isinstance(doc, dict):
        return None
    values = tuple(doc.get(field) for field in fields)
    if any(value is None or value == "" for value in values):
        return None
    return tuple(str(value) for value in values)


def fold_vertices(
    records: Iterable[dict[str, Any]],
    identity: Sequence[str],
    union: Collection[str] = (),
) -> list[dict[str, Any]]:
    """One document per entity of *records*, keyed by the *identity* fields."""
    out: list[dict[str, Any]] = []
    position: dict[tuple[Any, ...], int] = {}
    for record in records:
        key = _key(record, identity)
        if key is None:
            out.append(record)
            continue
        at = position.get(key)
        if at is None:
            position[key] = len(out)
            out.append(_first(record, union))
        else:
            merge_record(out[at], record, union)
    return out


def fold_edges(
    records: Iterable[list[Any]],
    source_identity: Sequence[str],
    target_identity: Sequence[str],
    key_fields: Sequence[str] = (),
    union: Collection[str] = (),
) -> list[list[Any]]:
    """One ``[source, target, properties]`` record per edge of *records*.

    An edge is keyed by its endpoints' identities and the values of
    *key_fields* among its properties (the relation is the collection's own).
    """
    out: list[list[Any]] = []
    position: dict[tuple[Any, ...], int] = {}
    for record in records:
        if not isinstance(record, list) or len(record) < 2:
            out.append(record)
            continue
        source, target = record[0], record[1]
        properties = (
            record[2] if len(record) > 2 and isinstance(record[2], dict) else {}
        )
        source_key = _key(source, source_identity)
        target_key = _key(target, target_identity)
        if source_key is None or target_key is None:
            out.append(record)
            continue
        extra = tuple(str(properties.get(field)) for field in key_fields)
        key = (source_key, target_key, extra)
        at = position.get(key)
        if at is None:
            position[key] = len(out)
            out.append([dict(source), dict(target), _first(properties, union)])
        else:
            folded = out[at]
            folded[0].update(source)
            folded[1].update(target)
            merge_record(folded[2], properties, union)
    return out


def _first(record: dict[str, Any], union: Collection[str]) -> dict[str, Any]:
    """A copy of an entity's first record, its union fields already deduplicated."""
    out = dict(record)
    for field in union:
        if field in out:
            out[field] = _union(None, out[field])
    return out
