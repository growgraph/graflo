"""Helpers for resolving vertices by an arbitrary field-set.

Edge-only sources reference their endpoints by a *secondary* identity — a field
set that identifies the vertex but is not its upsert key.  Locating those
endpoints means mapping ``secondary key -> primary key`` immediately before the
edge write, which every backend can do through :meth:`Connection.resolve_vertices`.

This module holds the backend-agnostic pieces of that lookup: extracting key
tuples from documents, building the ``OR``-of-``AND`` filter that selects a chunk
of keys, and bucketing returned documents back by key.
"""

from __future__ import annotations

from typing import Any, Iterator, Sequence

#: Distinct key tuples per lookup query. Keeps generated predicates well below
#: query-size limits on every backend while still amortizing the round trip.
DEFAULT_RESOLVE_CHUNK_SIZE = 200

#: A vertex key: the values of a field-set, in field order.
KeyTuple = tuple[Any, ...]


def key_tuple(doc: dict[str, Any], match_keys: Sequence[str]) -> KeyTuple | None:
    """Extract the key tuple of *doc* for *match_keys*.

    Returns ``None`` when any field is absent or ``None`` — a partial key must
    never be partially matched, so such documents are unresolvable by design.
    """
    values: list[Any] = []
    for field in match_keys:
        value = doc.get(field)
        if value is None:
            return None
        values.append(value)
    return tuple(values)


def distinct_keys(
    docs: list[dict[str, Any]], match_keys: Sequence[str]
) -> list[KeyTuple]:
    """Deduplicated key tuples of *docs*, preserving first-occurrence order."""
    seen: set[KeyTuple] = set()
    out: list[KeyTuple] = []
    for doc in docs:
        key = key_tuple(doc, match_keys)
        if key is None or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def chunked(items: Sequence[Any], size: int) -> Iterator[list[Any]]:
    """Yield *items* in lists of at most *size*."""
    if size < 1:
        raise ValueError(f"chunk size must be positive, got {size}")
    for start in range(0, len(items), size):
        yield list(items[start : start + size])


def build_match_filter(
    match_keys: Sequence[str], keys: Sequence[KeyTuple]
) -> list[Any] | None:
    """Build a filter selecting any vertex whose *match_keys* equal one of *keys*.

    Uses the list form consumed by :class:`~graflo.filter.onto.FilterExpression`
    so it renders on every backend flavor (AQL, Cypher, nGQL, GSQL, SQL):
    ``["OR", [["AND", [["==", value, field], ...]], ...]]``.

    A single key with a single field collapses to a bare leaf, and a single key
    collapses to a bare ``AND`` — smaller predicates for the common unary case.
    """
    if not keys or not match_keys:
        return None

    def _conjunction(key: KeyTuple) -> list[Any]:
        leaves = [
            ["==", value, field] for field, value in zip(match_keys, key, strict=True)
        ]
        if len(leaves) == 1:
            return leaves[0]
        return ["AND", leaves]

    conjunctions = [_conjunction(key) for key in keys]
    if len(conjunctions) == 1:
        return conjunctions[0]
    return ["OR", conjunctions]


def bucket_by_key(
    docs: list[dict[str, Any]], match_keys: Sequence[str]
) -> dict[KeyTuple, list[dict[str, Any]]]:
    """Group *docs* by their *match_keys* tuple, preserving multiplicity."""
    buckets: dict[KeyTuple, list[dict[str, Any]]] = {}
    for doc in docs:
        key = key_tuple(doc, match_keys)
        if key is None:
            continue
        buckets.setdefault(key, []).append(doc)
    return buckets


def index_matches_by_doc(
    key_docs: list[dict[str, Any]],
    match_keys: Sequence[str],
    buckets: dict[KeyTuple, list[dict[str, Any]]],
) -> dict[int, list[dict[str, Any]]]:
    """Map each input document's position to the vertices it matched.

    Positions with an unresolvable key, or with no match, are absent from the
    result — callers distinguish the two by recomputing :func:`key_tuple`.
    """
    result: dict[int, list[dict[str, Any]]] = {}
    for position, doc in enumerate(key_docs):
        key = key_tuple(doc, match_keys)
        if key is None:
            continue
        matches = buckets.get(key)
        if matches:
            result[position] = matches
    return result
