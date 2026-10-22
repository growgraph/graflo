"""Resolve a traversal anchor to the identity it names (:mod:`~graflo.architecture.schema.anchor`).

A bare value and a primary identity are returned as given, with no read: a
vertex that does not exist simply has no neighbours. A derived key set is
digested. A secondary identity is looked up through :func:`~graflo.hq.identify.identify`,
under the read guard, and must find exactly one vertex.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from graflo.architecture.schema.anchor import (
    AnchorAmbiguousError,
    AnchorIncompleteError,
    AnchorUnmatchedError,
    classify_anchor,
)
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.key_contract import key_contract
from graflo.filter.onto import FilterExpression
from graflo.hq.identify import identify


def anchor_identity(
    schema: Schema,
    vertex_type: str,
    key: Any,
    *,
    db: Any,
    guard: FilterExpression | None = None,
) -> Any:
    """The identity *key* names: a bare value, or a primary identity mapping.

    Args:
        schema: Logical schema of the graph.
        vertex_type: Logical type of the anchor.
        key: A bare value or a mapping of logical field names to values.
        db: The open connection a secondary identity is looked up through.
        guard: Trusted read guard for that lookup.

    Raises:
        ValueError: *vertex_type* is not in the schema.
        AnchorNotAKeyError: the mapping names no key.
        AnchorIncompleteError: part of a key, or an empty value.
        AnchorAmbiguousError: a secondary identity found several vertices.
        AnchorUnmatchedError: a secondary identity found none.
    """
    vertex_config = schema.core_schema.vertex_config
    if vertex_type not in vertex_config.vertex_set:
        raise ValueError(
            f"Unknown vertex type {vertex_type!r}; declared: "
            f"{sorted(vertex_config.vertex_set)}"
        )
    vertex = vertex_config[vertex_type]
    contract = key_contract(vertex)
    form, key_set = classify_anchor(contract, key)
    if form == "bare":
        return key
    assert isinstance(key, Mapping)
    if form == "identity":
        return {f: key[f] for f in contract.identity_fields}
    if form == "key_set":
        found = identify(schema, vertex_type, key)
    else:
        found = identify(schema, vertex_type, key, db=db, guard=guard)
    name = key_set.name if key_set is not None else ""
    if found.outcome == "identified" and found.identity is not None:
        return found.identity
    if found.outcome == "ambiguous":
        raise AnchorAmbiguousError(
            f"{name} {dict(key)} names {len(found.identities)} {vertex_type!r} "
            "vertices; anchor on the primary identity",
            vertex_type=vertex_type,
            matches=len(found.identities),
        )
    if found.outcome == "unmatched":
        raise AnchorUnmatchedError(
            f"{name} {dict(key)} names no {vertex_type!r} vertex",
            vertex_type=vertex_type,
        )
    raise AnchorIncompleteError(
        f"{dict(key)} does not complete a key of {vertex_type!r}",
        vertex_type=vertex_type,
        missing=found.missing,
    )
