"""A vertex type's key contract, in terms any schema language can state.

A producer that only knows some properties of an entity -- an extractor reading
text, a partial record -- needs to know whether what it holds identifies the
entity. :func:`key_contract` states that for a :class:`Vertex`:

| ``source`` | graflo declaration | Primary key comes from |
|---|---|---|
| ``stated`` | ``identity`` | the identity fields themselves |
| ``derived`` | ``hash_identity_properties``, ``identity_funnel`` | a digest of source fields |
| ``minted`` | ``assigned``, ``blank`` | the writer; never from the data |

``keys`` are the field sets that make a complete primary key (one, or one per
funnel branch); ``alternates`` are the secondary identities, which find a
vertex but never create one. :func:`key_status` says which a set of values
completes.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal

from pydantic import Field

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.schema.identity_digest import compute_vertex_identity
from graflo.architecture.schema.vertex import Vertex

KeySource = Literal["stated", "derived", "minted"]
KeyState = Literal["primary", "alternate", "partial", "none", "keyless"]


class KeySet(ConfigBaseModel):
    """Fields that identify a vertex when all of them hold a value."""

    name: str = Field(
        description="Funnel branch id, secondary identity name, or ``identity``."
    )
    fields: tuple[str, ...] = Field(description="Fields that must all be non-empty.")


class KeyContract(ConfigBaseModel):
    """How a vertex type is identified."""

    vertex: str
    source: KeySource
    identity_fields: tuple[str, ...] = Field(
        description="Stored primary identity fields (``id`` for a derived key)."
    )
    keys: tuple[KeySet, ...] = Field(
        default=(),
        description="Field sets that complete the primary key; empty when minted.",
    )
    alternates: tuple[KeySet, ...] = Field(
        default=(), description="Secondary identities: lookup keys, never upserted on."
    )

    @property
    def key_fields(self) -> frozenset[str]:
        """Every field that takes part in a primary or alternate key."""
        return frozenset(
            f for key in (*self.keys, *self.alternates) for f in key.fields
        )


class KeyStatus(ConfigBaseModel):
    """What a set of values completes under a :class:`KeyContract`."""

    state: KeyState = Field(
        description=(
            "``primary``: a key set is complete. ``alternate``: only a secondary "
            "identity is. ``partial``: some key fields hold values, no set is "
            "complete. ``none``: no key field holds a value. ``keyless``: the "
            "type takes no key from the data."
        )
    )
    key: KeySet | None = Field(
        default=None, description="The complete primary key set."
    )
    alternates: tuple[KeySet, ...] = Field(
        default=(), description="Complete secondary identities, in declaration order."
    )
    missing: tuple[str, ...] = Field(
        default=(), description="Fields still needed for the nearest primary key set."
    )


def _empty(value: Any) -> bool:
    return value is None or value == ""


def _complete(key: KeySet, values: Mapping[str, Any]) -> bool:
    return all(not _empty(values.get(f)) for f in key.fields)


def key_contract(vertex: Vertex) -> KeyContract:
    """The key contract of *vertex*."""
    alternates = tuple(
        KeySet(name=s.name or f"secondary_{i}", fields=tuple(s.fields))
        for i, s in enumerate(vertex.secondary_identities)
    )
    mode = vertex.identity_mode
    identity = tuple(vertex.identity) if vertex.identity else ("id",)
    if mode in ("assigned", "blank"):
        return KeyContract(
            vertex=vertex.name,
            source="minted",
            identity_fields=identity,
            alternates=alternates,
        )
    if mode == "hash":
        if vertex.identity_funnel is not None:
            keys = tuple(
                KeySet(name=b.id, fields=tuple(b.required_fields))
                for b in vertex.identity_funnel.branches
            )
        else:
            keys = (
                KeySet(name="identity", fields=tuple(vertex.hash_identity_properties)),
            )
        return KeyContract(
            vertex=vertex.name,
            source="derived",
            identity_fields=identity,
            keys=keys,
            alternates=alternates,
        )
    return KeyContract(
        vertex=vertex.name,
        source="stated",
        identity_fields=identity,
        keys=(KeySet(name="identity", fields=identity),),
        alternates=alternates,
    )


def key_status(contract: KeyContract, values: Mapping[str, Any]) -> KeyStatus:
    """Which key of *contract* the non-empty *values* complete."""
    alternates = tuple(a for a in contract.alternates if _complete(a, values))
    if contract.source == "minted":
        return KeyStatus(state="keyless", alternates=alternates)
    for key in contract.keys:
        if _complete(key, values):
            return KeyStatus(state="primary", key=key, alternates=alternates)
    nearest = min(
        contract.keys,
        key=lambda k: sum(_empty(values.get(f)) for f in k.fields),
    )
    missing = tuple(f for f in nearest.fields if _empty(values.get(f)))
    if alternates:
        return KeyStatus(state="alternate", alternates=alternates, missing=missing)
    stated = any(not _empty(values.get(f)) for f in contract.key_fields)
    return KeyStatus(state="partial" if stated else "none", missing=missing)


def primary_identity(
    vertex: Vertex, contract: KeyContract, values: Mapping[str, Any]
) -> dict[str, Any] | None:
    """The primary identity *values* complete, or ``None``.

    A derived key is digested only once a key set is complete: graflo's flat
    digest accepts some source fields missing, which would give a different key
    for the same entity.
    """
    status = key_status(contract, values)
    if status.state != "primary":
        return None
    if contract.source == "derived":
        digest = compute_vertex_identity(values, vertex)
        return None if digest is None else {contract.identity_fields[0]: digest}
    return {f: values[f] for f in contract.identity_fields}
