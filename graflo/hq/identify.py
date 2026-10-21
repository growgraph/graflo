"""Which entity a set of property values identifies, by the key contract alone.

:func:`identify` is the executable form of a vertex type's key contract
(:func:`~graflo.architecture.schema.key_contract.key_contract`). Given the values
a producer holds for one entity, it answers:

1. A complete primary key **identifies** the entity, without a lookup. A derived
   key is digested only once every source field of a key set holds a value:
   graflo's flat digest accepts missing fields and would give the same entity
   a different key.
2. Otherwise a complete secondary identity is looked up in the graph, in
   declaration order: exactly one entity **identifies** it, several make it
   **ambiguous**, none leaves it **unmatched**.
3. Otherwise the values are **incomplete**, with the fields still missing.

It is stateless: it never writes, never mints a key, and knows nothing of
batches or of who owns a type. :func:`identify_many` answers the same for many
value sets of one type with one lookup per secondary identity.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Literal

from pydantic import Field

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.key_contract import (
    KeyContract,
    KeyState,
    key_contract,
    key_status,
    primary_identity,
)
from graflo.architecture.schema.vertex import Vertex
from graflo.filter.onto import FilterExpression

if TYPE_CHECKING:
    from graflo.hq.lookup import _Reader

IdentifyOutcome = Literal["identified", "ambiguous", "unmatched", "incomplete"]


class Identity(ConfigBaseModel):
    """What a set of values identifies under a vertex type's key contract."""

    vertex_type: str
    outcome: IdentifyOutcome = Field(
        description=(
            "``identified``: one entity, in ``identity``. ``ambiguous``: a "
            "secondary identity found several, in ``identities``. ``unmatched``: "
            "the secondary identities found nothing. ``incomplete``: no primary "
            "key is complete and nothing could be looked up; ``missing`` says what "
            "is needed."
        )
    )
    identity: dict[str, Any] | None = Field(
        default=None, description="Identity fields and values of the entity."
    )
    identities: list[dict[str, Any]] = Field(
        default_factory=list,
        description="``ambiguous``: the entities the secondary identity found.",
    )
    by: Literal["key", "alternate"] | None = Field(
        default=None,
        description=(
            "``key``: a complete primary key. ``alternate``: a secondary identity "
            "found exactly one entity."
        ),
    )
    alternate: str | None = Field(
        default=None,
        description="Secondary identity that identified it, or found several.",
    )
    found: bool | None = Field(
        default=None,
        description=(
            "Whether the graph holds the entity, as far as the guard shows; "
            "``None`` when nothing was read."
        ),
    )
    state: KeyState = Field(description="What the values complete (``key_status``).")
    alternates: tuple[str, ...] = Field(
        default=(), description="Names of the secondary identities the values complete."
    )
    missing: tuple[str, ...] = Field(
        default=(), description="Fields still needed for the nearest primary key."
    )


def _empty(value: Any) -> bool:
    return value is None or value == ""


def _frozen(identity: dict[str, Any]) -> tuple[tuple[str, Any], ...]:
    return tuple(sorted(identity.items()))


def _vertex(schema: Schema, vertex_type: str) -> Vertex:
    try:
        return schema.core_schema.vertex_config[vertex_type]
    except KeyError as error:
        raise ValueError(f"unknown vertex type {vertex_type!r}") from error


def _covers(guard: FilterExpression, vertex: Vertex) -> bool:
    return guard.fields() <= {f.name for f in vertex.properties}


def _lookup(
    reader: _Reader,
    vertex_type: str,
    fields: list[str],
    docs: list[dict[str, Any]],
    identity_fields: list[str],
) -> dict[int, list[dict[str, Any]]]:
    """Distinct complete identities matching each of *docs*, by position."""
    found = reader.find_by(vertex_type, fields, docs, identity_fields)
    out: dict[int, list[dict[str, Any]]] = {}
    for position, matches in found.items():
        unique: dict[tuple, dict[str, Any]] = {}
        for match in matches:
            identity = {f: match.get(f) for f in identity_fields}
            if all(not _empty(v) for v in identity.values()):
                unique.setdefault(_frozen(identity), identity)
        out[position] = list(unique.values())
    return out


def identify_many(
    schema: Schema,
    vertex_type: str,
    values: Sequence[Mapping[str, Any]],
    *,
    db: Any | None = None,
    guard: FilterExpression | None = None,
) -> list[Identity]:
    """:func:`identify` for each of *values*, all of type *vertex_type*.

    The graph is read once for the primary keys and once per secondary
    identity, not once per value set.

    Args:
        schema: Schema whose key contract decides.
        vertex_type: Vertex type of every value set.
        values: Property values, one mapping per entity.
        db: An open connection, as for :func:`identify`.
        guard: A trusted read guard, as for :func:`identify`.

    Returns:
        One :class:`Identity` per value set, in order.

    Raises:
        ValueError: *vertex_type* is not in the schema.
    """
    vertex = _vertex(schema, vertex_type)
    contract: KeyContract = key_contract(vertex)
    identity_fields = list(contract.identity_fields)
    reader: _Reader | None = None
    if db is not None:
        # Deferred: hq.lookup imports pandas, and an offline resolve reads no graph.
        from graflo.hq.lookup import _Reader

        applied = guard if guard is not None and _covers(guard, vertex) else None
        reader = _Reader(db, schema, guard=applied)

    out: list[Identity] = []
    keyed: list[int] = []
    waiting: list[int] = []
    for position, given in enumerate(values):
        status = key_status(contract, given)
        identity = None
        if status.state == "primary":
            identity = primary_identity(vertex, contract, given)
        out.append(
            Identity(
                vertex_type=vertex_type,
                outcome="identified" if identity is not None else "incomplete",
                identity=identity,
                by="key" if identity is not None else None,
                state=status.state,
                alternates=tuple(a.name for a in status.alternates),
                missing=status.missing,
            )
        )
        if reader is None:
            continue
        if identity is not None:
            keyed.append(position)
        elif status.state != "primary" and status.alternates:
            waiting.append(position)

    if reader is not None and keyed:
        docs = [dict(out[p].identity or {}) for p in keyed]
        present = _lookup(reader, vertex_type, identity_fields, docs, identity_fields)
        for at, position in enumerate(keyed):
            out[position] = out[position].model_copy(
                update={"found": bool(present.get(at))}
            )

    if reader is None or not waiting:
        return out
    widest: dict[int, tuple[str, list[dict[str, Any]]]] = {}
    for alternate in contract.alternates:
        positions = [p for p in waiting if alternate.name in out[p].alternates]
        if not positions:
            continue
        docs = [{f: values[p][f] for f in alternate.fields} for p in positions]
        found = _lookup(
            reader, vertex_type, list(alternate.fields), docs, identity_fields
        )
        for at, position in enumerate(positions):
            matches = found.get(at, [])
            if len(matches) == 1:
                out[position] = out[position].model_copy(
                    update={
                        "outcome": "identified",
                        "identity": matches[0],
                        "by": "alternate",
                        "alternate": alternate.name,
                        "found": True,
                    }
                )
                waiting.remove(position)
            elif len(matches) > len(widest.get(position, ("", []))[1]):
                widest[position] = (alternate.name, matches)
    for position in waiting:
        if position in widest:
            name, matches = widest[position]
            update: dict[str, Any] = {
                "outcome": "ambiguous",
                "identities": matches,
                "alternate": name,
                "found": True,
            }
        else:
            update = {"outcome": "unmatched", "found": False}
        out[position] = out[position].model_copy(update=update)
    return out


def identify(
    schema: Schema,
    vertex_type: str,
    values: Mapping[str, Any],
    *,
    db: Any | None = None,
    guard: FilterExpression | None = None,
) -> Identity:
    """Which entity of *vertex_type* the property *values* identify.

    A complete primary key identifies without a lookup; with *db*, ``found``
    then says whether the graph holds the entity. Otherwise, with *db*, the
    complete secondary identities are looked up in declaration order: the first
    to find exactly one entity identifies it; failing that, several make it
    ``ambiguous`` and none ``unmatched``. Without *db*, values that complete
    only a secondary identity are ``incomplete``.

    Args:
        schema: Schema whose key contract decides.
        vertex_type: Vertex type of the entity.
        values: Property values held for it; ``None`` and ``""`` count as absent.
        db: An open connection to read the graph through.
        guard: A trusted read guard every read passes, so the entity is never
            one the guard hides. It applies only when *vertex_type* declares
            every field it names; other types are read unguarded. Refused by a
            backend without read guards.

    Raises:
        ValueError: *vertex_type* is not in the schema.
    """
    (identity,) = identify_many(schema, vertex_type, [values], db=db, guard=guard)
    return identity
