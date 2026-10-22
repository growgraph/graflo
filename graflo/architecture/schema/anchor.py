"""What a traversal may be anchored on: a complete key, never a property match.

An anchor names **one** vertex. A key contract
(:func:`~graflo.architecture.schema.key_contract.key_contract`) says which field
sets do that, so an anchor is one of:

| Form | Given as | Resolved by |
|---|---|---|
| ``bare`` | a scalar, for a type whose primary identity is one field | nothing: it is that field's value |
| ``identity`` | a mapping naming exactly the primary identity fields | nothing |
| ``key_set`` | a mapping naming exactly one source field set of a derived key | the digest |
| ``alternate`` | a mapping naming exactly one secondary identity | a lookup, which may find one, several or none |

Anything else is a lookup by description, not an anchor: a mapping on a field
that is no key could name many vertices, and walking from an arbitrary one of
them answers a question nobody asked. Such a mapping is refused, as is one that
names only part of a key set.

:func:`classify_anchor` is pure; :func:`graflo.hq.anchor.anchor_identity` runs
the lookup an ``alternate`` anchor needs.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal

from graflo.architecture.schema.key_contract import KeyContract, KeySet

AnchorForm = Literal["bare", "identity", "key_set", "alternate"]


class AnchorKeyError(ValueError):
    """An anchor that does not name exactly one vertex.

    ``code`` classifies the refusal without parsing its message; ``seed`` is the
    position of the traversal seed that carried it, when there was one.
    """

    code: str = "anchor_key"

    def __init__(self, message: str, *, vertex_type: str) -> None:
        super().__init__(message)
        self.vertex_type = vertex_type
        self.seed: int | None = None


class AnchorNotAKeyError(AnchorKeyError):
    """The anchor mapping names fields that form no key of the type."""

    code = "anchor_not_a_key"


class AnchorIncompleteError(AnchorKeyError):
    """The anchor names part of a key, or a key with an empty value."""

    code = "anchor_incomplete"

    def __init__(
        self, message: str, *, vertex_type: str, missing: tuple[str, ...]
    ) -> None:
        super().__init__(message, vertex_type=vertex_type)
        self.missing = missing


class AnchorAmbiguousError(AnchorKeyError):
    """A secondary identity anchor names several vertices."""

    code = "anchor_ambiguous"

    def __init__(self, message: str, *, vertex_type: str, matches: int) -> None:
        super().__init__(message, vertex_type=vertex_type)
        self.matches = matches


class AnchorUnmatchedError(AnchorKeyError):
    """A secondary identity anchor names no vertex the reader can see."""

    code = "anchor_unmatched"


def _empty(value: Any) -> bool:
    return value is None or value == ""


def _scalar(value: Any) -> bool:
    return isinstance(value, str | int | float | bool)


def _keys_text(contract: KeyContract) -> str:
    sets = [("identity", contract.identity_fields)]
    if contract.source == "derived":
        sets += [(k.name, k.fields) for k in contract.keys]
    sets += [(a.name, a.fields) for a in contract.alternates]
    return "; ".join(f"{name} {list(fields)}" for name, fields in sets)


def check_anchor_shape(vertex_type: str, key: Any) -> None:
    """Refuse an anchor that is neither a scalar nor a mapping of scalars.

    Raises:
        AnchorNotAKeyError: a container value, or a mapping key that is not text.
        AnchorIncompleteError: an empty mapping, or an empty value.
    """
    if isinstance(key, Mapping):
        if not key:
            raise AnchorIncompleteError(
                f"empty anchor for {vertex_type!r}",
                vertex_type=vertex_type,
                missing=(),
            )
        for field, value in key.items():
            if not isinstance(field, str) or not (value is None or _scalar(value)):
                raise AnchorNotAKeyError(
                    f"anchor for {vertex_type!r} maps field names to scalar values",
                    vertex_type=vertex_type,
                )
        values = list(key.values())
    elif _scalar(key):
        values = [key]
    else:
        raise AnchorNotAKeyError(
            f"anchor for {vertex_type!r} is a value or a mapping of field values",
            vertex_type=vertex_type,
        )
    empty = [v for v in values if _empty(v)]
    if empty:
        raise AnchorIncompleteError(
            f"anchor for {vertex_type!r} has an empty value",
            vertex_type=vertex_type,
            missing=tuple(
                f
                for f, v in (key.items() if isinstance(key, Mapping) else ())
                if _empty(v)
            ),
        )


def classify_anchor(
    contract: KeyContract, key: Any
) -> tuple[AnchorForm, KeySet | None]:
    """Which form *key* takes under *contract*, and the key set it names.

    Args:
        contract: Key contract of the anchor's type.
        key: A scalar or a mapping of field names to scalars.

    Returns:
        The form, and for ``key_set`` and ``alternate`` the set it names.

    Raises:
        AnchorNotAKeyError: the mapping names fields that form no key.
        AnchorIncompleteError: a scalar for a composite primary identity, part
            of a key set, or an empty value.
    """
    vertex_type = contract.vertex
    check_anchor_shape(vertex_type, key)
    identity = tuple(contract.identity_fields)
    if not isinstance(key, Mapping):
        if len(identity) == 1:
            return "bare", None
        raise AnchorIncompleteError(
            f"a bare value cannot anchor {vertex_type!r}: its identity is "
            f"{list(identity)}; give every field as a mapping",
            vertex_type=vertex_type,
            missing=identity,
        )
    named = frozenset(key)
    if named == frozenset(identity):
        return "identity", None
    derived = contract.keys if contract.source == "derived" else ()
    for key_set in derived:
        if named == frozenset(key_set.fields):
            return "key_set", key_set
    for alternate in contract.alternates:
        if named == frozenset(alternate.fields):
            return "alternate", alternate
    candidates = [identity, *(k.fields for k in derived)]
    candidates += [a.fields for a in contract.alternates]
    partial = [
        tuple(f for f in fields if f not in named)
        for fields in candidates
        if named < frozenset(fields)
    ]
    if partial:
        raise AnchorIncompleteError(
            f"anchor {sorted(named)} names part of a key of {vertex_type!r}; keys: "
            f"{_keys_text(contract)}",
            vertex_type=vertex_type,
            missing=min(partial, key=len),
        )
    raise AnchorNotAKeyError(
        f"cannot anchor {vertex_type!r} on {sorted(named)}: not a key; keys: "
        f"{_keys_text(contract)}. A match on other fields is a lookup, not an anchor",
        vertex_type=vertex_type,
    )
