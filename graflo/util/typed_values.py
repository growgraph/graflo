"""Typed identity keys and lossless values, shared by the bulk fold and the writers.

An identity value keys as ``[kind, value]`` (:func:`key_member`), so ``1`` and
``1.0`` are one key and ``1`` and ``"1"`` two. Values JSON cannot hold (dates,
times, UUIDs, decimals) encode as ``{"$graflo": <type>, "v": <text>}`` and
decode back to the same Python value.
"""

from __future__ import annotations

import datetime as dt
import json
import math
from collections.abc import Iterable
from decimal import Decimal
from typing import Any
from uuid import UUID

TAG = "$graflo"


class IdentityKeyError(ValueError):
    """An identity value that cannot key a row: null, empty, NaN or not a scalar."""


def key_member(value: Any) -> list[Any]:
    """One identity value as ``[kind, value]``, compared as the targets compare it.

    Numbers compare by value (``1`` and ``1.0`` are one key), booleans apart
    from numbers, strings apart from numbers (``1`` and ``"1"`` are two keys).
    A UUID keys as its text, the way every backend stores it.

    Raises:
        IdentityKeyError: The value cannot key a row.
    """
    if value is None or value == "":
        raise IdentityKeyError("empty identity value")
    if isinstance(value, bool):
        return ["b", value]
    if isinstance(value, int):
        return ["n", value]
    if isinstance(value, float | Decimal):
        if isinstance(value, float) and not math.isfinite(value):
            raise IdentityKeyError(f"identity value {value!r} is not finite")
        if value == int(value):
            return ["n", int(value)]
        return ["n", float(value)]
    if isinstance(value, str):
        return ["s", value]
    if isinstance(value, UUID):
        return ["s", str(value)]
    if isinstance(value, dt.datetime):
        return ["t", value.isoformat()]
    if isinstance(value, dt.date):
        return ["d", value.isoformat()]
    if isinstance(value, dt.time):
        return ["h", value.isoformat()]
    raise IdentityKeyError(
        f"identity value of type {type(value).__name__} is not a scalar"
    )


def typed_key(values: Iterable[Any]) -> str:
    """The canonical text of a tuple of identity values; equal keys, equal text.

    A composite identity may lack members, as an upsert matches them: an
    absent one keys as null, an empty string as itself.

    Raises:
        IdentityKeyError: No member has a value, or one cannot key a row.
    """
    values = list(values)
    if all(v is None or v == "" for v in values):
        raise IdentityKeyError("no identity value")
    members = [
        None if v is None else ["s", ""] if v == "" else key_member(v) for v in values
    ]
    return json.dumps(members, separators=(",", ":"), ensure_ascii=False)


def encode(value: Any) -> Any:
    """*value* as JSON-ready data, non-JSON scalars tagged.

    Raises:
        TypeError: A value graflo does not write.
    """
    if value is None or isinstance(value, bool | int | str):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else {TAG: "float", "v": repr(value)}
    if isinstance(value, dict):
        return {str(k): encode(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [encode(v) for v in value]
    if isinstance(value, dt.datetime):
        return {TAG: "datetime", "v": value.isoformat()}
    if isinstance(value, dt.date):
        return {TAG: "date", "v": value.isoformat()}
    if isinstance(value, dt.time):
        return {TAG: "time", "v": value.isoformat()}
    if isinstance(value, UUID):
        return {TAG: "uuid", "v": str(value)}
    if isinstance(value, Decimal):
        return {TAG: "decimal", "v": str(value)}
    raise TypeError(f"cannot stage a value of type {type(value).__name__}")


_DECODERS = {
    "float": float,
    "datetime": dt.datetime.fromisoformat,
    "date": dt.date.fromisoformat,
    "time": dt.time.fromisoformat,
    "uuid": UUID,
    "decimal": Decimal,
}


def decode(value: Any) -> Any:
    """The inverse of :func:`encode`."""
    if isinstance(value, dict):
        if len(value) == 2 and TAG in value and "v" in value:
            return _DECODERS[value[TAG]](value["v"])
        return {k: decode(v) for k, v in value.items()}
    if isinstance(value, list):
        return [decode(v) for v in value]
    return value


def identity_member(value: Any) -> Any:
    """An edge identity field's value as part of the edge key; absent is null.

    A value that cannot key a vertex keys an edge by its encoded form.
    """
    if value is None:
        return None
    try:
        return key_member(value)
    except IdentityKeyError:
        return encode(value)
