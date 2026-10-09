"""Coerce a raw value — typically text read off a document — to a field's declared type.

The inverse of :func:`~graflo.architecture.onto_sample.infer_field_type`: that
one reads a type off values, this one makes a value conform to a declared type.
Every refusal is a :class:`CoercionError` whose ``kind`` says what went wrong,
so a caller can group failures and ask for a targeted correction.
"""

from __future__ import annotations

import math
import re
from datetime import date, datetime
from typing import Any, Literal, TypeAlias

from graflo.architecture.onto_sample import ISO_DATETIME_PATTERN
from graflo.architecture.schema.identity_uuid import validate_uuid_value
from graflo.architecture.schema.vertex import Field, FieldType

CoercionKind: TypeAlias = Literal["type", "unit_mismatch", "list_item", "empty"]

_INT_TYPES = frozenset({FieldType.INT, FieldType.UINT})
_FLOAT_TYPES = frozenset({FieldType.FLOAT, FieldType.DOUBLE})
_NUMERIC_TYPES = _INT_TYPES | _FLOAT_TYPES

_INT_TEXT = re.compile(r"^[+-]?\d+$")
_NUMBER = r"[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?"
_FLOAT_TEXT = re.compile(rf"^{_NUMBER}$")
# A number followed by a unit token that starts with a letter or a unit sign,
# so ``1,000`` and ``1_000`` are malformed numbers rather than units.
_NUMBER_WITH_UNIT = re.compile(
    rf"^(?P<number>{_NUMBER})\s*(?P<unit>(?:[^\W\d_]|[%°‰$€£¥])\S*)$"
)
_PARTIAL_DATE = re.compile(r"^\d{4}(?:-(?:0[1-9]|1[0-2]))?$")

_TRUE_TEXT = frozenset({"true", "yes", "1"})
_FALSE_TEXT = frozenset({"false", "no", "0"})


class CoercionError(ValueError):
    """A value that does not conform to its field's declared type.

    Attributes:
        field: Name of the field being coerced.
        value: The raw value as received.
        kind: ``type`` (wrong shape for the type), ``unit_mismatch`` (a unit
            other than the declared one), ``list_item`` (an item of a list
            failed; see ``index``) or ``empty`` (nothing to coerce).
        index: Position of the failing item when ``kind`` is ``list_item``.
    """

    def __init__(
        self,
        message: str,
        *,
        field: str,
        value: Any,
        kind: CoercionKind,
        index: int | None = None,
    ) -> None:
        super().__init__(message)
        self.field = field
        self.value = value
        self.kind = kind
        self.index = index


def coerce_value(value: Any, field: Field) -> Any:
    """Return *value* as the Python value *field* declares.

    - ``INT`` / ``UINT``: an ``int``; integral floats and digit strings are
      accepted, ``bool`` and grouped digits (``1,000``) are not. ``UINT``
      refuses negatives.
    - ``FLOAT`` / ``DOUBLE``: a finite ``float``.
    - ``BOOL``: a ``bool``; ``true/false/yes/no/1/0`` in any case.
    - ``STRING``: a stripped ``str``; numbers are stringified.
    - ``DATETIME``: an ISO 8601 **string**, not a ``datetime``. A space
      separator becomes ``T`` and ``z`` becomes ``Z``. The partial dates
      ``YYYY`` and ``YYYY-MM`` are kept as written, never padded.
    - ``UUID``: a lower-cased UUID string.
    - ``LIST``: a list of ``item_type`` values; a bare scalar becomes a
      one-item list.
    - Untyped: the value unchanged.

    For a numeric field (or a list of numerics) whose ``semantics.unit`` is
    set, a string carrying exactly that unit (``"200 kV"`` for ``kV``) has it
    stripped. Unit matching is case-sensitive; no conversion is attempted.

    Raises:
        CoercionError: when *value* is empty or does not conform.
    """
    if _is_empty(value):
        raise CoercionError(
            f"{field.name}: no value", field=field.name, value=value, kind="empty"
        )
    if field.type is None:
        return value
    unit = field.semantics.unit if field.semantics is not None else None
    ftype = FieldType(field.type)
    if ftype == FieldType.LIST:
        item_type = FieldType(field.item_type or FieldType.STRING)
        items = list(value) if isinstance(value, (list, tuple)) else [value]
        out = []
        for index, item in enumerate(items):
            try:
                out.append(_coerce_scalar(item, item_type, unit, field.name))
            except CoercionError as error:
                raise CoercionError(
                    f"{field.name}[{index}]: {error}",
                    field=field.name,
                    value=value,
                    kind="list_item",
                    index=index,
                ) from error
        return out
    return _coerce_scalar(value, ftype, unit, field.name)


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, tuple)):
        return not value
    return False


def _coerce_scalar(value: Any, ftype: FieldType, unit: str | None, name: str) -> Any:
    if _is_empty(value):
        raise CoercionError(f"{name}: no value", field=name, value=value, kind="empty")
    if isinstance(value, (list, tuple, dict)):
        raise CoercionError(
            f"{name}: expected a scalar {ftype.value}, got {type(value).__name__}",
            field=name,
            value=value,
            kind="type",
        )
    if ftype in _NUMERIC_TYPES and isinstance(value, str):
        value = _strip_unit(value.strip(), unit, name)
    if ftype in _INT_TYPES:
        return _coerce_int(value, ftype, name)
    if ftype in _FLOAT_TYPES:
        return _coerce_float(value, ftype, name)
    if ftype == FieldType.BOOL:
        return _coerce_bool(value, name)
    if ftype == FieldType.STRING:
        return _coerce_string(value, name)
    if ftype == FieldType.DATETIME:
        return _coerce_datetime(value, name)
    if ftype == FieldType.UUID:
        try:
            return validate_uuid_value(str(value).strip(), context=name).lower()
        except ValueError as error:
            raise CoercionError(
                str(error), field=name, value=value, kind="type"
            ) from error
    raise CoercionError(
        f"{name}: unsupported type {ftype.value}", field=name, value=value, kind="type"
    )


def _strip_unit(text: str, unit: str | None, name: str) -> str:
    if _FLOAT_TEXT.match(text):
        return text
    match = _NUMBER_WITH_UNIT.match(text)
    if match is None:
        return text
    found = match.group("unit")
    if unit is not None and found == unit:
        return match.group("number")
    expected = f"declared unit {unit!r}" if unit else "no declared unit"
    raise CoercionError(
        f"{name}: unit {found!r} does not match {expected}",
        field=name,
        value=text,
        kind="unit_mismatch",
    )


def _type_error(name: str, ftype: FieldType, value: Any) -> CoercionError:
    return CoercionError(
        f"{name}: {value!r} is not a valid {ftype.value}",
        field=name,
        value=value,
        kind="type",
    )


def _coerce_int(value: Any, ftype: FieldType, name: str) -> int:
    result: int
    if isinstance(value, bool):
        raise _type_error(name, ftype, value)
    if isinstance(value, int):
        result = value
    elif isinstance(value, float):
        if not (math.isfinite(value) and value.is_integer()):
            raise _type_error(name, ftype, value)
        result = int(value)
    elif isinstance(value, str):
        text = value.strip()
        if _INT_TEXT.match(text):
            result = int(text)
        elif _FLOAT_TEXT.match(text) and float(text).is_integer():
            result = int(float(text))
        else:
            raise _type_error(name, ftype, value)
    else:
        raise _type_error(name, ftype, value)
    if ftype == FieldType.UINT and result < 0:
        raise _type_error(name, ftype, value)
    return result


def _coerce_float(value: Any, ftype: FieldType, name: str) -> float:
    if isinstance(value, bool):
        raise _type_error(name, ftype, value)
    if isinstance(value, (int, float)):
        result = float(value)
    elif isinstance(value, str) and _FLOAT_TEXT.match(value.strip()):
        result = float(value.strip())
    else:
        raise _type_error(name, ftype, value)
    if not math.isfinite(result):
        raise _type_error(name, ftype, value)
    return result


def _coerce_bool(value: Any, name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in _TRUE_TEXT:
            return True
        if text in _FALSE_TEXT:
            return False
    raise _type_error(name, FieldType.BOOL, value)


def _coerce_string(value: Any, name: str) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    raise _type_error(name, FieldType.STRING, value)


def _coerce_datetime(value: Any, name: str) -> str:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if not isinstance(value, str):
        raise _type_error(name, FieldType.DATETIME, value)
    text = value.strip()
    if _PARTIAL_DATE.match(text):
        return text
    if len(text) > 10 and text[10] == " ":
        text = f"{text[:10]}T{text[11:]}"
    if text.endswith("z"):
        text = f"{text[:-1]}Z"
    if not ISO_DATETIME_PATTERN.match(text):
        raise _type_error(name, FieldType.DATETIME, value)
    try:
        datetime.fromisoformat(text)
    except ValueError:
        raise _type_error(name, FieldType.DATETIME, value) from None
    return text
