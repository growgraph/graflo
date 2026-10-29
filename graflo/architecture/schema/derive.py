"""Normal forms, parts and keys of field values, and the fields derived from them.

A field's ``match`` declares how its values split into parts and which
normalizer they go through (:class:`~graflo.architecture.schema.vertex.FieldMatch`);
a ``derived`` field stores one form of another field's value
(:class:`~graflo.architecture.schema.vertex.FieldDerived`). Everything here is
pure and reads no database.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from graflo.architecture.schema.vertex import Field, Vertex
from graflo.util.transform import NORMALIZERS


def _empty(value: Any) -> bool:
    return value is None or value == ""


def normal_value(field: Field, value: Any) -> Any:
    """*value* through the field's ``match.normalizer``, or unchanged without one.

    Returns ``None`` for a missing value or one that normalizes to nothing.
    """
    if _empty(value):
        return None
    match = field.match
    if match is None or match.normalizer is None:
        return value
    return NORMALIZERS[match.normalizer](value)


def value_parts(field: Field, value: Any) -> list[str] | None:
    """The normalized parts of *value*, as the field's ``match.parts`` splits it.

    Separators split the raw value and each part is then normalized; ``words``
    splits the normalized value on whitespace. Empty parts are dropped.
    ``None`` when the field declares no parts or *value* has none.
    """
    match = field.match
    if match is None or match.parts is None or _empty(value):
        return None
    split = match.parts.split
    if split == "words":
        normal = normal_value(field, value)
        parts = str(normal).split() if normal is not None else []
    else:
        pieces = [str(value)]
        for separator in split:
            pieces = [p for piece in pieces for p in piece.split(separator)]
        parts = []
        for piece in pieces:
            normal = normal_value(field, piece.strip())
            if normal is not None:
                parts.append(str(normal))
    return parts or None


def value_keys(field: Field, value: Any) -> list[str]:
    """Every less specific form of *value*, joined by spaces, general first (``keys_v1``).

    The runs of its parts that keep the general end: leading runs for
    ``general_first``, trailing runs for ``specific_first``. "Berlin, pump hall
    2" split on ``,`` gives ``berlin``, ``berlin pump hall 2``. ``unordered``
    parts give no keys: every part would be one, and a common part reaches every
    value that contains it.
    """
    parts = value_parts(field, value)
    match = field.match
    if not parts or match is None or match.parts is None:
        return []
    order = match.parts.order
    if order == "general_first":
        runs = [parts[:size] for size in range(1, len(parts) + 1)]
    elif order == "specific_first":
        runs = [parts[start:] for start in range(len(parts) - 1, -1, -1)]
    else:
        return []
    return list(dict.fromkeys(" ".join(run) for run in runs))


def derived_value(vertex: Vertex, field: Field, doc: Mapping[str, Any]) -> Any:
    """The value of derived *field* for *doc*, from its source's value."""
    derived = field.derived
    if derived is None:
        raise ValueError(f"{vertex.name}.{field.name} is not derived")
    source = next(f for f in vertex.properties if f.name == derived.source)
    value = doc.get(derived.source)
    if derived.form == "normal":
        return normal_value(source, value)
    return value_keys(source, value) or None


def derive_fields(vertex: Vertex, doc: dict[str, Any]) -> dict[str, Any]:
    """Set every derived field of *vertex* on *doc* from its source, in place.

    Idempotent. A stated value is always overwritten, and a derived field whose
    source is missing is removed: core computes these, a document never supplies
    them.

    Returns:
        *doc*.
    """
    for field in vertex.derived_fields:
        value = derived_value(vertex, field, doc)
        if value is None:
            doc.pop(field.name, None)
        else:
            doc[field.name] = value
    return doc


def complete_derived(vertex: Vertex, values: Mapping[str, Any]) -> dict[str, Any]:
    """*values* with each derived field its source states set from that source.

    For reads: a derived value stated without its source is kept, so a stored
    normal form can address a vertex. Writes use :func:`derive_fields`.
    """
    out = dict(values)
    for field in vertex.derived_fields:
        derived = field.derived
        if derived is None or _empty(values.get(derived.source)):
            continue
        value = derived_value(vertex, field, values)
        if value is None:
            out.pop(field.name, None)
        else:
            out[field.name] = value
    return out


def derived_sources(vertex: Vertex, names: list[str]) -> list[str]:
    """*names* with each derived field replaced by its source, order kept."""
    by_name = {f.name: f for f in vertex.properties}
    out: list[str] = []
    for name in names:
        field = by_name.get(name)
        out.append(
            field.derived.source if field is not None and field.derived else name
        )
    return list(dict.fromkeys(out))
