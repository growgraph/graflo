"""Backend support checks for schema field types (native or raise — no soft conversion)."""

from __future__ import annotations

from collections.abc import Iterable

from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.vertex import (
    Field,
    FieldType,
    format_field_type_label,
    is_list_field_type,
)
from graflo.onto import DBType


class UnsupportedFieldTypeError(ValueError):
    """Raised when a field type cannot be stored natively on the target backend."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


# Homogeneous LIST of scalars is storable as a property / column.
_LIST_NATIVE_DBS: frozenset[DBType] = frozenset(
    {
        DBType.TIGERGRAPH,
        DBType.NEO4J,
        DBType.MEMGRAPH,
        DBType.FALKORDB,
        DBType.ARANGO,
        DBType.POSTGRES,
        DBType.GRAFLO_BACKEND,
    }
)


def assert_field_type_supported(db_type: DBType, field: Field) -> None:
    """Raise if ``field`` cannot be stored natively on ``db_type``.

    Soft conversions (e.g. LIST → STRING/JSON) are intentionally not performed.
    """
    if not is_list_field_type(field.type):
        return
    if db_type in _LIST_NATIVE_DBS:
        return
    label = format_field_type_label(field)
    raise UnsupportedFieldTypeError(
        f"Field '{field.name}' has type {label}, which cannot be stored as a "
        f"property on backend '{db_type.value}'. "
        "Use a backend that supports list properties, or declare an explicit "
        "STRING field if JSON encoding is intentional."
    )


def iter_schema_fields(schema: Schema) -> Iterable[Field]:
    """Yield all typed property fields from vertices and edges in ``schema``."""
    for vertex in schema.core_schema.vertex_config.vertices:
        yield from vertex.properties
    for edge in schema.core_schema.edge_config.values():
        if edge.properties:
            yield from edge.properties


def assert_schema_field_types_supported(db_type: DBType, schema: Schema) -> None:
    """Validate every schema field against backend type support."""
    for field in iter_schema_fields(schema):
        assert_field_type_supported(db_type, field)


def tigergraph_type_for_field(field: Field) -> str:
    """Return a TigerGraph attribute type string (e.g. ``LIST<STRING>``, ``INT``).

    Logical ``UUID`` is stored as ``STRING`` (TigerGraph has no native UUID type).
    """
    assert_field_type_supported(DBType.TIGERGRAPH, field)
    if field.type is None:
        return FieldType.STRING.value
    if is_list_field_type(field.type):
        item = field.item_type
        item_val = item.value if isinstance(item, FieldType) else str(item).upper()
        if item_val == FieldType.UUID.value:
            item_val = FieldType.STRING.value
        return f"LIST<{item_val}>"
    if isinstance(field.type, FieldType):
        if field.type == FieldType.UUID:
            return FieldType.STRING.value
        return field.type.value
    type_upper = str(field.type).upper()
    if type_upper == FieldType.UUID.value:
        return FieldType.STRING.value
    return type_upper
