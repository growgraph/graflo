"""Backend support checks for schema field types and index kinds (native or raise — no soft conversion)."""

from __future__ import annotations

from collections.abc import Iterable

from graflo.architecture.graph_types import IndexType
from graflo.architecture.graph_types.index_config import Index
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
    # ``db_flavor`` reaches this function as a bare string from validated config
    # models, so the enum's ``.value`` is not always there to read.
    flavor = getattr(db_type, "value", db_type)
    raise UnsupportedFieldTypeError(
        f"Field '{field.name}' has type {label}, which cannot be stored as a "
        f"property on backend '{flavor}'. "
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


class UnsupportedIndexKindError(ValueError):
    """Raised when a declared index kind cannot be built on the target backend."""


#: Index kinds each backend's DDL builds, beyond a plain index. A plain index
#: (``persistent``, and its ArangoDB-era aliases ``hash`` and ``skiplist``) is
#: built everywhere an index is built; the file backend builds none, and has no
#: queries for one to serve.
_INDEX_KINDS_BUILT: dict[DBType, frozenset[IndexType]] = {
    DBType.ARANGO: frozenset({IndexType.FULLTEXT}),
}

_PLAIN_INDEX_KINDS = frozenset(
    {IndexType.PERSISTENT, IndexType.HASH, IndexType.SKIPLIST}
)


def assert_index_kind_supported(db_type: DBType, index: Index, owner: str) -> None:
    """Raise if *db_type* cannot build *index* as declared on *owner*.

    A kind the target cannot build would otherwise become a plain index, so a
    full-text search declared in the schema would silently match nothing.
    """
    if db_type == DBType.GRAFLO_BACKEND or index.type in _PLAIN_INDEX_KINDS:
        return
    if index.type in _INDEX_KINDS_BUILT.get(db_type, frozenset()):
        return
    flavor = getattr(db_type, "value", db_type)
    raise UnsupportedIndexKindError(
        f"A {getattr(index.type, 'value', index.type)} index on {index.fields} of '{owner}' cannot be "
        f"built on backend '{flavor}'. Declare a plain index, or remove it."
    )


def assert_schema_index_kinds_supported(db_type: DBType, schema: Schema) -> None:
    """Validate every declared vertex and edge index against backend support."""
    profile = schema.db_profile
    for vertex in schema.core_schema.vertex_config.vertices:
        for index in profile.vertex_secondary_indexes(vertex.name):
            assert_index_kind_supported(db_type, index, vertex.name)
    for edge in schema.core_schema.edge_config.values():
        for index in profile.edge_secondary_indexes(edge.edge_id):
            assert_index_kind_supported(db_type, index, str(edge.edge_id))


def assert_schema_supported(db_type: DBType, schema: Schema) -> None:
    """Refuse a schema whose field types or index kinds *db_type* cannot store."""
    assert_schema_field_types_supported(db_type, schema)
    assert_schema_index_kinds_supported(db_type, schema)
