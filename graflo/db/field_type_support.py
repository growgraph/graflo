"""Backend support checks for schema field types and indexes (native or raise — no soft conversion)."""

from __future__ import annotations

from collections.abc import Iterable

from graflo.architecture.graph_types import IndexType
from graflo.architecture.graph_types.index_config import Index
from graflo.architecture.schema.db_aware import edge_identity_index
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


class UnsupportedIndexError(ValueError):
    """Raised when a declared index cannot be built as declared on the target backend."""


#: Index shapes each backend cannot build: an index over several fields, a
#: uniqueness constraint, or (``"edge"``) any edge index. A declared index that
#: needs one is refused; one GraFlo derives (identity, secondary identity, edge
#: identity) is skipped by the backend instead, since no lookup depends on it.
_INDEX_LIMITS: dict[DBType, frozenset[str]] = {
    DBType.MEMGRAPH: frozenset({"composite edge", "unique edge"}),
    DBType.NEBULA: frozenset({"unique vertex", "unique edge"}),
    DBType.TIGERGRAPH: frozenset({"composite vertex", "unique vertex", "edge"}),
}


def index_limit(db_type: DBType, index: Index, *, on_edge: bool) -> str | None:
    """The index shape *db_type* cannot build that *index* needs, or ``None``."""
    limits = _INDEX_LIMITS.get(db_type, frozenset())
    if on_edge and "edge" in limits:
        return "edge"
    owner = "edge" if on_edge else "vertex"
    if len(index.fields) > 1 and f"composite {owner}" in limits:
        return f"composite {owner}"
    if index.unique and f"unique {owner}" in limits:
        return f"unique {owner}"
    return None


def _assert_index_buildable(
    db_type: DBType, index: Index, owner: str, *, on_edge: bool
) -> None:
    limit = index_limit(db_type, index, on_edge=on_edge)
    if limit is None:
        return
    flavor = getattr(db_type, "value", db_type)
    article = "An" if limit == "edge" else "A"
    raise UnsupportedIndexError(
        f"{article} {limit} index on {index.fields} of '{owner}' cannot be built on "
        f"backend '{flavor}'. Remove it from the database profile."
    )


def assert_schema_indexes_supported(db_type: DBType, schema: Schema) -> None:
    """Validate every declared vertex and edge index against backend support.

    An index over exactly the fields of an identity or a secondary identity is
    GraFlo's own, so only its kind is checked: the backend skips it if it
    cannot build it.
    """
    profile = schema.db_profile
    vertex_config = schema.core_schema.vertex_config
    for vertex in vertex_config.vertices:
        derived = {
            tuple(vertex_config.identity_fields(vertex.name)),
            *(tuple(entry.fields) for entry in vertex.secondary_identities),
        }
        for index in profile.vertex_secondary_indexes(vertex.name):
            assert_index_kind_supported(db_type, index, vertex.name)
            if tuple(index.fields) not in derived:
                _assert_index_buildable(db_type, index, vertex.name, on_edge=False)
    for edge in schema.core_schema.edge_config.values():
        derived = {
            tuple(index.fields)
            for key in edge.identities
            if (index := edge_identity_index(key, db_type)) is not None
        }
        for index in profile.edge_secondary_indexes(edge.edge_id):
            assert_index_kind_supported(db_type, index, str(edge.edge_id))
            if tuple(index.fields) not in derived:
                _assert_index_buildable(db_type, index, str(edge.edge_id), on_edge=True)


def assert_schema_supported(db_type: DBType, schema: Schema) -> None:
    """Refuse a schema whose field types or indexes *db_type* cannot build."""
    assert_schema_field_types_supported(db_type, schema)
    assert_schema_indexes_supported(db_type, schema)
