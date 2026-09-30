"""PostgreSQL graph target write operations (DDL/DML for vertices and edge tables)."""

from __future__ import annotations

import logging
from collections.abc import Collection, Sequence
from typing import Any, Protocol

from psycopg2 import sql
from psycopg2.extras import execute_values

from graflo.architecture.graph_types import EdgeDirection
from graflo.architecture.schema import Schema
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.vertex import (
    Field,
    FieldType,
    VertexConfig,
    field_type_value,
    is_list_field_type,
)
from graflo.db.conn import (
    DEFAULT_DELETE_CHUNK_SIZE,
    NamespaceNotFoundError,
    SchemaExistsError,
    deletable_docs,
    deletable_endpoints,
)
from graflo.db.field_type_support import assert_field_type_supported
from graflo.filter.onto import BoundParams, FilterExpression, parse_filter_expression
from graflo.onto import AggregationType, DBType, ExpressionFlavor


class _Psycopg2Conn(Protocol):
    def cursor(self, *args: Any, **kwargs: Any) -> Any: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def close(self) -> None: ...


logger = logging.getLogger(__name__)

_PG_TEXT = "TEXT"

_LIST_ITEM_TO_PG_ARRAY: dict[str, str] = {
    FieldType.INT.value: "INTEGER[]",
    FieldType.UINT.value: "INTEGER[]",
    FieldType.FLOAT.value: "DOUBLE PRECISION[]",
    FieldType.DOUBLE.value: "DOUBLE PRECISION[]",
    FieldType.BOOL.value: "BOOLEAN[]",
    FieldType.STRING.value: "TEXT[]",
    FieldType.DATETIME.value: "TEXT[]",
    FieldType.UUID.value: "TEXT[]",
}


def _pg_column_type_for_field(field: Field) -> str:
    """Map a Field to a PostgreSQL column type (arrays for LIST; TEXT otherwise)."""
    assert_field_type_supported(DBType.POSTGRES, field)
    if is_list_field_type(field.type):
        item_val = field_type_value(field.item_type)
        if item_val is None or item_val not in _LIST_ITEM_TO_PG_ARRAY:
            raise ValueError(
                f"Field '{field.name}': cannot emit PostgreSQL array type for "
                f"LIST item_type '{item_val}'"
            )
        return _LIST_ITEM_TO_PG_ARRAY[item_val]
    return _PG_TEXT


#: PostgreSQL type (``information_schema`` name or ``udt_name``) -> ``FieldType``.
#: Not the inverse of :func:`_pg_column_type_for_field`, which writes every
#: scalar as ``TEXT``: reading back a graflo-written table therefore reports
#: ``STRING`` throughout, which is accurate. The wider mapping matters for a
#: graph-shaped database graflo did not create.
_PG_TYPE_TO_FIELD_TYPE: dict[str, FieldType] = {
    "text": FieldType.STRING,
    "varchar": FieldType.STRING,
    "character varying": FieldType.STRING,
    "bpchar": FieldType.STRING,
    "character": FieldType.STRING,
    "int2": FieldType.INT,
    "int4": FieldType.INT,
    "int8": FieldType.INT,
    "smallint": FieldType.INT,
    "integer": FieldType.INT,
    "bigint": FieldType.INT,
    "float4": FieldType.FLOAT,
    "real": FieldType.FLOAT,
    "float8": FieldType.DOUBLE,
    "double precision": FieldType.DOUBLE,
    "numeric": FieldType.DOUBLE,
    "bool": FieldType.BOOL,
    "boolean": FieldType.BOOL,
    "date": FieldType.DATETIME,
    "timestamp": FieldType.DATETIME,
    "timestamptz": FieldType.DATETIME,
    "timestamp without time zone": FieldType.DATETIME,
    "timestamp with time zone": FieldType.DATETIME,
    "uuid": FieldType.UUID,
}

#: Columns graflo writes onto an edge table for its own bookkeeping. They are
#: the structure, not properties of the relation, so they must not surface as
#: `Field`s on a recovered edge. A composite endpoint adds the
#: ``source__<field>`` / ``target__<field>`` columns of :func:`edge_endpoint_columns`.
EDGE_ENDPOINT_COLUMNS = frozenset({"source_id", "target_id"})


def edge_endpoint_columns(side: str, fields: Sequence[str]) -> list[str]:
    """Columns of an edge table holding the identity of its *side* endpoint.

    A one-field identity is stored in ``{side}_id``. A composite identity gets
    one ``{side}__{field}`` column per field, in identity order, so endpoints
    sharing a first field value stay apart.
    """
    fields = list(fields) or ["id"]
    if len(fields) == 1:
        return [f"{side}_id"]
    return [f"{side}__{field}" for field in fields]


def is_edge_bookkeeping_column(column: str) -> bool:
    """Whether *column* of an edge table is its row id or an endpoint column."""
    return (
        column == "id"
        or column in EDGE_ENDPOINT_COLUMNS
        or column.startswith(("source__", "target__"))
    )


def field_type_from_postgres(declared: str | None) -> FieldType | None:
    """Map a PostgreSQL column type back to a ``FieldType``, or ``None``."""
    if not declared:
        return None
    base = declared.strip().lower().split("(", 1)[0].strip()
    if base.endswith("[]"):
        return FieldType.LIST
    return _PG_TYPE_TO_FIELD_TYPE.get(base)


def _pg_schema_name(config) -> str:
    return config.schema_name or "public"


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def vertex_table_name(vertex_name: str) -> str:
    return vertex_name


#: SQL aggregate per graflo ``AggregationType``. ``SORTED_UNIQUE`` has no plain
#: aggregate equivalent and is deliberately absent rather than silently mapped.
_PG_AGGREGATIONS: dict[AggregationType, str] = {
    AggregationType.COUNT: "COUNT",
    AggregationType.MAX: "MAX",
    AggregationType.MIN: "MIN",
    AggregationType.AVERAGE: "AVG",
}


#: Suffix every graflo edge table carries. Vertex tables never end in it because
#: :func:`vertex_table_name` is the identity function on the vertex type name.
EDGE_TABLE_SUFFIX = "_edges"


def edge_table_name(source: str, target: str, relation: str | None) -> str:
    rel = relation or "relates"
    return f"{source}_{target}_{rel}{EDGE_TABLE_SUFFIX}"


def split_edge_table_name(
    table: str, vertex_names: Collection[str]
) -> tuple[str, str, str] | None:
    """Recover ``(source, target, relation)`` from an edge table name.

    ``{source}_{target}_{relation}_edges`` is ambiguous on its own -- every
    component may itself contain underscores. Resolving it needs the universe of
    vertex type names to anchor the first two components; ``relation`` is then
    whatever remains. Returns ``None`` when no split against ``vertex_names``
    works, which callers must treat as "not a table I own" rather than guessing.
    """
    if not table.endswith(EDGE_TABLE_SUFFIX):
        return None
    stem = table[: -len(EDGE_TABLE_SUFFIX)]
    # Longest candidate first so `person_group` wins over `person` when both are
    # vertex types and the table is `person_group_person_knows_edges`.
    ordered = sorted(set(vertex_names), key=len, reverse=True)
    for source in ordered:
        if not stem.startswith(f"{source}_"):
            continue
        rest = stem[len(source) + 1 :]
        for target in ordered:
            if rest.startswith(f"{target}_"):
                return source, target, rest[len(target) + 1 :]
    return None


def delete_rows_query(schema: str, table: str, columns: list[str]) -> str:
    """Remove the rows of *table* whose *columns* match a row of ``VALUES %s``."""
    cols = ", ".join(_quote_ident(c) for c in columns)
    return (
        f"DELETE FROM {_quote_ident(schema)}.{_quote_ident(table)} "
        f"WHERE ({cols}) IN (VALUES %s)"
    )


def delete_referencing_rows_query(
    schema: str,
    table: str,
    columns: list[str],
    vertex_table: str,
    match_keys: list[str],
) -> str:
    """Remove the rows of *table* whose endpoint *columns* hold a vertex matched in ``VALUES %s``.

    *columns* hold the vertex's *match_keys*, in order. Edge tables hold
    endpoint values as text, so the comparison is on text.
    """
    held = ", ".join(f"{_quote_ident(c)}::text" for c in columns)
    if len(columns) > 1:
        held = f"({held})"
    referenced = ", ".join(f"v.{_quote_ident(k)}::text" for k in match_keys)
    cols = ", ".join(f"v.{_quote_ident(k)}" for k in match_keys)
    return (
        f"DELETE FROM {_quote_ident(schema)}.{_quote_ident(table)} "
        f"WHERE {held} IN ("
        f"SELECT {referenced} "
        f"FROM {_quote_ident(schema)}.{_quote_ident(vertex_table)} v "
        f"WHERE ({cols}) IN (VALUES %s))"
    )


def edge_table_ddl(
    schema: str,
    table: str,
    *,
    source_table: str,
    source_fields: Sequence[str],
    target_table: str,
    target_fields: Sequence[str],
    properties: Sequence[tuple[str, str]],
) -> tuple[str, str, str | None]:
    """``CREATE TABLE`` for an edge table, the same without foreign keys, and its unique index.

    *properties* are ``(column, type)`` pairs. The unique index covers the
    endpoint columns and the properties, and there is none without properties.
    """
    source_columns = edge_endpoint_columns("source", source_fields)
    target_columns = edge_endpoint_columns("target", target_fields)
    qualified = f"{_quote_ident(schema)}.{_quote_ident(table)}"
    column_defs = [
        f"{_quote_ident('id')} BIGSERIAL PRIMARY KEY",
        *(f"{_quote_ident(c)} {_PG_TEXT} NOT NULL" for c in source_columns),
        *(f"{_quote_ident(c)} {_PG_TEXT} NOT NULL" for c in target_columns),
        *(f"{_quote_ident(name)} {kind}" for name, kind in properties),
    ]

    def foreign_key(columns: list[str], vertex: str, fields: Sequence[str]) -> str:
        return (
            f"FOREIGN KEY ({', '.join(_quote_ident(c) for c in columns)}) "
            f"REFERENCES {_quote_ident(schema)}.{_quote_ident(vertex)} "
            f"({', '.join(_quote_ident(f) for f in list(fields) or ['id'])})"
        )

    keys = [
        foreign_key(source_columns, source_table, source_fields),
        foreign_key(target_columns, target_table, target_fields),
    ]
    create = (
        f"CREATE TABLE IF NOT EXISTS {qualified} ({', '.join([*column_defs, *keys])})"
    )
    create_without_keys = (
        f"CREATE TABLE IF NOT EXISTS {qualified} ({', '.join(column_defs)})"
    )
    unique = None
    if properties:
        indexed = [*source_columns, *target_columns, *(name for name, _ in properties)]
        unique = (
            f"CREATE UNIQUE INDEX IF NOT EXISTS "
            f"{_quote_ident(_edge_unique_index_name(table))} ON {qualified} "
            f"({', '.join(_quote_ident(c) for c in indexed)})"
        )
    return create, create_without_keys, unique


def _edge_unique_index_name(table: str) -> str:
    return f"{table}_edge_uniq"


def _identity_fields(schema: Schema | None, vertex_name: str) -> list[str]:
    """Identity fields of *vertex_name* in *schema*; ``["id"]`` when unknown."""
    if schema is not None:
        fields = schema.core_schema.vertex_config.identity_fields(vertex_name)
        if fields:
            return list(fields)
    return ["id"]


def _endpoint_fields(
    match_keys: tuple[str, ...] | None,
    schema: Schema | None,
    vertex_name: str,
    side: str,
    row: dict[str, Any],
) -> list[str]:
    """The identity fields an edge row stores for its *side* endpoint.

    An exported endpoint document is keyed by those fields, since ``DBWriter``
    resolves endpoints on them. Without *match_keys* or a schema they are read
    off a composite endpoint's column names.
    """
    if match_keys:
        return list(match_keys)
    if schema is not None:
        return _identity_fields(schema, vertex_name)
    prefix = f"{side}__"
    composite = [c[len(prefix) :] for c in row if c.startswith(prefix)]
    return composite or ["id"]


def _edge_table_columns(conn: Any, pg_schema: str, table: str) -> frozenset[str]:
    """Column names of *table*, read from the catalogue once per connection."""
    cache: dict[str, frozenset[str]] = conn.__dict__.setdefault(
        "_edge_table_column_cache", {}
    )
    if table not in cache:
        cache[table] = frozenset(
            str(column["name"])
            for column in conn.get_table_columns(table, schema_name=pg_schema)
            if column.get("name")
        )
    return cache[table]


def _edge_weight_columns_from_schema(
    schema: Schema | None,
    source_class: str,
    target_class: str,
    relation_name: str | None,
) -> list[str]:
    if schema is None:
        return []
    for edge in schema.core_schema.edge_config.values():
        if (
            edge.source == source_class
            and edge.target == target_class
            and edge.relation == relation_name
        ):
            return [field.name for field in edge.properties]
    return []


class PostgresTargetWriteMixin:
    """Mixin implementing :class:`~graflo.db.conn.Connection` target operations."""

    flavor = DBType.POSTGRES
    supports_schema_introspection = True
    # fetch_all_docs / fetch_all_edges are both implemented below.
    supports_graph_export = True
    # The graph shape lives in the table layout, which `information_schema`
    # reports in full; nothing here is sampled.
    schema_introspection_is_sampled = False
    supports_instance_delete = True
    config: Any
    conn: _Psycopg2Conn
    # Supplied by Connection, which follows this mixin in the MRO: annotate
    # rather than stub, so the real implementation is not shadowed.
    define_indexes: Any
    report_edge_direction_support: Any

    def read(
        self, query: str, params: tuple | dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    def get_tables(self, schema_name: str | None = None) -> list[dict[str, Any]]:
        raise NotImplementedError

    def get_table_columns(
        self, table_name: str, schema_name: str | None = None
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    def get_foreign_keys(
        self, table_name: str, schema_name: str | None = None
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    def _execute_write(self, query: str, params: tuple | list | None = None) -> None:
        with self.conn.cursor() as cursor:
            if params is not None:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
        self.conn.commit()

    def create_database(self, name: str) -> None:
        schema_name = name
        q = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
            sql.Identifier(schema_name)
        )
        with self.conn.cursor() as cursor:
            cursor.execute(q)
        self.conn.commit()

    def delete_database(self, name: str) -> None:
        q = sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(name))
        with self.conn.cursor() as cursor:
            cursor.execute(q)
        self.conn.commit()

    def execute(self, query: str | Any, **kwargs: Any) -> Any:
        params = kwargs.get("params")
        if isinstance(query, str) and query.strip().upper().startswith("SELECT"):
            return self.read(query, params)
        self._execute_write(str(query), params)
        return None

    def define_schema(self, schema: Schema) -> None:
        self._target_schema = schema
        from graflo.db.field_type_support import assert_schema_supported

        assert_schema_supported(DBType.POSTGRES, schema)
        self._define_postgres_tables(schema)

    def define_vertex_classes(self, schema: Schema) -> None:
        self._define_vertex_tables(schema)

    def define_edge_classes(self, edges: list[Edge]) -> None:
        for edge in edges:
            self._create_edge_table(edge)

    def delete_graph_structure(
        self,
        vertex_types: tuple[str, ...] | list[str] = (),
        graph_names: tuple[str, ...] | list[str] = (),
        delete_all: bool = False,
    ) -> None:
        pg_schema = _pg_schema_name(self.config)
        present = [row["table_name"] for row in self.get_tables(schema_name=pg_schema)]
        tables: list[str] = []
        if delete_all:
            tables = list(present)
        else:
            requested = [vertex_table_name(v) for v in vertex_types]
            tables.extend(requested)
            # Dropping a vertex type must drop the edges incident to it, the way
            # every graph backend does (Neo4j DETACH DELETE, Arango dropping the
            # graph's edge collections). PostgreSQL stores edges in freestanding
            # tables that no foreign key ties to the vertex table, so without
            # this they survive every drop and accumulate in the namespace.
            dropped = set(requested)
            vertex_universe = dropped | {
                t for t in present if not t.endswith(EDGE_TABLE_SUFFIX)
            }
            for table in present:
                parts = split_edge_table_name(table, vertex_universe)
                if parts is None:
                    continue
                source, target, _ = parts
                if source in dropped or target in dropped:
                    tables.append(table)
        for table in tables:
            q = sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
                sql.Identifier(pg_schema),
                sql.Identifier(table),
            )
            with self.conn.cursor() as cursor:
                cursor.execute(q)
        self.conn.commit()

    def _pg_schema_exists(self, schema_name: str) -> bool:
        rows = self.read(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s",
            (schema_name,),
        )
        return bool(rows)

    def ensure_target_namespace(self, schema: Schema, *, create: bool) -> None:
        """Ensure the PostgreSQL schema namespace exists."""
        pg_schema = _pg_schema_name(self.config)
        if self._pg_schema_exists(pg_schema):
            return
        if not create:
            raise NamespaceNotFoundError(
                f"PostgreSQL schema '{pg_schema}' does not exist. "
                "Create it manually or call with create_namespace=True."
            )
        self.create_database(pg_schema)

    def apply_target_schema(
        self,
        schema: Schema,
        *,
        recreate: bool,
        create_namespace: bool = True,
    ) -> None:
        """Create vertex/edge tables for the schema."""
        self.report_edge_direction_support(schema)
        pg_schema = _pg_schema_name(self.config)
        existing = {row["table_name"] for row in self.get_tables(schema_name=pg_schema)}
        expected_vertices = {
            vertex_table_name(v.name) for v in schema.core_schema.vertex_config.vertices
        }
        expected_edges = {
            edge_table_name(e.source, e.target, e.relation)
            for e in schema.core_schema.edge_config.values()
        }
        expected = expected_vertices | expected_edges
        overlap = existing & expected
        if overlap and not recreate:
            raise SchemaExistsError(
                f"PostgreSQL tables already exist in schema '{pg_schema}': "
                f"{sorted(overlap)}"
            )
        if recreate and overlap:
            self.delete_graph_structure(vertex_types=tuple(expected), delete_all=False)
        if create_namespace and not self._pg_schema_exists(pg_schema):
            self.create_database(pg_schema)
        self.define_schema(schema)
        self.define_indexes(schema)

    def init_db(
        self,
        schema: Schema,
        recreate_schema: bool = False,
        *,
        create_namespace: bool = True,
    ) -> None:
        """Convenience wrapper: ensure schema namespace then apply tables."""
        self.ensure_target_namespace(schema, create=create_namespace)
        self.apply_target_schema(
            schema, recreate=recreate_schema, create_namespace=create_namespace
        )

    def clear_data(self, schema: Schema) -> None:
        pg_schema = _pg_schema_name(self.config)
        table_names = [
            vertex_table_name(v.name) for v in schema.core_schema.vertex_config.vertices
        ]
        table_names.extend(
            edge_table_name(e.source, e.target, e.relation)
            for e in schema.core_schema.edge_config.values()
        )
        with self.conn.cursor() as cursor:
            for table in table_names:
                q = sql.SQL("TRUNCATE TABLE {}.{} CASCADE").format(
                    sql.Identifier(pg_schema),
                    sql.Identifier(table),
                )
                try:
                    cursor.execute(q)
                except Exception:
                    logger.debug("Skipping truncate for missing table %s", table)
        self.conn.commit()

    def _define_postgres_tables(self, schema: Schema) -> None:
        self._define_vertex_tables(schema)
        self.define_edge_classes(list(schema.core_schema.edge_config.values()))

    def _define_vertex_tables(self, schema: Schema) -> None:
        pg_schema = _pg_schema_name(self.config)
        for vertex in schema.core_schema.vertex_config.vertices:
            columns = {f.name: _pg_column_type_for_field(f) for f in vertex.properties}
            for ident in vertex.identity:
                columns.setdefault(ident, _PG_TEXT)
            if not columns:
                columns["id"] = _PG_TEXT
            identity = vertex.identity or ["id"]
            col_defs = [
                sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(col_type))
                for name, col_type in columns.items()
            ]
            pk = sql.SQL(", ").join(sql.Identifier(i) for i in identity)
            create_q = sql.SQL(
                "CREATE TABLE IF NOT EXISTS {}.{} ({}, PRIMARY KEY ({}))"
            ).format(
                sql.Identifier(pg_schema),
                sql.Identifier(vertex_table_name(vertex.name)),
                sql.SQL(", ").join(col_defs),
                pk,
            )
            with self.conn.cursor() as cursor:
                cursor.execute(create_q)
        self.conn.commit()

    def _create_edge_table(self, edge: Edge) -> None:
        pg_schema = _pg_schema_name(self.config)
        table = edge_table_name(edge.source, edge.target, edge.relation)
        schema = getattr(self, "_target_schema", None)
        create, create_without_keys, unique = edge_table_ddl(
            pg_schema,
            table,
            source_table=vertex_table_name(edge.source),
            source_fields=_identity_fields(schema, edge.source),
            target_table=vertex_table_name(edge.target),
            target_fields=_identity_fields(schema, edge.target),
            properties=[
                (field.name, _pg_column_type_for_field(field))
                for field in edge.properties
            ],
        )
        with self.conn.cursor() as cursor:
            try:
                cursor.execute(create)
            except Exception as exc:
                logger.warning(
                    "Edge table %s creation with FK failed: %s; creating without FK",
                    table,
                    exc,
                )
                cursor.execute(create_without_keys)
            if unique is not None:
                cursor.execute(unique)
        self.conn.commit()

    def upsert_docs_batch(
        self,
        docs: list[dict[str, Any]],
        class_name: str,
        match_keys: list[str] | tuple[str, ...],
        **kwargs: Any,
    ) -> None:
        if kwargs.get("dry") or not docs:
            return
        pg_schema = _pg_schema_name(self.config)
        table = vertex_table_name(class_name)
        match_keys = tuple(match_keys) or ("id",)
        all_keys: list[str] = []
        for doc in docs:
            all_keys.extend(doc.keys())
        columns = sorted({k for k in all_keys if not k.startswith("_")})
        if not columns:
            return
        update_cols = [c for c in columns if c not in match_keys]
        col_idents = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
        conflict = sql.SQL(", ").join(sql.Identifier(k) for k in match_keys)
        if update_cols:
            set_clause = sql.SQL(", ").join(
                sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
                for c in update_cols
            )
            upsert_q = sql.SQL(
                "INSERT INTO {}.{} ({}) VALUES %s ON CONFLICT ({}) DO UPDATE SET {}"
            ).format(
                sql.Identifier(pg_schema),
                sql.Identifier(table),
                col_idents,
                conflict,
                set_clause,
            )
        else:
            upsert_q = sql.SQL(
                "INSERT INTO {}.{} ({}) VALUES %s ON CONFLICT ({}) DO NOTHING"
            ).format(
                sql.Identifier(pg_schema),
                sql.Identifier(table),
                col_idents,
                conflict,
            )
        values = [tuple(doc.get(c) for c in columns) for doc in docs]
        with self.conn.cursor() as cursor:
            execute_values(cursor, upsert_q, values)
        self.conn.commit()

    def insert_edges_batch(
        self,
        docs_edges: list[list[dict[str, Any]]] | list[Any] | None,
        source_class: str,
        target_class: str,
        relation_name: str | None,
        match_keys_source: tuple[str, ...],
        match_keys_target: tuple[str, ...],
        filter_uniques: bool = True,
        head: int | None = None,
        **kwargs: Any,
    ) -> None:
        if kwargs.get("dry") or not docs_edges:
            return
        if head is not None:
            docs_edges = docs_edges[:head]
        pg_schema = _pg_schema_name(self.config)
        table = edge_table_name(source_class, target_class, relation_name)
        match_keys_source = match_keys_source or ("id",)
        match_keys_target = match_keys_target or ("id",)

        rows: list[tuple[tuple, tuple, dict[str, Any]]] = []
        weight_keys: set[str] = set()
        for item in docs_edges:
            if not isinstance(item, (list, tuple)) or len(item) < 2:
                continue
            source_doc, target_doc = item[0], item[1]
            weight = item[2] if len(item) > 2 and isinstance(item[2], dict) else {}
            weight_keys.update(weight.keys())
            rows.append(
                (
                    tuple(source_doc.get(k) for k in match_keys_source),
                    tuple(target_doc.get(k) for k in match_keys_target),
                    weight,
                )
            )
        if not rows:
            return

        columns = [
            *edge_endpoint_columns("source", match_keys_source),
            *edge_endpoint_columns("target", match_keys_target),
            *sorted(weight_keys),
        ]
        col_idents = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
        # No conflict target: the edge table's unique index covers the endpoint
        # columns plus any weight columns, so naming a fixed set fails with
        # "no unique or exclusion constraint matching" as soon as the edge
        # carries properties. A bare DO NOTHING matches whichever index exists.
        upsert_q = sql.SQL(
            "INSERT INTO {}.{} ({}) VALUES %s ON CONFLICT DO NOTHING"
        ).format(
            sql.Identifier(pg_schema),
            sql.Identifier(table),
            col_idents,
        )
        values = [
            (*source, *target, *[weight.get(k) for k in sorted(weight_keys)])
            for source, target, weight in rows
            if None not in source and None not in target
        ]
        if not values:
            return
        with self.conn.cursor() as cursor:
            execute_values(cursor, upsert_q, values)
        self.conn.commit()

    def delete_vertices(
        self,
        class_name: str,
        key_docs: list[dict[str, Any]],
        match_keys: tuple[str, ...],
        *,
        chunk_size: int = DEFAULT_DELETE_CHUNK_SIZE,
    ) -> None:
        """Remove vertex rows and, first, the rows of every edge table naming them.

        Edge tables are found by name, as :meth:`delete_graph_structure` finds
        them: a foreign key is not always there to follow.
        """
        keys = list(match_keys)
        rows = deletable_docs(key_docs, keys)
        if not rows:
            return
        pg_schema = _pg_schema_name(self.config)
        table = vertex_table_name(class_name)
        present = [row["table_name"] for row in self.get_tables(pg_schema)]
        vertex_tables = {t for t in present if not t.endswith(EDGE_TABLE_SUFFIX)}
        referencing: list[tuple[str, list[str]]] = []
        for other in present:
            parts = split_edge_table_name(other, vertex_tables | {table})
            if parts is None:
                continue
            source, target, _ = parts
            for side, end in (("source", source), ("target", target)):
                if end == table:
                    referencing.append((other, edge_endpoint_columns(side, keys)))
        values = [tuple(doc[k] for k in keys) for doc in rows]
        with self.conn.cursor() as cursor:
            for start in range(0, len(values), chunk_size):
                chunk = values[start : start + chunk_size]
                for other, columns in referencing:
                    execute_values(
                        cursor,
                        delete_referencing_rows_query(
                            pg_schema, other, columns, table, keys
                        ),
                        chunk,
                    )
                execute_values(cursor, delete_rows_query(pg_schema, table, keys), chunk)
        self.conn.commit()

    def delete_edges(
        self,
        source_class: str,
        target_class: str,
        relation_name: str | None,
        endpoints: list[tuple[dict[str, Any], dict[str, Any]]],
        match_keys_source: tuple[str, ...],
        match_keys_target: tuple[str, ...],
        *,
        collection_name: str | None = None,
        chunk_size: int = DEFAULT_DELETE_CHUNK_SIZE,
    ) -> None:
        """Remove edge rows between endpoint pairs, by the values an insert stores."""
        source_keys = match_keys_source or ("id",)
        target_keys = match_keys_target or ("id",)
        pairs = deletable_endpoints(endpoints, source_keys, target_keys)
        if not pairs:
            return
        query = delete_rows_query(
            _pg_schema_name(self.config),
            edge_table_name(source_class, target_class, relation_name),
            [
                *edge_endpoint_columns("source", source_keys),
                *edge_endpoint_columns("target", target_keys),
            ],
        )
        values = [
            (*(str(s[k]) for k in source_keys), *(str(t[k]) for k in target_keys))
            for s, t in pairs
        ]
        with self.conn.cursor() as cursor:
            for start in range(0, len(values), chunk_size):
                execute_values(cursor, query, values[start : start + chunk_size])
        self.conn.commit()

    def insert_return_batch(
        self, docs: list[dict[str, Any]], class_name: str
    ) -> list[dict[str, Any]] | str:
        raise NotImplementedError(
            "insert_return_batch is not implemented for PostgreSQL"
        )

    def fetch_docs(
        self,
        class_name: str,
        filters: list[Any] | dict[str, Any] | None = None,
        limit: int | None = None,
        return_keys: list[str] | None = None,
        unset_keys: list[str] | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        pg_schema = _pg_schema_name(self.config)
        table = vertex_table_name(class_name)

        if return_keys:
            keep = [k for k in return_keys if not unset_keys or k not in unset_keys]
            select_clause = ", ".join(_quote_ident(k) for k in keep) if keep else "*"
        else:
            select_clause = "*"

        where_clause = ""
        params = BoundParams(ExpressionFlavor.SQL)
        if filters is not None:
            expr = parse_filter_expression(filters)
            rendered = str(expr(kind=ExpressionFlavor.SQL, params=params))
            if rendered:
                where_clause = f" WHERE {rendered}"

        limit_clause = f" LIMIT {int(limit)}" if limit is not None else ""
        q = (
            f"SELECT {select_clause} FROM "
            f"{_quote_ident(pg_schema)}.{_quote_ident(table)}"
            f"{where_clause}{limit_clause}"
        )
        return self.read(q, params.values or None)

    def fetch_edges(
        self,
        from_type: str,
        from_id: str,
        edge_type: str | None = None,
        to_type: str | None = None,
        to_id: str | None = None,
        filters: list | dict | None = None,
        limit: int | None = None,
        return_keys: list | None = None,
        unset_keys: list | None = None,
        direction: EdgeDirection = EdgeDirection.OUT,
        **kwargs,
    ) -> list[dict[str, Any]]:
        """Edges incident to one vertex, read from the edge table.

        ``edge_type`` names the edge table (the storage name), matching
        ``fetch_all_edges``'s ``collection_name``. Endpoints live in
        ``source_id`` / ``target_id``; ``define_edge_indexes`` indexes the latter,
        so the inbound branch is not a sequential scan.

        Raises:
            NotImplementedError: When either endpoint has a composite identity,
                which one address cannot name.
        """
        if edge_type is None:
            raise ValueError(
                "PostgreSQL fetch_edges requires edge_type (the edge table name)"
            )
        pg_schema = _pg_schema_name(self.config)
        columns = _edge_table_columns(self, pg_schema, edge_type)
        if columns and not EDGE_ENDPOINT_COLUMNS <= columns:
            raise NotImplementedError(
                f"PostgreSQL edge table {edge_type!r} stores an endpoint with a "
                "composite identity, and an edge query addresses a vertex by one "
                "value; read the table with fetch_all_edges"
            )
        qualified = f"{_quote_ident(pg_schema)}.{_quote_ident(edge_type)}"

        extra = ""
        bound = BoundParams(ExpressionFlavor.SQL)
        if filters is not None:
            # Both branches of an ANY read name the same placeholders.
            rendered = str(
                parse_filter_expression(filters)(
                    kind=ExpressionFlavor.SQL, params=bound
                )
            )
            if rendered:
                extra = f" AND ({rendered})"
        far_clause = ""
        if to_id is not None:
            far_clause = " AND {far} = %(to_id)s"

        def branch(anchor_column: str, far_column: str) -> str:
            clause = far_clause.format(far=_quote_ident(far_column))
            return (
                f"SELECT * FROM {qualified} "
                f"WHERE {_quote_ident(anchor_column)} = %(from_id)s{clause}{extra}"
            )

        if direction is EdgeDirection.OUT:
            sql = branch("source_id", "target_id")
        elif direction is EdgeDirection.IN:
            sql = branch("target_id", "source_id")
        else:
            # No edge is both outgoing and incoming for the same anchor unless it
            # is a self-loop, so UNION (not UNION ALL) also dedupes that case.
            sql = f"{branch('source_id', 'target_id')} UNION {branch('target_id', 'source_id')}"

        if limit is not None:
            sql = f"{sql} LIMIT {int(limit)}"

        params: dict[str, Any] = {"from_id": from_id, **bound.values}
        if to_id is not None:
            params["to_id"] = to_id
        rows = self.read(sql, params)

        if return_keys or unset_keys:
            keep = set(return_keys) if return_keys else None
            drop = set(unset_keys) if unset_keys else set()
            rows = [
                {
                    k: v
                    for k, v in row.items()
                    if (keep is None or k in keep) and k not in drop
                }
                for row in rows
            ]
        return rows

    def fetch_present_documents(
        self,
        batch: list[dict[str, Any]],
        class_name: str,
        match_keys: list[str] | tuple[str, ...],
        keep_keys: list[str] | tuple[str, ...] | None = None,
        flatten: bool = False,
        filters: list[Any] | dict[str, Any] | None = None,
    ) -> list[dict[str, Any]] | dict[int, list[dict[str, Any]]]:
        raise NotImplementedError(
            "fetch_present_documents is not implemented for PostgreSQL"
        )

    def aggregate(
        self,
        class_name: str,
        aggregation_function: AggregationType,
        discriminant: str | None = None,
        aggregated_field: str | None = None,
        filters: FilterExpression | list | dict | None = None,
    ) -> int | float | list[dict[str, Any]] | dict[str, int | float] | None:
        """Aggregate over a vertex table, optionally grouped by *discriminant*.

        Mirrors the shape the other backends return: a list of
        ``{discriminant, _value}`` rows when grouping, otherwise a single
        ``{_value}`` row.
        """
        pg_schema = _pg_schema_name(self.config)
        table = vertex_table_name(class_name)
        qualified = f"{_quote_ident(pg_schema)}.{_quote_ident(table)}"

        sql_function = _PG_AGGREGATIONS.get(aggregation_function)
        if sql_function is None:
            raise ValueError(
                f"Aggregation {aggregation_function!r} is not supported on PostgreSQL; "
                f"supported: {sorted(a.value for a in _PG_AGGREGATIONS)}"
            )

        if aggregation_function == AggregationType.COUNT and aggregated_field is None:
            expression = "COUNT(*)"
        elif aggregated_field is None:
            raise ValueError(
                f"Aggregation {aggregation_function!r} requires aggregated_field"
            )
        else:
            expression = f"{sql_function}({_quote_ident(aggregated_field)})"

        where_clause = ""
        params = BoundParams(ExpressionFlavor.SQL)
        if filters is not None:
            rendered = str(
                parse_filter_expression(filters)(
                    kind=ExpressionFlavor.SQL, params=params
                )
            )
            if rendered:
                where_clause = f" WHERE {rendered}"

        if discriminant is None:
            q = f"SELECT {expression} AS _value FROM {qualified}{where_clause}"
        else:
            column = _quote_ident(discriminant)
            q = (
                f"SELECT {column} AS {_quote_ident(discriminant)}, "
                f"{expression} AS _value FROM {qualified}{where_clause} "
                f"GROUP BY {column}"
            )
        return self.read(q, params.values or None)

    def keep_absent_documents(
        self,
        batch: list[dict[str, Any]],
        class_name: str,
        match_keys: list[str] | tuple[str, ...],
        keep_keys: list[str] | tuple[str, ...] | None = None,
        filters: list[Any] | dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError(
            "keep_absent_documents is not implemented for PostgreSQL"
        )

    def define_vertex_indexes(
        self, vertex_config: VertexConfig, schema: Schema | None = None
    ) -> None:
        """Create the secondary indexes declared in the database profile.

        The primary identity is already covered by the table's PRIMARY KEY, so
        only profile-declared indexes (which include secondary identities) are
        created here.
        """
        if schema is None:
            logger.warning(
                "Schema is None: vertex secondary indexes cannot be ensured without schema"
            )
            return

        pg_schema = _pg_schema_name(self.config)
        for vertex_name in vertex_config.vertex_set:
            table = vertex_table_name(vertex_name)
            for index in schema.db_profile.vertex_secondary_indexes(vertex_name):
                fields = [str(f) for f in index.fields]
                if not fields:
                    continue
                index_name = f"ix_{table}_{'_'.join(fields)}"
                unique_clause = sql.SQL("UNIQUE ") if index.unique else sql.SQL("")
                q = sql.SQL("CREATE {}INDEX IF NOT EXISTS {} ON {}.{} ({})").format(
                    unique_clause,
                    sql.Identifier(index_name),
                    sql.Identifier(pg_schema),
                    sql.Identifier(table),
                    sql.SQL(", ").join(sql.Identifier(f) for f in fields),
                )
                try:
                    with self.conn.cursor() as cursor:
                        cursor.execute(q)
                    self.conn.commit()
                except Exception as error:
                    self.conn.rollback()
                    logger.warning(
                        "Failed to create index %s on %s.%s: %s",
                        index_name,
                        pg_schema,
                        table,
                        error,
                    )

    def define_edge_indexes(
        self, edges: list[Edge], schema: Schema | None = None
    ) -> None:
        """Index the target columns of every edge table, making reverse lookup viable.

        The only pre-existing edge index is the composite uniqueness constraint,
        whose leading columns are the source's — it cannot serve a lookup keyed
        on the target, so reaching an edge from its target end meant a sequential
        scan. That is the whole cost of an undirected edge on PostgreSQL, and it
        is one index per table.
        """
        pg_schema = _pg_schema_name(self.config)
        for edge in edges:
            table = edge_table_name(edge.source, edge.target, edge.relation)
            index_name = f"ix_{table}_target_id"
            columns = edge_endpoint_columns(
                "target",
                _identity_fields(
                    schema or getattr(self, "_target_schema", None), edge.target
                ),
            )
            q = (
                f"CREATE INDEX IF NOT EXISTS {_quote_ident(index_name)} ON "
                f"{_quote_ident(pg_schema)}.{_quote_ident(table)} "
                f"({', '.join(_quote_ident(c) for c in columns)})"
            )
            try:
                with self.conn.cursor() as cursor:
                    cursor.execute(q)
                self.conn.commit()
            except Exception as error:
                self.conn.rollback()
                logger.warning(
                    "Failed to create reverse-lookup index %s on %s.%s: %s",
                    index_name,
                    pg_schema,
                    table,
                    error,
                )

    def fetch_all_docs(
        self,
        class_name: str,
        *,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        return self.fetch_docs(class_name, limit=limit)

    def introspect_graph_schema(
        self,
        schema_name: str | None = None,
        *,
        sample_limit: int = 100,
    ) -> Schema:
        """Recover a graflo Schema from a graph-shaped PostgreSQL namespace.

        Reads the catalogue rather than sampling rows: the graph shape lives in
        the table layout graflo writes -- one table per vertex type, and
        ``{source}_{target}_{relation}_edges`` with the endpoint columns of
        :func:`edge_endpoint_columns` for each edge type -- so ``information_schema`` answers the whole
        question and ``sample_limit`` is accepted only for interface symmetry.

        Distinct from :meth:`introspect_schema`, which infers a graph from an
        *arbitrary* relational database by following foreign keys. This one
        assumes the graflo layout and recovers exactly what was written.
        """
        from graflo.db.graph_introspection import (
            GraphEdgeIntrospection,
            GraphIntrospectionResult,
            GraphSchemaInferencer,
            GraphVertexIntrospection,
            infer_identity_fields,
        )

        pg_schema = _pg_schema_name(self.config)
        present = [row["table_name"] for row in self.get_tables(schema_name=pg_schema)]
        vertex_tables = [t for t in present if not t.endswith(EDGE_TABLE_SUFFIX)]

        def columns(table: str) -> tuple[list[str], dict[str, FieldType]]:
            names: list[str] = []
            types: dict[str, FieldType] = {}
            for column in self.get_table_columns(table, schema_name=pg_schema):
                name = column.get("name")
                if not name:
                    continue
                names.append(name)
                declared = field_type_from_postgres(column.get("type"))
                if declared is not None:
                    types[name] = declared
            return names, types

        vertices: list[GraphVertexIntrospection] = []
        for table in vertex_tables:
            properties, types = columns(table)
            vertices.append(
                GraphVertexIntrospection(
                    name=table,
                    properties=properties,
                    identity=infer_identity_fields(properties),
                    property_types=types,
                )
            )

        edges: list[GraphEdgeIntrospection] = []
        for table in present:
            parts = split_edge_table_name(table, vertex_tables)
            if parts is None:
                continue
            source, target, relation = parts
            names, types = columns(table)
            weights = [c for c in names if not is_edge_bookkeeping_column(c)]
            edges.append(
                GraphEdgeIntrospection(
                    source=source,
                    target=target,
                    relation=relation,
                    properties=weights,
                    property_types={
                        k: v for k, v in types.items() if k in set(weights)
                    },
                    collection_name=table,
                )
            )

        introspection = GraphIntrospectionResult(
            name=schema_name or pg_schema, vertices=vertices, edges=edges
        )
        return GraphSchemaInferencer(db_flavor=DBType.POSTGRES).infer_schema(
            introspection, schema_name=schema_name or pg_schema
        )

    def fetch_all_edges(
        self,
        source_class: str,
        target_class: str,
        relation_name: str | None,
        *,
        match_keys_source: tuple[str, ...] | None = None,
        match_keys_target: tuple[str, ...] | None = None,
        limit: int | None = None,
        collection_name: str | None = None,
    ) -> list[list[dict[str, Any]]]:
        pg_schema = _pg_schema_name(self.config)
        table = collection_name or edge_table_name(
            source_class, target_class, relation_name
        )
        limit_clause = f" LIMIT {int(limit)}" if limit is not None else ""
        q = (
            f"SELECT * FROM {_quote_ident(pg_schema)}.{_quote_ident(table)}"
            f"{limit_clause}"
        )
        rows = self.read(q)
        schema = getattr(self, "_target_schema", None)
        result: list[list[dict[str, Any]]] = []
        for row in rows:
            docs: list[dict[str, Any]] = []
            for side, keys, vertex in (
                ("source", match_keys_source, source_class),
                ("target", match_keys_target, target_class),
            ):
                fields = _endpoint_fields(keys, schema, vertex, side, row)
                columns = edge_endpoint_columns(side, fields)
                docs.append({f: row.get(c) for f, c in zip(fields, columns)})
            source_doc, target_doc = docs
            weight = {k: v for k, v in row.items() if not is_edge_bookkeeping_column(k)}
            if relation_name and "relation" not in weight:
                weight["relation"] = relation_name
            result.append([source_doc, target_doc, weight])
        return result
