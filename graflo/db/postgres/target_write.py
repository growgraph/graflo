"""PostgreSQL graph target write operations (DDL/DML for vertices and edge tables)."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Protocol

from psycopg2 import sql
from psycopg2.extras import execute_values

from graflo.architecture.schema import Schema
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.vertex import (
    Field,
    FieldType,
    VertexConfig,
    field_type_value,
    is_list_field_type,
)
from graflo.db.conn import NamespaceNotFoundError, SchemaExistsError
from graflo.db.field_type_support import assert_field_type_supported
from graflo.onto import AggregationType, DBType

if TYPE_CHECKING:
    pass


class _Psycopg2Conn(Protocol):
    def cursor(self, *args: Any, **kwargs: Any) -> Any: ...
    def commit(self) -> None: ...
    def close(self) -> None: ...


class _PostgresTargetHost(Protocol):
    config: Any
    conn: _Psycopg2Conn

    def read(self, query: str, params: tuple | None = None) -> list[dict[str, Any]]: ...
    def get_tables(self, schema_name: str | None = None) -> list[dict[str, Any]]: ...


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


def _pg_schema_name(config) -> str:
    return config.schema_name or "public"


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def vertex_table_name(vertex_name: str) -> str:
    return vertex_name


def edge_table_name(source: str, target: str, relation: str | None) -> str:
    rel = relation or "relates"
    return f"{source}_{target}_{rel}_edges"


def _edge_unique_index_name(table: str) -> str:
    return f"{table}_edge_uniq"


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
    config: Any
    conn: _Psycopg2Conn

    def read(self, query: str, params: tuple | None = None) -> list[dict[str, Any]]:
        raise NotImplementedError

    def get_tables(self, schema_name: str | None = None) -> list[dict[str, Any]]:
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
        from graflo.db.field_type_support import assert_schema_field_types_supported

        assert_schema_field_types_supported(DBType.POSTGRES, schema)
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
        tables: list[str] = []
        if delete_all:
            tables = [
                row["table_name"] for row in self.get_tables(schema_name=pg_schema)
            ]
        else:
            for v in vertex_types:
                tables.append(vertex_table_name(v))
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
        source_table = vertex_table_name(edge.source)
        target_table = vertex_table_name(edge.target)

        src_pk = "id"
        tgt_pk = "id"
        schema = getattr(self, "_target_schema", None)
        if schema is not None:
            vc = schema.core_schema.vertex_config
            src_fields = vc.identity_fields(edge.source)
            tgt_fields = vc.identity_fields(edge.target)
            if src_fields:
                src_pk = src_fields[0]
            if tgt_fields:
                tgt_pk = tgt_fields[0]

        weight_cols = list(edge.properties) if edge.properties else []
        col_defs: list[sql.Composable] = [
            sql.SQL("{} BIGSERIAL PRIMARY KEY").format(sql.Identifier("id")),
            sql.SQL("{} {} NOT NULL").format(
                sql.Identifier("source_id"), sql.SQL(_PG_TEXT)
            ),
            sql.SQL("{} {} NOT NULL").format(
                sql.Identifier("target_id"), sql.SQL(_PG_TEXT)
            ),
        ]
        for field in weight_cols:
            col_defs.append(
                sql.SQL("{} {}").format(
                    sql.Identifier(field.name),
                    sql.SQL(_pg_column_type_for_field(field)),
                )
            )
        fk_clauses: list[sql.Composable] = []
        fk_source = sql.SQL("FOREIGN KEY (source_id) REFERENCES {}.{} ({})").format(
            sql.Identifier(pg_schema),
            sql.Identifier(source_table),
            sql.Identifier(src_pk),
        )
        fk_target = sql.SQL("FOREIGN KEY (target_id) REFERENCES {}.{} ({})").format(
            sql.Identifier(pg_schema),
            sql.Identifier(target_table),
            sql.Identifier(tgt_pk),
        )
        fk_clauses = [fk_source, fk_target]
        create_q = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
            sql.Identifier(pg_schema),
            sql.Identifier(table),
            sql.SQL(", ").join([*col_defs, *fk_clauses]),
        )
        with self.conn.cursor() as cursor:
            try:
                cursor.execute(create_q)
            except Exception as exc:
                logger.warning(
                    "Edge table %s creation with FK failed: %s; creating without FK",
                    table,
                    exc,
                )
                create_q_no_fk = sql.SQL(
                    "CREATE TABLE IF NOT EXISTS {}.{} ({})"
                ).format(
                    sql.Identifier(pg_schema),
                    sql.Identifier(table),
                    sql.SQL(", ").join(col_defs),
                )
                cursor.execute(create_q_no_fk)
            if weight_cols:
                unique_cols = sql.SQL(", ").join(
                    sql.Identifier(column)
                    for column in ("source_id", "target_id", *weight_cols)
                )
                idx_q = sql.SQL(
                    "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {}.{} ({})"
                ).format(
                    sql.Identifier(_edge_unique_index_name(table)),
                    sql.Identifier(pg_schema),
                    sql.Identifier(table),
                    unique_cols,
                )
                cursor.execute(idx_q)
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
        src_key = match_keys_source[0]
        tgt_key = match_keys_target[0]

        rows: list[tuple] = []
        weight_keys: set[str] = set()
        for item in docs_edges:
            if not isinstance(item, (list, tuple)) or len(item) < 2:
                continue
            source_doc, target_doc = item[0], item[1]
            weight = item[2] if len(item) > 2 and isinstance(item[2], dict) else {}
            weight_keys.update(weight.keys())
            rows.append(
                (
                    source_doc.get(src_key),
                    target_doc.get(tgt_key),
                    weight,
                )
            )
        if not rows:
            return

        columns = ["source_id", "target_id", *sorted(weight_keys)]
        col_idents = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
        upsert_q = sql.SQL(
            "INSERT INTO {}.{} ({}) VALUES %s ON CONFLICT (source_id, target_id) DO NOTHING"
        ).format(
            sql.Identifier(pg_schema),
            sql.Identifier(table),
            col_idents,
        )
        values = [
            (
                source_id,
                target_id,
                *[weight.get(k) for k in sorted(weight_keys)],
            )
            for source_id, target_id, weight in rows
            if source_id is not None and target_id is not None
        ]
        if not values:
            return
        with self.conn.cursor() as cursor:
            execute_values(cursor, upsert_q, values)
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
        limit_clause = f" LIMIT {int(limit)}" if limit is not None else ""
        q = f"SELECT * FROM {_quote_ident(pg_schema)}.{_quote_ident(table)}{limit_clause}"
        return self.read(q)

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
        **kwargs,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError("fetch_edges is not implemented for PostgreSQL")

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
        filters: list | dict | None = None,
    ) -> int | float | list[dict[str, Any]] | dict[str, int | float] | None:
        raise NotImplementedError("aggregate is not implemented for PostgreSQL")

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
        pass

    def define_edge_indexes(
        self, edges: list[Edge], schema: Schema | None = None
    ) -> None:
        pass

    def fetch_all_docs(
        self,
        class_name: str,
        *,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        return self.fetch_docs(class_name, limit=limit)

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
        result: list[list[dict[str, Any]]] = []
        for row in rows:
            source_doc = {"id": row.get("source_id")}
            target_doc = {"id": row.get("target_id")}
            weight = {
                k: v for k, v in row.items() if k not in ("source_id", "target_id")
            }
            result.append([source_doc, target_doc, weight])
        return result
