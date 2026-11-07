"""PostgreSQL bulk writer: ``COPY`` into tables whose keys are built after the load.

Vertex tables are created without primary keys and edge tables without foreign
keys or their unique key. Every chunk is one transaction: its ``COPY`` and a
row naming it in a load-log table, so a chunk in doubt is found there when
replayed. The primary keys are added once every vertex is written; the unique
edge keys, then foreign keys (``NOT VALID``, then validated), at the end.

Values are written as the text the upsert path's statements leave in each
column, so both paths store the same graph.
"""

from __future__ import annotations

import datetime as dt
import io
import logging
import math
from collections.abc import Callable, Iterable, Sequence
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from graflo.architecture.schema import Schema
from graflo.architecture.schema.edge import Edge
from graflo.connections.bulk import BulkConfig
from graflo.db.bulk import (
    BulkCounts,
    BulkEdgeChunk,
    BulkPlan,
    BulkVertexChunk,
    StoredEdgeId,
)
from graflo.db.postgres.target_write import (
    PostgresTargetWriteMixin,
    _create_edge_key,
    _identity_fields,
    _pg_schema_name,
    _quote_ident,
    edge_endpoint_columns,
    edge_foreign_keys,
    edge_table_key,
    edge_table_name,
    vertex_key_ddl,
    vertex_table_name,
)
from graflo.onto import DBType

logger = logging.getLogger(__name__)

#: Table, in the target schema, of the chunks committed so far; dropped at the end.
LOAD_LOG = "_graflo_bulk_log"

#: ``maintenance_work_mem`` for the index builds, unless the session has more.
INDEX_BUILD_MEMORY = "1GB"


# -- values as text ------------------------------------------------------------


def _numeric_text(value: Decimal) -> str:
    """*value* as ``numeric`` prints it: positional, no negative zero."""
    if value.is_zero():
        value = value.copy_abs()
    return format(value, "f")


def _clock_text(value: dt.time | dt.datetime) -> str:
    """``HH:MM:SS``, then the fraction without trailing zeros, as PostgreSQL prints it."""
    text = f"{value.hour:02d}:{value.minute:02d}:{value.second:02d}"
    if value.microsecond:
        text += f".{value.microsecond:06d}".rstrip("0")
    return text


def _offset_text(offset: dt.timedelta) -> str:
    """A UTC offset as PostgreSQL prints it: ``+HH``, ``+HH:MM`` or ``+HH:MM:SS``."""
    seconds = int(offset.total_seconds())
    sign = "-" if seconds < 0 else "+"
    hours, rest = divmod(abs(seconds), 3600)
    minutes, secs = divmod(rest, 60)
    text = f"{sign}{hours:02d}"
    if minutes or secs:
        text += f":{minutes:02d}"
    if secs:
        text += f":{secs:02d}"
    return text


def _temporal_text(value: dt.date | dt.time, tz: dt.tzinfo | None, iso: bool) -> str:
    if not iso:
        raise ValueError(
            "PostgreSQL bulk load writes dates and times as the session prints "
            "them with DateStyle ISO; set DateStyle to ISO for the role or database"
        )
    if isinstance(value, dt.datetime):
        if value.utcoffset() is None:
            return f"{value.date().isoformat()} {_clock_text(value)}"
        if tz is None:
            raise ValueError(
                "PostgreSQL bulk load cannot print a datetime with a time zone: "
                "the session TimeZone is not a zone Python knows"
            )
        local = value.astimezone(tz)
        offset = local.utcoffset()
        assert offset is not None
        return f"{local.date().isoformat()} {_clock_text(local)}{_offset_text(offset)}"
    if isinstance(value, dt.time):
        offset = value.utcoffset()
        text = _clock_text(value)
        return text if offset is None else text + _offset_text(offset)
    return value.isoformat()


#: Characters that make ``array_out`` quote an element.
_ARRAY_SPECIAL = frozenset('{},"\\ \t\n\r\v\f')


def _array_element(text: str) -> str:
    if text == "" or text.upper() == "NULL" or any(c in _ARRAY_SPECIAL for c in text):
        return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return text


def _array_text(
    items: Sequence[Any], *, typed: bool, tz: dt.tzinfo | None, iso: bool
) -> str:
    parts: list[str] = []
    for item in items:
        if item is None:
            parts.append("NULL")
        elif isinstance(item, list | tuple):
            parts.append(_array_text(item, typed=typed, tz=tz, iso=iso))
        elif isinstance(item, bool) and not typed:
            # An array stored as text prints its booleans as ``bool[]`` does.
            parts.append("t" if item else "f")
        else:
            parts.append(_array_element(_scalar_text(item, tz=tz, iso=iso)))
    return "{" + ",".join(parts) + "}"


def _scalar_text(value: Any, *, tz: dt.tzinfo | None, iso: bool) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        # The upsert path sends a float as a numeric literal.
        return _numeric_text(Decimal(repr(value)))
    if isinstance(value, Decimal):
        if value.is_nan():
            return "NaN"
        if value.is_infinite():
            return "Infinity" if value > 0 else "-Infinity"
        return _numeric_text(value)
    if isinstance(value, str):
        return value
    if isinstance(value, dt.date | dt.time):
        return _temporal_text(value, tz, iso)
    # psycopg2 adapts no other type (dict, UUID, ...), so the upsert path
    # refuses them too.
    raise TypeError(f"PostgreSQL cannot store a value of type {type(value).__name__}")


def column_text(
    value: Any,
    *,
    array: bool = False,
    tz: dt.tzinfo | None = None,
    iso: bool = True,
) -> str | None:
    """The text ``COPY`` takes for *value* in a column, ``None`` for ``NULL``.

    It is what the column holds after the upsert path's ``INSERT`` of the
    value psycopg2 adapts: numbers, booleans, dates and times printed as
    PostgreSQL prints them, lists as array literals.

    Args:
        value: The value.
        array: The column is an array column.
        tz: The session time zone, for a datetime with a time zone.
        iso: The session's DateStyle is ISO.

    Raises:
        TypeError: psycopg2 cannot adapt the value.
        ValueError: A date or time the session would print differently.
    """
    if value is None:
        return None
    if isinstance(value, list | tuple):
        return _array_text(value, typed=array, tz=tz, iso=iso)
    return _scalar_text(value, tz=tz, iso=iso)


def csv_text(rows: Iterable[Sequence[str | None]]) -> str:
    """*rows* as CSV for ``COPY ... (FORMAT csv)``: every value quoted, ``NULL`` empty."""
    out = io.StringIO()
    for row in rows:
        out.write(
            ",".join(
                "" if text is None else '"' + text.replace('"', '""') + '"'
                for text in row
            )
        )
        out.write("\n")
    return out.getvalue()


# -- rows ----------------------------------------------------------------------


class SessionText:
    """How one session prints values: its time zone and whether DateStyle is ISO."""

    def __init__(self, tz: dt.tzinfo | None = None, iso: bool = True) -> None:
        self.tz = tz
        self.iso = iso

    def text(self, value: Any, *, array: bool = False) -> str | None:
        return column_text(value, array=array, tz=self.tz, iso=self.iso)


def _row_text(
    values: Sequence[Any],
    columns: Sequence[str],
    arrays: frozenset[str],
    s: SessionText,
) -> list[str | None]:
    return [s.text(v, array=c in arrays) for v, c in zip(values, columns)]


def vertex_copy_rows(
    chunk: BulkVertexChunk, arrays: frozenset[str], session: SessionText
) -> tuple[list[str], list[list[str | None]]]:
    """Columns and text rows of a vertex chunk.

    Columns are every field a row carries, less ``_``-prefixed ones, as the
    upsert path writes; a row lacking one writes ``NULL``. A row lacking a key
    field is dropped, as the upsert path drops it.
    """
    keys = chunk.match_keys or ["id"]
    kept = [r for r in chunk.rows if all(r.get(k) is not None for k in keys)]
    _warn_dropped(len(chunk.rows) - len(kept), chunk.class_name)
    columns = sorted({k for r in kept for k in r if not k.startswith("_")})
    return columns, [
        _row_text([r.get(c) for c in columns], columns, arrays, session) for r in kept
    ]


def edge_copy_rows(
    chunk: BulkEdgeChunk, arrays: frozenset[str], session: SessionText
) -> tuple[list[str], list[list[str | None]]]:
    """Columns and text rows of an edge chunk: endpoint identity values, then properties.

    Endpoints are stored by their identity values, as the upsert path stores
    them; an edge lacking one is dropped, as there.
    """
    source_keys = chunk.match_keys_source or ["id"]
    target_keys = chunk.match_keys_target or ["id"]
    endpoints = [
        *edge_endpoint_columns("source", source_keys),
        *edge_endpoint_columns("target", target_keys),
    ]
    kept: list[tuple[list[Any], dict[str, Any]]] = []
    for source, target, props in chunk.rows:
        ends = [source.get(k) for k in source_keys] + [
            target.get(k) for k in target_keys
        ]
        if None in ends:
            continue
        kept.append((ends, {k: v for k, v in props.items() if not k.startswith("_")}))
    _warn_dropped(len(chunk.rows) - len(kept), edge_table_name(*_triple(chunk)))
    weights = sorted({k for _, props in kept for k in props})
    columns = [*endpoints, *weights]
    return columns, [
        _row_text([*ends, *(props.get(w) for w in weights)], columns, arrays, session)
        for ends, props in kept
    ]


def _triple(chunk: BulkEdgeChunk) -> tuple[str, str, str | None]:
    return chunk.source_class, chunk.target_class, chunk.relation_name


def _warn_dropped(dropped: int, table: str) -> None:
    if dropped:
        logger.warning(
            "Skipped %s row(s) of '%s' with an incomplete key; key columns "
            "cannot be NULL.",
            dropped,
            table,
        )


# -- statements ----------------------------------------------------------------


def _qualified(schema: str, table: str) -> str:
    return f"{_quote_ident(schema)}.{_quote_ident(table)}"


def copy_sql(schema: str, table: str, columns: Sequence[str]) -> str:
    """``COPY`` of CSV rows into *columns* of *table*."""
    cols = ", ".join(_quote_ident(c) for c in columns)
    return f"COPY {_qualified(schema, table)} ({cols}) FROM STDIN WITH (FORMAT csv)"


def load_log_ddl(schema: str) -> tuple[str, str]:
    """``DROP`` and ``CREATE`` of the load-log table."""
    table = _qualified(schema, LOAD_LOG)
    return (
        f"DROP TABLE IF EXISTS {table}",
        f"CREATE TABLE IF NOT EXISTS {table} (chunk_id TEXT PRIMARY KEY)",
    )


def logged_sql(schema: str) -> str:
    return f"SELECT 1 FROM {_qualified(schema, LOAD_LOG)} WHERE chunk_id = %s"


def log_sql(schema: str) -> str:
    return f"INSERT INTO {_qualified(schema, LOAD_LOG)} (chunk_id) VALUES (%s)"


def add_foreign_keys_sql(schema: str, table: str, keys: Sequence[str]) -> str:
    """One ``ALTER TABLE`` adding *keys* unchecked, so they are validated apart."""
    adds = ", ".join(f"ADD {key} NOT VALID" for key in keys)
    return f"ALTER TABLE {_qualified(schema, table)} {adds}"


_ARRAY_COLUMNS_SQL = (
    "SELECT a.attname FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid "
    "WHERE a.attrelid = to_regclass(%s) AND a.attnum > 0 AND NOT a.attisdropped "
    "AND t.typcategory = 'A'"
)
_HAS_PRIMARY_KEY_SQL = (
    "SELECT 1 FROM pg_constraint WHERE conrelid = to_regclass(%s) AND contype = 'p'"
)
_HAS_FOREIGN_KEY_SQL = (
    "SELECT 1 FROM pg_constraint WHERE conrelid = to_regclass(%s) AND contype = 'f'"
)
_UNVALIDATED_SQL = (
    "SELECT conname FROM pg_constraint WHERE conrelid = to_regclass(%s) "
    "AND contype = 'f' AND NOT convalidated ORDER BY conname"
)


def should_write(replay: bool, logged: bool) -> bool:
    """Whether a chunk is written: always, unless a replay finds it in the log."""
    return not (replay and logged)


# -- the writer ----------------------------------------------------------------

#: Builds a chunk's columns and text rows from the table's array columns.
_Build = Callable[
    [frozenset[str], SessionText], tuple[list[str], list[list[str | None]]]
]


class PostgresBulkMixin(PostgresTargetWriteMixin):
    """The bulk-load protocol of :mod:`graflo.db.bulk` on PostgreSQL."""

    supports_bulk_load = True

    def bulk_begin(
        self,
        schema: Schema,
        cfg: BulkConfig,
        *,
        recreate: bool = False,
        resume: bool = False,
    ) -> BulkPlan:
        """Create the namespace, keyless tables and an empty load log.

        Raises:
            SchemaExistsError: The namespace holds a table of *schema* and
                neither *recreate* nor *resume* is set.
        """
        from graflo.db.field_type_support import assert_schema_supported

        assert_schema_supported(DBType.POSTGRES, schema)
        self.report_edge_direction_support(schema)
        self.ensure_target_namespace(schema, create=True)
        if not resume:
            self._guard_first_write(schema, recreate=recreate)
        self._target_schema = schema
        self._define_vertex_tables(schema, keyed=False)
        for edge in schema.core_schema.edge_config.values():
            self._create_edge_table(edge, keyed=False)
        drop, create = load_log_ddl(_pg_schema_name(self.config))
        with self.conn.cursor() as cursor:
            if not resume:
                # A log left by an earlier run would skip this run's chunks.
                cursor.execute(drop)
            cursor.execute(create)
        self.conn.commit()
        return BulkPlan()

    def bulk_write_vertices(self, schema: Schema, chunk: BulkVertexChunk) -> None:
        table = vertex_table_name(chunk.class_name)
        self._bulk_copy(
            chunk.chunk_id,
            chunk.replay,
            table,
            lambda arrays, session: vertex_copy_rows(chunk, arrays, session),
        )

    def bulk_prepare_edges(self, schema: Schema) -> None:
        """Add the vertex primary keys, which the foreign keys will reference.

        Raises:
            psycopg2.Error: Two rows share an identity.
        """
        pg_schema = _pg_schema_name(self.config)
        self._bulk_index_session()
        for vertex in schema.core_schema.vertex_config.vertices:
            table = _qualified(pg_schema, vertex_table_name(vertex.name))
            try:
                with self.conn.cursor() as cursor:
                    cursor.execute(_HAS_PRIMARY_KEY_SQL, (table,))
                    if cursor.fetchone() is None:
                        cursor.execute(vertex_key_ddl(pg_schema, vertex))
            except Exception:
                self.conn.rollback()
                raise
            self.conn.commit()

    def bulk_write_edges(self, schema: Schema, chunk: BulkEdgeChunk) -> None:
        table = edge_table_name(*_triple(chunk))
        self._bulk_copy(
            chunk.chunk_id,
            chunk.replay,
            table,
            lambda arrays, session: edge_copy_rows(chunk, arrays, session),
        )

    def bulk_finish(self, schema: Schema) -> BulkCounts:
        """Build the edge keys, indexes and foreign keys; count; drop the load log.

        Raises:
            RuntimeError: An edge table holds two rows with one key.
            psycopg2.Error: An edge names a vertex that is not stored.
        """
        pg_schema = _pg_schema_name(self.config)
        self._target_schema = schema
        self._bulk_index_session()
        edges = list(schema.core_schema.edge_config.values())
        for edge in edges:
            table = edge_table_name(edge.source, edge.target, edge.relation)
            try:
                with self.conn.cursor() as cursor:
                    _create_edge_key(
                        cursor, pg_schema, table, edge_table_key(schema, edge)
                    )
            except Exception:
                self.conn.rollback()
                raise
            self.conn.commit()
        self.define_indexes(schema)
        for vertex in schema.core_schema.vertex_config.vertices:
            table = vertex_table_name(vertex.name)
            self._bulk_execute(f"ANALYZE {_qualified(pg_schema, table)}")
        for edge in edges:
            self._bulk_foreign_keys(schema, pg_schema, edge)
        counts = self._bulk_counts(schema, pg_schema)
        drop, _ = load_log_ddl(pg_schema)
        self._bulk_execute(drop)
        return counts

    # -- helpers ---------------------------------------------------------------

    def _bulk_execute(self, query: str) -> None:
        """Run *query* in a transaction of its own."""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query)
        except Exception:
            self.conn.rollback()
            raise
        self.conn.commit()

    def _bulk_session(self) -> SessionText:
        """Set the session up for chunk writes, once per connection.

        ``synchronous_commit`` is off: a server crash may lose the last commits
        with their log rows, which the count check at the end reports.
        """
        session = self.__dict__.get("_bulk_session_text")
        if session is not None:
            return session
        with self.conn.cursor() as cursor:
            cursor.execute("SET synchronous_commit = off")
            cursor.execute(
                "SELECT current_setting('TimeZone'), current_setting('DateStyle')"
            )
            zone, datestyle = cursor.fetchone()
        self.conn.commit()
        try:
            tz: dt.tzinfo | None = ZoneInfo(zone)
        except (ValueError, KeyError, OSError):
            tz = None
        session = SessionText(tz=tz, iso=str(datestyle).upper().startswith("ISO"))
        self.__dict__["_bulk_session_text"] = session
        return session

    def _bulk_index_session(self) -> None:
        """Give this session's index builds more memory and parallel workers."""
        with self.conn.cursor() as cursor:
            cursor.execute(
                "SELECT set_config('maintenance_work_mem', %s, false) "
                "WHERE pg_size_bytes(current_setting('maintenance_work_mem')) "
                "< pg_size_bytes(%s)",
                (INDEX_BUILD_MEMORY, INDEX_BUILD_MEMORY),
            )
            cursor.execute(
                "SELECT set_config('max_parallel_maintenance_workers', "
                "GREATEST(current_setting('max_parallel_maintenance_workers')::int, "
                "current_setting('max_parallel_workers')::int)::text, false)"
            )
        self.conn.commit()

    def _bulk_array_columns(
        self, cursor: Any, schema: str, table: str
    ) -> frozenset[str]:
        cache: dict[str, frozenset[str]] = self.__dict__.setdefault(
            "_bulk_array_column_cache", {}
        )
        if table not in cache:
            cursor.execute(_ARRAY_COLUMNS_SQL, (_qualified(schema, table),))
            cache[table] = frozenset(str(row[0]) for row in cursor.fetchall())
        return cache[table]

    def _bulk_copy(
        self,
        chunk_id: str,
        replay: bool,
        table: str,
        build: _Build,
    ) -> None:
        """``COPY`` a chunk's rows and log it, in one transaction.

        *build* takes the table's array columns and the session's printing
        and returns the columns and text rows.
        """
        pg_schema = _pg_schema_name(self.config)
        session = self._bulk_session()
        try:
            with self.conn.cursor() as cursor:
                logged = False
                if replay:
                    cursor.execute(logged_sql(pg_schema), (chunk_id,))
                    logged = cursor.fetchone() is not None
                if should_write(replay, logged):
                    arrays = self._bulk_array_columns(cursor, pg_schema, table)
                    columns, rows = build(arrays, session)
                    if rows:
                        cursor.copy_expert(
                            copy_sql(pg_schema, table, columns),
                            io.StringIO(csv_text(rows)),
                        )
                    cursor.execute(log_sql(pg_schema), (chunk_id,))
        except Exception:
            self.conn.rollback()
            raise
        self.conn.commit()

    def _bulk_foreign_keys(self, schema: Schema, pg_schema: str, edge: Edge) -> None:
        """Add *edge*'s foreign keys unchecked unless present, then validate them.

        Adding them falls back to none, with a warning, where the table could
        not take them when defined either (an identity the key cannot reference).
        """
        table = edge_table_name(edge.source, edge.target, edge.relation)
        qualified = _qualified(pg_schema, table)
        keys = edge_foreign_keys(
            pg_schema,
            source_table=vertex_table_name(edge.source),
            source_fields=_identity_fields(schema, edge.source),
            target_table=vertex_table_name(edge.target),
            target_fields=_identity_fields(schema, edge.target),
        )
        with self.conn.cursor() as cursor:
            cursor.execute(f"ANALYZE {qualified}")
            cursor.execute(_HAS_FOREIGN_KEY_SQL, (qualified,))
            if cursor.fetchone() is None:
                cursor.execute("SAVEPOINT graflo_bulk_fk")
                try:
                    cursor.execute(add_foreign_keys_sql(pg_schema, table, keys))
                except Exception as exc:
                    cursor.execute("ROLLBACK TO SAVEPOINT graflo_bulk_fk")
                    logger.warning(
                        "Edge table %s takes no foreign keys: %s", table, exc
                    )
                else:
                    cursor.execute("RELEASE SAVEPOINT graflo_bulk_fk")
            cursor.execute(_UNVALIDATED_SQL, (qualified,))
            names = [str(row[0]) for row in cursor.fetchall()]
        self.conn.commit()
        for name in names:
            self._bulk_execute(
                f"ALTER TABLE {qualified} VALIDATE CONSTRAINT {_quote_ident(name)}"
            )

    def _bulk_counts(self, schema: Schema, pg_schema: str) -> BulkCounts:
        def count(table: str) -> int:
            with self.conn.cursor() as cursor:
                cursor.execute(f"SELECT count(*) FROM {_qualified(pg_schema, table)}")
                (n,) = cursor.fetchone()
            return int(n)

        vertices = {
            vertex_table_name(v.name): count(vertex_table_name(v.name))
            for v in schema.core_schema.vertex_config.vertices
        }
        edges: dict[StoredEdgeId, int] = {
            (e.source, e.relation, e.target): count(
                edge_table_name(e.source, e.target, e.relation)
            )
            for e in schema.core_schema.edge_config.values()
        }
        self.conn.commit()
        return BulkCounts(vertices=vertices, edges=edges)
