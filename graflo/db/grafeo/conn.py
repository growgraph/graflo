"""Grafeo connection: an embedded graph database queried in Cypher.

Grafeo runs inside this process. A :class:`~graflo.connections.grafeo.GrafeoConfig`
names a database (a file, or memory) and, as ``database``, a named graph in it:
the target namespace. Every query runs in that graph through a graph handle, so
connections to different graphs of one database do not interfere.

Values: a datetime is stored as an instant in UTC. A timezone-aware value is
converted to UTC on write, and every datetime is read back timezone-aware in
UTC, since the database keeps no offset.

Example:
    >>> conn = GrafeoConnection(GrafeoConfig(path=Path("g.grafeo"), database="kg"))
    >>> conn.init_db(schema, recreate_schema=True)
    >>> conn.upsert_docs_batch(docs, "User", match_keys=["email"])
"""

from __future__ import annotations

import datetime as dt
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from graflo.architecture.graph_types import EdgeDirection, GraphContainer, Index
from graflo.architecture.schema import Schema
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.vertex import VertexConfig
from graflo.connections.bulk import BulkConfig
from graflo.connections.grafeo import GrafeoConfig
from graflo.db.aggregation import (
    KEY,
    VALUE,
    aggregation_result,
    check_aggregation,
    cypher_aggregation_return,
)
from graflo.db.bulk import BulkCounts, BulkEdgeChunk, BulkPlan, BulkVertexChunk
from graflo.db.conn import (
    DEFAULT_DELETE_CHUNK_SIZE,
    Connection,
    NamespaceNotFoundError,
    SchemaExistsError,
    consume_insert_edges_kwargs,
)
from graflo.db.cypher import (
    cypher_key_equals,
    cypher_keyed_node,
    cypher_name,
    cypher_rel_pattern,
    partition_by_absent_merge_props,
    rel_union_rows,
    rel_upsert_clause,
)
from graflo.db.cypher.bulk import (
    bulk_counts,
    create_nodes_query,
    create_relationships_query,
)
from graflo.db.cypher.delete import (
    delete_nodes,
    delete_relationships,
    delete_scoped_nodes,
    delete_scoped_relationships,
)
from graflo.db.cypher.sync import (
    changed_owners,
    ensure_nodes_query,
    hash_rows,
    owner_statements,
    stored_hashes_query,
)
from graflo.db.cypher.targets import relationship_targets
from graflo.db.cypher.traversal import cypher_graph_neighbors
from graflo.db.cypher.union import union_upsert_query, union_upsert_rows
from graflo.db.field_type_support import assert_schema_supported
from graflo.db.grafeo.store import GrafeoStore, open_store
from graflo.db.graph_introspection import (
    GraphSchemaInferencer,
    strip_internal_properties,
)
from graflo.db.resolve import absent_documents, present_documents
from graflo.db.sync import SyncEdgeType, SyncOwnerChunk, SyncWritten
from graflo.filter.onto import (
    BoundParams,
    FilterExpression,
    parse_filter_expression,
    render_conjunct,
)
from graflo.onto import AggregationType, DBType

if TYPE_CHECKING:
    import grafeo as _grafeo

logger = logging.getLogger(__name__)

Rows = list[dict[str, Any]]


@dataclass(frozen=True)
class GrafeoResult:
    """The rows of one query, by column name and by position."""

    columns: list[str]
    rows: Rows

    @property
    def result_set(self) -> list[list[Any]]:
        """Each row as a list of values, in column order."""
        return [[row.get(column) for column in self.columns] for row in self.rows]


def _to_grafeo(value: Any) -> Any:
    """*value* as written: a timezone-aware datetime becomes naive UTC."""
    if isinstance(value, dt.datetime):
        if value.tzinfo is not None:
            return value.astimezone(dt.UTC).replace(tzinfo=None)
        return value
    if isinstance(value, dict):
        return {k: _to_grafeo(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_grafeo(v) for v in value]
    return value


def _from_grafeo(value: Any) -> Any:
    """*value* as read: a datetime is an instant in UTC."""
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=dt.UTC)
        return value.astimezone(dt.UTC)
    if isinstance(value, dict):
        return {k: _from_grafeo(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_from_grafeo(v) for v in value]
    return value


class GrafeoConnection(Connection):
    """Connection to one named graph of a Grafeo database.

    Attributes:
        flavor: Database flavor identifier (GRAFEO).
        config: The connection configuration; ``database`` is the graph.
        db: The open database, shared by every connection to its path.
    """

    flavor = DBType.GRAFEO
    supports_instance_delete = True
    supports_edge_target_read = True
    supports_scoped_delete = True
    supports_graph_export = True
    supports_schema_introspection = True
    supports_read_guard = True
    supports_union_merge = True
    supports_membership_read = True
    supports_bulk_load = True
    supports_sync = True
    # Only GQL queries take a time limit; reads here are Cypher, so a
    # ``timeout_s`` on a read is accepted and ignored.
    supports_query_timeout = False

    def __init__(self, config: GrafeoConfig):
        """Open (or join) the database *config* names.

        Args:
            config: Database path (or memory) and graph name.

        Raises:
            MissingExtraError: The ``grafeo`` extra is not installed.
        """
        super().__init__()
        self.config = config
        self._store: GrafeoStore = open_store(config.path)
        self.db: _grafeo.GrafeoDB = self._store.db

    # -- queries -----------------------------------------------------------------

    def _graph(self, *, create: bool = True) -> Any:
        """The handle queries run through: the named graph, or the default one.

        A named graph that does not exist yet is created, as writing to a new
        graph key creates it on other engines; with ``create=False`` it is
        ``None``.
        """
        name = self.config.database
        if name is None:
            return self.db
        if name not in self.db.list_graphs():
            if not create:
                return None
            self.db.create_graph(name)
        return self.db.graph(name)

    def _run(self, query: str, params: dict[str, Any] | None) -> GrafeoResult:
        with self._store.lock:
            graph = self._graph()
            result = graph.execute_cypher(query, _to_grafeo(params or {}))
            columns = list(result.columns)
            rows = [_from_grafeo(dict(row)) for row in result]
        return GrafeoResult(columns=columns, rows=rows)

    def _rows(self, query: str, params: dict[str, Any] | None = None) -> Rows:
        """Run *query* in this graph and return its rows as plain dicts."""
        return self._run(query, params).rows

    def execute(self, query: str, **kwargs: Any) -> GrafeoResult:
        """Run a Cypher query in this graph.

        Args:
            query: Cypher query text.
            **kwargs: Query parameters.

        Returns:
            The result: ``rows`` keyed by column, ``result_set`` positional.
        """
        return self._run(query, kwargs)

    def _write(self, query: str, rows: list[Any], *, name: str = "rows") -> None:
        """Run *query* over ``$<name>`` as one write."""
        self._rows(query, {name: rows})

    def close(self):
        """Release this connection; the database stays open for the process.

        :func:`graflo.db.grafeo.store.close_database` closes it.
        """

    # -- namespace and DDL -------------------------------------------------------

    def create_database(self, name: str):
        """Create the named graph *name*; an existing one is kept."""
        with self._store.lock:
            self.db.create_graph(name)

    def delete_database(self, name: str):
        """Drop the named graph *name* (this connection's graph when empty)."""
        target = name or self.config.database
        with self._store.lock:
            if target is None:
                self._clear_graph()
                return
            self.db.drop_graph(target)

    def _resolve_graph_name(self, schema: Schema) -> str:
        return self.config.database or schema.effective_namespace(DBType.GRAFEO)

    def ensure_target_namespace(self, schema: Schema, *, create: bool) -> None:
        """Ensure the named graph exists, and use it.

        Raises:
            NamespaceNotFoundError: The graph is missing and *create* is off.
        """
        name = self._resolve_graph_name(schema)
        with self._store.lock:
            if name not in self.db.list_graphs():
                if not create:
                    raise NamespaceNotFoundError(
                        f"Grafeo graph '{name}' does not exist. "
                        "Create it or call with create_namespace=True."
                    )
                self.db.create_graph(name)
        self.config.database = name

    def _node_count(self) -> int:
        if self._graph(create=False) is None:
            return 0
        rows = self._rows("MATCH (n) RETURN count(n) AS c")
        return int(rows[0]["c"]) if rows else 0

    def apply_target_schema(
        self,
        schema: Schema,
        *,
        recreate: bool,
        create_namespace: bool = True,
    ) -> None:
        """Define indexes for the schema; labels and relationship types are implicit.

        Raises:
            SchemaExistsError: The graph holds data and *recreate* is off.
        """
        assert_schema_supported(self.flavor, schema)
        self.report_edge_direction_support(schema)
        if self._node_count() > 0 and not recreate:
            raise SchemaExistsError(
                f"Schema/data already exists in Grafeo graph "
                f"'{self._resolve_graph_name(schema)}'. Set recreate_schema=True "
                "to replace, or use clear_data=True before ingestion."
            )
        if recreate:
            self._clear_graph()
        self.define_indexes(schema)

    def _clear_graph(self) -> None:
        """Remove every node, relationship and index of this graph."""
        self._drop_all_indexes()
        self._rows("MATCH (n) DETACH DELETE n")

    def _drop_all_indexes(self) -> None:
        for row in self._rows("SHOW INDEXES"):
            if row.get("name"):
                self._rows(
                    f"DROP INDEX {cypher_name(str(row['name']), quoted=True)} IF EXISTS"
                )

    def define_schema(self, schema: Schema):
        """Validate field types; labels and relationship types are implicit."""
        assert_schema_supported(self.flavor, schema)

    def define_vertex_indexes(
        self, vertex_config: VertexConfig, schema: Schema | None = None
    ):
        """Create each vertex label's indexes, the identity first.

        Args:
            vertex_config: Vertices whose indexes to create.
            schema: Supplies the identity and secondary indexes; without it,
                no identity is ensured.
        """
        if schema is None:
            logger.warning(
                "Schema is None: identity indexes cannot be ensured without schema"
            )
        db_vertex = (
            schema.resolve_db_aware(DBType.GRAFEO).vertex_config if schema else None
        )
        for vertex in vertex_config.vertex_set:
            indexes = list(
                schema.db_profile.vertex_secondary_indexes(vertex)
                if schema is not None
                else []
            )
            if db_vertex:
                identity = db_vertex.identity_fields(vertex)
                if identity and tuple(identity) not in {
                    tuple(i.fields) for i in indexes
                }:
                    indexes = [Index(fields=identity, unique=True), *indexes]
            label = db_vertex.vertex_dbname(vertex) if db_vertex else vertex
            for index in indexes:
                self._add_index(label, index)

    def define_edge_indexes(self, edges: list[Edge], schema: Schema | None = None):
        """Build nothing: Grafeo has no index on relationship properties.

        A declared edge index is refused when the schema is validated; the
        edge identity index GraFlo derives is not needed for any lookup.
        """

    def _add_index(self, label: str, index: Index) -> None:
        """A plain index on *label*, also for a unique one.

        Grafeo keeps uniqueness constraints in one catalog shared by every
        graph of the database, so a constraint would reach past this graph.
        The identity is kept unique by ``MERGE``, which runs under the store
        lock; a declared unique index is refused when the schema is validated.
        """
        if not index.fields:
            return
        name = cypher_name(f"{label}_{'_'.join(index.fields)}", quoted=True)
        target = f"(x:{cypher_name(label, quoted=True)})"
        fields = ", ".join(f"x.{cypher_name(f, quoted=True)}" for f in index.fields)
        self._rows(f"CREATE INDEX {name} IF NOT EXISTS FOR {target} ON ({fields})")

    def delete_graph_structure(
        self,
        vertex_types: tuple[str, ...] | list[str] = (),
        graph_names: tuple[str, ...] | list[str] = (),
        delete_all: bool = False,
    ) -> None:
        """Delete nodes of *vertex_types*, whole named graphs, or everything.

        Args:
            vertex_types: Labels whose nodes (and their relationships) to delete.
            graph_names: Named graphs to drop.
            delete_all: Remove every node, relationship and index of this
                connection's graph.
        """
        for name in graph_names:
            with self._store.lock:
                self.db.drop_graph(name)
        if vertex_types:
            for label in vertex_types:
                self._rows(
                    f"MATCH (n:{cypher_name(label, quoted=True)}) DETACH DELETE n"
                )
            return
        if delete_all and self._graph(create=False) is not None:
            self._clear_graph()

    def clear_data(self, schema: Schema) -> None:
        """Delete the nodes (and relationships) of the schema's labels."""
        vc = schema.resolve_db_aware(DBType.GRAFEO).vertex_config
        labels = tuple(vc.vertex_dbname(v) for v in vc.vertex_set)
        if labels:
            self.delete_graph_structure(vertex_types=labels)

    # -- bulk load ---------------------------------------------------------------

    def bulk_begin(
        self,
        schema: Schema,
        cfg: BulkConfig,
        *,
        recreate: bool = False,
        resume: bool = False,
    ) -> BulkPlan:
        """Guard the first write; indexes wait until the vertices are written.

        Grafeo runs one query at a time, so the load takes one chunk at a time.
        """
        assert_schema_supported(self.flavor, schema)
        self.report_edge_direction_support(schema)
        self.ensure_target_namespace(schema, create=True)
        if not resume:
            if recreate:
                self._clear_graph()
            elif self._node_count() > 0:
                raise SchemaExistsError(
                    f"Schema/data already exists in Grafeo graph "
                    f"'{self._resolve_graph_name(schema)}'. Set recreate_schema=True "
                    "to replace it."
                )
        return BulkPlan(max_parallel=1)

    def bulk_write_vertices(self, schema: Schema, chunk: BulkVertexChunk) -> None:
        """``CREATE`` the chunk's nodes; a replay ``MERGE``s them on their identity."""
        if chunk.replay:
            self._add_index(
                chunk.class_name, Index(fields=chunk.match_keys, unique=True)
            )
            self.upsert_docs_batch(chunk.rows, chunk.class_name, chunk.match_keys)
            return
        self._write(create_nodes_query(chunk.class_name), chunk.rows)

    def bulk_prepare_edges(self, schema: Schema) -> None:
        """Index every vertex identity, so edges find their endpoints."""
        vc = schema.resolve_db_aware(DBType.GRAFEO).vertex_config
        for vertex in vc.vertex_set:
            fields = vc.identity_fields(vertex)
            if fields:
                self._add_index(
                    vc.vertex_dbname(vertex), Index(fields=fields, unique=True)
                )

    def bulk_write_edges(self, schema: Schema, chunk: BulkEdgeChunk) -> None:
        """``CREATE`` the chunk's relationships; a replay ``MERGE``s them."""
        relation = chunk.relation_name
        if relation is None:
            raise ValueError(f"edge chunk {chunk.chunk_id} has no relation name")
        rows = [list(row) for row in chunk.rows]
        if chunk.replay:
            self.insert_edges_batch(
                rows,
                chunk.source_class,
                chunk.target_class,
                relation,
                tuple(chunk.match_keys_source),
                tuple(chunk.match_keys_target),
                relationship_merge_properties=chunk.merge_properties or None,
            )
            return
        query = create_relationships_query(
            chunk.source_class,
            chunk.target_class,
            relation,
            chunk.match_keys_source,
            chunk.match_keys_target,
        )
        self._write(query, rows)

    def bulk_finish(self, schema: Schema) -> BulkCounts:
        """Build the remaining indexes; count what the graph holds."""
        self.define_indexes(schema)
        return bulk_counts(lambda query: self._rows(query))

    # -- record sync -------------------------------------------------------------

    def sync_begin(self, schema: Schema, edge_types: list[SyncEdgeType]) -> None:
        """Index every vertex identity.

        An edge's owner property is not indexed: Grafeo indexes no
        relationship property.
        """
        assert_schema_supported(self.flavor, schema)
        self.bulk_prepare_edges(schema)

    def sync_ensure_vertices(self, schema: Schema, chunk: BulkVertexChunk) -> None:
        """``MERGE`` each vertex on its key; only a created one takes the row."""
        self._write(ensure_nodes_query(chunk.class_name, chunk.match_keys), chunk.rows)

    def sync_write_owners(self, schema: Schema, chunk: SyncOwnerChunk) -> SyncWritten:
        """Rewrite the changed owners in one transaction."""
        read = stored_hashes_query(chunk.class_name, chunk.match_keys, chunk.hash_field)
        stored = {
            row["i"]: row["h"] for row in self._rows(read, {"rows": hash_rows(chunk)})
        }
        changed = changed_owners(chunk, stored)
        statements = owner_statements(chunk, changed)
        if statements:
            with self._store.lock:
                tx = self._graph().begin_transaction()
                try:
                    for query, params in statements:
                        tx.execute_cypher(query, _to_grafeo(params))
                except BaseException:
                    tx.rollback()
                    raise
                tx.commit()
        return SyncWritten(
            written=len(changed), skipped=len(chunk.owners) - len(changed)
        )

    # -- writes ------------------------------------------------------------------

    def upsert_docs_batch(
        self,
        docs: list[dict[str, Any]],
        class_name: str,
        match_keys: list[str] | tuple[str, ...],
        **kwargs: Any,
    ) -> None:
        """``MERGE`` each document on *match_keys* and set its properties.

        Args:
            docs: Documents to upsert.
            class_name: Label to upsert into.
            match_keys: Properties a document is matched on.
            **kwargs: ``dry`` (build, do not run); ``union_fields`` (list
                properties written as the union of the stored and the given
                members rather than replaced).
        """
        dry = kwargs.pop("dry", False)
        union_fields: tuple[str, ...] = tuple(kwargs.pop("union_fields", ()) or ())
        if union_fields:
            query = union_upsert_query(class_name, match_keys, union_fields)
            params = {"data": union_upsert_rows(docs, union_fields)}
        else:
            keys = ", ".join(
                f"{cypher_name(k)}: row.{cypher_name(k)}" for k in match_keys
            )
            query = (
                "UNWIND $batch AS row "
                f"MERGE (n:{cypher_name(class_name)} {{ {keys} }}) "
                "SET n += row"
            )
            params = {"batch": docs}
        if not dry:
            self._rows(query, params)

    def insert_edges_batch(
        self,
        docs_edges: list[list[dict[str, Any]]] | list[Any] | None,
        source_class: str,
        target_class: str,
        relation_name: str,
        match_keys_source: tuple[str, ...],
        match_keys_target: tuple[str, ...],
        filter_uniques: bool = True,
        head: int | None = None,
        **kwargs: Any,
    ) -> None:
        """``MERGE`` relationships between nodes matched on their keys.

        Args:
            docs_edges: Rows ``[source_doc, target_doc]`` or
                ``[source_doc, target_doc, properties]``.
            source_class: Source label.
            target_class: Target label.
            relation_name: Relationship type.
            match_keys_source: Properties the source is matched on.
            match_keys_target: Properties the target is matched on.
            filter_uniques: Unused; ``MERGE`` keeps relationships unique.
            head: Write only the first *head* rows.
            **kwargs: See :func:`~graflo.db.conn.consume_insert_edges_kwargs`.
        """
        opts = consume_insert_edges_kwargs(kwargs)
        rows = list(docs_edges or [])
        if head is not None:
            rows = rows[:head]
        merge_props: tuple[str, ...] = tuple(opts.relationship_merge_properties or ())
        union = opts.union_fields
        where = " AND ".join(
            [f"source.{k} = row[0].{k}" for k in map(cypher_name, match_keys_source)]
            + [f"target.{k} = row[1].{k}" for k in map(cypher_name, match_keys_target)]
        )
        groups = (
            partition_by_absent_merge_props(
                rows,
                merge_props,
                props_of=lambda row: row[2] if len(row) > 2 else None,
                ends_of=lambda row: (row[0], row[1]),
                union=union,
            )
            if merge_props or union
            else [((), rows)]
        )
        for absent, group in groups:
            upsert = rel_upsert_clause(relation_name, merge_props, absent, union=union)
            if union:
                group = rel_union_rows(group, union)
            query = (
                "UNWIND $batch AS row "
                f"MATCH (source:{cypher_name(source_class)}), "
                f"(target:{cypher_name(target_class)}) WHERE {where} "
                f"{upsert}"
            )
            if not opts.dry:
                self._rows(query, {"batch": group})

    def insert_return_batch(
        self, docs: list[dict[str, Any]], class_name: str
    ) -> list[dict[str, Any]] | str:
        """Not supported: nodes are written with :meth:`upsert_docs_batch`.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(
            f"insert_return_batch is not supported by {type(self).__name__}; "
            "write nodes with upsert_docs_batch and read them back with fetch_docs"
        )

    # -- reads -------------------------------------------------------------------

    def fetch_docs(
        self,
        class_name: str,
        filters: list | dict | None = None,
        limit: int | None = None,
        return_keys: list | None = None,
        unset_keys: list | None = None,
        *,
        timeout_s: float | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Nodes of a label, optionally filtered, projected and limited.

        Args:
            class_name: Label to read.
            filters: Filter expression on the node (``n``).
            limit: Maximum number of nodes.
            return_keys: Properties to return; all when ``None``.
            unset_keys: Unused.
            timeout_s: Accepted and ignored (see ``supports_query_timeout``).
        """
        params = BoundParams(self.expression_flavor())
        where = ""
        if filters is not None:
            expression = parse_filter_expression(filters)
            where = f"WHERE {expression(doc_name='n', kind=params.kind, params=params)}"
        if return_keys is not None:
            keys = ", ".join(f".{cypher_name(k)}" for k in return_keys)
            projection = f"n {{ {keys} }}"
        else:
            projection = "properties(n)"
        limit_clause = f"LIMIT {limit}" if isinstance(limit, int) else ""
        query = (
            f"MATCH (n:{cypher_name(class_name)}) {where} "
            f"RETURN {projection} AS n {limit_clause}"
        )
        return [row["n"] for row in self._rows(query, params.values)]

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
        *,
        from_key: str = "id",
        to_key: str = "id",
        timeout_s: float | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Relationships at a node matched on *from_key*, as property dicts.

        Args:
            from_type: Anchor label.
            from_id: Value of the anchor's *from_key* property.
            edge_type: Relationship type to follow; any when ``None``.
            to_type: Label of the other endpoint.
            to_id: Value of the other endpoint's *to_key* property.
            filters: Filter expression on the relationship (``r``).
            limit: Maximum number of relationships.
            return_keys: Relationship properties to return; all when ``None``.
            unset_keys: Unused.
            direction: Orientations to follow from the anchor.
            from_key: Property the anchor is matched on.
            to_key: Property the other endpoint is matched on.
            timeout_s: Accepted and ignored (see ``supports_query_timeout``).
        """
        params: dict[str, Any] = {"from_id": from_id}
        source = cypher_keyed_node("source", from_type, from_key, "from_id")
        pattern = cypher_rel_pattern(edge_type, direction)
        target = f"(target:{cypher_name(to_type)})" if to_type else "(target)"
        conditions: list[str] = []
        if to_id:
            conditions.append(cypher_key_equals("target", to_key, "to_id"))
            params["to_id"] = to_id
        if filters is not None:
            bound = BoundParams(self.expression_flavor())
            expression = parse_filter_expression(filters)
            conditions.append(
                render_conjunct(expression, doc_name="r", kind=bound.kind, params=bound)
            )
            params.update(bound.values)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        if return_keys is not None:
            keys = ", ".join(f".{cypher_name(k)}" for k in return_keys)
            projection = f"r {{ {keys} }}"
        else:
            projection = "properties(r)"
        limit_clause = f"LIMIT {limit}" if limit else ""
        query = (
            f"MATCH {source}{pattern}{target} {where} "
            f"RETURN {projection} AS r {limit_clause}"
        )
        return [row["r"] for row in self._rows(query, params)]

    def fetch_present_documents(
        self,
        batch: list[dict[str, Any]],
        class_name: str,
        match_keys: list[str] | tuple[str, ...],
        keep_keys: list[str] | tuple[str, ...] | None = None,
        flatten: bool = False,
        filters: list[Any] | dict[str, Any] | None = None,
    ) -> list[dict[str, Any]] | dict[int, list[dict[str, Any]]]:
        """Stored nodes matching *batch*, by batch position; see :func:`present_documents`.

        With *flatten*, a list of the matches in batch order.
        """
        present = present_documents(
            self, batch, class_name, match_keys, keep_keys=keep_keys, filters=filters
        )
        if flatten:
            return [present[position][0] for position in sorted(present)]
        return present

    def keep_absent_documents(
        self,
        batch: list[dict[str, Any]],
        class_name: str,
        match_keys: list[str] | tuple[str, ...],
        keep_keys: list[str] | tuple[str, ...] | None = None,
        filters: list[Any] | dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """The documents of *batch* with no stored node; see :func:`absent_documents`."""
        return absent_documents(
            self, batch, class_name, match_keys, keep_keys=keep_keys, filters=filters
        )

    def aggregate(
        self,
        class_name: str,
        aggregation_function: AggregationType,
        discriminant: str | None = None,
        aggregated_field: str | None = None,
        filters: FilterExpression | list | dict | None = None,
        *,
        timeout_s: float | None = None,
    ) -> Any:
        """Aggregate over the nodes of a label; shaped as in :mod:`graflo.db.aggregation`.

        Raises:
            ValueError: *aggregated_field* is missing where it is required.
        """
        function = check_aggregation(aggregation_function, aggregated_field)
        params = BoundParams(self.expression_flavor())
        where = ""
        if filters is not None:
            expression = parse_filter_expression(filters)
            where = (
                f" WHERE {expression(doc_name='n', kind=params.kind, params=params)}"
            )
        value = (
            f"n.{cypher_name(aggregated_field, quoted=True)}"
            if aggregated_field
            else None
        )
        key = f"n.{cypher_name(discriminant, quoted=True)}" if discriminant else None
        query = (
            f"MATCH (n:{cypher_name(str(class_name), quoted=True)}){where} "
            + cypher_aggregation_return(function, value, key)
        )
        rows = self._rows(query, params.values)
        return aggregation_result(
            function,
            [(row.get(KEY), row[VALUE]) for row in rows],
            grouped=discriminant is not None,
        )

    # -- deletes -----------------------------------------------------------------

    def _data_runner(self) -> Callable[[str, list[Any]], Rows]:
        return lambda query, rows: self._rows(query, {"data": rows})

    def _params_runner(self) -> Callable[[str, dict[str, Any]], Rows]:
        return lambda query, params: self._rows(query, params)

    def delete_vertices(
        self,
        class_name: str,
        key_docs: list[dict[str, Any]],
        match_keys: tuple[str, ...],
        *,
        chunk_size: int = DEFAULT_DELETE_CHUNK_SIZE,
    ) -> None:
        """Remove nodes with ``DETACH DELETE``, which removes their relationships too."""
        delete_nodes(self._data_runner(), class_name, key_docs, match_keys, chunk_size)

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
        """Remove the relationships of one type between the given node pairs."""
        delete_relationships(
            self._data_runner(),
            source_class,
            target_class,
            relation_name,
            endpoints,
            match_keys_source,
            match_keys_target,
            chunk_size,
        )

    def fetch_edge_targets(
        self,
        source_class: str,
        target_class: str,
        relation_name: str | None,
        sources: list[dict[str, Any]],
        match_keys_source: tuple[str, ...],
        match_keys_target: tuple[str, ...],
        *,
        collection_name: str | None = None,
        chunk_size: int = DEFAULT_DELETE_CHUNK_SIZE,
    ) -> dict[int, list[dict[str, Any]]]:
        """The stored targets of each source's relationships of one type."""
        return relationship_targets(
            lambda query, rows: [
                list(row.values()) for row in self._rows(query, {"data": rows})
            ],
            source_class,
            target_class,
            relation_name,
            sources,
            match_keys_source,
            match_keys_target,
            chunk_size,
        )

    def delete_scoped_vertices(self, class_name: str, field: str, value: Any) -> None:
        """Remove the nodes whose *field* equals *value*, with ``DETACH DELETE``."""
        delete_scoped_nodes(self._params_runner(), class_name, field, value)

    def delete_scoped_edges(
        self,
        source_class: str,
        target_class: str,
        relation_name: str | None,
        field: str,
        value: Any,
        *,
        collection_name: str | None = None,
    ) -> None:
        """Remove the relationships of one type whose *field* equals *value*."""
        delete_scoped_relationships(
            self._params_runner(),
            source_class,
            target_class,
            relation_name,
            field,
            value,
        )

    # -- introspection, traversal, export ---------------------------------------

    def introspect_graph_schema(
        self,
        schema_name: str | None = None,
        *,
        sample_limit: int = 100,
    ) -> Schema:
        """Infer a Schema from the graph's labels and sampled properties."""
        from graflo.db.cypher.introspection import collect_cypher_introspection

        name = schema_name or self.config.database or "grafeo"

        def run(query: str, keys: Sequence[str]) -> Rows:
            return self._rows(query)

        introspection = collect_cypher_introspection(
            name=name, run=run, sample_limit=sample_limit
        )
        return GraphSchemaInferencer(db_flavor=DBType.GRAFEO).infer_schema(
            introspection, schema_name=name
        )

    def _graph_neighbors(
        self,
        vertex_type: str,
        key: str | dict[str, Any],
        *,
        hops: int = 1,
        direction: EdgeDirection = EdgeDirection.OUT,
        edge_types: Sequence[str] | None = None,
        filters: Any | None = None,
        limit: int | None = None,
        schema: Schema | None = None,
        guard: FilterExpression | None = None,
        timeout_s: float | None = None,
    ) -> GraphContainer:
        """Bounded neighbourhood via one variable-length Cypher pattern."""
        return cypher_graph_neighbors(
            self,
            vertex_type=vertex_type,
            key=key,
            hops=hops,
            direction=direction,
            edge_types=edge_types,
            limit=limit,
            schema=schema,
            run=self._params_runner(),
            filters=filters,
            guard=guard,
        )

    def fetch_all_docs(
        self,
        class_name: str,
        *,
        limit: int | None = None,
        timeout_s: float | None = None,
    ) -> list[dict[str, Any]]:
        """Every node of a label, as property dicts."""
        limit_clause = f"LIMIT {limit}" if limit is not None else ""
        query = (
            f"MATCH (n:{cypher_name(class_name, quoted=True)}) "
            f"RETURN properties(n) AS props {limit_clause}"
        )
        return [
            strip_internal_properties(row["props"])
            for row in self._rows(query)
            if row.get("props")
        ]

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
        """Every relationship between two labels, as ``[source, target, properties]``."""
        relation = (
            f":{cypher_name(relation_name, quoted=True)}" if relation_name else ""
        )
        limit_clause = f"LIMIT {limit}" if limit is not None else ""
        query = (
            f"MATCH (source:{cypher_name(source_class, quoted=True)})"
            f"-[r{relation}]->(target:{cypher_name(target_class, quoted=True)}) "
            "RETURN properties(source) AS source_props, "
            "properties(target) AS target_props, properties(r) AS edge_props "
            f"{limit_clause}"
        )
        return [
            [
                strip_internal_properties(row.get("source_props") or {}),
                strip_internal_properties(row.get("target_props") or {}),
                strip_internal_properties(row.get("edge_props") or {}),
            ]
            for row in self._rows(query)
        ]
