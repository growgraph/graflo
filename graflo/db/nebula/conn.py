"""NebulaGraph connection implementation.

Supports NebulaGraph 3.x (nGQL via ``nebula3-python``) and 5.x (ISO GQL via
``nebula5-python``).  The version is selected by ``NebulaConfig.version``.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Sequence
from typing import Any, ClassVar

from graflo.architecture.graph_types import EdgeDirection, Index
from graflo.architecture.schema import Schema
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.vertex import Field, FieldType, VertexConfig
from graflo.connections.bulk import BulkConfig
from graflo.connections.onto import NebulaConfig
from graflo.db.aggregation import (
    KEY,
    VALUE,
    aggregation_result,
    check_aggregation,
)
from graflo.db.bulk import (
    BulkCounts,
    BulkEdgeChunk,
    BulkPlan,
    BulkVertexChunk,
    StoredEdgeId,
)
from graflo.db.conn import (
    DEFAULT_DELETE_CHUNK_SIZE,
    Connection,
    NamespaceNotFoundError,
    SchemaExistsError,
    consume_insert_edges_kwargs,
    deletable_docs,
    deletable_endpoints,
)
from graflo.db.field_type_support import assert_schema_supported
from graflo.db.nebula.adapter import (
    NebulaClientAdapter,
    NebulaResultSet,
    create_adapter,
)
from graflo.db.nebula.query import (
    EdgeRow,
    aggregate_gql,
    aggregate_ngql,
    batch_upsert_vertices_ngql,
    create_edge_index_ngql,
    create_edge_type_ngql,
    create_space_ngql,
    create_tag_index_ngql,
    create_tag_ngql,
    delete_edges_ngql,
    drop_space_ngql,
    fetch_docs_gql,
    fetch_docs_ngql,
    fetch_edges_ngql,
    fold_rows,
    insert_edges_ngql,
    insert_vertices_ngql,
    upsert_edge_ngql,
)
from graflo.db.nebula.util import (
    edge_rank,
    escape_nebula_string,
    field_type_from_nebula,
    fitted_vid_length,
    is_nebula_string_field,
    key_of_vid,
    make_vid,
    render_filters_cypher,
    render_filters_ngql,
    tag_vid,
    vertex_key,
    wait_for_space_ready,
)
from graflo.filter.onto import FilterExpression, render_conjunct
from graflo.onto import AggregationType, DBType, ExpressionFlavor

logger = logging.getLogger(__name__)

_SCHEMA_WAIT_INTERVAL = 1.0
_SCHEMA_WAIT_RETRIES = 30
#: How long a job that ends FAILED or STOPPED is submitted again. storaged
#: learns a new index only at its next metad heartbeat (``heartbeat_interval_secs``,
#: 10 s by default), and a rebuild submitted before then fails with
#: E_INDEX_NOT_FOUND; this covers three heartbeats.
_JOB_RETRY_SECONDS = 30.0
#: First ``SHOW JOB`` poll; doubles up to ``_SCHEMA_WAIT_INTERVAL``.
_JOB_POLL_FIRST = 0.05
_JOB_DONE = "FINISHED"
_JOB_ENDED = frozenset({"FINISHED", "FAILED", "STOPPED"})
#: Statements joined into one request on the upsert path.
_UPSERTS_PER_REQUEST = 50


def edge_type_name(edge: Edge) -> str:
    """The NebulaGraph edge type an edge is stored under."""
    return edge.relation or f"{edge.source}_{edge.target}"


def edge_types(edges: Sequence[Edge]) -> dict[str, list[Field]]:
    """Every edge type with its fields: the union over the edges stored under it."""
    types: dict[str, list[Field]] = {}
    for edge in edges:
        fields = types.setdefault(edge_type_name(edge), [])
        known = {f.name for f in fields}
        fields.extend(f for f in edge.properties if f.name not in known)
    return types


def tag_fields(schema: Schema) -> dict[str, list[str]]:
    """Every tag of *schema*, by the name it is created under, with its fields."""
    logical = schema.core_schema.vertex_config
    db_aware = schema.resolve_db_aware(DBType.NEBULA).vertex_config
    return {
        db_aware.vertex_dbname(v): [f.name for f in logical.properties(v)]
        for v in logical.vertex_set
    }


#: ``(tag or edge type, index name, CREATE statement)``.
IndexSpec = tuple[str, str, str]


def tag_index_specs(
    vertex_config: VertexConfig, schema: Schema | None
) -> list[IndexSpec]:
    """The tag indexes of *vertex_config*: identity first, then declared ones.

    NebulaGraph needs a tag index for ``LOOKUP`` and a label-only ``MATCH``,
    so the identity is indexed even where no index is declared.
    """
    # Index the tag under the name it was created with; the db_profile
    # lookups stay keyed on the logical name.
    db_aware = (
        schema.resolve_db_aware(DBType.NEBULA).vertex_config
        if schema is not None
        else None
    )
    specs: list[IndexSpec] = []
    for vname in sorted(vertex_config.vertex_set):
        tag = db_aware.vertex_dbname(vname) if db_aware is not None else vname
        fields = vertex_config.properties(vname)
        string_fields = {f.name for f in fields if is_nebula_string_field(f)}
        declared = (
            schema.db_profile.vertex_secondary_indexes(vname)
            if schema is not None
            else []
        )
        identity = Index(fields=vertex_config.identity_fields(vname), unique=True)
        seen: set[tuple[str, ...]] = set()
        for idx in [identity, *declared]:
            key = tuple(str(f) for f in idx.fields)
            if not key or key in seen:
                continue
            seen.add(key)
            name = f"idx_{tag}_{'_'.join(key)}"
            stmt = create_tag_index_ngql(
                name, tag, list(key), string_fields=string_fields
            )
            specs.append((tag, name, stmt))
    return specs


def edge_index_specs(edges: list[Edge], schema: Schema | None) -> list[IndexSpec]:
    """The declared edge indexes of *edges*, each index once."""
    specs: list[IndexSpec] = []
    seen: set[str] = set()
    for edge in edges:
        rel = edge_type_name(edge)
        declared = (
            schema.db_profile.edge_secondary_indexes(edge.edge_id)
            if schema is not None
            else []
        )
        for idx in declared:
            key = [str(f) for f in idx.fields]
            name = f"idx_{rel}_{'_'.join(key)}"
            if name in seen:
                continue
            seen.add(name)
            specs.append((rel, name, create_edge_index_ngql(name, rel, key)))
    return specs


class NebulaConnection(Connection):
    """NebulaGraph implementation of the ``Connection`` interface.

    Automatically selects the correct Python driver and query language based on
    ``config.version``:

    * **v3.x** -- ``nebula3-python``, nGQL
    * **v5.x** -- ``nebula5-python``, ISO GQL / Cypher

    Does not declare ``supports_query_timeout``: no per-statement time limit
    reaches the server, so ``timeout_s`` on a read is accepted and ignored.
    """

    flavor: ClassVar[DBType] = DBType.NEBULA
    supports_schema_introspection: ClassVar[bool] = True
    # Properties and their types come from `DESCRIBE`, but nGQL records no
    # endpoint tags on an edge type, so those must be observed on real edges.
    # Half-sampled is still sampled: an edge type with no stored edges is lost.
    schema_introspection_is_sampled: ClassVar[bool] = True
    # A VID names its tag (`util.tag_vid`), so removing one removes one vertex.
    supports_instance_delete: ClassVar[bool] = True
    # Edges address their endpoints by VID and lock nothing, so neither dense
    # ids nor edge rounds are needed.
    supports_bulk_load: ClassVar[bool] = True
    # No per-statement time limit reaches the server (the client's timeout is
    # a socket timeout, which cancels nothing), so ``timeout_s`` is accepted
    # and ignored.
    supports_query_timeout: ClassVar[bool] = False

    def __init__(self, config: NebulaConfig):
        super().__init__()
        self.config = config
        self._adapter: NebulaClientAdapter = create_adapter(config)
        self._space_name: str | None = None
        self._tag_fields: dict[str, list[str]] = {}
        self._edge_fields: dict[str, list[str]] = {}
        # Tags whose describe failure was already warned about on this connection.
        self._undescribed: set[str] = set()

        if config.schema_name:
            # A missing space is legitimate here — define_schema creates it —
            # so this is not fatal. But swallowing it silently also hides a bad
            # credential or an unreachable graphd behind the first confusing
            # error much later, so say what happened.
            try:
                self._use_space(config.schema_name)
            except Exception as e:
                logger.debug(
                    "Could not select space '%s' at connect time: %s. "
                    "It will be created by define_schema if missing.",
                    config.schema_name,
                    e,
                )

    # ------------------------------------------------------------------
    # Expression flavour override (instance-level, depends on version)
    # ------------------------------------------------------------------

    @classmethod
    def expression_flavor(cls) -> ExpressionFlavor:
        return ExpressionFlavor.NGQL

    def _expression_flavor(self) -> ExpressionFlavor:
        """Instance-level flavour dispatch."""
        if self.config.is_v3:
            return ExpressionFlavor.NGQL
        return ExpressionFlavor.CYPHER

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _execute(self, statement: str) -> NebulaResultSet:
        return self._adapter.execute(statement)

    def _use_space(self, space_name: str) -> None:
        self._adapter.use_space(space_name)
        self._space_name = space_name
        self._tag_fields = {}
        self._edge_fields = {}

    def _wait_for_dml_ready(self, tag_name: str) -> None:
        """Wait until DML operations are possible on a tag.

        NebulaGraph's storaged schema cache may lag behind graphd's metadata
        cache by several heartbeat cycles (~10 s with default settings).
        ``DESCRIBE TAG`` succeeds immediately, but DML like ``FETCH PROP`` or
        ``UPSERT VERTEX`` fails until the storage cache is warm.
        """
        check = f'FETCH PROP ON `{tag_name}` "__dml_check__" YIELD properties(vertex)'
        for attempt in range(_SCHEMA_WAIT_RETRIES):
            try:
                self._adapter.execute(check)
                logger.debug(
                    "DML ready for tag '%s' after %d attempt(s)", tag_name, attempt + 1
                )
                return
            except Exception:
                if attempt == _SCHEMA_WAIT_RETRIES - 1:
                    logger.warning(
                        "DML readiness check for tag '%s' did not succeed "
                        "after %d attempts",
                        tag_name,
                        _SCHEMA_WAIT_RETRIES,
                    )
                time.sleep(_SCHEMA_WAIT_INTERVAL)

    def _wait_for_edge_dml_ready(self, edge_type: str) -> None:
        """Wait until DML operations are possible on an edge type."""
        check = (
            f'FETCH PROP ON `{edge_type}` "__src__"->"__dst__" YIELD properties(edge)'
        )
        for attempt in range(_SCHEMA_WAIT_RETRIES):
            try:
                self._adapter.execute(check)
                logger.debug(
                    "DML ready for edge '%s' after %d attempt(s)",
                    edge_type,
                    attempt + 1,
                )
                return
            except Exception:
                if attempt == _SCHEMA_WAIT_RETRIES - 1:
                    logger.warning(
                        "DML readiness check for edge '%s' did not succeed "
                        "after %d attempts",
                        edge_type,
                        _SCHEMA_WAIT_RETRIES,
                    )
                time.sleep(_SCHEMA_WAIT_INTERVAL)

    def _render_filter(self, filters: Any, doc_name: str) -> str:
        if self.config.is_v3:
            return render_filters_ngql(filters, doc_name)
        return render_filters_cypher(filters, doc_name)

    def _tag_field_names(self, tag_name: str) -> list[str]:
        """The fields of *tag_name*, described on first use and then cached.

        Described per tag rather than for the whole space at connect, so a
        connection's cost does not grow with the number of tags. A tag that
        cannot be described has no fields and is described again next time.
        """
        fields = self._tag_fields.get(tag_name)
        if fields is not None:
            return fields
        try:
            desc = self._adapter.execute(f"DESCRIBE TAG `{tag_name}`")
        except Exception as e:
            # Warned once per tag and connection; a write per batch retries it.
            level = logging.DEBUG if tag_name in self._undescribed else logging.WARNING
            self._undescribed.add(tag_name)
            logger.log(
                level,
                "Could not describe tag '%s' in space '%s': %s. "
                "Writes to this tag may omit properties.",
                tag_name,
                self._space_name,
                e,
            )
            return []
        fields = [r.get("Field", r.get("field", "")) for r in desc.rows_as_dicts()]
        self._tag_fields[tag_name] = fields
        return fields

    def _edge_field_names(self, edge_type: str) -> list[str] | None:
        """The fields of *edge_type*, cached; ``None`` when it cannot be described."""
        fields = self._edge_fields.get(edge_type)
        if fields is not None:
            return fields
        try:
            desc = self._adapter.execute(f"DESCRIBE EDGE `{edge_type}`")
        except Exception as e:
            logger.warning(
                "Could not describe edge type '%s' in space '%s': %s",
                edge_type,
                self._space_name,
                e,
            )
            return None
        fields = [r.get("Field", r.get("field", "")) for r in desc.rows_as_dicts()]
        self._edge_fields[edge_type] = fields
        return fields

    # ------------------------------------------------------------------
    # Connection ABC – lifecycle
    # ------------------------------------------------------------------

    def execute(self, query: str | Any, **kwargs: Any) -> Any:
        rs = self._execute(str(query))
        return rs

    def close(self) -> None:
        self._adapter.close()

    # ------------------------------------------------------------------
    # Database (space) management
    # ------------------------------------------------------------------

    def _ensure_storage_hosts(self) -> None:
        """Register storaged hosts if not already present (v3.x requirement)."""
        if not self.config.storaged_addresses:
            return
        try:
            rs = self._adapter.execute("SHOW HOSTS")
            existing = {
                f"{r.get('Host', '')}:{r.get('Port', '')}" for r in rs.rows_as_dicts()
            }
        except Exception:
            existing = set()

        for addr in self.config.storaged_addresses:
            if addr not in existing:
                try:
                    host, port = addr.rsplit(":", 1)
                    self._adapter.execute(f'ADD HOSTS "{host}":{port}')
                    logger.info("Registered storage host %s", addr)
                except Exception:
                    logger.debug("ADD HOSTS %s (may already exist)", addr)

        for _ in range(30):
            try:
                rs = self._adapter.execute("SHOW HOSTS")
                statuses = [r.get("Status", "") for r in rs.rows_as_dicts()]
                if statuses and all(s == "ONLINE" for s in statuses):
                    return
            except Exception:
                pass
            time.sleep(1)
        logger.warning("Storage hosts may not all be ONLINE yet")

    def _storage_host_count(self) -> int:
        """Storage hosts online; 1 when ``SHOW HOSTS`` cannot be read."""
        try:
            rows = self._execute("SHOW HOSTS").rows_as_dicts()
        except Exception:
            logger.debug("SHOW HOSTS failed", exc_info=True)
            return 1
        return max(1, sum(1 for r in rows if str(r.get("Status", "")) == "ONLINE"))

    def _wait_for_leaders(self) -> None:
        """Wait until every partition of the current space has a leader.

        A write to a partition without one fails, and a new space elects
        them one by one.
        """
        for _ in range(_SCHEMA_WAIT_RETRIES):
            try:
                rows = self._execute("SHOW PARTS").rows_as_dicts()
                if rows and all(r.get("Leader") for r in rows):
                    return
            except Exception:
                logger.debug("SHOW PARTS failed", exc_info=True)
            time.sleep(_SCHEMA_WAIT_INTERVAL)
        logger.warning(
            "Space '%s' may have partitions without a leader", self._space_name
        )

    def create_database(self, name: str, *, schema: Schema | None = None) -> None:
        """Create space *name* with :meth:`NebulaConfig.space_options`.

        Args:
            name: The space.
            schema: Sizes the VID to its identities; without it, to one string.
        """
        self._ensure_storage_hosts()
        options = self.config.space_options(
            storage_hosts=self._storage_host_count(),
            vid_length=fitted_vid_length(schema, self.config.vid_string_length),
        )
        self._execute(
            create_space_ngql(
                name,
                vid_type=options.vid_type,
                partition_num=options.partition_num,
                replica_factor=options.replica_factor,
            )
        )
        wait_for_space_ready(
            self._adapter,
            name,
            max_retries=_SCHEMA_WAIT_RETRIES,
            interval=_SCHEMA_WAIT_INTERVAL,
        )
        self._use_space(name)
        self._wait_for_leaders()
        logger.info("Created NebulaGraph space '%s' (%s)", name, options)

    def delete_database(self, name: str) -> None:
        self._execute(drop_space_ngql(name))
        logger.info("Dropped NebulaGraph space '%s'", name)

    # ------------------------------------------------------------------
    # Schema definition
    # ------------------------------------------------------------------

    def define_schema(self, schema: Schema) -> None:
        """Create the tags and edge types, then wait once until both are writable.

        Neither depends on the other, so they share one wait for storaged's
        next heartbeat instead of one wait each.
        """
        assert_schema_supported(self.flavor, schema)
        tag = self._create_tags(schema)
        edge_type = self._create_edge_types(
            list(schema.core_schema.edge_config.values())
        )
        self._wait_until_writable(tag, edge_type)

    def define_vertex_classes(self, schema: Schema) -> None:
        """Create one tag per vertex type and wait until it is writable."""
        self._wait_until_writable(self._create_tags(schema), None)

    def define_edge_classes(self, edges: list[Edge]) -> None:
        """Create one edge type per relation and wait until it is writable."""
        self._wait_until_writable(None, self._create_edge_types(edges))

    def _create_tags(self, schema: Schema) -> str | None:
        """Create one tag per vertex type, named as the rest of this class reads it.

        The tag name comes from the db-aware projection, not the logical vertex
        name: reads, writes and ``clear_data`` all resolve through
        ``vertex_dbname``, so declaring a tag under the logical name instead
        leaves a schema nothing writes to, and every UPSERT fails with
        ``No schema found`` on any manifest that sets ``vertex_storage_names``.

        Returns:
            One tag created, to wait on; None when there is none.
        """
        logical = schema.core_schema.vertex_config
        db_aware = schema.resolve_db_aware(DBType.NEBULA).vertex_config
        created: str | None = None
        for vname in logical.vertex_set:
            tag = db_aware.vertex_dbname(vname)
            fields = logical.properties(vname)
            self._execute(create_tag_ngql(tag, fields))
            self._tag_fields[tag] = [f.name for f in fields]
            created = created or tag
            logger.debug("Created tag '%s' for vertex '%s'", tag, vname)
        return created

    def _create_edge_types(self, edges: list[Edge]) -> str | None:
        """Create one edge type per relation, with every field an edge of it has.

        Returns:
            One edge type created, to wait on; None when there is none.
        """
        created = edge_types(edges)
        for rel, fields in created.items():
            self._execute(create_edge_type_ngql(rel, fields))
            logger.debug("Created edge type '%s'", rel)
        return next(iter(created), None)

    def _wait_until_writable(self, tag: str | None, edge_type: str | None) -> None:
        """Wait until storaged accepts DML on *tag* and *edge_type*.

        Types created together reach storaged at the same heartbeat, so one of
        each kind stands for the rest.
        """
        if tag is not None:
            self._wait_for_dml_ready(tag)
        if edge_type is not None:
            self._wait_for_edge_dml_ready(edge_type)

    # ------------------------------------------------------------------
    # Index management
    # ------------------------------------------------------------------

    def define_vertex_indexes(
        self, vertex_config: VertexConfig, schema: Schema | None = None
    ) -> None:
        if schema is None:
            logger.warning(
                "Schema is None: identity indexes cannot be ensured without schema"
            )
        self._create_indexes("TAG", tag_index_specs(vertex_config, schema))

    def define_edge_indexes(
        self, edges: list[Edge], schema: Schema | None = None
    ) -> None:
        self._create_indexes("EDGE", edge_index_specs(edges, schema))

    def _create_indexes(self, kind: str, specs: list[IndexSpec]) -> None:
        """Create indexes, then build them in one job; a failure is logged, not raised."""
        created: list[IndexSpec] = []
        for spec in specs:
            try:
                self._execute(spec[2])
                created.append(spec)
            except Exception as e:
                self._warn_index_missing(kind, spec, e)
        if not created:
            return
        try:
            self._rebuild_indexes(kind, [name for _, name, _ in created])
        except Exception as e:
            for spec in created:
                self._warn_index_missing(kind, spec, e)

    @staticmethod
    def _warn_index_missing(kind: str, spec: IndexSpec, error: Exception) -> None:
        # Not debug: without this index every filtered read on it fails
        # with IndexNotFound, and the cause is three layers away.
        owner, name, _ = spec
        logger.warning(
            "%s index '%s' on '%s' was not built: %s. Filtered reads on "
            "these fields will fail with IndexNotFound.",
            kind.capitalize(),
            name,
            owner,
            error,
        )

    def _rebuild_indexes(self, kind: str, names: list[str]) -> None:
        """Build *names* in one ``REBUILD`` job, run to completion.

        Raises:
            RuntimeError: The job never finished.
        """
        listed = ", ".join(f"`{name}`" for name in names)
        self._run_job(f"REBUILD {kind} INDEX {listed}")

    # ------------------------------------------------------------------
    # init_db
    # ------------------------------------------------------------------

    def _resolve_space_name(self, schema: Schema) -> str:
        space_name = self.config.schema_name
        if not space_name:
            space_name = schema.effective_namespace(DBType.NEBULA)
            self.config.schema_name = space_name
        return space_name

    def _space_exists(self, space_name: str) -> bool:
        try:
            rs = self._adapter.execute("SHOW SPACES")
            names = {row.get("Name", row.get("name", "")) for row in rs.rows_as_dicts()}
            return space_name in names
        except Exception:
            try:
                self._adapter.use_space(space_name)
                return True
            except Exception:
                return False

    def ensure_target_namespace(self, schema: Schema, *, create: bool) -> None:
        """Ensure the NebulaGraph space exists."""
        space_name = self._resolve_space_name(schema)
        if self._space_exists(space_name):
            wait_for_space_ready(
                self._adapter,
                space_name,
                max_retries=_SCHEMA_WAIT_RETRIES,
                interval=_SCHEMA_WAIT_INTERVAL,
            )
            self._use_space(space_name)
            return
        if not create:
            raise NamespaceNotFoundError(
                f"NebulaGraph space '{space_name}' does not exist. "
                "Create it manually or call with create_namespace=True."
            )
        self.create_database(space_name, schema=schema)

    def _schema_has_artifacts(self) -> bool:
        """Whether the current space holds a tag or an edge type."""
        for statement in ("SHOW TAGS", "SHOW EDGES"):
            try:
                if self._execute(statement).rows_as_dicts():
                    return True
            except Exception:
                logger.debug("%s failed", statement, exc_info=True)
        return False

    def apply_target_schema(
        self,
        schema: Schema,
        *,
        recreate: bool,
        create_namespace: bool = True,
    ) -> None:
        """Define tags, edge types, and indexes in the current space."""
        self.report_edge_direction_support(schema)
        space_name = self._resolve_space_name(schema)
        # The selected target space with no schema is already what a recreate
        # would make: dropping and creating it again costs two heartbeats.
        if recreate and (
            self._space_name != space_name or self._schema_has_artifacts()
        ):
            if create_namespace:
                try:
                    self.delete_database(space_name)
                except Exception:
                    pass
                self.create_database(space_name, schema=schema)
            else:
                try:
                    rs = self._execute("SHOW TAGS")
                    for row in rs.rows_as_dicts():
                        tag = row.get("Name", row.get("name", ""))
                        if tag:
                            self._execute(f"DROP TAG IF EXISTS `{tag}`")
                except Exception as e:
                    logger.warning("Failed to drop tags during recreate: %s", e)
        elif not recreate and self._schema_has_artifacts():
            raise SchemaExistsError(
                f"Schema already exists in space '{space_name}'. "
                "Set recreate_schema=True to replace."
            )
        self.define_schema(schema)
        self.define_indexes(schema)

    def init_db(
        self,
        schema: Schema,
        recreate_schema: bool = False,
        *,
        create_namespace: bool = True,
    ) -> None:
        """Convenience wrapper: ensure space then apply schema."""
        self.ensure_target_namespace(schema, create=create_namespace)
        self.apply_target_schema(
            schema, recreate=recreate_schema, create_namespace=create_namespace
        )

    # ------------------------------------------------------------------
    # Bulk load (graflo.db.bulk)
    # ------------------------------------------------------------------

    def bulk_begin(
        self,
        schema: Schema,
        cfg: BulkConfig,
        *,
        recreate: bool = False,
        resume: bool = False,
    ) -> BulkPlan:
        """Space, tags and edge types; indexes wait for :meth:`bulk_finish`.

        Writes take no lock and read nothing, so the load runs at the
        configured parallelism.
        """
        assert_schema_supported(self.flavor, schema)
        self.report_edge_direction_support(schema)
        space_name = self._resolve_space_name(schema)
        if recreate and not resume:
            self.delete_database(space_name)
            self.create_database(space_name, schema=schema)
        else:
            self.ensure_target_namespace(schema, create=True)
            if not resume and self._schema_has_artifacts():
                raise SchemaExistsError(
                    f"Schema already exists in space '{space_name}'. "
                    "Set recreate_schema=True to replace."
                )
        # IF NOT EXISTS throughout: on resume only what is missing is created.
        self.define_schema(schema)
        for tag in tag_fields(schema):
            self._wait_for_dml_ready(tag)
        for edge_type in edge_types(list(schema.core_schema.edge_config.values())):
            self._wait_for_edge_dml_ready(edge_type)
        return BulkPlan()

    def _use_schema_space(self, schema: Schema) -> None:
        if self._space_name is None:
            self._use_space(self._resolve_space_name(schema))

    def bulk_write_vertices(self, schema: Schema, chunk: BulkVertexChunk) -> None:
        """``INSERT VERTEX`` the chunk; an insert overwrites, so a replay is harmless."""
        self._use_schema_space(schema)
        columns = tag_fields(schema).get(chunk.class_name)
        if columns is None:
            raise ValueError(
                f"vertex chunk {chunk.chunk_id}: no tag {chunk.class_name!r}"
            )
        rows = [
            (make_vid(chunk.class_name, row, chunk.match_keys), row)
            for row in chunk.rows
        ]
        for stmt in insert_vertices_ngql(
            chunk.class_name, columns, rows, max_bytes=self.config.max_statement_bytes
        ):
            self._execute(stmt)

    def bulk_prepare_edges(self, schema: Schema) -> None:
        """Nothing: an edge names its endpoints by VID, computed from their identity."""

    def bulk_write_edges(self, schema: Schema, chunk: BulkEdgeChunk) -> None:
        """``INSERT EDGE`` the chunk, ranked as the upsert path ranks edges."""
        relation = chunk.relation_name
        if relation is None:
            raise ValueError(f"edge chunk {chunk.chunk_id} has no relation name")
        self._use_schema_space(schema)
        fields = edge_types(list(schema.core_schema.edge_config.values())).get(relation)
        if fields is None:
            raise ValueError(f"edge chunk {chunk.chunk_id}: no edge type {relation!r}")
        edges: list[EdgeRow] = [
            (
                make_vid(chunk.source_class, source, chunk.match_keys_source),
                make_vid(chunk.target_class, target, chunk.match_keys_target),
                edge_rank(props, chunk.merge_properties),
                props,
            )
            for source, target, props in chunk.rows
        ]
        for stmt in insert_edges_ngql(
            relation,
            [f.name for f in fields],
            edges,
            max_bytes=self.config.max_statement_bytes,
        ):
            self._execute(stmt)

    def bulk_finish(self, schema: Schema) -> BulkCounts:
        """Create the indexes, build them over the load, and count what is stored."""
        self._use_schema_space(schema)
        edges = list(schema.core_schema.edge_config.values())
        tag_specs = tag_index_specs(schema.core_schema.vertex_config, schema)
        edge_specs = edge_index_specs(edges, schema)
        for _, _, stmt in [*tag_specs, *edge_specs]:
            self._execute(stmt)
        if tag_specs:
            self._rebuild_indexes("TAG", [name for _, name, _ in tag_specs])
        if edge_specs:
            self._rebuild_indexes("EDGE", [name for _, name, _ in edge_specs])
        self._run_job("SUBMIT JOB STATS")
        return self._stored_counts(schema)

    def _submit_job(self, statement: str) -> int:
        """Submit a job; retried while a new index has not yet reached graphd."""
        for attempt in range(_SCHEMA_WAIT_RETRIES):
            try:
                rows = self._execute(statement).rows_as_dicts()
                return int(rows[0]["New Job Id"])
            except Exception:
                if attempt == _SCHEMA_WAIT_RETRIES - 1:
                    raise
                time.sleep(_SCHEMA_WAIT_INTERVAL)
        raise AssertionError("unreachable")

    def _await_job(self, job_id: int) -> str:
        """Poll ``SHOW JOB`` until the job ends; return its status."""
        delay = _JOB_POLL_FIRST
        while True:
            rows = self._execute(f"SHOW JOB {job_id}").rows_as_dicts()
            status = str(rows[0].get("Status", "")).upper() if rows else ""
            if status in _JOB_ENDED:
                return status
            time.sleep(delay)
            delay = min(delay * 2, _SCHEMA_WAIT_INTERVAL)

    def _run_job(self, statement: str) -> None:
        """Run a job to completion, submitting it again while it fails.

        Raises:
            RuntimeError: It still failed after ``_JOB_RETRY_SECONDS``.
        """
        deadline = time.monotonic() + _JOB_RETRY_SECONDS
        while True:
            job_id = self._submit_job(statement)
            status = self._await_job(job_id)
            if status == _JOB_DONE:
                return
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"{statement} still ended {status} after {_JOB_RETRY_SECONDS:g} s"
                )
            logger.debug(
                "Job %s (%s) ended %s; submitting again", job_id, statement, status
            )
            time.sleep(_SCHEMA_WAIT_INTERVAL)

    def _stored_counts(self, schema: Schema) -> BulkCounts:
        """Vertices per tag and edges per stored edge id, from the last STATS job.

        An edge type that joins several tag pairs is counted per pair by
        ``MATCH``, which needs the source tag's identity index.
        """
        stats: dict[tuple[str, str], int] = {}
        for row in self._execute("SHOW STATS").rows_as_dicts():
            kind = str(row.get("Type", "")).lower()
            stats[(kind, str(row.get("Name", "")))] = int(row.get("Count") or 0)
        vertices = {tag: stats.get(("tag", tag), 0) for tag in tag_fields(schema)}
        db_aware = schema.resolve_db_aware(DBType.NEBULA).vertex_config
        pairs: dict[str, list[StoredEdgeId]] = {}
        for edge in schema.core_schema.edge_config.values():
            stored = (
                db_aware.vertex_dbname(edge.source),
                edge.relation,
                db_aware.vertex_dbname(edge.target),
            )
            joined = pairs.setdefault(edge_type_name(edge), [])
            if stored not in joined:
                joined.append(stored)
        edges: dict[StoredEdgeId, int] = {}
        for edge_type, stored_ids in pairs.items():
            if len(stored_ids) == 1:
                edges[stored_ids[0]] = stats.get(("edge", edge_type), 0)
                continue
            for source, relation, target in stored_ids:
                rows = self._execute(
                    f"MATCH (:`{source}`)-[e:`{edge_type}`]->(:`{target}`) "
                    "RETURN count(e) AS n"
                ).rows_as_dicts()
                edges[(source, relation, target)] = int(rows[0]["n"]) if rows else 0
        return BulkCounts(vertices=vertices, edges=edges)

    # ------------------------------------------------------------------
    # Data clearing
    # ------------------------------------------------------------------

    def clear_data(self, schema: Schema) -> None:
        vc = schema.resolve_db_aware(DBType.NEBULA).vertex_config
        for vertex in vc.vertex_set:
            vname = vc.vertex_dbname(vertex)
            try:
                self._execute(
                    f"LOOKUP ON `{vname}` YIELD id(vertex) AS vid "
                    f"| DELETE VERTEX $-.vid"
                )
            except Exception as e:
                logger.debug("clear_data for tag '%s': %s", vname, e)

    def delete_graph_structure(
        self,
        vertex_types: tuple[str, ...] | list[str] = (),
        graph_names: tuple[str, ...] | list[str] = (),
        delete_all: bool = False,
    ) -> None:
        if delete_all:
            space_name = self._space_name or self.config.schema_name
            if space_name:
                self.delete_database(space_name)
            return

        for vt in vertex_types:
            try:
                self._execute(f"DROP TAG IF EXISTS `{vt}`")
            except Exception as e:
                logger.warning("Failed to drop tag '%s': %s", vt, e)

        for gn in graph_names:
            try:
                self._execute(drop_space_ngql(gn))
            except Exception as e:
                logger.warning("Failed to drop space '%s': %s", gn, e)

    # ------------------------------------------------------------------
    # Document operations
    # ------------------------------------------------------------------

    def upsert_docs_batch(
        self,
        docs: list[dict[str, Any]],
        class_name: str,
        match_keys: list[str] | tuple[str, ...],
        **kwargs: Any,
    ) -> None:
        """Upsert vertices of one tag; a field a document lacks keeps its value.

        Documents are folded by VID first. One that carries every field of the
        tag is written by ``INSERT``, which replaces the tag whole and so equals
        the upsert, without reading; any other by ``UPSERT VERTEX``.
        """
        dry = kwargs.pop("dry", False)
        if not docs:
            return
        match = list(match_keys)
        rows = fold_rows((make_vid(class_name, doc, match), doc) for doc in docs)
        fields = self._tag_field_names(class_name)
        # An undescribed tag has unknown fields: an INSERT could null stored ones.
        complete = (
            [(vid, doc) for vid, doc in rows.items() if all(f in doc for f in fields)]
            if fields
            else []
        )
        inserted = {vid for vid, _ in complete}
        partial = [doc for vid, doc in rows.items() if vid not in inserted]
        statements = insert_vertices_ngql(
            class_name, fields, complete, max_bytes=self.config.max_statement_bytes
        )
        upserts = batch_upsert_vertices_ngql(
            class_name, partial, match, fields or list({k for d in partial for k in d})
        )
        if dry:
            return
        for stmt in statements:
            self._execute(stmt)
        for i in range(0, len(upserts), _UPSERTS_PER_REQUEST):
            self._execute("; ".join(upserts[i : i + _UPSERTS_PER_REQUEST]))

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
        """Upsert edges of one type; a property an edge lacks keeps its value.

        As :meth:`upsert_docs_batch`: edges folded by identity, complete ones
        written by ``INSERT EDGE``, the rest by ``UPSERT EDGE``. The identity
        fields besides the endpoints (``relationship_merge_properties``) give
        the rank (:func:`edge_rank`); without them every edge has rank 0.
        """
        opts = consume_insert_edges_kwargs(kwargs)
        if not docs_edges:
            return
        if head is not None:
            docs_edges = docs_edges[:head]

        identity = list(opts.relationship_merge_properties or ())
        keyed: list[tuple[tuple[str, str, int | None], dict[str, Any]]] = []
        for edge_doc in docs_edges:
            if not isinstance(edge_doc, (list, tuple)) or len(edge_doc) < 2:
                continue
            src_doc = edge_doc[0] if isinstance(edge_doc[0], dict) else {}
            dst_doc = edge_doc[1] if isinstance(edge_doc[1], dict) else {}
            props = (
                edge_doc[2]
                if len(edge_doc) > 2 and isinstance(edge_doc[2], dict)
                else {}
            )
            src_vid = make_vid(source_class, src_doc, match_keys_source)
            dst_vid = make_vid(target_class, dst_doc, match_keys_target)
            keyed.append(((src_vid, dst_vid, edge_rank(props, identity)), props))
        rows = fold_rows(keyed)
        if opts.dry or not rows:
            return

        fields = self._edge_field_names(relation_name)
        if fields is None:
            # Undescribed: the type's fields are unknown, so write what is given.
            fields = sorted({k for props in rows.values() for k in props})
        complete: list[EdgeRow] = []
        bare: list[EdgeRow] = []
        upserts: list[str] = []
        for (src, dst, rank), props in rows.items():
            if all(f in props for f in fields):
                complete.append((src, dst, rank, props))
            elif stmt := upsert_edge_ngql(relation_name, src, dst, props, fields, rank):
                upserts.append(stmt)
            else:
                # Nothing to set: create the edge if missing, else leave it.
                bare.append((src, dst, rank, {}))
        limit = self.config.max_statement_bytes
        for stmt in [
            *insert_edges_ngql(relation_name, fields, complete, max_bytes=limit),
            *insert_edges_ngql(
                relation_name, [], bare, max_bytes=limit, if_not_exists=True
            ),
        ]:
            self._execute(stmt)
        for i in range(0, len(upserts), _UPSERTS_PER_REQUEST):
            self._execute("; ".join(upserts[i : i + _UPSERTS_PER_REQUEST]))

    def insert_return_batch(
        self, docs: list[dict[str, Any]], class_name: str
    ) -> list[dict[str, Any]] | str:
        raise NotImplementedError(
            "insert_return_batch is not implemented for NebulaGraph"
        )

    def delete_vertices(
        self,
        class_name: str,
        key_docs: list[dict[str, Any]],
        match_keys: tuple[str, ...],
        *,
        chunk_size: int = DEFAULT_DELETE_CHUNK_SIZE,
    ) -> None:
        """Remove vertices of one tag by VID, ``WITH EDGE`` removing their edges."""
        vids = [
            f'"{escape_nebula_string(make_vid(class_name, doc, match_keys))}"'
            for doc in deletable_docs(key_docs, match_keys)
        ]
        for start in range(0, len(vids), chunk_size):
            chunk = ", ".join(vids[start : start + chunk_size])
            self._execute(f"DELETE VERTEX {chunk} WITH EDGE")

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
        """Remove every edge of one type between VID pairs, whatever its rank."""
        if relation_name is None:
            raise ValueError("NebulaGraph delete_edges requires the edge type")
        pairs = [
            (
                make_vid(source_class, s, match_keys_source),
                make_vid(target_class, t, match_keys_target),
            )
            for s, t in deletable_endpoints(
                endpoints, match_keys_source, match_keys_target
            )
        ]
        for start in range(0, len(pairs), chunk_size):
            targets: dict[str, list[str]] = {}
            for src, dst in pairs[start : start + chunk_size]:
                targets.setdefault(src, []).append(dst)
            self._execute(
                "; ".join(
                    delete_edges_ngql(relation_name, src, dsts)
                    for src, dsts in targets.items()
                )
            )

    # ------------------------------------------------------------------
    # Fetch operations
    # ------------------------------------------------------------------

    def fetch_docs(
        self,
        class_name: str,
        filters: list[Any] | dict[str, Any] | None = None,
        limit: int | None = None,
        return_keys: list[str] | None = None,
        unset_keys: list[str] | None = None,
        *,
        timeout_s: float | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        if self.config.is_v3:
            doc_name = f"v.`{class_name}`"
            fc = self._render_filter(filters, doc_name)
            q = fetch_docs_ngql(class_name, fc, limit, return_keys)
        else:
            fc = self._render_filter(filters, "v")
            q = fetch_docs_gql(class_name, fc, limit, return_keys)

        rs = self._execute(q)
        rows = rs.rows_as_dicts()

        if return_keys:
            return rows

        result: list[dict[str, Any]] = []
        for row in rows:
            v = row.get("v", row)
            if isinstance(v, dict) and "tags" in v:
                props: dict[str, Any] = {}
                for tag_props in v["tags"].values():
                    props.update(tag_props)
                result.append(props)
            elif isinstance(v, dict):
                result.append(v)
            else:
                result.append(row)
        return result

    def fetch_all_docs(
        self,
        class_name: str,
        *,
        limit: int | None = None,
        timeout_s: float | None = None,
    ) -> list[dict[str, Any]]:
        """Every vertex of a tag, as its properties; needs a tag index on it."""
        lim = f" LIMIT {int(limit)}" if limit is not None else ""
        rows = self._execute(
            f"MATCH (v:`{class_name}`) RETURN properties(v) AS props{lim}"
        ).rows_as_dicts()
        return [dict(row["props"] or {}) for row in rows]

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
        """Every edge between two tags, as ``[source, target, properties]``."""
        rel = f"[e:`{relation_name}`]" if relation_name else "[e]"
        lim = f" LIMIT {int(limit)}" if limit is not None else ""
        rows = self._execute(
            f"MATCH (a:`{source_class}`)-{rel}->(b:`{target_class}`) "
            f"RETURN properties(a) AS s, properties(b) AS t, properties(e) AS p{lim}"
        ).rows_as_dicts()
        return [
            [dict(row["s"] or {}), dict(row["t"] or {}), dict(row["p"] or {})]
            for row in rows
        ]

    def _describe_properties(
        self, kind: str, name: str
    ) -> tuple[list[str], dict[str, FieldType]]:
        """Read a tag's or edge type's declared columns via ``DESCRIBE``."""
        try:
            rows = self._execute(f"DESCRIBE {kind} `{name}`").rows_as_dicts()
        except Exception:
            logger.debug("DESCRIBE %s `%s` failed", kind, name, exc_info=True)
            return [], {}
        properties: list[str] = []
        types: dict[str, FieldType] = {}
        for row in rows:
            field = row.get("Field") or row.get("field")
            if not field:
                continue
            properties.append(field)
            declared = field_type_from_nebula(row.get("Type") or row.get("type"))
            if declared is not None:
                types[field] = declared
        return properties, types

    def _sample_edge_endpoints(
        self, edge_type: str, sample_limit: int
    ) -> list[tuple[str, str]]:
        """Sample the ``(source_tag, target_tag)`` pairs an edge type connects.

        An nGQL edge type carries no endpoint tags in its DDL -- unlike a
        TigerGraph ``CREATE ... EDGE``, which names them -- so the pairs have to
        be observed on real edges. An edge type present in the catalogue but with
        no stored edges yields nothing and is dropped: there is no way to tell
        what it would connect.
        """
        query = (
            f"MATCH (a)-[e:`{edge_type}`]->(b) "
            f"RETURN DISTINCT tags(a) AS s, tags(b) AS t LIMIT {int(sample_limit)}"
        )
        try:
            rows = self._execute(query).rows_as_dicts()
        except Exception:
            logger.debug("endpoint sampling failed for `%s`", edge_type, exc_info=True)
            return []
        pairs: list[tuple[str, str]] = []
        for row in rows:
            source, target = row.get("s"), row.get("t")
            if not isinstance(source, list) or not isinstance(target, list):
                continue
            if not source or not target:
                continue
            pair = (str(source[0]), str(target[0]))
            if pair not in pairs:
                pairs.append(pair)
        return pairs

    def introspect_graph_schema(
        self,
        schema_name: str | None = None,
        *,
        sample_limit: int = 100,
    ) -> Schema:
        """Infer a graflo Schema from NebulaGraph's tag and edge-type catalogue.

        Properties and their declared types come from ``DESCRIBE``, so they are
        complete rather than sampled. Edge *endpoints* are the exception -- nGQL
        does not record them -- so those are sampled, and ``sample_limit`` bounds
        only that half.
        """
        from graflo.db.graph_introspection import (
            GraphEdgeIntrospection,
            GraphIntrospectionResult,
            GraphSchemaInferencer,
            GraphVertexIntrospection,
            infer_identity_fields,
        )

        resolved_name = (
            schema_name or self._space_name or self.config.schema_name or "nebula"
        )
        try:
            tag_rows = self._execute("SHOW TAGS").rows_as_dicts()
        except Exception as error:
            raise RuntimeError(
                f"Cannot introspect NebulaGraph space {resolved_name!r}: SHOW TAGS failed"
            ) from error

        vertices: list[GraphVertexIntrospection] = []
        for row in tag_rows:
            tag = row.get("Name") or row.get("name")
            if not tag:
                continue
            properties, types = self._describe_properties("TAG", tag)
            vertices.append(
                GraphVertexIntrospection(
                    name=tag,
                    properties=properties,
                    identity=infer_identity_fields(properties),
                    property_types=types,
                )
            )

        try:
            edge_rows = self._execute("SHOW EDGES").rows_as_dicts()
        except Exception:
            logger.debug("SHOW EDGES failed", exc_info=True)
            edge_rows = []

        edges: list[GraphEdgeIntrospection] = []
        for row in edge_rows:
            edge_type = row.get("Name") or row.get("name")
            if not edge_type:
                continue
            properties, types = self._describe_properties("EDGE", edge_type)
            for source, target in self._sample_edge_endpoints(edge_type, sample_limit):
                edges.append(
                    GraphEdgeIntrospection(
                        source=source,
                        target=target,
                        relation=edge_type,
                        properties=properties,
                        property_types=types,
                        collection_name=edge_type,
                    )
                )

        introspection = GraphIntrospectionResult(
            name=resolved_name,
            vertices=vertices,
            edges=edges,
            sample_limit=sample_limit,
        )
        return GraphSchemaInferencer(db_flavor=DBType.NEBULA).infer_schema(
            introspection, schema_name=resolved_name
        )

    def vertex_address(
        self, doc: dict[str, Any], identity_fields: Sequence[str]
    ) -> str | None:
        """Every identity value joined as the write path joins them, without the tag.

        :meth:`fetch_edges` adds the tag it is given. Falling back to the base
        implementation would address a composite-identity vertex by its first
        field alone — a VID that exists nowhere, so every edge query anchored
        on it returns empty instead of raising.
        """
        keys = list(identity_fields)
        if not keys or any(doc.get(k) is None for k in keys):
            return None
        return vertex_key(doc, keys)

    def fetch_edges(
        self,
        from_type: str,
        from_id: str,
        edge_type: str | None = None,
        to_type: str | None = None,
        to_id: str | None = None,
        filters: list[Any] | dict[str, Any] | None = None,
        limit: int | None = None,
        return_keys: list[str] | None = None,
        unset_keys: list[str] | None = None,
        direction: EdgeDirection = EdgeDirection.OUT,
        *,
        timeout_s: float | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Fetch edges incident to one vertex via ``GO``.

        *from_id* and *to_id* are addresses (:meth:`vertex_address`), and
        ``_src`` / ``_dst`` in the rows are too: the tag is added and removed
        here. Nebula keeps an out-key and an in-key per edge, so ``IN`` /
        ``ANY`` are as cheap as ``OUT`` once ``REVERSELY`` / ``BIDIRECT`` is
        asked for.
        """
        fc = ""
        if filters is not None:
            if not isinstance(filters, FilterExpression):
                ff = FilterExpression.from_dict(filters)
            else:
                ff = filters
            # Joined with the far-end predicate by AND in the query builder.
            fc = render_conjunct(ff, doc_name="e", kind=self._expression_flavor())

        q = fetch_edges_ngql(
            from_type,
            tag_vid(from_type, from_id),
            edge_type=edge_type,
            to_tag=to_type,
            to_vid=tag_vid(to_type, to_id) if to_type and to_id else None,
            to_key=to_id if to_id and not to_type else None,
            filter_clause=fc,
            limit=limit,
            direction=direction,
        )
        rs = self._execute(q)
        rows = rs.rows_as_dicts()

        result: list[dict[str, Any]] = []
        for row in rows:
            entry = row.get("props", row)
            if isinstance(entry, dict):
                entry["_src"] = key_of_vid(str(row.get("src", "")))
                entry["_dst"] = key_of_vid(str(row.get("dst", "")))
                entry["_type"] = row.get("edge_type", "")
            if return_keys and isinstance(entry, dict):
                entry = {k: entry.get(k) for k in return_keys}
            result.append(entry)
        return result

    # ------------------------------------------------------------------
    # Presence / absence checks
    # ------------------------------------------------------------------

    def fetch_present_documents(
        self,
        batch: list[dict[str, Any]],
        class_name: str,
        match_keys: list[str] | tuple[str, ...],
        keep_keys: list[str] | tuple[str, ...] | None = None,
        flatten: bool = False,
        filters: list[Any] | dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        if not batch:
            return []

        results: list[dict[str, Any]] = []
        for doc in batch:
            vid = make_vid(class_name, doc, match_keys)
            try:
                rs = self._execute(
                    f'FETCH PROP ON `{class_name}` "{escape_nebula_string(vid)}" '
                    f"YIELD properties(vertex) AS props"
                )
                rows = rs.rows_as_dicts()
                for row in rows:
                    props = row.get("props", row)
                    if isinstance(props, dict):
                        if keep_keys:
                            props = {k: props.get(k) for k in keep_keys}
                        results.append(props)
            except Exception as e:
                logger.debug("fetch_present_documents error for vid '%s': %s", vid, e)

        return results

    def keep_absent_documents(
        self,
        batch: list[dict[str, Any]],
        class_name: str,
        match_keys: list[str] | tuple[str, ...],
        keep_keys: list[str] | tuple[str, ...] | None = None,
        filters: list[Any] | dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        if not batch:
            return []

        present = self.fetch_present_documents(
            batch, class_name, match_keys, list(match_keys), filters=filters
        )
        present_keys: set[tuple[Any, ...]] = set()
        for doc in present:
            key_tuple = tuple(doc.get(k) for k in match_keys)
            present_keys.add(key_tuple)

        absent: list[dict[str, Any]] = []
        for doc in batch:
            key_tuple = tuple(doc.get(k) for k in match_keys)
            if key_tuple not in present_keys:
                if keep_keys:
                    absent.append({k: doc.get(k) for k in keep_keys})
                else:
                    absent.append(doc)
        return absent

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------

    def aggregate(
        self,
        class_name: str,
        aggregation_function: AggregationType,
        discriminant: str | None = None,
        aggregated_field: str | None = None,
        filters: FilterExpression | list[Any] | dict[str, Any] | None = None,
        *,
        timeout_s: float | None = None,
    ) -> Any:
        """Aggregate over vertices of a tag; the result is shaped as in :mod:`graflo.db.aggregation`.

        Raises:
            ValueError: If *aggregated_field* is missing where it is required.
        """
        function = check_aggregation(aggregation_function, aggregated_field)
        if self.config.is_v3:
            fc = self._render_filter(filters, f"v.`{class_name}`")
            q = aggregate_ngql(class_name, function, discriminant, aggregated_field, fc)
        else:
            fc = self._render_filter(filters, "v")
            q = aggregate_gql(class_name, function, discriminant, aggregated_field, fc)

        rows = self._execute(q).rows_as_dicts()
        return aggregation_result(
            function,
            [(row.get(KEY), row[VALUE]) for row in rows],
            grouped=discriminant is not None,
        )
