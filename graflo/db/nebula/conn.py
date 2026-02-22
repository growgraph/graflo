"""NebulaGraph connection implementation.

Supports NebulaGraph 3.x (nGQL via ``nebula3-python``) and 5.x (ISO GQL via
``nebula5-python``).  The version is selected by ``NebulaConfig.version``.
"""

from __future__ import annotations

import logging
from typing import Any, ClassVar

from graflo.architecture.edge import Edge
from graflo.architecture.onto import Index
from graflo.architecture.schema import Schema
from graflo.architecture.vertex import FieldType, VertexConfig
from graflo.db.conn import Connection, SchemaExistsError
from graflo.db.nebula.adapter import (
    NebulaClientAdapter,
    NebulaResultSet,
    create_adapter,
)
from graflo.db.nebula.query import (
    aggregate_gql,
    aggregate_ngql,
    batch_upsert_vertices_ngql,
    create_edge_index_ngql,
    create_edge_type_ngql,
    create_space_ngql,
    create_tag_index_ngql,
    create_tag_ngql,
    drop_space_ngql,
    fetch_docs_gql,
    fetch_docs_ngql,
    fetch_edges_ngql,
    insert_edges_ngql,
)
from graflo.db.nebula.util import (
    make_vid,
    render_filters_cypher,
    render_filters_ngql,
    wait_for_space_ready,
)
from graflo.filter.onto import FilterExpression
from graflo.onto import AggregationType, DBType, ExpressionFlavor

from ..connection.onto import NebulaConfig

logger = logging.getLogger(__name__)

_SCHEMA_WAIT_INTERVAL = 1.0
_SCHEMA_WAIT_RETRIES = 30


class NebulaConnection(Connection):
    """NebulaGraph implementation of the ``Connection`` interface.

    Automatically selects the correct Python driver and query language based on
    ``config.version``:

    * **v3.x** -- ``nebula3-python``, nGQL
    * **v5.x** -- ``nebula5-python``, ISO GQL / Cypher
    """

    flavor: ClassVar[DBType] = DBType.NEBULA

    def __init__(self, config: NebulaConfig):
        super().__init__()
        self.config = config
        self._adapter: NebulaClientAdapter = create_adapter(config)
        self._space_name: str | None = None
        self._tag_fields: dict[str, list[str]] = {}

        if config.schema_name:
            try:
                self._use_space(config.schema_name)
            except Exception:
                pass

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
        self._load_tag_fields()

    def _load_tag_fields(self) -> None:
        """Discover existing tags and their fields from the current space."""
        try:
            rs = self._adapter.execute("SHOW TAGS")
            tag_names = [r.get("Name", r.get("name", "")) for r in rs.rows_as_dicts()]
        except Exception:
            return
        for tag in tag_names:
            if not tag:
                continue
            try:
                desc = self._adapter.execute(f"DESCRIBE TAG `{tag}`")
                self._tag_fields[tag] = [
                    r.get("Field", r.get("field", "")) for r in desc.rows_as_dicts()
                ]
            except Exception:
                pass

    def _wait_for_dml_ready(self, tag_name: str) -> None:
        """Wait until DML operations are possible on a tag.

        NebulaGraph's storaged schema cache may lag behind graphd's metadata
        cache by several heartbeat cycles (~10 s with default settings).
        ``DESCRIBE TAG`` succeeds immediately, but DML like ``FETCH PROP`` or
        ``UPSERT VERTEX`` fails until the storage cache is warm.
        """
        import time

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
        import time

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
        return self._tag_fields.get(tag_name, [])

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

        import time

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

    def create_database(self, name: str) -> None:
        self._ensure_storage_hosts()
        stmt = create_space_ngql(
            name,
            vid_type=self.config.vid_type,
            partition_num=self.config.partition_num,
            replica_factor=self.config.replica_factor,
        )
        self._execute(stmt)
        wait_for_space_ready(
            self._adapter,
            name,
            max_retries=_SCHEMA_WAIT_RETRIES,
            interval=_SCHEMA_WAIT_INTERVAL,
        )
        self._use_space(name)
        logger.info("Created NebulaGraph space '%s'", name)

    def delete_database(self, name: str) -> None:
        self._execute(drop_space_ngql(name))
        logger.info("Dropped NebulaGraph space '%s'", name)

    # ------------------------------------------------------------------
    # Schema definition
    # ------------------------------------------------------------------

    def define_schema(self, schema: Schema) -> None:
        self.define_vertex_classes(schema)
        edges = schema.edge_config.edges_list(include_aux=True)
        self.define_edge_classes(edges)

    def define_vertex_classes(self, schema: Schema) -> None:
        for vname in schema.vertex_config.vertex_set:
            fields = schema.vertex_config.fields(vname)
            stmt = create_tag_ngql(vname, fields)
            self._execute(stmt)
            self._tag_fields[vname] = [f.name for f in fields]
            logger.debug("Created tag '%s'", vname)

        if schema.vertex_config.vertex_set:
            sample_tag = next(iter(schema.vertex_config.vertex_set))
            self._wait_for_dml_ready(sample_tag)

    def define_edge_classes(self, edges: list[Edge]) -> None:
        created: set[str] = set()
        for edge in edges:
            rel = edge.relation or f"{edge.source}_{edge.target}"
            if rel in created:
                continue
            edge_fields = []
            if edge.weights and edge.weights.direct:
                edge_fields = list(edge.weights.direct)
            stmt = create_edge_type_ngql(rel, edge_fields)
            self._execute(stmt)
            created.add(rel)
            logger.debug("Created edge type '%s'", rel)

        if created:
            sample_et = next(iter(created))
            self._wait_for_edge_dml_ready(sample_et)

    # ------------------------------------------------------------------
    # Index management
    # ------------------------------------------------------------------

    def define_vertex_indices(self, vertex_config: VertexConfig) -> None:
        for vname in vertex_config.vertex_set:
            fields = vertex_config.fields(vname)
            string_fields = {f.name for f in fields if f.type == FieldType.STRING}
            for idx in vertex_config.indexes(vname):
                self._add_tag_index(vname, idx, string_fields=string_fields)

    def define_edge_indices(self, edges: list[Edge]) -> None:
        for edge in edges:
            rel = edge.relation or f"{edge.source}_{edge.target}"
            for idx in edge.indexes:
                self._add_edge_index(rel, idx)

    def _add_tag_index(
        self,
        tag_name: str,
        index: Index,
        string_fields: set[str] | None = None,
    ) -> None:
        idx_fields = [str(f) for f in index.fields]
        idx_name = f"idx_{tag_name}_{'_'.join(idx_fields)}"
        stmt = create_tag_index_ngql(
            idx_name, tag_name, idx_fields, string_fields=string_fields
        )
        try:
            self._execute(stmt)
            self._rebuild_index(idx_name, kind="TAG")
            logger.debug("Created tag index '%s'", idx_name)
        except Exception as e:
            logger.debug("Tag index '%s' note: %s", idx_name, e)

    def _add_edge_index(self, edge_type: str, index: Index) -> None:
        idx_fields = [str(f) for f in index.fields]
        idx_name = f"idx_{edge_type}_{'_'.join(idx_fields)}"
        stmt = create_edge_index_ngql(idx_name, edge_type, idx_fields)
        try:
            self._execute(stmt)
            self._rebuild_index(idx_name, kind="EDGE")
            logger.debug("Created edge index '%s'", idx_name)
        except Exception as e:
            logger.debug("Edge index '%s' note: %s", idx_name, e)

    def _rebuild_index(self, idx_name: str, kind: str = "TAG") -> None:
        """Rebuild an index, waiting for propagation first, then for completion."""
        import time

        rebuild_stmt = f"REBUILD {kind} INDEX `{idx_name}`"
        for attempt in range(_SCHEMA_WAIT_RETRIES):
            try:
                self._adapter.execute(rebuild_stmt)
                break
            except Exception:
                if attempt == _SCHEMA_WAIT_RETRIES - 1:
                    logger.warning("Could not start rebuild for '%s'", idx_name)
                    return
                time.sleep(_SCHEMA_WAIT_INTERVAL)

        for _ in range(_SCHEMA_WAIT_RETRIES):
            try:
                rs = self._adapter.execute(f"SHOW {kind} INDEX STATUS")
                for row in rs.rows_as_dicts():
                    name = row.get("Name", row.get("name", ""))
                    status = row.get("Index Status", row.get("index_status", ""))
                    if name == idx_name and status.upper() == "FINISHED":
                        return
            except Exception:
                pass
            time.sleep(_SCHEMA_WAIT_INTERVAL)
        logger.warning("Index rebuild for '%s' may not be complete", idx_name)

    # ------------------------------------------------------------------
    # init_db
    # ------------------------------------------------------------------

    def init_db(self, schema: Schema, recreate_schema: bool) -> None:
        space_name = self.config.schema_name
        if not space_name:
            space_name = schema.general.name
            self.config.schema_name = space_name

        if recreate_schema:
            try:
                self.delete_database(space_name)
            except Exception:
                pass
            self.create_database(space_name)
        else:
            try:
                self.create_database(space_name)
            except Exception:
                # Space may already exist
                wait_for_space_ready(
                    self._adapter,
                    space_name,
                    max_retries=_SCHEMA_WAIT_RETRIES,
                    interval=_SCHEMA_WAIT_INTERVAL,
                )
                self._use_space(space_name)

            # Check if tags already exist
            try:
                rs = self._execute("SHOW TAGS")
                rows = rs.rows_as_dicts()
                if rows:
                    raise SchemaExistsError(
                        f"Schema already exists in space '{space_name}' "
                        f"({len(rows)} tags). Set recreate_schema=True to replace."
                    )
            except SchemaExistsError:
                raise
            except Exception:
                pass

        self.define_schema(schema)
        self.define_indexes(schema)

    # ------------------------------------------------------------------
    # Data clearing
    # ------------------------------------------------------------------

    def clear_data(self, schema: Schema) -> None:
        for vname in schema.vertex_config.vertex_set:
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
        dry = kwargs.pop("dry", False)
        if not docs:
            return

        match_keys_list = list(match_keys)
        tag_fields = self._tag_field_names(class_name)
        if not tag_fields:
            tag_fields = list({k for doc in docs for k in doc})

        statements = batch_upsert_vertices_ngql(
            class_name, docs, match_keys_list, tag_fields
        )
        if dry or not statements:
            return

        # Execute in batches to avoid hitting statement-size limits
        batch_size = 50
        for i in range(0, len(statements), batch_size):
            chunk = statements[i : i + batch_size]
            combined = "; ".join(chunk)
            self._execute(combined)

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
        dry = kwargs.pop("dry", False)
        kwargs.pop("collection_name", None)
        kwargs.pop("uniq_weight_fields", None)
        kwargs.pop("uniq_weight_collections", None)
        kwargs.pop("upsert_option", None)

        if not docs_edges:
            return

        if head is not None:
            docs_edges = docs_edges[:head]

        # Build (src_vid, dst_vid, props) tuples
        edge_tuples: list[tuple[str, str, dict[str, Any]]] = []
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

            src_vid = make_vid(src_doc, list(match_keys_source))
            dst_vid = make_vid(dst_doc, list(match_keys_target))
            edge_tuples.append((src_vid, dst_vid, props))

        if dry or not edge_tuples:
            return

        # Determine edge property fields from schema or from data
        all_prop_keys: set[str] = set()
        for _, _, p in edge_tuples:
            all_prop_keys.update(p.keys())
        edge_fields = sorted(all_prop_keys) if all_prop_keys else None

        batch_size = 200
        for i in range(0, len(edge_tuples), batch_size):
            chunk = edge_tuples[i : i + batch_size]
            stmt = insert_edges_ngql(relation_name, chunk, edge_fields)
            if stmt:
                self._execute(stmt)

    def insert_return_batch(
        self, docs: list[dict[str, Any]], class_name: str
    ) -> list[dict[str, Any]] | str:
        raise NotImplementedError(
            "insert_return_batch is not implemented for NebulaGraph"
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
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        fc = ""
        if filters is not None:
            if not isinstance(filters, FilterExpression):
                ff = FilterExpression.from_dict(filters)
            else:
                ff = filters
            fc = str(ff(doc_name="e", kind=self._expression_flavor()))

        q = fetch_edges_ngql(
            from_type,
            from_id,
            edge_type=edge_type,
            to_tag=to_type,
            to_vid=to_id,
            filter_clause=fc,
            limit=limit,
        )
        rs = self._execute(q)
        rows = rs.rows_as_dicts()

        result: list[dict[str, Any]] = []
        for row in rows:
            entry = row.get("props", row)
            if isinstance(entry, dict):
                entry["_src"] = row.get("src", "")
                entry["_dst"] = row.get("dst", "")
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
            vid = make_vid(doc, list(match_keys))
            try:
                rs = self._execute(
                    f'FETCH PROP ON `{class_name}` "{vid}" '
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
        filters: list[Any] | dict[str, Any] | None = None,
    ) -> int | float | list[dict[str, Any]] | dict[str, int | float] | None:
        agg_name = (
            aggregation_function.value
            if isinstance(aggregation_function, AggregationType)
            else str(aggregation_function)
        )
        if agg_name == "AVERAGE":
            agg_name = "AVG"

        if self.config.is_v3:
            doc_name = f"v.`{class_name}`"
            fc = self._render_filter(filters, doc_name)
            q = aggregate_ngql(class_name, agg_name, discriminant, aggregated_field, fc)
        else:
            fc = self._render_filter(filters, "v")
            q = aggregate_gql(class_name, agg_name, discriminant, aggregated_field, fc)

        rs = self._execute(q)
        rows = rs.rows_as_dicts()

        if agg_name == "COUNT" and discriminant:
            return {row["key"]: row["count"] for row in rows}
        if agg_name == "COUNT":
            return rows[0]["count"] if rows else 0
        if agg_name == "SORTED_UNIQUE":
            return [row["val"] for row in rows]
        if rows:
            return rows[0].get("val")
        return None
