"""GraFlo file backend connection implementation."""

from __future__ import annotations

import logging
import shutil
from collections.abc import Callable, Sequence
from typing import Any, cast

from graflo.architecture.backend import GraFloBackendReader, GraFloBackendWriter
from graflo.architecture.backend.layout import GraFloLayout
from graflo.architecture.backend.reader import (
    edge_chunk_key,
    stored_schema,
)
from graflo.architecture.graph_types import EdgeDirection, GraphContainer
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.physical_keys import PhysicalKeys
from graflo.architecture.schema.vertex import VertexConfig
from graflo.connections.bulk import BulkConfig
from graflo.connections.graflo_backend import GraFloBackendConfig
from graflo.db.aggregation import (
    aggregation_result,
    check_aggregation,
    hashable_key,
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
)
from graflo.db.traversal import bfs_neighbors, edge_query_name
from graflo.filter.onto import FilterExpression, conjoin
from graflo.onto import AggregationType, DBType

logger = logging.getLogger(__name__)

#: Ceiling on rows held in the in-process edge index. Traversal over a file
#: backend larger than this is the wrong tool; the bound is logged, never silent.
_EDGE_INDEX_MAX_ROWS = 2_000_000


def _matcher(*filters: Any) -> Callable[[dict[str, Any]], bool]:
    """A predicate for the ``AND`` of *filters*; ``None`` operands are skipped."""
    predicate = conjoin(*filters)
    if predicate is None:
        return lambda _doc: True

    def _keep(doc: dict[str, Any]) -> bool:
        try:
            return predicate.matches(doc)
        except Exception:
            # A document missing a filtered field simply does not match.
            return False

    return _keep


def _first_value(doc: Any, fields: list[str]) -> str | None:
    """First present identity value in *doc*, as a string."""
    if not isinstance(doc, dict):
        return None
    for field in [*fields, "_key", "id"]:
        value = doc.get(field)
        if value is not None:
            return str(value)
    return None


def _key_of(doc: Any, fields: Sequence[str]) -> tuple[str, ...] | None:
    """The values of *fields* in *doc* as strings, or ``None`` when one is missing."""
    if not isinstance(doc, dict):
        return None
    values = [doc.get(field) for field in fields]
    if any(value is None for value in values):
        return None
    return tuple(str(value) for value in values)


#: Index-only keys: each edge row's stored endpoint types, never returned.
_FROM_TYPE, _TO_TYPE = "_from_type", "_to_type"


def _aggregate(
    function: AggregationType, docs: list[dict[str, Any]], field: str | None
) -> Any:
    """The raw aggregate of *docs*, before :func:`aggregation_result` shapes it."""
    if function == AggregationType.COUNT:
        return len(docs)
    values = [doc[field] for doc in docs if field and doc.get(field) is not None]
    if function == AggregationType.SORTED_UNIQUE:
        distinct: list[Any] = []
        for value in values:
            if value not in distinct:
                distinct.append(value)
        return distinct
    if not values:
        return None
    if function == AggregationType.MAX:
        return max(values)
    if function == AggregationType.MIN:
        return min(values)
    return sum(values) / len(values)


class GraFloBackendConnection(Connection):
    """Read/write graph data through a chunked on-disk GraFlo backend.

    Writes append; reads fold the appended records by identity
    (:mod:`~graflo.architecture.backend.fold`), so an upsert behaves as on a
    database, ``merge: union`` fields included. Every read therefore scans the
    whole collection it reads, whatever its ``limit``: this backend holds dumps
    and serves tests; it is not a database to serve a graph from.

    Does not declare ``supports_query_timeout``: reads run in process with
    nothing to cancel them, so ``timeout_s`` is accepted and ignored.
    """

    flavor = DBType.GRAFLO_BACKEND
    supports_graph_export = True
    supports_schema_introspection = True
    #: Introspection reads the ``schema.yaml`` catalogue the writer keeps.
    schema_introspection_is_sampled = False
    #: Guards are evaluated in process, on every document and edge row read.
    supports_read_guard = True
    supports_union_merge = True
    supports_membership_read = True
    supports_bulk_load = True
    #: Appends cannot be deleted, so a ``merge: replace`` edge is folded on
    #: read to one record per source; :meth:`fetch_edge_targets` reads the fold.
    supports_single_valued_fold = True
    supports_edge_target_read = True
    #: Reads run in process with nothing to cancel them, so ``timeout_s`` is
    #: accepted and ignored.
    supports_query_timeout = False

    def __init__(self, config: GraFloBackendConfig) -> None:
        super().__init__()
        self.config = config
        self._edge_index_cache: dict[str, list[dict[str, Any]]] | None = None
        self._writer = GraFloBackendWriter(
            config.output_dir,
            chunk_size=config.chunk_size,
            resume=True,
        )
        self._reader = GraFloBackendReader(config.output_dir)
        self._schema_written = False

    def create_database(self, name: str) -> None:
        self.config.output_dir.mkdir(parents=True, exist_ok=True)

    def delete_database(self, name: str) -> None:
        if self.config.output_dir.exists():
            shutil.rmtree(self.config.output_dir)

    def execute(self, query: str | Any, **kwargs: Any) -> Any:
        raise NotImplementedError("GraFlo file backend does not support ad-hoc queries")

    def close(self) -> None:
        if (
            self._schema_written
            or self._writer._vertex_writers
            or self._writer._edge_writers
        ):
            self._writer.flush_index()

    def define_schema(self, schema: Schema) -> None:
        self._writer.write_schema(schema)
        self._schema_written = True

    def delete_graph_structure(
        self,
        vertex_types: tuple[str, ...] | list[str] = (),
        graph_names: tuple[str, ...] | list[str] = (),
        delete_all: bool = False,
    ) -> None:
        if delete_all:
            self._writer.reset_data()

    def ensure_target_namespace(self, schema: Schema, *, create: bool) -> None:
        if self.config.output_dir.exists():
            return
        if not create:
            raise NamespaceNotFoundError(
                f"GraFlo backend output directory {self.config.output_dir} "
                "does not exist. Create it manually or call with create_namespace=True."
            )
        self.config.output_dir.mkdir(parents=True, exist_ok=True)

    def apply_target_schema(
        self,
        schema: Schema,
        *,
        recreate: bool,
        create_namespace: bool = True,
    ) -> None:
        self.report_edge_direction_support(schema)
        layout = GraFloLayout(self.config.output_dir)
        if layout.schema_path.exists() and not recreate:
            raise SchemaExistsError(
                f"GraFlo backend already exists at {self.config.output_dir}"
            )
        if recreate and self.config.output_dir.exists():
            self._writer.reset_data()
        if create_namespace:
            self.config.output_dir.mkdir(parents=True, exist_ok=True)
        self._writer.write_schema(schema)
        self._schema_written = True

    def init_db(
        self,
        schema: Schema,
        recreate_schema: bool = False,
        *,
        create_namespace: bool = True,
    ) -> None:
        """Convenience wrapper: ensure output dir then write schema."""
        self.ensure_target_namespace(schema, create=create_namespace)
        self.apply_target_schema(
            schema, recreate=recreate_schema, create_namespace=create_namespace
        )

    def clear_data(self, schema: Schema) -> None:
        self._writer.reset_data()

    def upsert_docs_batch(
        self,
        docs: list[dict[str, Any]],
        class_name: str,
        match_keys: list[str] | tuple[str, ...],
        **kwargs: Any,
    ) -> None:
        # ``union_fields`` needs no handling here: reads fold every record by
        # the schema's identity and merge modes, the same ones it was built from.
        if kwargs.get("dry"):
            return
        self._writer.write_vertex_batch(class_name, docs)

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
        if kwargs.get("dry") or not docs_edges:
            return
        edge_key = (source_class, target_class, relation_name or None)
        self._writer.write_edge_batch(edge_key, list(docs_edges))

    def insert_return_batch(
        self, docs: list[dict[str, Any]], class_name: str
    ) -> list[dict[str, Any]]:
        self.upsert_docs_batch(docs, class_name, match_keys=[])
        return docs

    # -- bulk load: appends, folded on read like any write ---------------------

    def bulk_begin(
        self,
        schema: Schema,
        cfg: BulkConfig,
        *,
        recreate: bool = False,
        resume: bool = False,
    ) -> BulkPlan:
        """Write the schema through the first-write guard; one writer at a time."""
        self.ensure_target_namespace(schema, create=True)
        if not resume or not GraFloLayout(self.config.output_dir).schema_path.exists():
            self.apply_target_schema(schema, recreate=recreate)
        return BulkPlan(max_parallel=1)

    def bulk_write_vertices(self, schema: Schema, chunk: BulkVertexChunk) -> None:
        # Published before returning, so a chunk recorded as written is on disk;
        # a replayed chunk appends records the read-side fold merges.
        self._writer.write_vertex_batch(chunk.class_name, chunk.rows)
        self._writer.flush_index()

    def bulk_prepare_edges(self, schema: Schema) -> None:
        """Nothing to prepare: edges name their endpoints by key."""

    def bulk_write_edges(self, schema: Schema, chunk: BulkEdgeChunk) -> None:
        edge_key = (chunk.source_class, chunk.target_class, chunk.relation_name or None)
        self._writer.write_edge_batch(edge_key, [list(row) for row in chunk.rows])
        self._writer.flush_index()

    def bulk_finish(self, schema: Schema) -> BulkCounts:
        self._sync_for_read()
        index = self._reader.read_index()
        edges: dict[StoredEdgeId, int] = {}
        for name in index.edges:
            source, target, relation = GraFloLayout.index_name_to_edge_key(name)
            edges[(source, relation, target)] = len(
                self._reader.edge_records((source, target, relation))
            )
        return BulkCounts(
            vertices={
                name: len(self._reader.vertex_records(name)) for name in index.vertices
            },
            edges=edges,
        )

    def _sync_for_read(self) -> None:
        """Persist buffered writes, and see what any writer has published since.

        Chunk writers buffer records in memory and the index is only written on
        flush, so a read issued mid-ingest would otherwise miss data — or find
        no ``INDEX.json`` at all. Every other backend is read-your-writes;
        flushing here gives the file backend the same contract. Another
        connection may write the same directory, so the index is re-read and
        the edge index dropped when it changed.
        """
        try:
            self._writer.flush_index()
        except ValueError:
            # No schema written yet, so nothing has been ingested to observe.
            pass
        if self._reader.reload_index():
            self._edge_index_cache = None

    def fetch_docs(
        self,
        class_name: str,
        filters: list[Any] | dict[str, Any] | FilterExpression | None = None,
        limit: int | None = None,
        return_keys: list[str] | None = None,
        unset_keys: list[str] | None = None,
        guard: FilterExpression | None = None,
        *,
        timeout_s: float | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Documents of *class_name*, one per entity, that match *filters*.

        Args:
            guard: A trusted predicate every document must also satisfy.
        """
        self._sync_for_read()
        keep = _matcher(filters, guard)

        def _project(doc: dict[str, Any]) -> dict[str, Any]:
            if return_keys:
                doc = {k: doc.get(k) for k in return_keys}
            if unset_keys:
                doc = {k: v for k, v in doc.items() if k not in unset_keys}
            return doc

        # `limit` bounds the *result* size, so filtering happens before slicing.
        docs = [
            _project(doc)
            for doc in self._reader.vertex_records(class_name)
            if keep(doc)
        ]
        return docs if limit is None else docs[:limit]

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
        """Edges incident to one vertex, served from a lazily built index.

        Direction is this backend's storage partition key, so the reverse
        orientation has to be materialized to be answerable at all. That is a
        statement about *storage*; an in-process read index is not storage, so
        building one here does not contradict the ``MATERIALIZATION_REQUIRED``
        tier in the capability matrix — it is exactly what that tier prescribes.
        """
        self._sync_for_read()
        index = self._edge_index()
        entries = index.get(edge_type) if edge_type is not None else None
        if entries is None:
            # No edge_type: search every indexed edge type.
            entries = [row for rows in index.values() for row in rows]

        # Filtered before `limit`, which bounds the result.
        keep = _matcher(filters)
        matched: list[dict[str, Any]] = []
        for row in entries:
            out = row.get(_FROM_TYPE) == from_type and row.get("_from_key") == from_id
            into = row.get(_TO_TYPE) == from_type and row.get("_to_key") == from_id
            if out and direction is not EdgeDirection.IN:
                far, far_type = row.get("_to_key"), row.get(_TO_TYPE)
            elif into and direction is not EdgeDirection.OUT:
                far, far_type = row.get("_from_key"), row.get(_FROM_TYPE)
            else:
                continue
            if to_type is not None and far_type != to_type:
                continue
            if to_id is not None and far != to_id:
                continue
            if not keep(row):
                continue
            matched.append(
                {k: v for k, v in row.items() if k not in (_FROM_TYPE, _TO_TYPE)}
            )
            if limit is not None and len(matched) >= limit:
                break

        if return_keys or unset_keys:
            keep = set(return_keys) if return_keys else None
            drop = set(unset_keys) if unset_keys else set()
            matched = [
                {
                    k: v
                    for k, v in row.items()
                    if (keep is None or k in keep) and k not in drop
                }
                for row in matched
            ]
        return matched

    def _edge_index(self) -> dict[str, list[dict[str, Any]]]:
        """Edge query name -> flat edge rows, built once per connection.

        Rows carry ``_from_key`` / ``_to_key`` so both orientations are
        answerable from one pass over the chunked files, and their endpoint
        types, since one relation name may join several pairs of types.
        """
        if self._edge_index_cache is not None:
            return self._edge_index_cache

        schema = self._reader.read_schema()
        db_aware = schema.resolve_db_aware(self.flavor)
        keys = PhysicalKeys(stored_schema(schema).db_profile)
        index: dict[str, list[dict[str, Any]]] = {}
        total = 0
        for edge in schema.core_schema.edge_config.edges:
            # Keyed by the name a walk asks for the edge by.
            name = edge_query_name(db_aware, edge, self.flavor)
            if name is None:
                continue
            rows = index.setdefault(name, [])
            # Stored records carry stored property names.
            source_identity = keys.vertex_fields(
                edge.source, db_aware.vertex_config.identity_fields(edge.source)
            )
            target_identity = keys.vertex_fields(
                edge.target, db_aware.vertex_config.identity_fields(edge.target)
            )
            types = {
                _FROM_TYPE: db_aware.vertex_config.vertex_dbname(edge.source),
                _TO_TYPE: db_aware.vertex_config.vertex_dbname(edge.target),
            }
            for record in self._reader.edge_records(
                edge_chunk_key(schema, edge.edge_id)
            ):
                if not isinstance(record, list) or len(record) < 2:
                    continue
                source_doc, target_doc = record[0], record[1]
                weight = record[2] if len(record) > 2 else {}
                rows.append(
                    {
                        **(weight if isinstance(weight, dict) else {}),
                        "_from_key": _first_value(source_doc, source_identity),
                        "_to_key": _first_value(target_doc, target_identity),
                        **types,
                    }
                )
                total += 1
                if total >= _EDGE_INDEX_MAX_ROWS:
                    logger.warning(
                        "GraFlo file backend edge index hit its %s-row bound; "
                        "traversal results may be incomplete",
                        _EDGE_INDEX_MAX_ROWS,
                    )
                    self._edge_index_cache = index
                    return index
        self._edge_index_cache = index
        return index

    def fetch_present_documents(
        self,
        batch: list[dict[str, Any]],
        class_name: str,
        match_keys: list[str] | tuple[str, ...],
        keep_keys: list[str] | tuple[str, ...] | None = None,
        flatten: bool = False,
        filters: list[Any] | dict[str, Any] | None = None,
    ) -> list[dict[str, Any]] | dict[int, list[dict[str, Any]]]:
        if not match_keys:
            return [] if flatten else {}
        existing = {
            tuple(doc.get(key) for key in match_keys): doc
            for doc in self.fetch_docs(class_name)
        }
        if flatten:
            present: list[dict[str, Any]] = []
            for doc in batch:
                key = tuple(doc.get(field) for field in match_keys)
                match = existing.get(key)
                if match is not None:
                    present.append(match)
            return present
        result: dict[int, list[dict[str, Any]]] = {}
        for index, doc in enumerate(batch):
            key = tuple(doc.get(field) for field in match_keys)
            match = existing.get(key)
            if match is not None:
                result[index] = [match]
        return result

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
        """Aggregate over the documents of a vertex type, read in full.

        The result is shaped as in :mod:`graflo.db.aggregation`.

        Raises:
            ValueError: If *aggregated_field* is missing where it is required.
        """
        function = check_aggregation(aggregation_function, aggregated_field)
        docs = self.fetch_docs(class_name, filters=cast(Any, filters))
        if discriminant is None:
            return aggregation_result(
                function,
                [(None, _aggregate(function, docs, aggregated_field))],
                grouped=False,
            )
        groups: dict[Any, list[dict[str, Any]]] = {}
        for doc in docs:
            groups.setdefault(hashable_key(doc.get(discriminant)), []).append(doc)
        return aggregation_result(
            function,
            [
                (key, _aggregate(function, members, aggregated_field))
                for key, members in groups.items()
            ],
            grouped=True,
        )

    def keep_absent_documents(
        self,
        batch: list[dict[str, Any]],
        class_name: str,
        match_keys: list[str] | tuple[str, ...],
        keep_keys: list[str] | tuple[str, ...] | None = None,
        filters: list[Any] | dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        if not match_keys:
            return batch
        existing = {
            tuple(doc.get(key) for key in match_keys)
            for doc in self.fetch_docs(class_name)
        }
        return [
            doc
            for doc in batch
            if tuple(doc.get(field) for field in match_keys) not in existing
        ]

    def define_vertex_indexes(
        self, vertex_config: VertexConfig, schema: Schema | None = None
    ) -> None:
        return None

    def define_edge_indexes(
        self, edges: list[Edge], schema: Schema | None = None
    ) -> None:
        return None

    def introspect_graph_schema(
        self,
        schema_name: str | None = None,
        *,
        sample_limit: int = 100,
    ) -> Schema:
        return self._reader.read_schema()

    def fetch_all_docs(
        self,
        class_name: str,
        *,
        limit: int | None = None,
        timeout_s: float | None = None,
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
        self._sync_for_read()
        docs = self._reader.edge_records((source_class, target_class, relation_name))
        return docs if limit is None else docs[:limit]

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
        """The stored targets of each source's edges, read from the folded records."""
        found: dict[int, list[dict[str, Any]]] = {}
        if not match_keys_source or not match_keys_target:
            return found
        self._sync_for_read()
        stored: dict[tuple[str, ...], list[dict[str, Any]]] = {}
        for record in self._reader.edge_records(
            (source_class, target_class, relation_name)
        ):
            if not isinstance(record, list) or len(record) < 2:
                continue
            source_key = _key_of(record[0], match_keys_source)
            target_key = _key_of(record[1], match_keys_target)
            if source_key is None or target_key is None:
                continue
            stored.setdefault(source_key, []).append(
                {k: record[1][k] for k in match_keys_target}
            )
        for position, doc in enumerate(sources):
            key = _key_of(doc, match_keys_source)
            if key is not None and key in stored:
                found[position] = [dict(target) for target in stored[key]]
        return found

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
        """The default breadth-first walk, with *guard* applied to every element."""
        return bfs_neighbors(
            self,
            anchor_type=vertex_type,
            anchor_key=key,
            hops=hops,
            direction=direction,
            edge_types=edge_types,
            filters=filters,
            limit=limit,
            schema=schema,
            guard=guard,
        )

    def export_graph_container(
        self, schema: Schema, *, limit: int | None = None
    ) -> GraphContainer:
        """The graph as :meth:`GraFloBackendReader.load_graph_container` reads it.

        Keyed by the logical names of ``schema.yaml``, which is also the schema
        :meth:`introspect_graph_schema` returns.
        """
        self._sync_for_read()
        return self._reader.load_graph_container(limit=limit)
