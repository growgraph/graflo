"""GraFlo file backend connection implementation."""

from __future__ import annotations

import logging
import shutil
from typing import Any

from graflo.architecture.backend import GraFloBackendReader, GraFloBackendWriter
from graflo.architecture.backend.layout import GraFloLayout
from graflo.architecture.graph_types import EdgeDirection, GraphContainer
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.vertex import VertexConfig
from graflo.connections.graflo_backend import GraFloBackendConfig
from graflo.db.conn import Connection, NamespaceNotFoundError, SchemaExistsError
from graflo.filter.onto import FilterExpression, parse_filter_expression
from graflo.onto import AggregationType, DBType, ExpressionFlavor

logger = logging.getLogger(__name__)

#: Ceiling on rows held in the in-process edge index. Traversal over a file
#: backend larger than this is the wrong tool; the bound is logged, never silent.
_EDGE_INDEX_MAX_ROWS = 2_000_000


def _first_value(doc: Any, fields: list[str]) -> str | None:
    """First present identity value in *doc*, as a string."""
    if not isinstance(doc, dict):
        return None
    for field in [*fields, "_key", "id"]:
        value = doc.get(field)
        if value is not None:
            return str(value)
    return None


class GraFloBackendConnection(Connection):
    """Read/write graph data through a chunked on-disk GraFlo backend."""

    flavor = DBType.GRAFLO_BACKEND
    supports_graph_export = True
    supports_schema_introspection = True

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

    def _sync_for_read(self) -> None:
        """Persist buffered writes so reads observe them.

        Chunk writers buffer records in memory and the index is only written on
        flush, so a read issued mid-ingest would otherwise miss data — or find
        no ``INDEX.json`` at all. Every other backend is read-your-writes;
        flushing here gives the file backend the same contract.
        """
        try:
            self._writer.flush_index()
        except ValueError:
            # No schema written yet, so nothing has been ingested to observe.
            pass

    def fetch_docs(
        self,
        class_name: str,
        filters: list[Any] | dict[str, Any] | None = None,
        limit: int | None = None,
        return_keys: list[str] | None = None,
        unset_keys: list[str] | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        self._sync_for_read()
        predicate = parse_filter_expression(filters) if filters is not None else None

        def _keep(doc: dict[str, Any]) -> bool:
            if predicate is None:
                return True
            try:
                return bool(predicate(kind=ExpressionFlavor.PYTHON, **doc))
            except Exception:
                # A document missing a filtered field simply does not match.
                return False

        def _project(doc: dict[str, Any]) -> dict[str, Any]:
            if return_keys:
                doc = {k: doc.get(k) for k in return_keys}
            if unset_keys:
                doc = {k: v for k, v in doc.items() if k not in unset_keys}
            return doc

        docs: list[dict[str, Any]] = []
        # `limit` bounds the *result* size, so filtering happens before slicing.
        for batch in self._reader.iter_vertex_batches(class_name):
            docs.extend(_project(doc) for doc in batch if _keep(doc))
            if limit is not None and len(docs) >= limit:
                return docs[:limit]
        return docs

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
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Edges incident to one vertex, served from a lazily built index.

        Direction is this backend's storage partition key, so the reverse
        orientation has to be materialized to be answerable at all. That is a
        statement about *storage*; an in-process read index is not storage, so
        building one here does not contradict the ``MATERIALIZATION_REQUIRED``
        tier in the capability matrix — it is exactly what that tier prescribes.
        """
        index = self._edge_index()
        entries = index.get(edge_type) if edge_type is not None else None
        if entries is None:
            # No edge_type: search every indexed edge type.
            entries = [row for rows in index.values() for row in rows]

        matched: list[dict[str, Any]] = []
        for row in entries:
            source_id = row.get("_from_key")
            target_id = row.get("_to_key")
            if direction is EdgeDirection.OUT:
                anchored = source_id == from_id
                far = target_id
            elif direction is EdgeDirection.IN:
                anchored = target_id == from_id
                far = source_id
            else:
                if source_id == from_id:
                    anchored, far = True, target_id
                elif target_id == from_id:
                    anchored, far = True, source_id
                else:
                    anchored, far = False, None
            if not anchored:
                continue
            if to_id is not None and far != to_id:
                continue
            matched.append(row)
            if limit is not None and len(matched) >= limit:
                break

        if filters is not None:
            expression = parse_filter_expression(filters)
            matched = [
                row for row in matched if expression(row, kind=ExpressionFlavor.PYTHON)
            ]
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
        """Storage edge name -> flat edge rows, built once per connection.

        Rows carry ``_from_key`` / ``_to_key`` so both orientations are
        answerable from one pass over the chunked files.
        """
        if self._edge_index_cache is not None:
            return self._edge_index_cache

        schema = self._reader.read_schema()
        db_aware = schema.resolve_db_aware(self.flavor)
        index: dict[str, list[dict[str, Any]]] = {}
        total = 0
        for edge in schema.core_schema.edge_config.edges:
            storage = db_aware.edge_config.runtime(edge).storage_name()
            if storage is None:
                continue
            rows = index.setdefault(storage, [])
            source_identity = db_aware.vertex_config.identity_fields(edge.source)
            target_identity = db_aware.vertex_config.identity_fields(edge.target)
            for batch in self._reader.iter_edge_batches(edge.edge_id):
                for record in batch:
                    if not isinstance(record, list) or len(record) < 2:
                        continue
                    source_doc, target_doc = record[0], record[1]
                    weight = record[2] if len(record) > 2 else {}
                    rows.append(
                        {
                            **(weight if isinstance(weight, dict) else {}),
                            "_from_key": _first_value(source_doc, source_identity),
                            "_to_key": _first_value(target_doc, target_identity),
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
    ) -> int | float | list[dict[str, Any]] | dict[str, int | float] | None:
        raise NotImplementedError(
            "GraFlo file backend does not support aggregate queries"
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
        edge_key = (source_class, target_class, relation_name)
        docs: list[list[dict[str, Any]]] = []
        for batch in self._reader.iter_edge_batches(edge_key, limit=limit):
            docs.extend(batch)
            if limit is not None and len(docs) >= limit:
                return docs[:limit]
        return docs

    def bulk_load_append(
        self, session_id: str, gc: GraphContainer, schema: Schema
    ) -> None:
        for vertex_type, vertex_docs in gc.vertices.items():
            self._writer.write_vertex_batch(vertex_type, vertex_docs)
        for edge_key, edge_docs in gc.edges.items():
            self._writer.write_edge_batch(edge_key, edge_docs)
