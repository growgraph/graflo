"""GraFlo file backend connection implementation."""

from __future__ import annotations

import logging
import shutil
from typing import Any

from graflo.architecture.backend import GraFloBackendReader, GraFloBackendWriter
from graflo.architecture.backend.layout import GraFloLayout
from graflo.architecture.graph_types import GraphContainer
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.vertex import VertexConfig
from graflo.db.conn import Connection, SchemaExistsError
from graflo.db.graflo_backend.config import GraFloBackendConfig
from graflo.onto import AggregationType, DBType

logger = logging.getLogger(__name__)


class GraFloBackendConnection(Connection):
    """Read/write graph data through a chunked on-disk GraFlo backend."""

    flavor = DBType.GRAFLO_BACKEND
    supports_graph_export = True

    def __init__(self, config: GraFloBackendConfig) -> None:
        super().__init__()
        self.config = config
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

    def init_db(self, schema: Schema, recreate_schema: bool) -> None:
        layout = GraFloLayout(self.config.output_dir)
        if layout.schema_path.exists() and not recreate_schema:
            raise SchemaExistsError(
                f"GraFlo backend already exists at {self.config.output_dir}"
            )
        if recreate_schema and self.config.output_dir.exists():
            self._writer.reset_data()
        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        self._writer.write_schema(schema)
        self._schema_written = True

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

    def fetch_docs(
        self,
        class_name: str,
        filters: list[Any] | dict[str, Any] | None = None,
        limit: int | None = None,
        return_keys: list[str] | None = None,
        unset_keys: list[str] | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        docs: list[dict[str, Any]] = []
        for batch in self._reader.iter_vertex_batches(class_name, limit=limit):
            docs.extend(batch)
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
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError(
            "GraFlo file backend does not support filtered edge fetch; use fetch_all_edges"
        )

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
        filters: list[Any] | dict[str, Any] | None = None,
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
