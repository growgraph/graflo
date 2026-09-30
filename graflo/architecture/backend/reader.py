"""Streaming reader for GraFlo file backend directories."""

from __future__ import annotations

import gzip
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from graflo.architecture.backend.index import GraFloIndex
from graflo.architecture.backend.layout import GraFloLayout
from graflo.architecture.graph_types import EdgeId, GraphContainer
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.physical_keys import PhysicalKeys
from graflo.onto import DBType


def stored_schema(schema: Schema) -> Schema:
    """*schema* with every stored name filled in, as the writer used them."""
    from graflo.architecture.evolution.sanitize import with_physical_names

    return with_physical_names(schema, DBType.GRAFLO_BACKEND)


def vertex_chunk_key(schema: Schema, vertex: str) -> str:
    """The name the chunks of *vertex* are stored under: its storage name.

    It is the name a writer passes the connection, so a directory written for
    a target that renames things is read back under the same name.
    """
    return stored_schema(schema).db_profile.vertex_storage_name(vertex)


def edge_chunk_key(schema: Schema, edge_id: EdgeId) -> EdgeId:
    """The key the chunks of the edge *edge_id* are stored under.

    The storage names of both endpoints and the logical relation, which is
    what a writer passes the connection.
    """
    source, target, relation = edge_id
    return (
        vertex_chunk_key(schema, source),
        vertex_chunk_key(schema, target),
        relation,
    )


class GraFloBackendReader:
    """Read schema and chunked graph data from a GraFlo backend directory."""

    def __init__(self, input_dir: Path) -> None:
        self._layout = GraFloLayout(input_dir)
        self._index: GraFloIndex | None = None

    def read_index(self) -> GraFloIndex:
        if self._index is None:
            with open(self._layout.index_path, encoding="utf-8") as fin:
                payload = json.load(fin)
            self._index = GraFloIndex.model_validate(payload)
        return self._index

    def read_schema(self) -> Schema:
        return Schema.from_yaml(str(self._layout.schema_path))

    def iter_vertex_batches(
        self,
        vertex_type: str,
        *,
        batch_size: int = 1000,
        limit: int | None = None,
    ) -> Iterator[list[dict[str, Any]]]:
        index = self.read_index()
        entry = index.vertices.get(vertex_type)
        if entry is None:
            return
        yielded = 0
        batch: list[dict[str, Any]] = []
        for chunk in entry.chunks:
            for record in self._iter_chunk_records(chunk):
                batch.append(record)
                if len(batch) >= batch_size:
                    yield batch
                    yielded += len(batch)
                    batch = []
                    if limit is not None and yielded >= limit:
                        return
            if limit is not None and yielded >= limit:
                return
        if batch and (limit is None or yielded < limit):
            if limit is not None:
                batch = batch[: limit - yielded]
            if batch:
                yield batch

    def iter_edge_batches(
        self,
        edge_key: tuple[str, str, str | None],
        *,
        batch_size: int = 1000,
        limit: int | None = None,
    ) -> Iterator[list[list[Any]]]:
        index = self.read_index()
        index_name = GraFloLayout.edge_key_to_index_name(edge_key)
        entry = index.edges.get(index_name)
        if entry is None:
            return
        yielded = 0
        batch: list[list[Any]] = []
        for chunk in entry.chunks:
            for record in self._iter_chunk_records(chunk):
                batch.append(record)
                if len(batch) >= batch_size:
                    yield batch
                    yielded += len(batch)
                    batch = []
                    if limit is not None and yielded >= limit:
                        return
            if limit is not None and yielded >= limit:
                return
        if batch and (limit is None or yielded < limit):
            if limit is not None:
                batch = batch[: limit - yielded]
            if batch:
                yield batch

    def load_graph_container(self, *, limit: int | None = None) -> GraphContainer:
        """The whole graph, keyed by the logical names of ``schema.yaml``.

        Args:
            limit: At most this many records per vertex type and per edge.
        """
        schema = self.read_schema()
        keys = PhysicalKeys(stored_schema(schema).db_profile)
        vertices: dict[str, list] = {}
        edges: dict[EdgeId, list] = {}

        for vertex in schema.core_schema.vertex_config.vertices:
            docs: list[dict[str, Any]] = []
            for batch in self.iter_vertex_batches(
                vertex_chunk_key(schema, vertex.name), limit=limit
            ):
                docs.extend(keys.logical_vertex_doc(vertex.name, doc) for doc in batch)
            if docs:
                vertices[vertex.name] = docs

        for edge in schema.core_schema.edge_config.values():
            edge_docs: list[list[Any]] = []
            for batch in self.iter_edge_batches(
                edge_chunk_key(schema, edge.edge_id), limit=limit
            ):
                edge_docs.extend(
                    keys.logical_edge_triples(
                        edge.edge_id, edge.source, edge.target, batch
                    )
                )
            if edge_docs:
                edges[edge.edge_id] = edge_docs

        return GraphContainer(vertices=vertices, edges=edges, linear=[])

    def _iter_chunk_records(self, relative_chunk: str) -> Iterator[Any]:
        chunk_path = self._layout.root / relative_chunk
        with gzip.open(chunk_path, "rt", encoding="utf-8") as fin:
            for line in fin:
                line = line.strip()
                if not line:
                    continue
                yield json.loads(line)
