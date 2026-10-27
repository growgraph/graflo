"""Streaming reader for GraFlo file backend directories."""

from __future__ import annotations

import gzip
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any, NamedTuple

from graflo.architecture.backend.fold import fold_edges, fold_vertices
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


class _Folding(NamedTuple):
    """What folds the records of one stored collection, in stored names."""

    identity: tuple[str, ...]
    union: frozenset[str]


class _EdgeFolding(NamedTuple):
    source_identity: tuple[str, ...]
    target_identity: tuple[str, ...]
    key_fields: tuple[str, ...]
    union: frozenset[str]


class GraFloBackendReader:
    """Read schema and chunked graph data from a GraFlo backend directory.

    :meth:`iter_vertex_batches` and :meth:`iter_edge_batches` yield records as
    written; :meth:`vertex_records`, :meth:`edge_records` and
    :meth:`load_graph_container` fold them by identity
    (:mod:`~graflo.architecture.backend.fold`), so a read sees one document per
    entity, as a database would.
    """

    def __init__(self, input_dir: Path) -> None:
        self._layout = GraFloLayout(input_dir)
        self._index: GraFloIndex | None = None
        self._schema: Schema | None = None
        self._vertex_folding: dict[str, tuple[str, _Folding]] | None = None
        self._edge_folding: dict[str, tuple[EdgeId, _EdgeFolding]] | None = None

    def read_index(self) -> GraFloIndex:
        if self._index is None:
            with open(self._layout.index_path, encoding="utf-8") as fin:
                payload = json.load(fin)
            self._index = GraFloIndex.model_validate(payload)
        return self._index

    def reload_index(self) -> bool:
        """Re-read ``INDEX.json``, which any writer may have published since.

        Returns:
            Whether it changed since the last read; on a change, the cached
            schema is dropped too.
        """
        previous = self._index
        if not self._layout.index_path.exists():
            self._index = None
        else:
            self._index = None
            self.read_index()
        changed = self._index != previous
        if changed:
            self._schema = None
            self._vertex_folding = None
            self._edge_folding = None
        return changed

    def read_schema(self) -> Schema:
        if self._schema is None:
            self._schema = Schema.from_yaml(str(self._layout.schema_path))
        return self._schema

    def _foldings(
        self,
    ) -> tuple[dict[str, tuple[str, _Folding]], dict[str, tuple[EdgeId, _EdgeFolding]]]:
        """Per stored collection: its logical type and how its records fold."""
        if self._vertex_folding is not None and self._edge_folding is not None:
            return self._vertex_folding, self._edge_folding
        schema = self.read_schema()
        stored = stored_schema(schema)
        db_aware = schema.resolve_db_aware(DBType.GRAFLO_BACKEND)
        keys = PhysicalKeys(stored.db_profile)
        vc = db_aware.vertex_config

        def identity(vertex: str) -> tuple[str, ...]:
            return tuple(keys.vertex_fields(vertex, vc.identity_fields(vertex)))

        vertices: dict[str, tuple[str, _Folding]] = {}
        for vertex in schema.core_schema.vertex_config.vertices:
            union = [f.name for f in vc.properties(vertex.name) if f.merge == "union"]
            vertices[vertex_chunk_key(schema, vertex.name)] = (
                vertex.name,
                _Folding(
                    identity(vertex.name),
                    frozenset(keys.vertex_fields(vertex.name, union)),
                ),
            )
        edges: dict[str, tuple[EdgeId, _EdgeFolding]] = {}
        for edge in schema.core_schema.edge_config.values():
            merge = db_aware.edge_config.relationship_merge_property_names(edge)
            union = [f.name for f in edge.properties if f.merge == "union"]
            name = GraFloLayout.edge_key_to_index_name(
                edge_chunk_key(schema, edge.edge_id)
            )
            edges[name] = (
                edge.edge_id,
                _EdgeFolding(
                    identity(edge.source),
                    identity(edge.target),
                    tuple(keys.edge_fields(edge.edge_id, merge)),
                    frozenset(keys.edge_fields(edge.edge_id, union)),
                ),
            )
        self._vertex_folding, self._edge_folding = vertices, edges
        return vertices, edges

    def vertex_type_of(self, collection: str) -> str | None:
        """The logical vertex type stored under *collection*, if one is."""
        found = self._foldings()[0].get(collection)
        return found[0] if found is not None else None

    def edge_id_of(self, edge_key: tuple[str, str, str | None]) -> EdgeId | None:
        """The logical edge stored under *edge_key*, if one is."""
        name = GraFloLayout.edge_key_to_index_name(edge_key)
        found = self._foldings()[1].get(name)
        return found[0] if found is not None else None

    def vertex_records(self, collection: str) -> list[dict[str, Any]]:
        """Every entity stored under *collection*, one folded record each."""
        records = [
            record for batch in self.iter_vertex_batches(collection) for record in batch
        ]
        found = self._foldings()[0].get(collection)
        if found is None:
            return records
        folding = found[1]
        return fold_vertices(records, folding.identity, folding.union)

    def edge_records(self, edge_key: tuple[str, str, str | None]) -> list[list[Any]]:
        """Every edge stored under *edge_key*, one folded record each."""
        records = [
            record for batch in self.iter_edge_batches(edge_key) for record in batch
        ]
        found = self._foldings()[1].get(GraFloLayout.edge_key_to_index_name(edge_key))
        if found is None:
            return records
        folding = found[1]
        return fold_edges(
            records,
            folding.source_identity,
            folding.target_identity,
            folding.key_fields,
            folding.union,
        )

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
            records = self.vertex_records(vertex_chunk_key(schema, vertex.name))
            docs = [
                keys.logical_vertex_doc(vertex.name, doc) for doc in records[:limit]
            ]
            if docs:
                vertices[vertex.name] = docs

        for edge in schema.core_schema.edge_config.values():
            stored_edges = self.edge_records(edge_chunk_key(schema, edge.edge_id))
            edge_docs = keys.logical_edge_triples(
                edge.edge_id, edge.source, edge.target, stored_edges[:limit]
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
