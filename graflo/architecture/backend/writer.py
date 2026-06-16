"""Streaming writer for GraFlo file backend directories."""

from __future__ import annotations

import gzip
import json
import shutil
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from types import TracebackType
from typing import Any

from graflo.architecture.backend.index import (
    CollectionEntry,
    GraFloIndex,
    backend_schema_hash,
)
from graflo.architecture.backend.layout import GraFloLayout
from graflo.architecture.schema.document import Schema


def _graflo_package_version() -> str:
    try:
        return version("graflo")
    except PackageNotFoundError:
        return "unknown"


class _CollectionWriter:
    """Accumulate records for one vertex type or edge collection."""

    def __init__(
        self,
        layout: GraFloLayout,
        *,
        chunk_size: int,
        vertex_type: str | None = None,
        edge_key: tuple[str, str, str | None] | None = None,
        existing: CollectionEntry | None = None,
    ) -> None:
        if (vertex_type is None) == (edge_key is None):
            raise ValueError("Exactly one of vertex_type or edge_key must be set")
        self._layout = layout
        self._chunk_size = chunk_size
        self._vertex_type = vertex_type
        self._edge_key = edge_key
        self._buffer: list[dict[str, Any] | list[Any]] = []
        self._chunk_index = len(existing.chunks) if existing is not None else 0
        self._record_count = existing.record_count if existing is not None else 0
        self._chunks = list(existing.chunks) if existing is not None else []
        self._file_obj: gzip.GzipFile | None = None

    def push_many(self, records: list[dict[str, Any]] | list[list[Any]]) -> None:
        for record in records:
            self.push(record)

    def push(self, record: dict[str, Any] | list[Any]) -> None:
        self._buffer.append(record)
        if len(self._buffer) >= self._chunk_size:
            self._flush()

    def snapshot(self) -> CollectionEntry:
        return CollectionEntry(chunks=self._chunks, record_count=self._record_count)

    def flush(self) -> None:
        self._flush()

    def _flush(self) -> None:
        if not self._buffer:
            return
        self._open_chunk()
        assert self._file_obj is not None
        for record in self._buffer:
            line = json.dumps(record, separators=(",", ":"), default=str)
            self._file_obj.write((line + "\n").encode("utf-8"))
        self._record_count += len(self._buffer)
        self._buffer = []
        self._close_file()

    def _open_chunk(self) -> None:
        if self._file_obj is not None:
            return
        if self._vertex_type is not None:
            path = self._layout.vertex_chunk_path(self._vertex_type, self._chunk_index)
            relative = self._layout.relative_vertex_chunk(
                self._vertex_type, self._chunk_index
            )
        else:
            assert self._edge_key is not None
            path = self._layout.edge_chunk_path(self._edge_key, self._chunk_index)
            relative = self._layout.relative_edge_chunk(
                self._edge_key, self._chunk_index
            )
        path.parent.mkdir(parents=True, exist_ok=True)
        self._file_obj = gzip.open(path, "wb")
        self._chunks.append(relative)
        self._chunk_index += 1

    def _close_file(self) -> None:
        if self._file_obj is not None:
            self._file_obj.close()
            self._file_obj = None


class GraFloBackendWriter:
    """Write schema and chunked graph data to a GraFlo backend directory."""

    def __init__(
        self,
        output_dir: Path,
        *,
        chunk_size: int = 50_000,
        resume: bool = False,
    ) -> None:
        self._layout = GraFloLayout(output_dir)
        self._chunk_size = chunk_size
        self._resume = resume
        self._schema: Schema | None = None
        self._index: GraFloIndex | None = None
        self._vertex_writers: dict[str, _CollectionWriter] = {}
        self._edge_writers: dict[tuple[str, str, str | None], _CollectionWriter] = {}
        if resume and self._layout.index_path.exists():
            with open(self._layout.index_path, encoding="utf-8") as fin:
                payload = json.load(fin)
            self._index = GraFloIndex.model_validate(payload)

    def __enter__(self) -> GraFloBackendWriter:
        self._layout.ensure_dirs()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if exc is None:
            self.flush_index()

    @property
    def layout(self) -> GraFloLayout:
        return self._layout

    def reset_data(self) -> None:
        """Remove data chunks and index while keeping schema if present."""
        for path in (self._layout.vertices_dir, self._layout.edges_dir):
            if path.exists():
                shutil.rmtree(path)
        if self._layout.index_path.exists():
            self._layout.index_path.unlink()
        self._index = None
        self._vertex_writers = {}
        self._edge_writers = {}
        self._layout.ensure_dirs()

    def write_schema(self, schema: Schema) -> None:
        self._schema = schema
        self._layout.ensure_dirs()
        import yaml

        with open(self._layout.schema_path, "w", encoding="utf-8") as fout:
            yaml.safe_dump(
                schema.model_dump(mode="json", by_alias=True, exclude_none=True),
                fout,
                default_flow_style=False,
                sort_keys=False,
            )

    def write_vertex_batch(self, vertex_type: str, docs: list[dict[str, Any]]) -> None:
        if not docs:
            return
        writer = self._vertex_writers.setdefault(
            vertex_type,
            self._make_vertex_writer(vertex_type),
        )
        writer.push_many(docs)

    def write_edge_batch(
        self,
        edge_key: tuple[str, str, str | None],
        docs: list[list[Any]],
    ) -> None:
        if not docs:
            return
        writer = self._edge_writers.setdefault(
            edge_key,
            self._make_edge_writer(edge_key),
        )
        writer.push_many(docs)

    def flush_index(self) -> GraFloIndex:
        if self._schema is None and self._layout.schema_path.exists():
            self._schema = Schema.from_yaml(str(self._layout.schema_path))
        if self._schema is None:
            raise ValueError("Cannot flush GraFlo backend index without schema")

        for writer in self._vertex_writers.values():
            writer.flush()
        for writer in self._edge_writers.values():
            writer.flush()

        vertices = self._collect_vertex_entries()
        edges = self._collect_edge_entries()
        index = GraFloIndex(
            graflo_version=_graflo_package_version(),
            schema_hash=backend_schema_hash(self._schema),
            vertices=vertices,
            edges=edges,
        )
        with open(self._layout.index_path, "w", encoding="utf-8") as fout:
            fout.write(
                json.dumps(
                    index.model_dump(mode="json", by_alias=True, exclude_none=True),
                    indent=2,
                    sort_keys=True,
                )
                + "\n"
            )
        self._index = index
        return index

    def _make_vertex_writer(self, vertex_type: str) -> _CollectionWriter:
        existing = self._index.vertices.get(vertex_type) if self._index else None
        return _CollectionWriter(
            self._layout,
            chunk_size=self._chunk_size,
            vertex_type=vertex_type,
            existing=existing,
        )

    def _make_edge_writer(
        self, edge_key: tuple[str, str, str | None]
    ) -> _CollectionWriter:
        index_name = GraFloLayout.edge_key_to_index_name(edge_key)
        existing = self._index.edges.get(index_name) if self._index else None
        return _CollectionWriter(
            self._layout,
            chunk_size=self._chunk_size,
            edge_key=edge_key,
            existing=existing,
        )

    def _collect_vertex_entries(self) -> dict[str, CollectionEntry]:
        entries = dict(self._index.vertices) if self._index is not None else {}
        for name, writer in self._vertex_writers.items():
            entries[name] = writer.snapshot()
        return entries

    def _collect_edge_entries(self) -> dict[str, CollectionEntry]:
        entries = dict(self._index.edges) if self._index is not None else {}
        for edge_key, writer in self._edge_writers.items():
            index_name = GraFloLayout.edge_key_to_index_name(edge_key)
            entries[index_name] = writer.snapshot()
        return entries
