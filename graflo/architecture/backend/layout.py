"""Path conventions for GraFlo file backend directories."""

from __future__ import annotations

import base64
import re
from pathlib import Path

from graflo.architecture.graph_types.identifiers import (
    deserialize_edge_key,
    serialize_edge_key,
)

INDEX_FILENAME = "INDEX.json"
SCHEMA_FILENAME = "schema.yaml"
VERTICES_DIR = "vertices"
EDGES_DIR = "edges"
CHUNK_SUFFIX = ".jsonl.gz"
_EDGE_DELIM = "__"
_SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9_]+$")


class GraFloLayout:
    """Deterministic path builder for a GraFlo backend root directory."""

    def __init__(self, root: Path) -> None:
        self.root = root.resolve()

    @property
    def index_path(self) -> Path:
        return self.root / INDEX_FILENAME

    @property
    def schema_path(self) -> Path:
        return self.root / SCHEMA_FILENAME

    @property
    def vertices_dir(self) -> Path:
        return self.root / VERTICES_DIR

    @property
    def edges_dir(self) -> Path:
        return self.root / EDGES_DIR

    def ensure_dirs(self) -> None:
        self.vertices_dir.mkdir(parents=True, exist_ok=True)
        self.edges_dir.mkdir(parents=True, exist_ok=True)

    def vertex_chunk_path(self, vertex_type: str, chunk_index: int) -> Path:
        stem = self._vertex_stem(vertex_type)
        filename = f"{stem}.{chunk_index:03d}{CHUNK_SUFFIX}"
        return self.vertices_dir / filename

    def edge_chunk_path(
        self, edge_key: tuple[str, str, str | None], chunk_index: int
    ) -> Path:
        stem = self._edge_stem(edge_key)
        filename = f"{stem}.{chunk_index:03d}{CHUNK_SUFFIX}"
        return self.edges_dir / filename

    def relative_vertex_chunk(self, vertex_type: str, chunk_index: int) -> str:
        return (
            self.vertex_chunk_path(vertex_type, chunk_index)
            .relative_to(self.root)
            .as_posix()
        )

    def relative_edge_chunk(
        self, edge_key: tuple[str, str, str | None], chunk_index: int
    ) -> str:
        return (
            self.edge_chunk_path(edge_key, chunk_index)
            .relative_to(self.root)
            .as_posix()
        )

    @staticmethod
    def edge_key_to_index_name(edge_key: tuple[str, str, str | None]) -> str:
        """Human-readable edge key when safe, otherwise JSON-array encoding."""
        source, target, relation = edge_key
        if (
            _SAFE_NAME_RE.fullmatch(source)
            and _SAFE_NAME_RE.fullmatch(target)
            and (relation is None or _SAFE_NAME_RE.fullmatch(relation))
        ):
            rel = relation or ""
            return f"{source}{_EDGE_DELIM}{rel}{_EDGE_DELIM}{target}"
        return serialize_edge_key(edge_key)

    @staticmethod
    def index_name_to_edge_key(name: str) -> tuple[str, str, str | None]:
        if name.startswith("["):
            return deserialize_edge_key(name)
        parts = name.split(_EDGE_DELIM)
        if len(parts) != 3:
            raise ValueError(f"Invalid edge index name: {name!r}")
        source, relation, target = parts
        return (source, target, relation or None)

    @staticmethod
    def _vertex_stem(vertex_type: str) -> str:
        if _SAFE_NAME_RE.fullmatch(vertex_type):
            return vertex_type
        return GraFloLayout._encode_stem(vertex_type)

    @staticmethod
    def _edge_stem(edge_key: tuple[str, str, str | None]) -> str:
        index_name = GraFloLayout.edge_key_to_index_name(edge_key)
        if _SAFE_NAME_RE.fullmatch(index_name.replace(_EDGE_DELIM, "_")):
            return index_name
        return GraFloLayout._encode_stem(serialize_edge_key(edge_key))

    @staticmethod
    def _encode_stem(value: str) -> str:
        encoded = base64.urlsafe_b64encode(value.encode("utf-8")).decode("ascii")
        return encoded.rstrip("=")
