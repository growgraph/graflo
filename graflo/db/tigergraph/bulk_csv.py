"""Append :class:`~graflo.architecture.graph_types.GraphContainer` rows to TigerGraph CSV staging files."""

from __future__ import annotations

import csv
import logging
import re
import threading
from pathlib import Path
from typing import Any, TextIO

from graflo.architecture.graph_types import GraphContainer
from graflo.architecture.schema import Schema
from graflo.architecture.schema.db_aware import EdgeRuntime, SchemaDBAware
from graflo.architecture.schema.edge import Edge
from graflo.db.connection import TigergraphBulkLoadConfig
from graflo.db.tigergraph.bulk_ids import clean_document_for_staging
from graflo.db.tigergraph.ddl_utils import (
    edge_identity_discriminator_field_names,
    tigergraph_ddl_edge_projection,
)
from graflo.onto import DBType

logger = logging.getLogger(__name__)


def _slug_filename_token(token: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", token.strip())
    return cleaned.strip("-") or "type"


def vertex_column_order(logical_name: str, schema_db: SchemaDBAware) -> list[str]:
    """CSV column order for a logical vertex (aligned with typical ADD VERTEX ordering)."""
    vconf = schema_db.vertex_config
    index_fields = tuple(vconf.identity_fields(logical_name))
    all_names = [f.name for f in vconf.properties(logical_name)]
    if len(index_fields) == 1:
        primary = index_fields[0]
        rest = [n for n in all_names if n != primary]
        return [primary, *rest]
    return list(all_names)


def edge_column_order(edge: Edge, schema_db: SchemaDBAware) -> list[str]:
    """Columns: source PKs, target PKs, discriminator fields, remaining attributes."""
    ec = schema_db.edge_config
    vc = schema_db.vertex_config
    ddl_edge = tigergraph_ddl_edge_projection(edge, ec)
    match_src = list(vc.identity_fields(edge.source))
    match_tgt = list(vc.identity_fields(edge.target))
    disc = edge_identity_discriminator_field_names(ddl_edge)
    other: list[str] = []
    for f in ddl_edge.properties:
        if f.name not in disc:
            other.append(f.name)
    return [*match_src, *match_tgt, *disc, *other]


def _project_tg_edge_triples(
    docs: list,
    relation: str | None,
    runtime: EdgeRuntime,
) -> tuple[list, str | None]:
    """Match :meth:`DBWriter._project_edge_docs_for_db` for TigerGraph."""
    relation_name = runtime.relation_name
    relation_field = runtime.effective_relation_field
    if not runtime.store_extracted_relation_as_weight or relation_field is None:
        return docs, relation_name
    projected: list = []
    for source_doc, target_doc, weight in docs:
        next_weight = dict(weight)
        if relation is not None:
            next_weight[relation_field] = relation
        projected.append((source_doc, target_doc, next_weight))
    return projected, relation_name


def _format_csv_value(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


class BulkCsvAppender:
    """Thread-safe appender for per-type CSV files under a staging directory."""

    def __init__(
        self,
        *,
        staging_dir: Path,
        bulk_cfg: TigergraphBulkLoadConfig,
        schema_db: SchemaDBAware,
    ) -> None:
        if schema_db.db_profile.db_flavor != DBType.TIGERGRAPH:
            raise ValueError("BulkCsvAppender requires TigerGraph db_flavor")
        self._staging_dir = staging_dir
        self._bulk_cfg = bulk_cfg
        self._schema_db = schema_db
        self._lock = threading.Lock()
        self._open_files: dict[str, TextIO] = {}
        self._writers: dict[str, Any] = {}
        self._manifest: dict[str, Path] = {}

    @property
    def staged_file_paths(self) -> dict[str, Path]:
        """Map manifest keys ``v:<physical>`` / ``e:<edge>`` to local CSV paths."""
        return dict(self._manifest)

    def _csv_params(self) -> dict[str, Any]:
        return {
            "delimiter": self._bulk_cfg.separator,
            "quotechar": self._bulk_cfg.quote_char,
            "lineterminator": self._bulk_cfg.line_terminator,
        }

    def _path_for_vertex(self, physical: str) -> Path:
        return self._staging_dir / f"{_slug_filename_token(physical)}.csv"

    def _path_for_edge(self, physical_edge: str) -> Path:
        return self._staging_dir / f"edge_{_slug_filename_token(physical_edge)}.csv"

    def _ensure_writer(self, key: str, path: Path, columns: list[str]) -> Any:
        if key not in self._open_files:
            path.parent.mkdir(parents=True, exist_ok=True)
            self._manifest.setdefault(key, path)
            write_header = not path.exists() or path.stat().st_size == 0
            fh = open(path, "a", encoding="utf-8", newline="")
            self._open_files[key] = fh
            writer = csv.writer(fh, **self._csv_params())
            self._writers[key] = writer
            if self._bulk_cfg.include_header and write_header:
                writer.writerow(columns)
        return self._writers[key]

    def append_graph_container(self, gc: GraphContainer, schema: Schema) -> None:
        with self._lock:
            vc = self._schema_db.vertex_config
            for vlogical, docs in gc.vertices.items():
                phys = vc.vertex_dbname(vlogical)
                cols = vertex_column_order(vlogical, self._schema_db)
                w = self._ensure_writer(f"v:{phys}", self._path_for_vertex(phys), cols)
                for doc in docs:
                    clean = clean_document_for_staging(doc)
                    row = [_format_csv_value(clean.get(c)) for c in cols]
                    w.writerow(row)

            ec = self._schema_db.edge_config
            for edge_id, docs in gc.edges.items():
                if edge_id not in schema.core_schema.edge_config:
                    continue
                edge = schema.core_schema.edge_config.edge_for(edge_id)
                if not docs:
                    continue
                runtime = ec.runtime(edge)
                relation_name = runtime.relation_name
                if not relation_name:
                    logger.warning("Skipping edge without relation name: %s", edge_id)
                    continue
                phys_edge = relation_name
                _, _, rel_key = edge_id
                projected, _ = _project_tg_edge_triples(docs, rel_key, runtime=runtime)
                cols = edge_column_order(edge, self._schema_db)
                w = self._ensure_writer(
                    f"e:{phys_edge}", self._path_for_edge(phys_edge), cols
                )
                match_src = tuple(vc.identity_fields(edge.source))
                match_tgt = tuple(vc.identity_fields(edge.target))
                ddl_edge = tigergraph_ddl_edge_projection(edge, ec)
                disc = edge_identity_discriminator_field_names(ddl_edge)
                n_src = len(match_src)
                n_tgt = len(match_tgt)
                attr_tail = cols[n_src + n_tgt + len(disc) :]
                for src_doc, tgt_doc, weight in projected:
                    clean_w = clean_document_for_staging(weight)
                    src_part = [_format_csv_value(src_doc.get(k)) for k in match_src]
                    tgt_part = [_format_csv_value(tgt_doc.get(k)) for k in match_tgt]
                    mid = [_format_csv_value(clean_w.get(k)) for k in disc]
                    tail = [_format_csv_value(clean_w.get(k)) for k in attr_tail]
                    w.writerow([*src_part, *tgt_part, *mid, *tail])

    def close(self) -> None:
        with self._lock:
            for fh in self._open_files.values():
                fh.close()
            self._open_files.clear()
            self._writers.clear()
