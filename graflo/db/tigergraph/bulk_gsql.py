"""Generate GSQL ``CREATE LOADING JOB`` / ``RUN LOADING JOB`` for bulk CSV staging."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

from graflo.architecture.schema.db_aware import EdgeConfigDBAware, SchemaDBAware
from graflo.architecture.schema.edge import Edge
from graflo.db.connection import TigergraphBulkLoadConfig, TigergraphBulkLoadJobOptions
from graflo.db.tigergraph.bulk_csv import edge_column_order, vertex_column_order
from graflo.onto import DBType

if TYPE_CHECKING:
    from graflo.hq.connection_provider import S3GeneralizedConnConfig


def _logical_vertex_for_physical(vc, physical: str) -> str:
    for logical in vc.vertex_set:
        if vc.vertex_dbname(logical) == physical:
            return logical
    raise KeyError(f"No logical vertex for physical type {physical!r}")


def _first_edge_for_physical_relation(
    ec: EdgeConfigDBAware, physical_relation: str
) -> Edge:
    for edge in ec.values():
        if ec.runtime(edge).relation_name == physical_relation:
            return edge
    raise KeyError(f"No edge with relation/type {physical_relation!r}")


def _values_placeholders(n: int) -> str:
    return ", ".join(f"${i}" for i in range(n))


_DATA_SOURCE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _tigergraph_s3_data_source_dict(cfg: "S3GeneralizedConnConfig") -> dict[str, Any]:
    """JSON object for ``CREATE DATA_SOURCE`` (TigerGraph cloud / S3 loader)."""
    if not cfg.aws_access_key_id or not cfg.aws_secret_access_key:
        raise ValueError(
            "TigerGraph S3 DATA_SOURCE requires aws_access_key_id and aws_secret_access_key"
        )
    out: dict[str, Any] = {
        "type": "s3",
        "access.key": cfg.aws_access_key_id,
        "secret.key": cfg.aws_secret_access_key,
    }
    ep = cfg.loader_endpoint_url or cfg.endpoint_url
    if ep:
        out["file.reader.settings.fs.s3a.endpoint"] = ep
        out["file.reader.settings.fs.s3a.path.style.access"] = "true"
    return out


def _define_filename_rhs(path: str, *, data_source_name: str | None) -> str:
    """Right-hand side of ``DEFINE FILENAME f = ...`` (quoted)."""
    if path.startswith("s3://") and data_source_name is not None:
        inner = f"${data_source_name}:{path}"
        return f'"{inner}"'
    escaped = path.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def build_create_and_run_loading_job(
    *,
    graph_name: str,
    job_name: str,
    schema_db: SchemaDBAware,
    staged_files: dict[str, Path],
    bulk_cfg: TigergraphBulkLoadConfig,
    path_for_gsql: dict[str, str],
    tigergraph_s3_loader: "S3GeneralizedConnConfig | None" = None,
    tigergraph_s3_data_source_name: str | None = None,
) -> str:
    """Build a GSQL script: USE GRAPH, optional DATA_SOURCE, CREATE LOADING JOB, RUN.

    *path_for_gsql* maps manifest keys (``v:...``, ``e:...``) to absolute local paths
    or ``s3://`` URLs.

    TigerGraph requires a ``CREATE DATA_SOURCE`` with credentials and (for MinIO) a
    custom endpoint when using ``s3://`` paths; filenames must look like
    ``"$data_source_name:s3://bucket/key"``. Pass *tigergraph_s3_loader* when any path
    is ``s3://`` (typically the same :class:`~graflo.hq.connection_provider.S3GeneralizedConnConfig`
    used for boto3 upload).
    """
    if schema_db.db_profile.db_flavor != DBType.TIGERGRAPH:
        raise ValueError("bulk_gsql requires TigerGraph schema projection")
    vc = schema_db.vertex_config
    ec = schema_db.edge_config
    sep = bulk_cfg.separator
    header_flag = "true" if bulk_cfg.include_header else "false"
    sep_esc = sep.replace("\\", "\\\\").replace('"', '\\"')

    needs_s3_data_source = any(v.startswith("s3://") for v in path_for_gsql.values())
    ds_name: str | None = None
    if needs_s3_data_source:
        if tigergraph_s3_loader is None:
            raise ValueError(
                "TigerGraph LOADING JOB with s3:// paths requires "
                "tigergraph_s3_loader (S3GeneralizedConnConfig): "
                "use CREATE DATA_SOURCE with credentials and a MinIO endpoint. "
                "Bare s3:// URLs are resolved against AWS and will not load from MinIO."
            )
        ds_name = tigergraph_s3_data_source_name or "gf_s3_loader"
        if not _DATA_SOURCE_NAME_RE.match(ds_name):
            raise ValueError(
                f"Invalid GSQL data source name {ds_name!r} "
                "(use letters, digits, underscore; start with letter or _)"
            )
        _tigergraph_s3_data_source_dict(tigergraph_s3_loader)

    header_lines: list[str] = [f"USE GRAPH {graph_name}"]
    if (
        needs_s3_data_source
        and ds_name is not None
        and tigergraph_s3_loader is not None
    ):
        ds_json = json.dumps(
            _tigergraph_s3_data_source_dict(tigergraph_s3_loader),
            separators=(",", ":"),
        )
        header_lines.append(
            f'CREATE DATA_SOURCE {ds_name} = """{ds_json}""" FOR GRAPH {graph_name}'
        )

    body_lines: list[str] = [
        *header_lines,
        f"CREATE LOADING JOB {job_name} FOR GRAPH {graph_name} {{",
    ]

    fn_counter = 0

    def append_load(label: str, physical: str, ncols: int, edge_mode: bool) -> None:
        nonlocal fn_counter
        if ncols <= 0:
            return
        gsql_path = path_for_gsql.get(label) or str(staged_files[label].resolve())
        fname = f"f{fn_counter}"
        fn_counter += 1
        vals = _values_placeholders(ncols)
        target_kw = "EDGE" if edge_mode else "VERTEX"
        rhs = _define_filename_rhs(gsql_path, data_source_name=ds_name)
        body_lines.append(f"  DEFINE FILENAME {fname} = {rhs};")
        body_lines.append(
            f"  LOAD {fname} TO {target_kw} {physical} VALUES ({vals}) "
            f'USING HEADER="{header_flag}", SEPARATOR="{sep_esc}"'
        )

    for key in sorted(k for k in staged_files if k.startswith("v:")):
        physical = key[2:]
        logical = _logical_vertex_for_physical(vc, physical)
        append_load(
            key,
            physical,
            len(vertex_column_order(logical, schema_db)),
            edge_mode=False,
        )

    for key in sorted(k for k in staged_files if k.startswith("e:")):
        physical = key[2:]
        try:
            edge = _first_edge_for_physical_relation(ec, physical)
        except KeyError:
            continue
        append_load(
            key,
            physical,
            len(edge_column_order(edge, schema_db)),
            edge_mode=True,
        )

    body_lines.append("}")

    job_opts = bulk_cfg.loading_job
    body_lines.append(
        f"RUN LOADING JOB {job_name} "
        f"USING CONCURRENCY={job_opts.concurrency}, BATCH_SIZE={job_opts.batch_size}"
    )
    if job_opts.drop_job_after_run:
        body_lines.append(f"DROP JOB {job_name}")
        if needs_s3_data_source and ds_name is not None:
            body_lines.append(f"DROP DATA_SOURCE {ds_name}")

    return "\n".join(body_lines) + "\n"


def build_run_loading_job_only(
    *,
    job_name: str,
    opts: TigergraphBulkLoadJobOptions,
) -> str:
    return (
        f"RUN LOADING JOB {job_name} "
        f"USING CONCURRENCY={opts.concurrency}, BATCH_SIZE={opts.batch_size}\n"
    )
