"""Generate GSQL ``CREATE LOADING JOB`` / ``RUN LOADING JOB`` for bulk CSV staging."""

from __future__ import annotations

from pathlib import Path

from graflo.architecture.schema.db_aware import EdgeConfigDBAware, SchemaDBAware
from graflo.architecture.schema.edge import Edge
from graflo.db.connection import TigergraphBulkLoadConfig, TigergraphBulkLoadJobOptions
from graflo.db.tigergraph.bulk_csv import edge_column_order, vertex_column_order
from graflo.onto import DBType


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


def build_create_and_run_loading_job(
    *,
    graph_name: str,
    job_name: str,
    schema_db: SchemaDBAware,
    staged_files: dict[str, Path],
    bulk_cfg: TigergraphBulkLoadConfig,
    path_for_gsql: dict[str, str],
) -> str:
    """Build a GSQL script: USE GRAPH, CREATE LOADING JOB, RUN, optional DROP.

    *path_for_gsql* maps manifest keys (``v:...``, ``e:...``) to absolute local paths
    or ``s3://`` URLs visible to the TigerGraph loader.
    """
    if schema_db.db_profile.db_flavor != DBType.TIGERGRAPH:
        raise ValueError("bulk_gsql requires TigerGraph schema projection")
    vc = schema_db.vertex_config
    ec = schema_db.edge_config
    sep = bulk_cfg.separator
    header_flag = "true" if bulk_cfg.include_header else "false"
    sep_esc = sep.replace("\\", "\\\\").replace('"', '\\"')

    body_lines: list[str] = [
        f"USE GRAPH {graph_name}",
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
        body_lines.append(f'  DEFINE FILENAME {fname} = "{gsql_path}";')
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
