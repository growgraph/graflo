"""TigerGraph bulk CSV load session management."""

from __future__ import annotations

import threading
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any

from graflo.architecture.contract.bindings import Bindings
from graflo.architecture.graph_types import GraphContainer
from graflo.architecture.schema import Schema
from graflo.db.connection import TigergraphBulkLoadConfig
from graflo.db.tigergraph.bulk_csv import BulkCsvAppender
from graflo.db.tigergraph.bulk_gsql import (
    build_create_and_run_loading_job,
    build_run_loading_job_only,
)
from graflo.object_storage import upload_staged_csvs
from graflo.onto import DBType

if TYPE_CHECKING:
    from graflo.hq.connection_provider import (
        ConnectionProvider,
        S3GeneralizedConnConfig,
    )
    from graflo.db.tigergraph.conn import TigerGraphConnection

_tiger_bulk_sessions_lock = threading.Lock()
_tiger_bulk_sessions: dict[
    str, tuple[BulkCsvAppender, TigergraphBulkLoadConfig, Any, Path]
] = {}


def bulk_load_begin(
    conn: TigerGraphConnection, schema: Schema, bulk_cfg: TigergraphBulkLoadConfig
) -> str:
    """Start CSV staging session under ``bulk_cfg.staging_dir /<session_id>``."""
    if not bulk_cfg.enabled:
        raise ValueError(
            "bulk_load_begin requires TigergraphBulkLoadConfig.enabled=True"
        )
    if not bulk_cfg.staging_dir:
        raise ValueError(
            "TigergraphBulkLoadConfig.staging_dir is required for bulk load"
        )
    schema_db = schema.resolve_db_aware(DBType.TIGERGRAPH)
    if schema_db.vertex_config.blank_vertices:
        raise ValueError(
            "TigerGraph bulk_load does not support blank_vertices in this release; "
            "use REST ingest or remove blank vertex placeholders."
        )
    session_id = uuid.uuid4().hex[:12]
    staging_root = Path(bulk_cfg.staging_dir) / session_id
    staging_root.mkdir(parents=True, exist_ok=True)
    appender = BulkCsvAppender(
        staging_dir=staging_root,
        bulk_cfg=bulk_cfg,
        schema_db=schema_db,
    )
    with _tiger_bulk_sessions_lock:
        _tiger_bulk_sessions[session_id] = (
            appender,
            bulk_cfg,
            schema_db,
            staging_root,
        )
    return session_id


def bulk_load_append(
    conn: TigerGraphConnection, session_id: str, gc: GraphContainer, schema: Schema
) -> None:
    with _tiger_bulk_sessions_lock:
        if session_id not in _tiger_bulk_sessions:
            raise KeyError(f"Unknown TigerGraph bulk session {session_id!r}")
        appender, _, _, _ = _tiger_bulk_sessions[session_id]
        appender.append_graph_container(gc, schema)


def bulk_load_finalize(  # noqa: PLR0912
    conn: TigerGraphConnection,
    session_id: str,
    schema: Schema,
    *,
    bindings: Bindings | None = None,
    connection_provider: ConnectionProvider | None = None,
) -> str:
    """Upload to S3 when configured, then CREATE/RUN/DROP LOADING JOB."""
    _ = schema
    with _tiger_bulk_sessions_lock:
        if session_id not in _tiger_bulk_sessions:
            raise KeyError(f"Unknown TigerGraph bulk session {session_id!r}")
        appender, bulk_cfg, schema_db, _staging_root = _tiger_bulk_sessions.pop(
            session_id
        )
    appender.close()
    staged = appender.staged_file_paths
    if not staged:
        return ""
    graph_name = conn._require_configured_graph_name()
    job_name = f"{bulk_cfg.loading_job.job_name_prefix}_{session_id}"
    path_for_gsql: dict[str, str] = {k: str(v.resolve()) for k, v in staged.items()}
    proxy = bulk_cfg.resolve_s3_conn_proxy(bindings)
    bucket = bulk_cfg.s3_bucket
    tigergraph_s3_loader: S3GeneralizedConnConfig | None = None
    if proxy and connection_provider is not None:
        from graflo.hq.connection_provider import S3GeneralizedConnConfig

        gen = connection_provider.get_generalized_config_by_proxy(proxy)
        if isinstance(gen, S3GeneralizedConnConfig):
            tigergraph_s3_loader = gen
            resolved_bucket = bucket or gen.bucket
            if not resolved_bucket:
                raise ValueError(
                    "S3 bulk staging requires TigergraphBulkLoadConfig.s3_bucket "
                    "or S3GeneralizedConnConfig.bucket"
                )
            path_for_gsql = upload_staged_csvs(
                staged_files=staged,
                bucket=resolved_bucket,
                key_prefix=bulk_cfg.s3_key_prefix,
                session_id=session_id,
                s3_cfg=gen,
            )
    if bulk_cfg.loading_job.run_mode == "run_only":
        gsql = build_run_loading_job_only(job_name=job_name, opts=bulk_cfg.loading_job)
    else:
        gsql = build_create_and_run_loading_job(
            graph_name=graph_name,
            job_name=job_name,
            schema_db=schema_db,
            staged_files=staged,
            bulk_cfg=bulk_cfg,
            path_for_gsql=path_for_gsql,
            tigergraph_s3_loader=tigergraph_s3_loader,
            tigergraph_s3_data_source_name=f"gf_s3_{session_id}",
        )
    return str(conn._execute_gsql(gsql))
