"""Shared helpers for example 10 (TigerGraph bulk load + S3 staging)."""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlparse

from suthing import FileHandle

from graflo import GraphManifest
from graflo.connections import TigergraphConfig
from graflo.object_storage import MinioConfig
from graflo.onto import DBType

EXAMPLE_DIR = Path(__file__).resolve().parent
MANIFEST_PATH = EXAMPLE_DIR / "manifest.yaml"
STAGING_DIR = EXAMPLE_DIR / "bulk_staging"

LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1", "0.0.0.0"}


@contextmanager
def example_workdir() -> Iterator[Path]:
    """Manifest file connectors use ``sub_path: data`` relative to this example."""
    previous_cwd = os.getcwd()
    try:
        os.chdir(EXAMPLE_DIR)
        yield EXAMPLE_DIR
    finally:
        os.chdir(previous_cwd)


def load_manifest() -> GraphManifest:
    manifest = GraphManifest.from_config(FileHandle.load(MANIFEST_PATH))
    manifest.finish_init()
    return manifest


def physical_names() -> tuple[str, str]:
    """Storage names TigerGraph sees: ``(vertex type, edge relation)``.

    Derived rather than hard-coded — sanitization and the default relation name
    both belong to the schema, and the staged CSV file names follow them.
    """
    manifest = load_manifest()
    schema = manifest.graph_schema
    schema.db_profile.db_flavor = DBType.TIGERGRAPH
    schema_db = schema.resolve_db_aware(DBType.TIGERGRAPH)
    vertex = schema_db.vertex_config.vertex_dbname("company")
    edge = schema.core_schema.edge_config.edges[0]
    relation = schema_db.edge_config.runtime(edge).relation_name
    return vertex, relation or "relates"


def use_s3() -> bool:
    """``BULK_USE_S3`` — staging uploads to S3 (default) or local paths only."""
    return os.environ.get("BULK_USE_S3", "1").lower() in ("1", "true", "yes")


def tigergraph_config() -> TigergraphConfig:
    conn_conf = TigergraphConfig.from_docker_env()
    conn_conf.max_job_size = 5000
    return conn_conf


def minio_config() -> MinioConfig:
    conf = MinioConfig.from_docker_env()
    if bucket_override := os.environ.get("BULK_S3_BUCKET"):
        conf = conf.model_copy(update={"bucket": bucket_override})
    return conf


def loader_endpoint_is_loopback(conf: MinioConfig) -> bool:
    """True when TigerGraph would be handed a URL only this host can resolve.

    ``bulk_gsql`` falls back to ``endpoint_url`` when ``loader_endpoint_url`` is
    unset, so a loopback endpoint reaches the LOADING JOB unchanged. That is
    correct when TigerGraph runs on this host and wrong when it runs in Docker.
    """
    if conf.loader_endpoint_url is not None:
        return False
    return (urlparse(conf.endpoint_url).hostname or "") in LOOPBACK_HOSTS


def latest_session_dir() -> Path | None:
    """Newest ``bulk_staging/<session_id>/`` directory, if any run left one."""
    if not STAGING_DIR.is_dir():
        return None
    sessions = [p for p in STAGING_DIR.iterdir() if p.is_dir()]
    if not sessions:
        return None
    return max(sessions, key=lambda p: p.stat().st_mtime)
