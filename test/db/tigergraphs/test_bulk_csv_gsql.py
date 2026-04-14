"""Unit tests for TigerGraph bulk CSV column order and GSQL generation."""

from __future__ import annotations

from pathlib import Path

from graflo.architecture.contract.bindings import Bindings, StagingProxyBinding
from graflo.architecture.schema import Schema
from graflo.architecture.schema.core import CoreSchema
from graflo.architecture.schema.edge import EdgeConfig
from graflo.architecture.schema.metadata import GraphMetadata
from graflo.architecture.schema.vertex import Field, FieldType, Vertex, VertexConfig
from graflo.db.connection import TigergraphBulkLoadConfig, TigergraphBulkLoadJobOptions
from graflo.db.tigergraph.bulk_csv import vertex_column_order
from graflo.db.tigergraph.bulk_gsql import build_create_and_run_loading_job
from graflo.onto import DBType


def _toy_schema() -> Schema:
    return Schema(
        metadata=GraphMetadata(name="toy", version="1.0.0"),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(
                vertices=[
                    Vertex(
                        name="Person",
                        properties=[
                            Field(name="id", type=FieldType.STRING),
                            Field(name="name", type=FieldType.STRING),
                        ],
                        identity=["id"],
                    ),
                ],
            ),
            edge_config=EdgeConfig(edges=[]),
        ),
    )


def test_vertex_column_order_single_primary_first() -> None:
    schema_obj = _toy_schema()
    schema_obj.db_profile.db_flavor = DBType.TIGERGRAPH
    schema_obj.finish_init()
    sdb = schema_obj.resolve_db_aware(DBType.TIGERGRAPH)
    # First vertex in minimal_schema fixture — use first logical name
    v0 = next(iter(sdb.vertex_config.vertex_set))
    cols = vertex_column_order(v0, sdb)
    id_fields = sdb.vertex_config.identity_fields(v0)
    assert cols[0] == id_fields[0]
    assert set(cols) == set(sdb.vertex_config.property_names(v0))


def test_staging_proxy_bindings_resolve() -> None:
    b = Bindings(
        staging_proxy=[
            StagingProxyBinding(name="s3a", conn_proxy="proxy_a"),
        ]
    )
    cfg = TigergraphBulkLoadConfig(
        enabled=True,
        staging_dir="/tmp/x",
        s3_staging_name="s3a",
    )
    assert cfg.resolve_s3_conn_proxy(b) == "proxy_a"
    assert cfg.resolve_s3_conn_proxy(None) is None


def test_build_loading_job_contains_vertex_load(tmp_path: Path) -> None:
    schema_obj = _toy_schema()
    schema_obj.db_profile.db_flavor = DBType.TIGERGRAPH
    schema_obj.finish_init()
    sdb = schema_obj.resolve_db_aware(DBType.TIGERGRAPH)
    v0 = next(iter(sdb.vertex_config.vertex_set))
    phys = sdb.vertex_config.vertex_dbname(v0)
    csv_path = tmp_path / f"{phys}.csv"
    csv_path.write_text("id\nx\n", encoding="utf-8")
    staged = {f"v:{phys}": csv_path}
    bulk = TigergraphBulkLoadConfig(
        enabled=True,
        staging_dir=str(tmp_path),
        loading_job=TigergraphBulkLoadJobOptions(
            drop_job_after_run=False,
            job_name_prefix="tjob",
        ),
    )
    path_for = {f"v:{phys}": str(csv_path.resolve())}
    gsql = build_create_and_run_loading_job(
        graph_name="G",
        job_name="tjob_sess",
        schema_db=sdb,
        staged_files=staged,
        bulk_cfg=bulk,
        path_for_gsql=path_for,
    )
    assert "USE GRAPH G" in gsql
    assert "CREATE LOADING JOB tjob_sess" in gsql
    assert f"TO VERTEX {phys}" in gsql
    assert "RUN LOADING JOB tjob_sess" in gsql
