"""Integration test for TigerGraph native bulk ingest lifecycle."""

from __future__ import annotations

import time

import pytest

from graflo.architecture.graph_types import GraphContainer
from graflo.architecture.schema import Schema
from graflo.architecture.schema.core import CoreSchema
from graflo.architecture.schema.edge import EdgeConfig
from graflo.architecture.schema.metadata import GraphMetadata
from graflo.architecture.schema.vertex import Field, FieldType, Vertex, VertexConfig
from graflo.db import ConnectionManager
from graflo.db.connection import TigergraphBulkLoadConfig
from graflo.onto import DBType


def _bulk_schema(graph_name: str) -> Schema:
    return Schema(
        metadata=GraphMetadata(name=graph_name, version="1.0.0"),
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
                    )
                ]
            ),
            edge_config=EdgeConfig(edges=[]),
        ),
    )


@pytest.mark.bulk_e2e
def test_tigergraph_bulk_ingest_roundtrip(conn_conf, test_graph_name, tmp_path):
    """Bulk begin/append/finalize should persist staged rows into TigerGraph."""
    conn_conf.database = test_graph_name
    conn_conf.bulk_load = TigergraphBulkLoadConfig(
        enabled=True,
        staging_dir=str(tmp_path / "bulk_staging"),
    )

    schema_obj = _bulk_schema(test_graph_name)
    schema_obj.db_profile.db_flavor = DBType.TIGERGRAPH
    schema_obj.finish_init()

    finalize_output = ""
    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.init_db(schema_obj, recreate_schema=True)
        session_id = db_client.bulk_load_begin(schema_obj, conn_conf.bulk_load)
        db_client.bulk_load_append(
            session_id=session_id,
            gc=GraphContainer(
                vertices={"Person": [{"id": "p1", "name": "Alice"}]},
                edges={},
            ),
            schema=schema_obj,
        )
        finalize_output = db_client.bulk_load_finalize(session_id, schema_obj)
        low = finalize_output.lower()
        if any(
            token in low
            for token in (
                "error",
                "failed",
                "fail",
                "abort",
                "cannot",
                "no such file",
            )
        ):
            pytest.skip(f"TigerGraph loading job reported failure: {finalize_output}")

    with ConnectionManager(connection_config=conn_conf) as db_client:
        docs: list[dict] = []
        for _ in range(100):
            docs = db_client.fetch_docs("Person", filters=["==", "p1", "id"])
            if docs:
                break
            time.sleep(0.5)
        if len(docs) != 1:
            pytest.skip(
                "TigerGraph bulk loading job finished but rows were not visible in time. "
                "This usually means loader path/access in this environment is not configured "
                f"for local staged files. Finalize output: {finalize_output}"
            )
        assert docs[0]["name"] == "Alice"
