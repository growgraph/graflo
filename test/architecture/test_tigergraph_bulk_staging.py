"""TigerGraph bulk staging writes every edge the schema declares.

A relation read from the data is declared by the relation-less template
between its endpoints, so its edges are staged under that template, exactly as
the REST writer resolves them.
"""

from __future__ import annotations

from pathlib import Path

from graflo.architecture.graph_types import GraphContainer
from graflo.architecture.schema import CoreSchema, GraphMetadata, Schema
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig
from graflo.connections.onto import TigergraphBulkLoadConfig
from graflo.db.tigergraph.bulk_csv import BulkCsvAppender
from graflo.onto import DBType


def _schema() -> Schema:
    schema = Schema(
        metadata=GraphMetadata(name="supply"),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(
                vertices=[
                    Vertex(
                        name="company", properties=[Field(name="id")], identity=["id"]
                    )
                ]
            ),
            edge_config=EdgeConfig(
                edges=[
                    Edge(
                        source="company",
                        target="company",
                        identities=[["relation"]],
                        properties=[Field(name="date")],
                    )
                ]
            ),
        ),
    )
    schema.finish_init()
    return schema


def test_edges_with_a_relation_read_from_the_data_are_staged(tmp_path: Path) -> None:
    schema = _schema()
    appender = BulkCsvAppender(
        staging_dir=tmp_path,
        bulk_cfg=TigergraphBulkLoadConfig(enabled=True, staging_dir=str(tmp_path)),
        schema_db=schema.resolve_db_aware(DBType.TIGERGRAPH),
    )
    container = GraphContainer(
        vertices={"company": [{"id": "a"}, {"id": "b"}]},
        edges={
            ("company", "company", "supplies"): [
                ({"id": "a"}, {"id": "b"}, {"date": "2026-01-01"})
            ]
        },
        linear=[],
    )

    appender.append_graph_container(container, schema)
    appender.close()

    edge_files = [p for p in appender.staged_file_paths.values() if "edge_" in p.name]
    assert edge_files, "no edge file was staged"
    rows = edge_files[0].read_text().strip().splitlines()
    assert any("supplies" in row for row in rows)
