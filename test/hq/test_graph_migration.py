"""Tests for GraphEngine graph export/migration helpers."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from graflo.architecture.graph_types import GraphContainer
from graflo.architecture.schema import CoreSchema, GraphMetadata, Schema
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig
from graflo.connections.onto import DBConfig
from graflo.db.conn import ConnectionCapability
from graflo.hq.graph_engine import GraphEngine
from graflo.onto import DBType


def _sample_schema() -> Schema:
    return Schema(
        metadata=GraphMetadata(name="demo"),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(
                vertices=[
                    Vertex(
                        name="person",
                        properties=[Field(name="id"), Field(name="name")],
                        identity=["id"],
                    )
                ]
            ),
            edge_config=EdgeConfig(edges=[]),
        ),
    )


def test_export_graph_container_builds_vertices_and_edges() -> None:
    schema = _sample_schema()
    schema.core_schema.edge_config = EdgeConfig(
        edges=[
            Edge(source="person", target="person", relation="knows"),
        ]
    )
    conn = MagicMock()
    conn.fetch_all_docs.return_value = [{"id": "1", "name": "Alice"}]
    conn.fetch_all_edges.return_value = [
        [{"id": "1"}, {"id": "2"}, {"since": 2020}],
    ]

    gc = GraphEngine._export_graph_container(conn, schema)

    assert isinstance(gc, GraphContainer)
    assert gc.vertices["person"][0]["name"] == "Alice"
    assert ("person", "person", "knows") in gc.edges
    conn.fetch_all_docs.assert_called_once_with("person", limit=None)
    conn.fetch_all_edges.assert_called_once_with(
        "person", "person", "knows", limit=None
    )


@patch("graflo.hq.graph_engine.ConnectionManager.open_read_connection")
@patch("graflo.hq.graph_engine.Sanitizer.sanitize_manifest")
def test_infer_schema_from_graph(mock_sanitize, mock_open) -> None:
    mock_conn = MagicMock()
    mock_conn.introspect_graph_schema.return_value = _sample_schema()
    mock_open.return_value = mock_conn
    mock_sanitize.side_effect = lambda m: m

    source = MagicMock(spec=DBConfig)
    source.connection_type = DBType.NEO4J

    engine = GraphEngine(target_db_flavor=DBType.ARANGO)
    schema = engine.infer_schema_from_graph(source)

    assert schema.metadata.name == "demo"
    mock_conn.close.assert_called_once()


@patch("graflo.hq.graph_engine.ConnectionManager.open_read_connection")
@patch("graflo.hq.graph_engine.Sanitizer.sanitize_manifest")
def test_infer_schema_from_graph_requires_only_introspection(
    mock_sanitize, mock_open
) -> None:
    """It must not demand bulk export, which is a strictly stronger claim.

    Requiring export is what limited schema inference to the three backends that
    can dump a whole graph, even though every backend can describe itself.
    """
    mock_conn = MagicMock()
    mock_conn.introspect_graph_schema.return_value = _sample_schema()
    mock_open.return_value = mock_conn
    mock_sanitize.side_effect = lambda m: m

    source = MagicMock(spec=DBConfig)
    source.connection_type = DBType.NEO4J

    GraphEngine(target_db_flavor=DBType.ARANGO).infer_schema_from_graph(source)

    assert (
        mock_open.call_args.kwargs["require"]
        is ConnectionCapability.SCHEMA_INTROSPECTION
    )
