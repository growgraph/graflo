"""Live TigerGraph: vertex identity field with GSQL DEFAULT (PRIMARY KEY DDL path)."""

from __future__ import annotations

from graflo.architecture.database_features import DatabaseProfile, DefaultPropertyValues
from graflo.architecture.schema.core import CoreSchema
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.metadata import GraphMetadata
from graflo.architecture.schema.vertex import Field, FieldType, Vertex, VertexConfig
from graflo.db import ConnectionManager
from graflo.onto import DBType


def test_init_db_vertex_identity_string_default(
    conn_conf, test_graph_name, clean_db
) -> None:
    """Regression: DEFAULT on PK must not emit PRIMARY_ID ... DEFAULT (parse error)."""
    _ = clean_db
    vlog = f"v_{test_graph_name}"
    schema = Schema(
        metadata=GraphMetadata(name=test_graph_name),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(
                vertices=[
                    Vertex(
                        name=vlog,
                        properties=[Field(name="name", type=FieldType.STRING)],
                        identity=["name"],
                    )
                ]
            ),
            edge_config=EdgeConfig(
                edges=[
                    Edge(
                        source=vlog,
                        target=vlog,
                        identities=[["relation"]],
                        properties=[Field(name="date", type=FieldType.STRING)],
                    )
                ]
            ),
        ),
        db_profile=DatabaseProfile(
            db_flavor=DBType.TIGERGRAPH,
            default_property_values=DefaultPropertyValues(
                vertices={vlog: {"name": "ABC"}}
            ),
        ),
    )

    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.init_db(schema, recreate_schema=True)

    with ConnectionManager(connection_config=conn_conf) as db_client:
        assert db_client.graph_exists(test_graph_name)
        with db_client._ensure_graph_context(test_graph_name):
            vertex_types = db_client._get_vertex_types()
            assert len(vertex_types) > 0
