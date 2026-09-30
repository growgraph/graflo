"""A declared index is a lookup index unless it says ``unique: true``."""

from __future__ import annotations

from typing import Any, cast

from graflo.architecture.graph_types.index_config import Index
from graflo.architecture.schema import CoreSchema, GraphMetadata, Schema
from graflo.architecture.schema.edge import EdgeConfig
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig
from graflo.db.neo4j.conn import Neo4jConnection
from graflo.onto import DBType


def _schema() -> Schema:
    return Schema(
        metadata=GraphMetadata(name="people"),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(
                vertices=[
                    Vertex(
                        name="person",
                        properties=[Field(name="id"), Field(name="email")],
                        identity=["id"],
                    )
                ]
            ),
            edge_config=EdgeConfig(edges=[]),
        ),
    )


def test_a_declared_index_is_not_unique_by_default() -> None:
    assert Index(fields=["email"]).unique is False
    assert (
        Index.model_validate({"fields": ["email"]}).db_form(DBType.ARANGO)["unique"]
        is False
    )


def test_the_identity_index_stays_unique() -> None:
    vertex_config = _schema().resolve_db_aware(DBType.ARANGO).vertex_config

    assert vertex_config.index("person").unique is True


class _Recorder:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def execute(self, query: str) -> None:
        self.queries.append(query)


def test_a_declared_relationship_index_is_a_plain_index_on_neo4j() -> None:
    fake = _Recorder()

    Neo4jConnection._add_index(
        cast(Any, fake), "KNOWS", Index(fields=["since"]), is_vertex_index=False
    )
    Neo4jConnection._add_index(
        cast(Any, fake),
        "KNOWS",
        Index(fields=["since"], unique=True),
        is_vertex_index=False,
    )

    plain, constraint = fake.queries
    assert plain.startswith("CREATE INDEX")
    assert constraint.startswith("CREATE CONSTRAINT")
