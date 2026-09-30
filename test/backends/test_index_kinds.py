"""Index kinds a backend cannot build are refused by name."""

from __future__ import annotations

import pytest

from graflo.architecture.graph_types import IndexType
from graflo.architecture.graph_types.index_config import Index
from graflo.architecture.schema import CoreSchema, GraphMetadata, Schema
from graflo.architecture.schema.database_features import DatabaseProfile
from graflo.architecture.schema.edge import EdgeConfig
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig
from graflo.db.field_type_support import (
    UnsupportedIndexKindError,
    assert_schema_supported,
)
from graflo.onto import DBType


def _schema(kind: IndexType) -> Schema:
    return Schema(
        metadata=GraphMetadata(name="docs"),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(
                vertices=[
                    Vertex(
                        name="page",
                        properties=[Field(name="id"), Field(name="body")],
                        identity=["id"],
                    )
                ]
            ),
            edge_config=EdgeConfig(edges=[]),
        ),
        db_profile=DatabaseProfile(
            vertex_indexes={"page": [Index(fields=["body"], type=kind, unique=False)]}
        ),
    )


@pytest.mark.parametrize(
    "flavor",
    [
        DBType.NEO4J,
        DBType.MEMGRAPH,
        DBType.FALKORDB,
        DBType.TIGERGRAPH,
        DBType.NEBULA,
        DBType.POSTGRES,
    ],
)
def test_a_full_text_index_is_refused_where_it_cannot_be_built(flavor) -> None:
    with pytest.raises(UnsupportedIndexKindError, match="fulltext.*'page'"):
        assert_schema_supported(flavor, _schema(IndexType.FULLTEXT))


def test_arango_builds_a_full_text_index() -> None:
    assert_schema_supported(DBType.ARANGO, _schema(IndexType.FULLTEXT))


@pytest.mark.parametrize("flavor", list(DBType))
def test_a_plain_index_is_accepted_everywhere(flavor) -> None:
    assert_schema_supported(flavor, _schema(IndexType.PERSISTENT))
