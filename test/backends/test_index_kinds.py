"""Index kinds and shapes a backend cannot build are refused by name."""

from __future__ import annotations

import pytest

from graflo.architecture.graph_types import IndexType
from graflo.architecture.graph_types.index_config import Index
from graflo.architecture.schema import CoreSchema, GraphMetadata, Schema
from graflo.architecture.schema.database_features import DatabaseProfile
from graflo.architecture.schema.edge import EdgeConfig
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig
from graflo.db.field_type_support import (
    UnsupportedIndexError,
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


def _shaped(
    *, vertex_index: Index | None = None, edge_index: Index | None = None
) -> Schema:
    """``person -knows-> person``, keyed on ``(tenant, id)``, with one declared index."""
    return Schema.model_validate(
        {
            "metadata": {"name": "people"},
            "core_schema": {
                "vertex_config": {
                    "vertices": [
                        {
                            "name": "person",
                            "properties": ["tenant", "id", "name", "born", "code"],
                            "identity": ["tenant", "id"],
                            "secondary_identities": [
                                {"name": "by_name", "fields": ["name", "born"]}
                            ],
                        }
                    ]
                },
                "edge_config": {
                    "edges": [
                        {
                            "source": "person",
                            "target": "person",
                            "relation": "knows",
                            "properties": ["since", "role"],
                            "identities": [["source", "target", "since", "role"]],
                        }
                    ]
                },
            },
            "db_profile": {
                "vertex_indexes": (
                    {"person": [vertex_index.model_dump()]} if vertex_index else {}
                ),
                "edge_specs": [
                    {
                        "source": "person",
                        "target": "person",
                        "relation": "knows",
                        "indexes": [edge_index.model_dump()] if edge_index else [],
                    }
                ],
            },
        }
    )


_COMPOSITE = Index(fields=["name", "code"])
_UNIQUE = Index(fields=["code"], unique=True)
_EDGE_ONE = Index(fields=["role"])
_EDGE_COMPOSITE = Index(fields=["role", "since"])
_EDGE_UNIQUE = Index(fields=["role"], unique=True)

#: (backend, declared index, on an edge, refused as) -- every shape a backend
#: cannot build, named by the limit it runs into.
_REFUSED = [
    (DBType.TIGERGRAPH, _COMPOSITE, False, "composite vertex"),
    (DBType.TIGERGRAPH, _UNIQUE, False, "unique vertex"),
    (DBType.TIGERGRAPH, _EDGE_ONE, True, "edge"),
    (DBType.NEBULA, _UNIQUE, False, "unique vertex"),
    (DBType.NEBULA, _EDGE_UNIQUE, True, "unique edge"),
    (DBType.MEMGRAPH, _EDGE_COMPOSITE, True, "composite edge"),
    (DBType.MEMGRAPH, _EDGE_UNIQUE, True, "unique edge"),
]


@pytest.mark.parametrize(("flavor", "index", "on_edge", "limit"), _REFUSED)
def test_a_declared_index_a_backend_cannot_build_is_refused(
    flavor: DBType, index: Index, on_edge: bool, limit: str
) -> None:
    schema = _shaped(edge_index=index) if on_edge else _shaped(vertex_index=index)
    with pytest.raises(UnsupportedIndexError, match=f"An? {limit} index"):
        assert_schema_supported(flavor, schema)


@pytest.mark.parametrize(
    "flavor",
    [DBType.NEO4J, DBType.FALKORDB, DBType.ARANGO, DBType.POSTGRES],
)
@pytest.mark.parametrize(
    ("index", "on_edge"),
    [
        (_COMPOSITE, False),
        (_UNIQUE, False),
        (_EDGE_COMPOSITE, True),
        (_EDGE_UNIQUE, True),
    ],
)
def test_every_shape_is_built_where_the_backend_can(
    flavor: DBType, index: Index, on_edge: bool
) -> None:
    schema = _shaped(edge_index=index) if on_edge else _shaped(vertex_index=index)
    assert_schema_supported(flavor, schema)


@pytest.mark.parametrize("flavor", [DBType.TIGERGRAPH, DBType.MEMGRAPH, DBType.NEBULA])
def test_indexes_graflo_derives_are_never_refused(flavor: DBType) -> None:
    """The identity, a secondary identity and an edge identity, restated as indexes."""
    for schema in (
        _shaped(vertex_index=Index(fields=["tenant", "id"], unique=True)),
        _shaped(vertex_index=Index(fields=["name", "born"])),
        _shaped(edge_index=Index(fields=["since", "role"])),
    ):
        assert_schema_supported(flavor, schema)
