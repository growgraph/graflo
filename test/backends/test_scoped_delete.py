"""Removing a scope: the statements each backend sends, and the names they use."""

from __future__ import annotations

from typing import Any, cast

import pytest

from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.schema import CoreSchema, GraphMetadata, Schema
from graflo.architecture.schema.database_features import DatabaseProfile
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig
from graflo.connections.graflo_backend import GraFloBackendConfig
from graflo.connections.onto import Neo4jConfig
from graflo.db.conn import ConnectionCapability
from graflo.db.cypher.delete import (
    SCOPE_PARAM,
    delete_scoped_nodes,
    delete_scoped_nodes_query,
    delete_scoped_relationships_query,
)
from graflo.db.falkordb.conn import FalkordbConnection
from graflo.db.manager import ConnectionManager
from graflo.db.memgraph.conn import MemgraphConnection
from graflo.db.neo4j.conn import Neo4jConnection
from graflo.hq.db_writer import DBWriter
from graflo.onto import DBType


def test_the_backends_that_delete_by_scope() -> None:
    assert set(
        ConnectionManager.flavors_supporting(ConnectionCapability.SCOPED_DELETE)
    ) == {DBType.NEO4J, DBType.FALKORDB}


def test_a_backend_without_it_says_so() -> None:
    conn = cast(Any, object.__new__(MemgraphConnection))

    with pytest.raises(NotImplementedError, match="by scope"):
        conn.delete_scoped_vertices("Account", "installation", "T1")
    with pytest.raises(NotImplementedError, match="by scope"):
        conn.delete_scoped_edges("Account", "Zone", "holds", "installation", "T1")


# -- Cypher ---------------------------------------------------------------------


def test_the_cypher_statements() -> None:
    assert delete_scoped_nodes_query("Account", "installation") == (
        "MATCH (n:`Account`) WHERE n.`installation` = $scope DETACH DELETE n"
    )
    assert delete_scoped_relationships_query(
        "Account", "Zone", "holds", "installation"
    ) == (
        "MATCH (:`Account`)-[r:`holds`]->(:`Zone`) "
        "WHERE r.`installation` = $scope DELETE r"
    )
    assert "-[r]->" in delete_scoped_relationships_query("A", "B", None, "f")


def test_a_none_scope_is_refused_before_anything_is_sent() -> None:
    sent: list[str] = []

    with pytest.raises(ValueError, match="scope value"):
        delete_scoped_nodes(lambda query, params: sent.append(query), "A", "f", None)
    assert sent == []


class _Cypher:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def execute(self, query: str, **params: Any) -> None:
        self.calls.append((query, params))


@pytest.mark.parametrize("connection", [Neo4jConnection, FalkordbConnection])
def test_cypher_backends_bind_the_scope(connection) -> None:
    fake = _Cypher()

    connection.delete_scoped_edges(
        cast(Any, fake), "Account", "Zone", "holds", "installation", 'T1" OR 1=1'
    )
    connection.delete_scoped_vertices(
        cast(Any, fake), "Account", "installation", 'T1" OR 1=1'
    )

    assert [params for _, params in fake.calls] == [
        {SCOPE_PARAM: 'T1" OR 1=1'},
        {SCOPE_PARAM: 'T1" OR 1=1'},
    ]
    assert all("OR 1=1" not in query for query, _ in fake.calls)
    assert "DELETE r" in fake.calls[0][0]
    assert "DETACH DELETE n" in fake.calls[1][0]


# -- names, from the schema to the backend ---------------------------------------


def _schema() -> Schema:
    return Schema(
        metadata=GraphMetadata(name="g"),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(
                vertices=[
                    Vertex(
                        name="account",
                        properties=[Field(name="aid"), Field(name="installation")],
                        identity=["aid"],
                    ),
                    Vertex(
                        name="zone", properties=[Field(name="zid")], identity=["zid"]
                    ),
                ]
            ),
            edge_config=EdgeConfig(
                edges=[
                    Edge(
                        source="account",
                        target="zone",
                        relation="holds",
                        properties=[Field(name="installation")],
                    )
                ]
            ),
        ),
        db_profile=DatabaseProfile(
            vertex_storage_names={"account": "Account"},
            vertex_property_names={"account": {"installation": "inst"}},
        ),
    )


class _Recorder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple, dict]] = []

    def delete_scoped_vertices(self, *args: Any, **kwargs: Any) -> None:
        self.calls.append(("vertices", args, kwargs))

    def delete_scoped_edges(self, *args: Any, **kwargs: Any) -> None:
        self.calls.append(("edges", args, kwargs))


@pytest.fixture
def recorded(monkeypatch) -> _Recorder:
    recorder = _Recorder()

    monkeypatch.setattr(
        "graflo.hq.db_writer.ConnectionManager",
        type(
            "M",
            (),
            {
                "require": staticmethod(ConnectionManager.require),
                "__init__": lambda self, connection_config: None,
                "__enter__": lambda self: recorder,
                "__exit__": lambda self, *exc: False,
            },
        ),
    )
    return recorder


NEO4J = Neo4jConfig(uri="bolt://localhost:7687", username="u", password="p")
HOLDS = ("account", "zone", "holds")


def _writer(dry: bool = False) -> DBWriter:
    schema = _schema()
    model = IngestionModel(resources=[])
    model.finish_init(schema.core_schema)
    return DBWriter(schema=schema, ingestion_model=model, dry=dry)


def test_edges_go_first_under_their_stored_names(recorded: _Recorder) -> None:
    _writer().delete_scoped(
        NEO4J, "installation", "T1", vertices=["account"], edges=[HOLDS]
    )

    assert recorded.calls == [
        (
            "edges",
            ("Account", "zone", "holds", "installation", "T1"),
            {"collection_name": None},
        ),
        ("vertices", ("Account", "inst", "T1"), {}),
    ]


def test_a_type_without_the_field_is_refused(recorded: _Recorder) -> None:
    with pytest.raises(ValueError, match="declares no field"):
        _writer().delete_scoped(NEO4J, "installation", "T1", vertices=["zone"])
    assert recorded.calls == []


def test_an_undeclared_edge_is_refused(recorded: _Recorder) -> None:
    with pytest.raises(ValueError, match="not declared"):
        _writer().delete_scoped(
            NEO4J, "installation", "T1", edges=[("zone", "account", "holds")]
        )


def test_a_none_scope_is_refused(recorded: _Recorder) -> None:
    with pytest.raises(ValueError, match="scope value"):
        _writer().delete_scoped(NEO4J, "installation", None, vertices=["account"])


def test_a_dry_run_removes_nothing(recorded: _Recorder) -> None:
    _writer(dry=True).delete_scoped(NEO4J, "installation", "T1", vertices=["account"])
    assert recorded.calls == []


def test_the_file_backend_is_refused_before_anything_is_opened(tmp_path) -> None:
    with pytest.raises(ValueError, match="deleting the elements of a scope"):
        _writer().delete_scoped(
            GraFloBackendConfig(output_dir=tmp_path),
            "installation",
            "T1",
            vertices=["account"],
        )
