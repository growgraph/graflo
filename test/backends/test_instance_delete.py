"""Removing vertices and edges: the statements each backend sends, and the names."""

from __future__ import annotations

from typing import Any, Self, cast

import pytest

from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.schema import CoreSchema, GraphMetadata, Schema
from graflo.architecture.schema.database_features import DatabaseProfile
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig
from graflo.connections.graflo_backend import GraFloBackendConfig
from graflo.connections.onto import Neo4jConfig
from graflo.db.arango.conn import ArangoConnection
from graflo.db.conn import ConnectionCapability
from graflo.db.cypher.delete import delete_nodes_query, delete_relationships_query
from graflo.db.falkordb.conn import FalkordbConnection
from graflo.db.manager import ConnectionManager
from graflo.db.memgraph.conn import MemgraphConnection
from graflo.db.nebula.conn import NebulaConnection
from graflo.db.neo4j.conn import Neo4jConnection
from graflo.db.postgres import target_write
from graflo.db.postgres.conn import PostgresConnection
from graflo.db.tigergraph.data_ops import TigerGraphDataOps
from graflo.hq.db_writer import DBWriter
from graflo.onto import DBType


def test_the_backends_that_delete() -> None:
    assert set(
        ConnectionManager.flavors_supporting(ConnectionCapability.INSTANCE_DELETE)
    ) == {
        DBType.ARANGO,
        DBType.NEO4J,
        DBType.MEMGRAPH,
        DBType.FALKORDB,
        DBType.TIGERGRAPH,
        DBType.POSTGRES,
    }


def test_a_backend_without_it_says_so() -> None:
    with pytest.raises(NotImplementedError, match="deleting vertices"):
        NebulaConnection.delete_vertices(cast(Any, object()), "t", [{"id": 1}], ("id",))


# -- Cypher ---------------------------------------------------------------------


def test_the_cypher_statements() -> None:
    assert delete_nodes_query("Person", ["id", "org"]) == (
        "UNWIND $data AS row MATCH (n:`Person`) "
        "WHERE n.`id` = row.k0 AND n.`org` = row.k1 DETACH DELETE n"
    )
    assert delete_relationships_query("Person", "Org", "works_at", ["id"], ["id"]) == (
        "UNWIND $data AS row MATCH (s:`Person`)-[r:`works_at`]->(t:`Org`) "
        "WHERE s.`id` = row.s0 AND t.`id` = row.t0 DELETE r"
    )


class _Cypher:
    def __init__(self) -> None:
        self.calls: list[tuple[str, list[dict[str, Any]]]] = []

    def execute(self, query: str, data: list[dict[str, Any]]) -> None:
        self.calls.append((query, data))


@pytest.mark.parametrize(
    "connection", [Neo4jConnection, MemgraphConnection, FalkordbConnection]
)
def test_cypher_backends_remove_nodes_by_key(connection) -> None:
    fake = _Cypher()

    connection.delete_vertices(
        cast(Any, fake),
        "Person",
        [{"id": 1, "name": "a"}, {"name": "no id"}, {"id": 2}, {"id": 3}],
        ("id",),
        chunk_size=2,
    )

    assert [rows for _, rows in fake.calls] == [
        [{"k0": 1}, {"k0": 2}],
        [{"k0": 3}],
    ]
    assert all("DETACH DELETE" in query for query, _ in fake.calls)


@pytest.mark.parametrize(
    "connection", [Neo4jConnection, MemgraphConnection, FalkordbConnection]
)
def test_cypher_backends_remove_relationships_by_endpoints(connection) -> None:
    fake = _Cypher()

    connection.delete_edges(
        cast(Any, fake),
        "Person",
        "Org",
        "works_at",
        [({"id": 1}, {"id": 9}), ({"id": 2}, {})],
        ("id",),
        ("id",),
    )

    assert [rows for _, rows in fake.calls] == [[{"s0": 1, "t0": 9}]]


def test_nothing_to_remove_sends_nothing() -> None:
    fake = _Cypher()
    Neo4jConnection.delete_vertices(cast(Any, fake), "Person", [{"name": "x"}], ("id",))
    assert fake.calls == []


# -- ArangoDB -------------------------------------------------------------------


class _Aql:
    def __init__(self, removed: list[str]) -> None:
        self.removed = removed
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def execute(self, query: str, bind_vars: dict[str, Any]) -> Any:
        self.calls.append((query, bind_vars))
        return iter(self.removed if "RETURN OLD._id" in query else [])


class _ArangoDB:
    def __init__(self, removed: list[str]) -> None:
        self.aql = _Aql(removed)

    def collections(self) -> list[dict[str, Any]]:
        return [
            {"name": "person", "type": "document"},
            {"name": "person_org_edges", "type": "edge"},
            {"name": "person_person_edges", "type": 3},
            {"name": "_graphs", "type": "document"},
        ]


class _Arango:
    def __init__(self, removed: list[str]) -> None:
        self.conn = _ArangoDB(removed)

    def _edge_collection_names(self) -> list[str]:
        return ArangoConnection._edge_collection_names(cast(Any, self))


def test_arango_removes_documents_and_their_edges() -> None:
    fake = _Arango(removed=["person/1"])

    ArangoConnection.delete_vertices(cast(Any, fake), "person", [{"id": 1}], ("id",))

    queries = [query for query, _ in fake.conn.aql.calls]
    assert "REMOVE doc IN person RETURN OLD._id" in queries[0]
    assert fake.conn.aql.calls[0][1] == {"rows": [{"id": 1}]}
    assert [q.split(" FILTER")[0] for q in queries[1:]] == [
        "FOR e IN person_org_edges",
        "FOR e IN person_person_edges",
    ]
    assert all(bind == {"ids": ["person/1"]} for _, bind in fake.conn.aql.calls[1:])


def test_arango_leaves_edges_alone_when_nothing_was_removed() -> None:
    fake = _Arango(removed=[])

    ArangoConnection.delete_vertices(cast(Any, fake), "person", [{"id": 1}], ("id",))

    assert len(fake.conn.aql.calls) == 1


def test_arango_removes_edges_of_one_relation() -> None:
    fake = _Arango(removed=[])

    ArangoConnection.delete_edges(
        cast(Any, fake),
        "person",
        "org",
        "works_at",
        [({"id": 1}, {"id": 9})],
        ("id",),
        ("id",),
        collection_name="person_org_edges",
    )

    query, bind = fake.conn.aql.calls[0]
    assert "FOR e IN person_org_edges" in query
    assert "e.relation == @relation" in query
    assert bind == {"rows": [{"s": {"id": 1}, "t": {"id": 9}}], "relation": "works_at"}


# -- TigerGraph -----------------------------------------------------------------


class _TigerGraph:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def _require_configured_graph_name(self) -> str:
        return "g"

    def _call_restpp_api(self, endpoint: str, method: str = "GET") -> dict:
        self.calls.append((method, endpoint))
        return {}


def test_tigergraph_removes_vertices_by_id() -> None:
    ops = TigerGraphDataOps.__new__(TigerGraphDataOps)
    ops._conn = cast(Any, _TigerGraph())

    ops.delete_vertices("Person", [{"a": "x/1", "b": 2}, {"a": "y"}], ("a", "b"))

    assert ops._conn.calls == [("DELETE", "/graph/g/vertices/Person/x%2F1_2")]


def test_tigergraph_removes_edges_by_endpoint_ids() -> None:
    ops = TigerGraphDataOps.__new__(TigerGraphDataOps)
    ops._conn = cast(Any, _TigerGraph())

    ops.delete_edges(
        "Person", "Org", "works_at", [({"id": 1}, {"id": 9})], ("id",), ("id",)
    )

    assert ops._conn.calls == [("DELETE", "/graph/g/edges/Person/1/works_at/Org/9")]


# -- PostgreSQL -----------------------------------------------------------------


class _Cursor:
    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        return None


class _Pg:
    def __init__(self) -> None:
        self.config = type("C", (), {"schema_name": "g"})()
        self.committed = False
        outer = self

        class _Conn:
            def cursor(self) -> _Cursor:
                return _Cursor()

            def commit(self) -> None:
                outer.committed = True

        self.conn = _Conn()

    def get_tables(self, schema_name: str | None = None) -> list[dict[str, Any]]:
        return [
            {"table_name": t} for t in ("person", "org", "person_org_works_at_edges")
        ]

    def get_foreign_keys(
        self, table: str, schema_name: str | None = None
    ) -> list[dict]:
        if table != "person_org_works_at_edges":
            return []
        return [
            {
                "column": "source_id",
                "references_table": "person",
                "references_column": "id",
            },
            {
                "column": "target_id",
                "references_table": "org",
                "references_column": "id",
            },
        ]


def test_postgres_removes_referencing_edge_rows_first(monkeypatch) -> None:
    statements: list[tuple[str, list[tuple]]] = []
    monkeypatch.setattr(
        target_write,
        "execute_values",
        lambda cursor, query, values: statements.append((query, values)),
    )
    fake = _Pg()

    PostgresConnection.delete_vertices(
        cast(Any, fake), "person", [{"id": 1}, {"id": 2}], ("id",)
    )

    assert [query for query, _ in statements] == [
        (
            'DELETE FROM "g"."person_org_works_at_edges" WHERE "source_id"::text IN ('
            'SELECT v."id"::text FROM "g"."person" v WHERE (v."id") IN (VALUES %s))'
        ),
        'DELETE FROM "g"."person" WHERE ("id") IN (VALUES %s)',
    ]
    assert all(values == [(1,), (2,)] for _, values in statements)
    assert fake.committed


def test_postgres_removes_edge_rows_by_stored_endpoint_values(monkeypatch) -> None:
    statements: list[tuple[str, list[tuple]]] = []
    monkeypatch.setattr(
        target_write,
        "execute_values",
        lambda cursor, query, values: statements.append((query, values)),
    )

    PostgresConnection.delete_edges(
        cast(Any, _Pg()),
        "person",
        "org",
        "works_at",
        [({"id": 1}, {"id": 9})],
        ("id",),
        ("id",),
    )

    assert statements == [
        (
            (
                'DELETE FROM "g"."person_org_works_at_edges" '
                'WHERE ("source_id", "target_id") IN (VALUES %s)'
            ),
            [("1", "9")],
        )
    ]


# -- names, from the schema to the backend ---------------------------------------


def _schema() -> Schema:
    return Schema(
        metadata=GraphMetadata(name="g"),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(
                vertices=[
                    Vertex(
                        name="person", properties=[Field(name="pid")], identity=["pid"]
                    ),
                    Vertex(
                        name="org", properties=[Field(name="oid")], identity=["oid"]
                    ),
                ]
            ),
            edge_config=EdgeConfig(
                edges=[Edge(source="person", target="org", relation="works_at")]
            ),
        ),
        db_profile=DatabaseProfile(
            vertex_storage_names={"person": "Human"},
            vertex_property_names={"person": {"pid": "person_id"}},
        ),
    )


class _Recorder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple, dict]] = []

    def delete_vertices(self, *args: Any, **kwargs: Any) -> None:
        self.calls.append(("vertices", args, kwargs))

    def delete_edges(self, *args: Any, **kwargs: Any) -> None:
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


def _writer(dry: bool = False) -> DBWriter:
    schema = _schema()
    model = IngestionModel(resources=[])
    model.finish_init(schema.core_schema)
    return DBWriter(schema=schema, ingestion_model=model, dry=dry)


def test_a_vertex_is_removed_under_its_stored_names(recorded: _Recorder) -> None:
    _writer().delete_vertices(NEO4J, "person", [{"pid": 7}])

    assert recorded.calls == [
        ("vertices", ("Human", [{"person_id": 7}], ("person_id",)), {})
    ]


def test_an_edge_is_removed_under_its_stored_names(recorded: _Recorder) -> None:
    _writer().delete_edges(
        NEO4J, ("person", "org", "works_at"), [({"pid": 7}, {"oid": 9})]
    )

    ((kind, args, kwargs),) = recorded.calls
    assert kind == "edges"
    assert kwargs == {"collection_name": None}
    assert args == (
        "Human",
        "org",
        "works_at",
        [({"person_id": 7}, {"oid": 9})],
        ("person_id",),
        ("oid",),
    )


def test_an_undeclared_edge_is_refused(recorded: _Recorder) -> None:
    with pytest.raises(ValueError, match="not declared"):
        _writer().delete_edges(NEO4J, ("org", "person", "employs"), [])


def test_a_dry_run_removes_nothing(recorded: _Recorder) -> None:
    _writer(dry=True).delete_vertices(NEO4J, "person", [{"pid": 7}])
    assert recorded.calls == []


def test_the_file_backend_is_refused_before_anything_is_opened(tmp_path) -> None:
    with pytest.raises(ValueError, match="deleting vertices and edges"):
        _writer().delete_vertices(
            GraFloBackendConfig(output_dir=tmp_path), "person", [{"pid": 7}]
        )
