"""Tests for TigerGraph database (graph) creation and deletion functionality."""

import logging
import uuid
from unittest.mock import MagicMock

import pytest

from graflo.db import ConnectionManager
from graflo.db.connection import TigergraphConfig
from graflo.db.tigergraph.conn import TigerGraphConnection


def test_create_database(conn_conf, test_graph_name):
    """Test creating a new TigerGraph database (graph)."""
    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Graph should not exist initially (unique name from fixture)
        assert not db_client.graph_exists(test_graph_name)

        # Create the graph
        db_client.create_database(test_graph_name)

        # Verify the graph exists
        assert db_client.graph_exists(test_graph_name)


def test_create_database_already_exists(conn_conf, test_graph_name):
    """Test that creating an existing graph raises an exception."""
    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Create the graph first
        db_client.create_database(test_graph_name)
        assert db_client.graph_exists(test_graph_name)

        # Creating it again should raise an exception
        with pytest.raises(RuntimeError, match="already exists"):
            db_client.create_database(test_graph_name)


def test_delete_database(conn_conf, test_graph_name):
    """Test deleting a TigerGraph database (graph)."""
    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Create the graph first
        db_client.create_database(test_graph_name)
        assert db_client.graph_exists(test_graph_name)

        # Delete the graph
        db_client.delete_database(test_graph_name)

        # Graph should no longer exist after successful drop.
        assert not db_client.graph_exists(test_graph_name)


def test_delete_nonexistent_database(conn_conf):
    """Test deleting a graph that doesn't exist."""
    nonexistent_graph = f"nonexistent_{uuid.uuid4().hex[:8]}"

    with ConnectionManager(connection_config=conn_conf) as db_client:
        assert not db_client.graph_exists(nonexistent_graph)
        # Delete should not raise an exception even if graph doesn't exist
        db_client.delete_database(nonexistent_graph)
        assert not db_client.graph_exists(nonexistent_graph)


def test_create_and_delete_database(conn_conf, test_graph_name):
    """Test creating a graph and then deleting it."""
    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Create the graph
        db_client.create_database(test_graph_name)
        assert db_client.graph_exists(test_graph_name)

        # Delete it
        db_client.delete_database(test_graph_name)
        assert not db_client.graph_exists(test_graph_name)


def test_schema_creation(conn_conf, test_graph_name, schema_obj):
    """Test creating schema using init_db (follows ArangoDB pattern).

    Pattern: init_db creates graph, defines schema, then defines indexes.
    Uses schema.metadata.name as the graph name (from test_graph fixture).

    Note: In TigerGraph, vertex and edge types are global and shared between graphs.
    The test verifies that types are created and associated with the test graph.
    The test verifies that types are created and associated with the test graph.
    """
    schema_obj = schema_obj("review")
    # Set graph name in schema.metadata.name; conn_conf.database is set by fixture
    schema_obj.metadata.name = test_graph_name

    with ConnectionManager(connection_config=conn_conf) as db_client:
        # init_db will: create graph, define schema, define indexes
        # Graph name comes from schema.metadata.name
        db_client.init_db(schema_obj, recreate_schema=True)

    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Verify graph exists (using name from schema.metadata.name)
        assert db_client.graph_exists(test_graph_name)

        # Use the graph context to verify schema
        # getVertexTypes() and getEdgeTypes() require graph context via _ensure_graph_context
        with db_client._ensure_graph_context(test_graph_name):
            # Verify schema was created
            vertex_types = db_client._get_vertex_types()
            edge_types = db_client._get_edge_types()

            # Check expected types exist
            assert len(vertex_types) > 0, "No vertex types created"
            assert len(edge_types) > 0, "No edge types created"

            print(f"Created vertex types: {vertex_types}")
            print(f"Created edge types: {edge_types}")


def test_schema_creation_edges(conn_conf, test_graph_name, schema_obj):
    """Test creating schema using init_db (follows ArangoDB pattern).

    Pattern: init_db creates graph, defines schema, then defines indexes.
    Uses schema.metadata.name as the graph name (from test_graph fixture).

    Note: In TigerGraph, vertex and edge types are global and shared between graphs.
    The test verifies that types are created and associated with the test graph.
    The test verifies that types are created and associated with the test graph.
    """
    schema_obj = schema_obj("review-tigergraph-edges")
    # Set graph name in schema.metadata.name; conn_conf.database is set by fixture
    schema_obj.metadata.name = test_graph_name

    with ConnectionManager(connection_config=conn_conf) as db_client:
        # init_db will: create graph, define schema, define indexes
        # Graph name comes from schema.metadata.name
        db_client.init_db(schema_obj, recreate_schema=True)

    with ConnectionManager(connection_config=conn_conf) as db_client:
        # Verify graph exists (using name from schema.metadata.name)
        assert db_client.graph_exists(test_graph_name)

        # Use the graph context to verify schema
        # getVertexTypes() and getEdgeTypes() require graph context via _ensure_graph_context
        with db_client._ensure_graph_context(test_graph_name):
            # Verify schema was created
            vertex_types = db_client._get_vertex_types()
            edge_types = db_client._get_edge_types()

            # Check expected types exist
            assert len(vertex_types) > 0, "No vertex types created"
            assert len(edge_types) == 1, "No edge types created"
            assert len(edge_types["contains"]) == 2, (
                "Should have to edges for relation `contains`"
            )

            print(f"Created vertex types: {vertex_types}")
            print(f"Created edge types: {edge_types}")


def test_delete_database_scopes_query_cleanup_to_target_graph() -> None:
    """delete_database should clean only the target graph (queries, then jobs) before DROP GRAPH."""
    conn = TigerGraphConnection.__new__(TigerGraphConnection)
    dropped_graph_queries: list[str] = []
    executed_gsql: list[str] = []

    def _fake_drop_installed_queries_for_graph(graph_name: str) -> None:
        dropped_graph_queries.append(graph_name)

    def _fake_execute_gsql(gsql: str):
        executed_gsql.append(gsql)
        return {"accepted": True}

    conn._drop_installed_queries_for_graph = _fake_drop_installed_queries_for_graph
    conn._execute_gsql = _fake_execute_gsql

    graph_name = f"g_query_scope_a_{uuid.uuid4().hex[:8]}"
    conn.delete_database(graph_name)

    assert dropped_graph_queries == [graph_name]
    assert executed_gsql == [
        f"USE GRAPH {graph_name}\nSHOW JOB *",
        f"USE GLOBAL\nDROP GRAPH {graph_name}",
    ]


def test_drop_global_schema_types_skips_types_in_surviving_graph() -> None:
    """Global DROP EDGE/VERTEX must not run for types still listed under surviving graphs."""
    conn = TigerGraphConnection.__new__(TigerGraphConnection)
    executed: list[str] = []

    def fake_execute_gsql(gsql: str) -> dict[str, bool]:
        executed.append(gsql)
        return {"accepted": True}

    e_shared = MagicMock()
    e_shared.source = "A"
    e_shared.target = "B"
    e_orphan = MagicMock()
    e_orphan.source = "C"
    e_orphan.target = "D"

    core_schema = MagicMock()
    core_schema.edge_config = {"k1": e_shared, "k2": e_orphan}

    v_shared = MagicMock()
    v_shared.name = "SharedV"
    v_orphan = MagicMock()
    v_orphan.name = "OrphanV"
    core_schema.vertex_config.vertices = [v_shared, v_orphan]

    schema = MagicMock()
    schema.core_schema = core_schema

    db_schema = MagicMock()

    def runtime(edge: MagicMock) -> MagicMock:
        r = MagicMock()
        if edge is e_shared:
            r.relation_name = "shared_edge"
        else:
            r.relation_name = "orphan_edge"
        return r

    db_schema.edge_config.runtime.side_effect = runtime
    db_schema.vertex_config.vertex_dbname.side_effect = lambda name: name

    schema.resolve_db_aware.return_value = db_schema

    def fake_get_graph_types(_graph: str) -> tuple[set[str], set[str]]:
        return ({"SharedV"}, {"shared_edge"})

    conn._execute_gsql = fake_execute_gsql
    conn._get_graph_type_names = fake_get_graph_types

    conn._drop_global_schema_types(schema, ["other_graph"])

    assert any(cmd.strip() == "DROP EDGE orphan_edge" for cmd in executed)
    assert not any("DROP EDGE shared_edge" in cmd for cmd in executed)
    assert any(cmd.strip() == "DROP VERTEX OrphanV" for cmd in executed)
    assert not any("DROP VERTEX SharedV" in cmd for cmd in executed)


def test_delete_graph_structure_requires_confirm_for_delete_all() -> None:
    conn = TigerGraphConnection.__new__(TigerGraphConnection)
    with pytest.raises(ValueError, match="confirm_global_teardown=True"):
        conn.delete_graph_structure(delete_all=True)


def test_delete_graph_structure_skips_types_in_surviving_graph() -> None:
    """delete_all with confirm must not DROP global types still used by surviving graphs."""
    conn = TigerGraphConnection.__new__(TigerGraphConnection)
    conn.config = TigergraphConfig(
        uri="http://127.0.0.1:14240",
        gs_port=14240,
        username="u",
        password="p",
        database="drop_target",
    )
    gsql_log: list[str] = []

    def fake_gsql(q: str) -> str:
        gsql_log.append(q)
        if q.strip() == "SHOW EDGE *":
            return (
                "- DIRECTED EDGE Ekeep(FROM A, TO B)\n"
                "- DIRECTED EDGE Edrop(FROM A, TO B)"
            )
        if q.strip() == "SHOW VERTEX *":
            return "- VERTEX Vkeep(\n- VERTEX Vdrop("
        return ""

    conn._execute_gsql = fake_gsql
    conn.delete_database = lambda _name: None
    conn._get_all_graph_names = lambda: ["SurvivorGraph"]
    conn._get_graph_type_names = lambda _g: ({"Vkeep"}, {"Ekeep"})
    conn._delete_vertices = lambda _t: None

    conn.delete_graph_structure(
        graph_names=("drop_target",),
        delete_all=True,
        confirm_global_teardown=True,
    )

    assert any("DROP EDGE Edrop" in q for q in gsql_log)
    assert not any("DROP EDGE Ekeep" in q for q in gsql_log)
    assert any("DROP VERTEX Vdrop" in q for q in gsql_log)
    assert not any("DROP VERTEX Vkeep" in q for q in gsql_log)
    assert not any("SHOW JOB *" == q.strip() for q in gsql_log)


def test_init_db_recreate_logs_error_on_query_loss(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """After recreate_schema, any missing installed queries on other graphs must log ERROR."""
    conn = TigerGraphConnection.__new__(TigerGraphConnection)
    conn.config = TigergraphConfig(
        uri="http://127.0.0.1:14240",
        gs_port=14240,
        username="u",
        password="p",
        database="tgtest_reinit_main",
    )

    graph_calls = iter([False, True, False])

    def fake_graph_exists(_name: str) -> bool:
        return next(graph_calls)

    snapshots = [
        {"survivor_graph": ["lost_query"], "tgtest_reinit_main": ["self_q"]},
        {"survivor_graph": [], "tgtest_reinit_main": []},
    ]
    snap_i = [0]

    def fake_snapshot() -> dict[str, list[str]]:
        idx = snap_i[0]
        snap_i[0] += 1
        return snapshots[idx]

    schema = MagicMock()
    schema.metadata.name = "tgtest_reinit_main"

    conn.graph_exists = fake_graph_exists
    conn.delete_database = lambda _n: None
    conn._get_all_graph_names = lambda: ["survivor_graph"]
    conn._drop_global_schema_types = lambda _s, _g: None
    conn._snapshot_all_queries = fake_snapshot
    conn.create_database = lambda _n: None
    conn._define_schema_local = lambda _s: None
    conn.define_indexes = lambda _s: None

    with caplog.at_level(logging.ERROR, logger="graflo.db.tigergraph.conn"):
        conn.init_db(schema, recreate_schema=True)

    assert "QUERY LOSS DETECTED" in caplog.text
    assert "lost_query" in caplog.text
    assert "survivor_graph" in caplog.text
