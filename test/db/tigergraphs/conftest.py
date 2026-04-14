import logging
import os
import re
import time
import uuid

import pytest

from graflo.db import ConnectionManager
from graflo.db import TigergraphConfig

# Set GSQL_PASSWORD environment variable for TigerGraph tests
os.environ.setdefault("GSQL_PASSWORD", "tigergraph")
logger = logging.getLogger(__name__)
TEST_GRAPH_PREFIX = "tgtest_"


def _list_graph_names(conn_conf: TigergraphConfig) -> set[str]:
    """Return current TigerGraph graph names (best effort)."""
    try:
        with ConnectionManager(connection_config=conn_conf) as db_client:
            result = db_client._execute_gsql("USE GLOBAL\nSHOW GRAPH *")
            return set(db_client._parse_show_graph_output(str(result)))
    except Exception as list_error:
        logger.warning("Could not list TigerGraph graphs for cleanup: %s", list_error)
        return set()


def _delete_test_graph_best_effort(
    conn_conf: TigergraphConfig, graph_name: str, attempts: int = 3
) -> bool:
    """Try hard to delete a test graph and verify it is gone."""
    for attempt in range(1, attempts + 1):
        try:
            with ConnectionManager(connection_config=conn_conf) as db_client:
                db_client.delete_database(graph_name)
                if graph_name not in _list_graph_names(conn_conf):
                    return True

                # Fallback: aggressively drop graph-scoped queries then DROP GRAPH.
                try:
                    query_output = db_client._execute_gsql(
                        f"USE GRAPH {graph_name}\nSHOW QUERY *"
                    )
                    for query_name in sorted(
                        set(
                            re.findall(
                                r"CREATE QUERY\s+([A-Za-z_][A-Za-z0-9_]*)\(",
                                str(query_output),
                            )
                        )
                    ):
                        db_client._execute_gsql(
                            f"USE GRAPH {graph_name}\nDROP QUERY {query_name}"
                        )
                except Exception:
                    db_client._drop_installed_queries_for_graph(graph_name)
                db_client._execute_gsql(f"USE GLOBAL\nDROP GRAPH {graph_name}")
        except Exception as cleanup_error:
            logger.warning(
                "Attempt %s/%s failed deleting TigerGraph test graph '%s': %s",
                attempt,
                attempts,
                graph_name,
                cleanup_error,
            )

        if graph_name not in _list_graph_names(conn_conf):
            return True
        time.sleep(0.25)

    return graph_name not in _list_graph_names(conn_conf)


@pytest.fixture(scope="function")
def conn_conf():
    """Load TigerGraph config from docker/tigergraph/.env file."""
    conn_conf = TigergraphConfig.from_docker_env()
    # Ensure password is set from environment if not in .env
    if not conn_conf.password:
        conn_conf.password = os.environ.get("GSQL_PASSWORD", "tigergraph")
    return conn_conf


@pytest.fixture(scope="session")
def conn_conf_session() -> TigergraphConfig:
    """Session-scoped TigerGraph config for teardown cleanup."""
    conn_conf = TigergraphConfig.from_docker_env()
    if not conn_conf.password:
        conn_conf.password = os.environ.get("GSQL_PASSWORD", "tigergraph")
    return conn_conf


@pytest.fixture(scope="function")
def test_graph_name(conn_conf, tg_test_graph_registry: set[str]):
    """Fixture providing a test graph name for TigerGraph tests with automatic cleanup.

    The graph name is generated with a UUID suffix to make it less conspicuous.
    After the test completes, the graph and all global vertex/edge types will be deleted.

    Note: For schema-based tests, use test_graph fixture instead and set
    schema.metadata.name = test_graph.
    """
    # Generate a less conspicuous graph name with UUID suffix
    graph_uuid = str(uuid.uuid4()).replace("-", "")[:8]
    graph_name = f"{TEST_GRAPH_PREFIX}{graph_uuid}"
    tg_test_graph_registry.add(graph_name)

    # Set as default database/graph name for this test's connection
    conn_conf.database = graph_name

    yield graph_name

    # Cleanup the specific graph created for this test.
    deleted = _delete_test_graph_best_effort(conn_conf, graph_name)
    if not deleted:
        logger.warning(
            "TigerGraph test cleanup did not remove graph '%s'",
            graph_name,
        )


@pytest.fixture(scope="session")
def tg_test_graph_registry() -> set[str]:
    """Track ephemeral TigerGraph test graph names for session cleanup."""
    return set()


@pytest.fixture(scope="session", autouse=True)
def cleanup_tg_test_graphs_after_session(
    conn_conf_session: TigergraphConfig,
):
    """Delete any temporary TigerGraph test graphs left behind."""
    yield

    leaked_graphs = {
        name
        for name in _list_graph_names(conn_conf_session)
        if name.startswith(TEST_GRAPH_PREFIX)
    }
    if not leaked_graphs:
        return

    sorted_graphs = sorted(leaked_graphs)
    logger.warning("Cleaning up TigerGraph test graphs: %s", sorted_graphs)
    for graph_name in sorted_graphs:
        deleted = _delete_test_graph_best_effort(conn_conf_session, graph_name)
        if not deleted:
            logger.warning("Could not delete TigerGraph test graph '%s'", graph_name)
