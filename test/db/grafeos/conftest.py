"""Pytest fixtures for Grafeo embedded graph database tests.

Adapted from test/db/falkordbs/conftest.py.

No Docker or external services required: Grafeo runs in-process.
Each test gets a fresh file-backed database so data persists across
multiple ConnectionManager contexts within the same test.
"""

import pytest

from graflo.db import ConnectionManager, GrafeoConfig


# ---------------------------------------------------------------------------
# Compatibility shim: make Grafeo QueryResult support .result_set
# so FalkorDB-originated tests work without rewriting every assertion.
# ---------------------------------------------------------------------------


def _patch_grafeo_result_set():
    """Add a ``result_set`` property to Grafeo's ``QueryResult``.

    FalkorDB returns ``result.result_set`` as a list-of-tuples (positional).
    Grafeo returns ``result.to_list()`` as a list-of-dicts (keyed by alias).
    This shim bridges the gap by converting dicts to tuples in column order.
    """
    try:
        from grafeo import GrafeoDB

        db = GrafeoDB()
        r = db.execute("RETURN 1 AS x")
        result_cls = type(r)
        db.close()

        if not hasattr(result_cls, "result_set"):

            @property
            def result_set(self):
                rows = self.to_list()
                if not rows:
                    return []
                keys = list(rows[0].keys())
                return [tuple(row[k] for k in keys) for row in rows]

            result_cls.result_set = result_set
    except ImportError:
        pass


_patch_grafeo_result_set()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="function")
def conn_conf(tmp_path):
    """Create a file-backed Grafeo configuration.

    Uses a temp directory so the database persists across multiple
    ConnectionManager contexts within the same test, but is cleaned
    up between tests.
    """
    db_path = str(tmp_path / "test.grafeo")
    return GrafeoConfig(path=db_path)


@pytest.fixture(scope="function")
def test_graph_name(conn_conf):
    """Provide a graph name."""
    graph_name = "testgraph"
    conn_conf.database = graph_name
    yield graph_name


@pytest.fixture()
def clean_db(conn_conf, test_graph_name):
    """Provide a clean database (wipe all data)."""
    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.delete_graph_structure(delete_all=True)


@pytest.fixture(scope="function")
def test_db_name(test_graph_name):
    """Alias for test_graph_name (compatibility with shared test utilities)."""
    return test_graph_name
