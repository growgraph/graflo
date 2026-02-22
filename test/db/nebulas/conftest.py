"""Pytest fixtures for NebulaGraph connector tests.

A single shared space + schema is created once per session to avoid paying the
expensive NebulaGraph schema-propagation + index-rebuild cost more than once.

* **Session-scoped** -- ``_session_db`` creates one space + schema for the
  entire test session and tears it down at the end.
* **Module-scoped** -- ``_module_db`` clears data once at module start;
  read-only modules (aggregate, edges, fetch) seed data on top of this.
* **Function-scoped** -- ``nebula_db`` clears data before *every* test (only
  for write-mutation tests).  ``conn_conf`` / ``test_space_name`` /
  ``schema_obj`` are per-test for lifecycle tests that manage their own spaces.
"""

import uuid

import pytest

from graflo.architecture.schema import Schema
from graflo.db import ConnectionManager
from graflo.db.conn import Connection
from graflo.db.connection.onto import NebulaConfig


MINI_SCHEMA_DICT = {
    "general": {"name": "test_graflo", "version": "1.0.0"},
    "vertex_config": {
        "vertices": [
            {
                "name": "Person",
                "fields": [
                    {"name": "name", "type": "STRING"},
                    {"name": "age", "type": "INT"},
                ],
            },
            {
                "name": "City",
                "fields": [
                    {"name": "name", "type": "STRING"},
                    {"name": "population", "type": "INT"},
                ],
            },
        ],
    },
    "edge_config": {
        "edges": [
            {
                "source": "Person",
                "target": "City",
                "relation": "lives_in",
                "match_source": "name",
                "match_target": "name",
            },
            {
                "source": "Person",
                "target": "Person",
                "relation": "knows",
                "match_source": "name",
                "match_target": "name",
            },
        ],
    },
}


# ── Function-scoped fixtures (for space-lifecycle tests) ─────────────────


@pytest.fixture(scope="function")
def conn_conf():
    """Fresh config per-test; safe to mutate (e.g. set schema_name)."""
    cfg = NebulaConfig.from_docker_env()
    cfg.uri = f"nebula://localhost:{cfg.port}"
    return cfg


@pytest.fixture(scope="function")
def test_space_name():
    """Generate a unique space name for one-off lifecycle tests."""
    space_uuid = str(uuid.uuid4()).replace("-", "")[:8]
    return f"test_{space_uuid}"


@pytest.fixture(scope="function")
def schema_obj():
    return Schema.from_dict(MINI_SCHEMA_DICT)


# ── Session-scoped shared space ──────────────────────────────────────────


@pytest.fixture(scope="session")
def _session_conn_conf():
    cfg = NebulaConfig.from_docker_env()
    cfg.uri = f"nebula://localhost:{cfg.port}"
    return cfg


@pytest.fixture(scope="session")
def _session_schema():
    return Schema.from_dict(MINI_SCHEMA_DICT)


@pytest.fixture(scope="session")
def _session_db(_session_conn_conf, _session_schema):
    """Create one shared space + schema for the whole test session.

    The space is torn down at the end of the session.  Individual modules
    only need to clear data, which is orders of magnitude faster than
    re-creating the space and waiting for schema propagation + index rebuilds.
    """
    space_uuid = str(uuid.uuid4()).replace("-", "")[:8]
    space_name = f"test_session_{space_uuid}"
    conf = _session_conn_conf.model_copy()
    conf.schema_name = space_name

    with ConnectionManager(connection_config=conf) as db_client:
        db_client.init_db(_session_schema, recreate_schema=True)
        yield db_client

    try:
        teardown_conf = _session_conn_conf.model_copy()
        teardown_conf.schema_name = space_name
        with ConnectionManager(connection_config=teardown_conf) as db_client:
            db_client.delete_database(space_name)
    except Exception:
        pass


def _clear_test_data(db_client: Connection, schema: Schema) -> None:
    """Delete all vertices and their edges from every tag in the schema."""
    for vname in schema.vertex_config.vertex_set:
        try:
            db_client.execute(
                f"LOOKUP ON `{vname}` YIELD id(vertex) AS vid "
                f"| DELETE VERTEX $-.vid WITH EDGE"
            )
        except Exception:
            pass


# ── Module-scoped fixture (clear once per module) ────────────────────────


@pytest.fixture(scope="module")
def _module_db(_session_db, _session_schema):
    """Yield the session client with data cleared once at module start.

    Use this as the base for module-scoped seeded fixtures in test files
    whose tests are read-only (aggregate, fetch, edges).
    """
    _clear_test_data(_session_db, _session_schema)
    return _session_db


# ── Function-scoped fixture (clear before every test) ────────────────────


@pytest.fixture(scope="function")
def nebula_db(_session_db, _session_schema):
    """Yield the session-scoped client after clearing all data.

    Use for tests that mutate data and need a clean slate each time.
    """
    _clear_test_data(_session_db, _session_schema)
    return _session_db
