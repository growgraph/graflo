"""Neo4j sessions go to the named database: the configured one, or the schema's
namespace once it exists; an edition that cannot create it keeps the home one."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from graflo.architecture.schema import Schema
from graflo.connections.onto import Neo4jConfig
from graflo.db.conn import NamespaceNotFoundError
from graflo.db.neo4j.conn import Neo4jConnection

SCHEMA: dict[str, Any] = {
    "metadata": {"name": "library", "version": "1.0.0"},
    "core_schema": {
        "vertex_config": {"vertices": [{"name": "book", "properties": ["isbn"]}]},
        "edge_config": {"edges": []},
    },
}


def _connection(driver: MagicMock, *, database: str | None = None) -> Neo4jConnection:
    with patch("graflo.db.neo4j.conn.GraphDatabase.driver", return_value=driver):
        return Neo4jConnection(
            Neo4jConfig(uri="bolt://localhost:7687", password="x", database=database)
        )


def _databases(driver: MagicMock, *listings: list[str]) -> None:
    """Answer each ``SHOW DATABASES`` with the next listing; anything else with nothing."""
    pending = list(listings)

    def run(query: str, **_: Any) -> list[dict[str, str]]:
        if query == "SHOW DATABASES":
            return [{"name": n} for n in pending.pop(0)]
        return []

    driver.session.return_value.run.side_effect = run


def test_a_configured_database_names_every_session() -> None:
    driver = MagicMock()
    conn = _connection(driver, database="papers")

    driver.session.assert_called_once_with(database="papers")
    conn.reset_after_use()
    assert driver.session.call_args.kwargs == {"database": "papers"}


def test_without_a_database_sessions_go_to_the_home_database() -> None:
    driver = MagicMock()
    _connection(driver)
    driver.session.assert_called_once_with(database=None)


def test_the_schema_namespace_is_used_once_it_exists() -> None:
    driver = MagicMock()
    conn = _connection(driver)
    _databases(driver, ["neo4j", "system"], ["neo4j", "system", "library"])

    conn.ensure_target_namespace(Schema.model_validate(SCHEMA), create=True)

    assert conn.config.database == "library"
    assert driver.session.call_args.kwargs == {"database": "library"}


def test_an_edition_that_cannot_create_it_keeps_the_home_database() -> None:
    driver = MagicMock()
    conn = _connection(driver)
    _databases(driver, ["neo4j", "system"], ["neo4j", "system"])

    conn.ensure_target_namespace(Schema.model_validate(SCHEMA), create=True)

    assert conn.config.database is None
    assert driver.session.call_args.kwargs == {"database": None}


def test_a_named_database_the_server_cannot_create_is_refused() -> None:
    driver = MagicMock()
    conn = _connection(driver, database="papers")
    _databases(driver, ["neo4j", "system"], ["neo4j", "system"])

    with pytest.raises(NamespaceNotFoundError, match="papers"):
        conn.ensure_target_namespace(Schema.model_validate(SCHEMA), create=True)
