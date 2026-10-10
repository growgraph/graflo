"""Neo4j credentials: a password is never dropped."""

from __future__ import annotations

from unittest.mock import patch

from graflo.connections.onto import Neo4jConfig
from graflo.db.neo4j.conn import Neo4jConnection


@patch("graflo.db.neo4j.conn.GraphDatabase.driver")
def test_a_password_without_a_username_authenticates_as_neo4j(mock_driver):
    Neo4jConnection(Neo4jConfig(uri="bolt://localhost:7687", password="secret"))
    assert mock_driver.call_args.kwargs["auth"] == ("neo4j", "secret")


@patch("graflo.db.neo4j.conn.GraphDatabase.driver")
def test_no_password_connects_unauthenticated(mock_driver):
    Neo4jConnection(Neo4jConfig(uri="bolt://localhost:7687", username="neo4j"))
    assert mock_driver.call_args.kwargs["auth"] is None
