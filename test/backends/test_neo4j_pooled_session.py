"""A Neo4j write finishes before its connection is reused.

``session.run`` returns before the server commits an auto-commit query; the
commit (and any error) is read only when the result is consumed.
"""

from __future__ import annotations

from typing import Any, cast
from unittest.mock import Mock

from graflo.db.neo4j.conn import Neo4jConnection


class _Results:
    """Stands in for a connection: hands out results and records them."""

    def __init__(self) -> None:
        self.results: list[Mock] = []

    def execute(self, query: str, **kwargs: Any) -> Mock:
        result = Mock()
        self.results.append(result)
        return result


def test_a_vertex_upsert_consumes_its_result() -> None:
    fake = _Results()

    Neo4jConnection.upsert_docs_batch(
        cast(Any, fake), [{"id": "1"}], "person", match_keys=["id"]
    )

    (result,) = fake.results
    result.consume.assert_called_once_with()


def test_a_reused_connection_starts_a_fresh_session() -> None:
    old, new = Mock(), Mock()
    driver = Mock()
    driver.session.return_value = new
    fake = Mock(conn=old, _driver=driver)

    Neo4jConnection.reset_after_use(fake)

    old.close.assert_called_once_with()
    assert fake.conn is new


def test_an_edge_insert_runs_in_a_retried_write_transaction() -> None:
    """Edge types sharing endpoint nodes write concurrently; a deadlock between
    them is transient, and a managed write transaction retries it."""
    tx = Mock()
    session = Mock()
    session.execute_write.side_effect = lambda work: work(tx)
    fake = Mock(conn=session)

    Neo4jConnection.insert_edges_batch(
        fake,
        docs_edges=[[{"id": "1"}, {"id": "2"}, {}]],
        source_class="person",
        target_class="org",
        relation_name="works_at",
        match_keys_source=("id",),
        match_keys_target=("id",),
    )

    session.execute_write.assert_called()
    tx.run.return_value.consume.assert_called()
    fake.execute.assert_not_called()
