"""Tests for Cypher MERGE includng relationship identity properties (parallel edges)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from graflo.db.connection.onto import Neo4jConfig
from graflo.db.cypher import rel_merge_props_map_from_row_index
from graflo.db.neo4j.conn import Neo4jConnection


def test_neo4j_rel_merge_map_uses_bracket_access() -> None:
    clause = rel_merge_props_map_from_row_index(("date", "relation"))
    assert "`date`: row[2]['date']" in clause
    assert "`relation`: row[2]['relation']" in clause


@patch("graflo.db.neo4j.conn.GraphDatabase.driver")
def test_insert_edges_batch_includes_merge_properties_in_cypher(mock_driver) -> None:
    session = MagicMock()
    mock_driver.return_value = MagicMock()
    mock_driver.return_value.session.return_value = session

    cfg = Neo4jConfig(uri="bolt://localhost:7687")
    conn = Neo4jConnection(cfg)
    batch = [
        [{"id": "a"}, {"id": "b"}, {"kind": "x", "n": 1}],
        [{"id": "a"}, {"id": "b"}, {"kind": "y", "n": 2}],
    ]
    conn.insert_edges_batch(
        batch,
        source_class="S",
        target_class="T",
        relation_name="REL",
        match_keys_source=("id",),
        match_keys_target=("id",),
        relationship_merge_properties=("kind",),
        filter_uniques=False,
    )

    session.run.assert_called_once()
    query = session.run.call_args[0][0]
    collapsed = " ".join(query.split())
    assert "MERGE (source)-[r:REL {`kind`: row[2]['kind']}]->(target)" in collapsed
