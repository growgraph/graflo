"""Relationship upserts on Cypher backends when a merge property is missing."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from graflo.connections.onto import Neo4jConfig
from graflo.db.cypher import partition_by_absent_merge_props, rel_absent_upsert_clause
from graflo.db.neo4j.conn import Neo4jConnection


def _triples(rows):
    return partition_by_absent_merge_props(
        rows,
        ("since", "role"),
        props_of=lambda row: row[2],
        ends_of=lambda row: (row[0], row[1]),
    )


def test_rows_with_every_merge_property_keep_one_group_first():
    rows = [
        [{"id": "a"}, {"id": "b"}, {"since": "2024", "role": "x"}],
        [{"id": "a"}, {"id": "b"}, {"role": "x"}],
        [{"id": "a"}, {"id": "c"}, {"since": None, "role": "x"}],
        [{"id": "a"}, {"id": "b"}, {"since": "2023", "role": "y"}],
    ]
    groups = _triples(rows)
    assert [absent for absent, _ in groups] == [(), ("since",)]
    assert groups[0][1] == [rows[0], rows[3]]
    assert groups[1][1] == [rows[1], rows[2]]


def test_rows_lacking_a_property_are_deduplicated_later_props_winning():
    rows = [
        [{"id": "a"}, {"id": "b"}, {"role": "x", "note": "first"}],
        [{"id": "a"}, {"id": "b"}, {"role": "x", "note": "second", "w": 1}],
        [{"id": "a"}, {"id": "b"}, {"role": "y"}],
    ]
    ((absent, group),) = _triples(rows)
    assert absent == ("since",)
    assert group == [
        [{"id": "a"}, {"id": "b"}, {"role": "x", "note": "second", "w": 1}],
        [{"id": "a"}, {"id": "b"}, {"role": "y"}],
    ]


def test_dict_rows_are_merged_under_props():
    rows = [
        {"source": {"id": "a"}, "target": {"id": "b"}, "props": {"n": 1}},
        {"source": {"id": "a"}, "target": {"id": "b"}, "props": {"m": 2}},
    ]
    ((absent, group),) = partition_by_absent_merge_props(
        rows,
        ("since",),
        props_of=lambda row: row["props"],
        ends_of=lambda row: (row["source"], row["target"]),
    )
    assert absent == ("since",)
    assert group == [
        {"source": {"id": "a"}, "target": {"id": "b"}, "props": {"n": 1, "m": 2}}
    ]


def test_the_absent_clause_matches_unset_properties_and_never_merges_on_null():
    clause = " ".join(
        rel_absent_upsert_clause(
            "OPERATES", ("since", "role"), ("since",), props_expr="row[2]"
        ).split()
    )
    assert "MERGE" not in clause
    assert (
        "OPTIONAL MATCH (source)-[existing:OPERATES]->(target) "
        "WHERE existing.`since` IS NULL AND existing.`role` = row[2]['role']"
    ) in clause
    assert "CREATE (source)-[r:OPERATES]->(target) SET r += row[2]" in clause
    assert "SET existing += row[2]" in clause


@patch("graflo.db.neo4j.conn.GraphDatabase.driver")
def test_neo4j_writes_a_row_without_its_merge_property_apart(mock_driver):
    session = MagicMock()
    mock_driver.return_value = MagicMock()
    mock_driver.return_value.session.return_value = session
    conn = Neo4jConnection(Neo4jConfig(uri="bolt://localhost:7687"))
    dated = [{"email": "a@x"}, {"serial": "S-7"}, {"since": "2024-03"}]
    undated = [{"email": "b@x"}, {"serial": "S-7"}, {}]
    conn.insert_edges_batch(
        [dated, undated],
        source_class="Person",
        target_class="Machine",
        relation_name="OPERATES",
        match_keys_source=("email",),
        match_keys_target=("serial",),
        relationship_merge_properties=("since",),
    )
    calls = session.run.call_args_list
    assert len(calls) == 2
    first, second = (" ".join(call.args[0].split()) for call in calls)
    assert "MERGE (source)-[r:OPERATES {`since`: row[2]['since']}]->(target)" in first
    assert calls[0].kwargs["batch"] == [dated]
    assert "MERGE" not in second
    assert "existing.`since` IS NULL" in second
    assert calls[1].kwargs["batch"] == [undated]
