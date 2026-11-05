"""Relationship upserts on Cypher backends when a merge property is missing."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from graflo.connections.onto import Neo4jConfig
from graflo.db.cypher import (
    partition_by_absent_merge_props,
    rel_absent_upsert_clause,
    rel_union_rows,
    rel_upsert_clause,
)
from graflo.db.falkordb.conn import FalkordbConnection
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
    # Edge writes run in a managed write transaction; record its queries here.
    session.execute_write.side_effect = lambda work: work(session)
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


# -- union-merged edge fields --------------------------------------------------

TAGS_UNION = (
    "r.`tags` = reduce(acc = [], x IN [y IN coalesce(r.`tags`, []) "
    "+ coalesce(row[3]['tags'], []) WHERE y IS NOT NULL] "
    "| CASE WHEN x IN acc THEN acc ELSE acc + x END)"
)


def _flat(text: str) -> str:
    return " ".join(text.split())


def test_the_upsert_clause_without_union_is_the_plain_merge():
    assert rel_upsert_clause("about", (), ()) == (
        "MERGE (source)-[r:about]->(target)\nSET r += row[2]"
    )


def test_a_union_field_is_set_from_its_own_row_slot_after_the_replace():
    clause = _flat(rel_upsert_clause("about", (), (), union=("tags",)))

    assert clause == (
        f"MERGE (source)-[r:about]->(target) SET r += row[2] SET {TAGS_UNION}"
    )


def test_a_union_field_is_set_on_a_merge_with_identity_properties():
    clause = _flat(rel_upsert_clause("about", ("since",), (), union=("tags",)))

    assert clause.startswith(
        "MERGE (source)-[r:about {`since`: row[2]['since']}]->(target) SET r += row[2]"
    )
    assert clause.endswith(f"SET {TAGS_UNION}")


def test_the_absent_clause_unions_on_create_and_on_match():
    clause = _flat(rel_upsert_clause("about", ("since",), ("since",), union=("tags",)))

    assert f"CREATE (source)-[r:about]->(target) SET r += row[2] SET {TAGS_UNION})" in (
        clause
    )
    existing_union = TAGS_UNION.replace("r.`tags`", "existing.`tags`")
    assert f"SET existing += row[2] SET {existing_union})" in clause


def test_union_rows_move_the_union_fields_out_of_the_replaced_props():
    rows = [({"id": "a"}, {"id": "b"}, {"w": 1, "tags": ["x"]})]

    assert rel_union_rows(rows, ("tags",)) == [
        [{"id": "a"}, {"id": "b"}, {"w": 1}, {"tags": ["x"]}]
    ]


def test_with_union_fields_every_edge_is_one_row_holding_every_member():
    rows = [
        [{"id": "a"}, {"id": "b"}, {"tags": [1, 2], "note": "first"}],
        [{"id": "a"}, {"id": "c"}, {"tags": [5]}],
        [{"id": "a"}, {"id": "b"}, {"tags": [2, None, 3], "note": "second"}],
    ]

    ((absent, group),) = partition_by_absent_merge_props(
        rows,
        (),
        props_of=lambda row: row[2],
        ends_of=lambda row: (row[0], row[1]),
        union=("tags",),
    )

    assert absent == ()
    assert group == [
        [{"id": "a"}, {"id": "b"}, {"tags": [1, 2, 3], "note": "second"}],
        [{"id": "a"}, {"id": "c"}, {"tags": [5]}],
    ]


def test_without_union_fields_the_complete_group_is_left_as_given():
    rows = [
        [{"id": "a"}, {"id": "b"}, {"since": "2024", "tags": [1]}],
        [{"id": "a"}, {"id": "b"}, {"since": "2024", "tags": [2]}],
    ]

    ((_, group),) = _triples([[*row[:2], {**row[2], "role": "x"}] for row in rows])

    assert len(group) == 2


@patch("graflo.db.neo4j.conn.GraphDatabase.driver")
def test_neo4j_writes_union_edges_deduplicated_with_a_union_slot(mock_driver):
    session = MagicMock()
    session.execute_write.side_effect = lambda work: work(session)
    mock_driver.return_value = MagicMock()
    mock_driver.return_value.session.return_value = session
    conn = Neo4jConnection(Neo4jConfig(uri="bolt://localhost:7687"))
    conn.insert_edges_batch(
        [
            [{"key": "SUP-47"}, {"key": "SUP-42"}, {"access_zones": [1]}],
            [{"key": "SUP-47"}, {"key": "SUP-42"}, {"access_zones": [2]}],
        ],
        source_class="Issue",
        target_class="Issue",
        relation_name="duplicates",
        match_keys_source=("key",),
        match_keys_target=("key",),
        union_fields=("access_zones",),
    )

    ((call,),) = [session.run.call_args_list]
    query = _flat(call.args[0])
    assert "SET r += row[2] SET r.`access_zones` = reduce(" in query
    assert call.kwargs["batch"] == [
        [{"key": "SUP-47"}, {"key": "SUP-42"}, {}, {"access_zones": [1, 2]}]
    ]


class _Falkor(FalkordbConnection):
    def __init__(self) -> None:
        self.sent: list[tuple[str, dict]] = []

    def execute(self, query: str, **params):
        self.sent.append((query, params))


def test_falkordb_writes_union_edges_deduplicated_with_a_union_slot():
    conn = _Falkor()
    conn.insert_edges_batch(
        [
            [{"key": "SUP-47"}, {"key": "SUP-42"}, {"access_zones": [1], "w": 1}],
            [{"key": "SUP-47"}, {"key": "SUP-42"}, {"access_zones": [2], "w": 2}],
        ],
        source_class="Issue",
        target_class="Issue",
        relation_name="duplicates",
        match_keys_source=("key",),
        match_keys_target=("key",),
        union_fields=("access_zones",),
    )

    ((query, params),) = conn.sent
    assert "SET r += row[2] SET r.`access_zones` = reduce(" in _flat(query)
    assert params["data"] == [
        [{"key": "SUP-47"}, {"key": "SUP-42"}, {"w": 2}, {"access_zones": [1, 2]}]
    ]
