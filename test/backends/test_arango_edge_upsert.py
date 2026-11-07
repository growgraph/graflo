"""Arango edge upsert: the UPSERT match tells relations and weights apart."""

from __future__ import annotations

from typing import Any, cast

from graflo.db.arango.conn import ArangoConnection


class _Capture:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def execute(self, query: str, **kwargs: Any) -> None:
        self.queries.append(query)


def _upsert_query(**kwargs: Any) -> str:
    fake = _Capture()
    ArangoConnection.insert_edges_batch(
        cast(Any, fake),
        docs_edges=[[{"_key": "1"}, {"_key": "2"}, {"since": 2020}]],
        source_class="person",
        target_class="org",
        filter_uniques=False,
        **kwargs,
    )
    (query,) = fake.queries
    return query


def _upsert_match(query: str) -> str:
    return query.split("UPSERT ", 1)[1].split(" INSERT", 1)[0]


def test_the_upsert_match_names_the_relation_it_writes() -> None:
    query = _upsert_query(relation_name="works_at")

    assert "'relation': 'works_at'" in _upsert_match(query)


def test_the_upserted_document_carries_its_relation() -> None:
    query = _upsert_query(relation_name="works_at")

    assert "{'relation': 'works_at'}" in query.split("LET doc = ", 1)[1].split("\n")[0]


def test_the_upsert_match_reads_weights_from_the_edge_properties() -> None:
    match = _upsert_match(
        _upsert_query(relation_name=None, uniq_weight_fields=["since"])
    )

    assert "'since': edge[2].since" in match
    assert "edge.since" not in match


def test_an_upsert_without_relation_or_weights_matches_endpoints_only() -> None:
    match = _upsert_match(_upsert_query(relation_name=None))

    assert match.count(":") == 2


def test_a_relation_identity_token_is_matched_once_as_the_relation() -> None:
    """``identities: [[source, target, relation]]`` adds ``relation`` to the
    weight fields; the match names it once, by the relation being written."""
    match = _upsert_match(
        _upsert_query(relation_name="works_at", uniq_weight_fields=["relation"])
    )

    assert match.count("'relation'") == 1
    assert "'relation': 'works_at'" in match


def test_an_existing_edge_takes_the_rows_properties() -> None:
    query = _upsert_query(relation_name="works_at")

    assert "INSERT doc UPDATE doc" in query
    assert "ignoreErrors" not in query


def test_rows_of_one_edge_in_a_batch_are_written_once_later_values_winning() -> None:
    fake = _Capture()
    ArangoConnection.insert_edges_batch(
        cast(Any, fake),
        docs_edges=[
            [{"_key": "1"}, {"_key": "2"}, {"since": 2020, "w": 1, "x": "a"}],
            [{"_key": "1"}, {"_key": "2"}, {"since": 2021, "w": 2}],
            [{"_key": "1"}, {"_key": "2"}, {"since": 2020, "w": 3}],
        ],
        source_class="person",
        target_class="org",
        relation_name="works_at",
        filter_uniques=False,
        uniq_weight_fields=["since"],
    )
    (query,) = fake.queries
    rows = query.split("FOR edge in ", 1)[1].split(" LET", 1)[0]

    assert rows.count('"since"') == 2
    assert '{"since": 2020, "w": 3, "x": "a"}' in rows
