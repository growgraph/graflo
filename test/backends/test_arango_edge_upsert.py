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
        on_duplicate="upsert",
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
