"""Arango edge upsert: an edge's ``_key`` tells relations and identity values apart."""

from __future__ import annotations

import json
from typing import Any, cast

from graflo.db.arango.conn import ArangoConnection


class _Capture:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def execute(self, query: str, **kwargs: Any) -> None:
        self.queries.append(query)


def _write(rows: list[Any], **kwargs: Any) -> tuple[str, list[dict[str, Any]]]:
    fake = _Capture()
    ArangoConnection.insert_edges_batch(
        cast(Any, fake),
        docs_edges=rows,
        source_class="person",
        target_class="org",
        filter_uniques=False,
        **kwargs,
    )
    (query,) = fake.queries
    docs = json.loads(query.split("FOR doc IN ", 1)[1].split(" INSERT", 1)[0])
    return query, docs


ROW = [{"_key": "1"}, {"_key": "2"}, {"since": 2020}]


def test_an_edge_is_written_on_its_key_with_endpoints_built_on_the_client() -> None:
    query, (doc,) = _write([ROW], relation_name="works_at")

    assert 'INSERT doc INTO person_org_edges OPTIONS {overwriteMode: "update"}' in query
    assert "UPSERT" not in query and "exclusive" not in query
    assert "FOR v IN" not in query
    assert doc["_from"] == "person/1" and doc["_to"] == "org/2"
    assert doc["relation"] == "works_at" and doc["since"] == 2020


def test_two_relations_on_one_pair_have_two_keys() -> None:
    _, (works_at,) = _write([ROW], relation_name="works_at")
    _, (advises,) = _write([ROW], relation_name="advises")

    assert works_at["_key"] != advises["_key"]


def test_identity_fields_tell_parallel_edges_apart_and_others_do_not() -> None:
    other = [ROW[0], ROW[1], {"since": 2021}]
    _, (a,) = _write([ROW], relation_name=None, uniq_weight_fields=["since"])
    _, (b,) = _write([other], relation_name=None, uniq_weight_fields=["since"])
    _, (c,) = _write([ROW], relation_name=None)
    _, (d,) = _write([other], relation_name=None)

    assert a["_key"] != b["_key"]
    assert c["_key"] == d["_key"]


def test_a_relation_identity_token_is_keyed_once_as_the_relation() -> None:
    """``identities: [[source, target, relation]]`` adds ``relation`` to the
    identity fields; the key takes it once, as the relation being written."""
    _, (a,) = _write([ROW], relation_name="works_at", uniq_weight_fields=["relation"])
    _, (b,) = _write([ROW], relation_name="works_at")

    assert a["_key"] == b["_key"]


def test_rows_of_one_edge_in_a_batch_are_written_once_later_values_winning() -> None:
    _, docs = _write(
        [
            [{"_key": "1"}, {"_key": "2"}, {"since": 2020, "w": 1, "x": "a"}],
            [{"_key": "1"}, {"_key": "2"}, {"since": 2021, "w": 2}],
            [{"_key": "1"}, {"_key": "2"}, {"since": 2020, "w": 3}],
        ],
        relation_name="works_at",
        uniq_weight_fields=["since"],
    )

    assert len(docs) == 2
    first = next(d for d in docs if d["since"] == 2020)
    assert (first["w"], first["x"]) == (3, "a")


def test_endpoints_keyed_on_identity_fields_take_the_vertex_key() -> None:
    _, (doc,) = _write(
        [[{"pid": "p1"}, {"code": "ACME", "country": "FR"}, {}]],
        relation_name="works_at",
        match_keys_source=("pid",),
        match_keys_target=("code", "country"),
    )

    assert doc["_from"] == "person/p1"
    assert doc["_to"].startswith("org/") and len(doc["_to"]) == len("org/") + 64


def test_a_row_missing_an_endpoint_value_is_dropped() -> None:
    fake = _Capture()
    ArangoConnection.insert_edges_batch(
        cast(Any, fake),
        docs_edges=[[{"pid": None}, {"_key": "2"}, {}]],
        source_class="person",
        target_class="org",
        relation_name="works_at",
        match_keys_source=("pid",),
        filter_uniques=False,
    )

    assert fake.queries == []
