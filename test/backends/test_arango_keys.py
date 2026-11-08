"""ArangoDB keys: computed on the client, the same in upsert and bulk."""

from __future__ import annotations

import datetime as dt
import hashlib
from uuid import UUID

import pytest

from graflo.db.arango.keys import (
    edge_documents,
    edge_key,
    endpoint_key,
    vertex_documents,
    vertex_key,
)


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


@pytest.mark.parametrize(
    ("values", "key"),
    [
        (["p1"], "p1"),
        (["a-b:c.d@e(f)+g,h=i;j$k!l*m'n%o_"], "a-b:c.d@e(f)+g,h=i;j$k!l*m'n%o_"),
        ([12], "12"),
        ([-3], "-3"),
        ([1.0], "1"),
        ([UUID(int=1)], "00000000-0000-0000-0000-000000000001"),
    ],
)
def test_a_single_field_identity_is_its_own_key(values, key) -> None:
    assert vertex_key(values) == key


@pytest.mark.parametrize(
    ("values", "typed"),
    [
        (["Ann Smith"], '[["s","Ann Smith"]]'),
        (["a/b"], '[["s","a/b"]]'),
        (["é"], '[["s","é"]]'),
        (["x" * 255], '[["s","' + "x" * 255 + '"]]'),
        ([True], '[["b",true]]'),
        ([1.5], '[["n",1.5]]'),
        ([dt.date(2020, 1, 2)], '[["d","2020-01-02"]]'),
        (["p1", "FR"], '[["s","p1"],["s","FR"]]'),
        ([1, "FR"], '[["n",1],["s","FR"]]'),
    ],
)
def test_other_identities_key_by_the_digest_of_their_typed_tuple(values, typed) -> None:
    assert vertex_key(values) == _sha(typed)


def test_a_composite_key_is_pinned() -> None:
    assert (
        vertex_key(["p1", "FR"])
        == "8e93836311466c4446437163f45b1741d91db4c1c32376ae99b15eb7ca03e73b"
    )


def test_values_of_different_types_key_apart_in_a_composite() -> None:
    assert vertex_key([1, "FR"]) != vertex_key(["1", "FR"])
    assert vertex_key([1, "FR"]) == vertex_key([1.0, "FR"])


@pytest.mark.parametrize(
    "values", [[None], [""], [None, ""], ["p1", float("nan")], [[]]]
)
def test_an_identity_with_no_usable_value_has_no_key(values) -> None:
    assert vertex_key(values) is None


def test_a_composite_identity_lacking_members_keys_them_as_an_upsert_matches() -> None:
    assert vertex_key(["p1", None]) == _sha('[["s","p1"],null]')
    assert vertex_key(["p1", ""]) == _sha('[["s","p1"],["s",""]]')


def test_an_endpoint_keyed_on_key_takes_it_as_is() -> None:
    assert endpoint_key({"_key": "k"}, ("_key",)) == "k"
    assert endpoint_key({"pid": "p1", "_key": "stale"}, ("pid",)) == "p1"


def test_an_edge_key_is_pinned_and_depends_on_every_part() -> None:
    key = edge_key("Person/p1", "City/c1", "lives_in", ["home"])

    assert key == _sha('["Person/p1","City/c1","lives_in",[["s","home"]]]')
    assert key != edge_key("Person/p1", "City/c1", "lives_in", ["work"])
    assert key != edge_key("Person/p1", "City/c1", "visits", ["home"])
    assert key != edge_key("City/c1", "Person/p1", "lives_in", ["home"])
    assert edge_key("a/1", "b/2", None, [1]) != edge_key("a/1", "b/2", None, ["1"])
    assert edge_key("a/1", "b/2", None, [None]) != edge_key("a/1", "b/2", None, [""])


def test_vertex_documents_replace_a_key_unless_it_is_matched_on() -> None:
    docs = [{"pid": "p1", "_key": "stale"}, {"pid": "Ann Smith"}]

    keyed = vertex_documents(docs, ["pid"], strict=True)

    assert [d["_key"] for d in keyed] == ["p1", vertex_key(["Ann Smith"])]
    assert docs[0]["_key"] == "stale"
    assert vertex_documents(docs, ["_key"], strict=True) == docs


def test_a_vertex_that_cannot_be_keyed_is_refused_or_left_to_the_server() -> None:
    with pytest.raises(ValueError, match="cannot key"):
        vertex_documents([{"a": None, "b": None}], ["a", "b"], strict=True)

    (doc,) = vertex_documents(
        [{"a": None, "b": "", "_key": "stale"}], ["a", "b"], strict=False
    )
    assert "_key" not in doc


def test_edge_documents_set_system_fields_over_properties() -> None:
    (doc,) = edge_documents(
        [({"pid": "p1"}, {"cid": "c1"}, {"_from": "x/y", "role": "home"})],
        "Person",
        "City",
        ["pid"],
        ["cid"],
        "lives_in",
        ["role"],
        strict=True,
        fuse=False,
    )

    assert doc == {
        "_key": edge_key("Person/p1", "City/c1", "lives_in", ["home"]),
        "_from": "Person/p1",
        "_to": "City/c1",
        "relation": "lives_in",
        "role": "home",
    }


def test_edge_documents_refuse_an_unkeyed_endpoint_when_strict() -> None:
    with pytest.raises(ValueError, match="cannot key an edge"):
        edge_documents(
            [({"pid": None}, {"cid": "c1"}, {})],
            "Person",
            "City",
            ["pid"],
            ["cid"],
            None,
            [],
            strict=True,
            fuse=False,
        )


def test_an_edge_to_a_vertex_lacking_an_identity_member_keeps_its_endpoint() -> None:
    (vertex,) = vertex_documents(
        [{"arxiv": "2101.1", "created": "2021"}], ["arxiv", "doi"], strict=True
    )
    (edge,) = edge_documents(
        [({"arxiv": "2101.1"}, {"_key": "m1"}, {})],
        "publications",
        "mentions",
        ["arxiv", "doi"],
        ["_key"],
        None,
        [],
        strict=True,
        fuse=True,
    )

    assert edge["_from"] == f"publications/{vertex['_key']}"
    assert edge["_to"] == "mentions/m1"
