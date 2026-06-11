"""Unit tests for TigerGraph document helpers and keep_absent_documents."""

from __future__ import annotations

from unittest.mock import MagicMock

from graflo.db.tigergraph.conn import TigerGraphConnection
from graflo.db.tigergraph.data_ops import TigerGraphDataOps
from graflo.db.tigergraph.document_utils import clean_document, extract_id


def test_extract_id_composite_key() -> None:
    assert extract_id({"a": 1, "b": 2}, ["a", "b"]) == "1_2"


def test_extract_id_prefers_key() -> None:
    assert extract_id({"_key": "k", "email": "x@y.com"}, ["email"]) == "k"


def test_clean_document_strips_internal_keys() -> None:
    assert clean_document({"_foo": 1, "bar": 2, "_key": "k"}) == {
        "bar": 2,
        "_key": "k",
    }


def test_conn_extract_id_delegate() -> None:
    assert TigerGraphConnection._extract_id({"id": "42"}, ["id"]) == "42"


def test_keep_absent_documents_uses_extract_id() -> None:
    conn = TigerGraphConnection.__new__(TigerGraphConnection)
    conn._get_vertices_by_id = MagicMock(
        side_effect=lambda _cls, vid: (
            {"a@x.com": {"attributes": {"email": "a@x.com"}}}
            if vid == "a@x.com"
            else {}
        )
    )
    ops = TigerGraphDataOps(conn)
    absent = ops.keep_absent_documents(
        [{"email": "a@x.com"}, {"email": "b@x.com"}],
        "User",
        match_keys=["email"],
    )
    assert absent == [{"email": "b@x.com"}]
