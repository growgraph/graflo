"""Which documents of a batch are already stored, on the Cypher backends."""

from __future__ import annotations

from typing import Any

import pytest

from graflo.db.falkordb.conn import FalkordbConnection
from graflo.db.memgraph.conn import MemgraphConnection
from graflo.db.neo4j.conn import Neo4jConnection
from graflo.db.resolve import present_documents
from graflo.filter.onto import FilterExpression

CYPHER_CONNECTIONS = [Neo4jConnection, MemgraphConnection, FalkordbConnection]


class _Stored:
    """Answers ``fetch_docs`` from a list, as a filtered database read would."""

    def __init__(self, docs: list[dict[str, Any]]) -> None:
        self.docs = docs
        self.reads = 0

    def fetch_docs(
        self,
        class_name: str,
        filters: Any = None,
        limit: int | None = None,
        return_keys: list[str] | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        self.reads += 1
        expression = FilterExpression.from_dict(filters) if filters else None
        rows = [d for d in self.docs if expression is None or expression.matches(d)]
        if return_keys:
            rows = [{k: d.get(k) for k in return_keys} for d in rows]
        return rows


STORED = [
    {"id": 1, "name": "a", "kind": "x"},
    {"id": 2, "name": "b", "kind": "y"},
    {"id": 2, "name": "b2", "kind": "y"},
]
BATCH = [{"id": 2}, {"id": 3}, {"id": 1}, {"name": "no id"}]


def test_the_present_documents_by_batch_position() -> None:
    assert present_documents(_Stored(STORED), BATCH, "t", ["id"]) == {
        0: [{"id": 2, "name": "b", "kind": "y"}],
        2: [{"id": 1, "name": "a", "kind": "x"}],
    }


def test_keep_keys_project_the_match() -> None:
    present = present_documents(_Stored(STORED), BATCH, "t", ["id"], keep_keys=["name"])
    assert present == {0: [{"name": "b"}], 2: [{"name": "a"}]}


def test_filters_narrow_the_match() -> None:
    present = present_documents(
        _Stored(STORED),
        BATCH,
        "t",
        ["id"],
        filters={"field": "kind", "cmp_operator": "==", "value": "x"},
    )
    assert list(present) == [2]


def test_one_read_per_chunk_of_keys() -> None:
    stored = _Stored(STORED)
    present_documents(stored, [{"id": n} for n in range(450)], "t", ["id"])
    assert stored.reads == 3


@pytest.mark.parametrize("connection", CYPHER_CONNECTIONS)
def test_every_cypher_backend_answers_by_batch_position(connection) -> None:
    stored = _Stored(STORED)

    assert connection.fetch_present_documents(stored, BATCH, "t", ["id"]) == {
        0: [{"id": 2, "name": "b", "kind": "y"}],
        2: [{"id": 1, "name": "a", "kind": "x"}],
    }
    assert connection.fetch_present_documents(
        stored, BATCH, "t", ["id"], keep_keys=["id"], flatten=True
    ) == [{"id": 2}, {"id": 1}]


@pytest.mark.parametrize("connection", CYPHER_CONNECTIONS)
def test_every_cypher_backend_keeps_the_absent_ones(connection) -> None:
    stored = _Stored(STORED)

    assert connection.keep_absent_documents(stored, BATCH, "t", ["id"]) == [
        {"id": 3},
        {"name": "no id"},
    ]
    assert connection.keep_absent_documents(
        stored, BATCH, "t", ["id"], keep_keys=["id"]
    ) == [{"id": 3}, {"id": None}]


@pytest.mark.parametrize("connection", CYPHER_CONNECTIONS)
def test_insert_return_batch_says_it_is_not_supported(connection) -> None:
    with pytest.raises(NotImplementedError, match="upsert_docs_batch"):
        connection.insert_return_batch(_Stored([]), [{"id": 1}], "t")
