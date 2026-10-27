"""A Neo4j read returns plain Python: the driver's temporal types are converted.

``Result.data()`` keeps ``neo4j.time`` values, which no caller can serialize.
"""

from __future__ import annotations

import datetime
from typing import Any, cast
from unittest.mock import Mock

from neo4j.time import Date, DateTime, Time

from graflo.db.neo4j.conn import Neo4jConnection


class _Rows:
    """Stands in for a connection whose every query returns *rows*."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def execute(self, query: str, **kwargs: Any) -> Mock:
        result = Mock()
        result.data.return_value = self.rows
        return result


def test_fetched_documents_hold_python_temporals() -> None:
    released = DateTime(2026, 9, 1, 12, 30, 0, tzinfo=datetime.UTC)
    fake = _Rows(
        [
            {
                "props": {
                    "release_id": "R-3.2",
                    "released_at": released,
                    "dates": [Date(2026, 9, 1)],
                    "window": {"opens": Time(8, 0, 0)},
                }
            }
        ]
    )

    (doc,) = Neo4jConnection.fetch_all_docs(cast(Any, fake), "Release")

    # A driver temporal compares equal to its native value, so check the types.
    assert type(doc["released_at"]) is datetime.datetime
    assert doc["released_at"] == datetime.datetime(
        2026, 9, 1, 12, 30, tzinfo=datetime.UTC
    )
    assert [type(d) for d in doc["dates"]] == [datetime.date]
    assert type(doc["window"]["opens"]) is datetime.time


def test_other_values_pass_through() -> None:
    fake = _Rows([{"props": {"sku": "P-200", "ports": [1, 2], "active": True}}])

    (doc,) = Neo4jConnection.fetch_all_docs(cast(Any, fake), "Product")

    assert doc == {"sku": "P-200", "ports": [1, 2], "active": True}
