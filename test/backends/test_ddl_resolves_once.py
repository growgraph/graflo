"""Index DDL resolves the db-aware schema once, not once per vertex type."""

from __future__ import annotations

from typing import Any, cast

import pytest

from graflo.architecture.schema import Schema
from graflo.db.falkordb.conn import FalkordbConnection
from graflo.db.memgraph.conn import MemgraphConnection
from graflo.db.neo4j.conn import Neo4jConnection


def _schema(n: int, flavor: str) -> Schema:
    return Schema.model_validate(
        {
            "metadata": {"name": "many"},
            "core_schema": {
                "vertex_config": {
                    "vertices": [
                        {"name": f"v{i}", "properties": ["id"], "identity": ["id"]}
                        for i in range(n)
                    ]
                },
                "edge_config": {"edges": []},
            },
            "db_profile": {"db_flavor": flavor},
        }
    )


class _Cursor:
    def execute(self, query: str) -> None:
        return None

    def close(self) -> None:
        return None


class _Recorder:
    """Stands in for a connection: records the indexes it is asked to add."""

    def __init__(self) -> None:
        self.added: list[str] = []
        self.conn = self

    def cursor(self) -> _Cursor:
        return _Cursor()

    def _add_index(self, label: str, index: Any, **_: Any) -> None:
        self.added.append(label)


@pytest.mark.parametrize(
    ("connection", "flavor"),
    [
        (Neo4jConnection, "neo4j"),
        (MemgraphConnection, "memgraph"),
        (FalkordbConnection, "falkordb"),
    ],
)
def test_vertex_index_ddl_resolves_the_schema_once(
    monkeypatch, connection: Any, flavor: str
) -> None:
    schema = _schema(5, flavor)
    calls: list[object] = []
    resolve = Schema.resolve_db_aware

    def _counting(self: Schema, *args: Any, **kwargs: Any) -> Any:
        calls.append(args)
        return resolve(self, *args, **kwargs)

    monkeypatch.setattr(Schema, "resolve_db_aware", _counting)

    connection.define_vertex_indexes(
        cast(Any, _Recorder()), schema.core_schema.vertex_config, schema
    )

    assert len(calls) == 1
