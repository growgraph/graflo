"""Native neighbourhoods on Arango and FalkorDB: rows shaped as the default walk's.

No database: a fake driver answers with canned rows in each backend's own
result shape, so these pin how those rows become a container.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, cast

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.graph_types import EdgeDirection
from graflo.architecture.schema import Schema
from graflo.db.arango.conn import ArangoConnection, _arango_safe_collection_name
from graflo.db.conn import Connection
from graflo.db.cypher.traversal import NEIGHBOR_COLUMNS, cypher_neighbors_query
from graflo.db.falkordb.conn import FalkordbConnection
from graflo.db.traversal import edge_query_name
from graflo.onto import DBType

# work_order -targets-> machine <-uses- line
SCHEMA: dict[str, Any] = {
    "metadata": {"name": "impact", "version": "1.0.0"},
    "graph": {
        "vertex_config": {
            "vertices": [
                {
                    "name": name,
                    "properties": [{"name": "key", "type": "STRING"}],
                    "identity": ["key"],
                }
                for name in ("work_order", "machine", "line")
            ]
        },
        "edge_config": {
            "edges": [
                {"source": "work_order", "target": "machine", "relation": "targets"},
                {"source": "line", "target": "machine", "relation": "uses"},
            ]
        },
    },
}


@pytest.fixture
def schema() -> Schema:
    manifest = GraphManifest.model_validate({"schema": SCHEMA})
    manifest.finish_init()
    return manifest.require_schema()


# -- ArangoDB -------------------------------------------------------------------


class _Aql:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.queries: list[str] = []

    def execute(self, query: str, bind_vars: dict[str, Any]) -> Iterator[Any]:
        self.queries.append(query)
        return iter(self.rows)


class _ArangoDB:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.aql = _Aql(rows)


class _Arango:
    vertex_address = Connection.vertex_address

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.conn = _ArangoDB(rows)


def _edge_collection(schema: Schema, relation: str) -> str:
    db_aware = schema.resolve_db_aware(DBType.ARANGO)
    (edge,) = [
        e for e in schema.core_schema.edge_config.edges if e.relation == relation
    ]
    name = edge_query_name(db_aware, edge, DBType.ARANGO)
    assert name is not None
    return _arango_safe_collection_name(name)


def _vertex(collection: str, key: str) -> dict[str, Any]:
    return {"_id": f"{collection}/{key}", "_key": key, "_rev": "r", "key": key}


def _walked(
    collection: str, number: int, start: dict[str, Any], end: dict[str, Any], **props
) -> dict[str, Any]:
    edge = {
        "_id": f"{collection}/{number}",
        "_key": str(number),
        "_from": start["_id"],
        "_to": end["_id"],
        **props,
    }
    return {"edge": edge, "start": start, "end": end}


def _arango_neighbors(schema: Schema, rows: list[dict[str, Any]], **kwargs: Any):
    fake = _Arango(rows)
    container = ArangoConnection._graph_neighbors(
        cast(Any, fake), "work_order", "WO1", schema=schema, **kwargs
    )
    return fake, container


def test_arango_returns_the_edges_of_the_walk(schema: Schema) -> None:
    targets, uses = (
        _edge_collection(schema, "targets"),
        _edge_collection(schema, "uses"),
    )
    wo, press, line = (
        _vertex("work_order", "WO1"),
        _vertex("machine", "press"),
        _vertex("line", "stamping"),
    )
    to_press = _walked(targets, 1, wo, press, w=1)
    rows = [
        {"far": press, "edges": [to_press]},
        {"far": line, "edges": [to_press, _walked(uses, 2, line, press)]},
    ]
    fake, container = _arango_neighbors(
        schema, rows, hops=2, direction=EdgeDirection.ANY
    )

    assert "COLLECT far_id = v._id" in fake.conn.aql.queries[0]
    assert container.edges == {
        ("work_order", "machine", "targets"): [
            {"_key": "1", "w": 1, "source": "WO1", "target": "press"}
        ],
        ("line", "machine", "uses"): [
            {"_key": "2", "source": "stamping", "target": "press"}
        ],
    }
    assert sorted(container.vertices) == ["line", "machine"]


def test_arango_leaves_out_an_edge_of_an_unselected_relation(schema: Schema) -> None:
    uses = _edge_collection(schema, "uses")
    line, press = _vertex("line", "stamping"), _vertex("machine", "press")
    rows = [{"far": line, "edges": [_walked(uses, 2, line, press)]}]
    _, container = _arango_neighbors(schema, rows, edge_types=["targets"])

    assert container.edges == {}


def test_arango_caps_the_edge_rows_at_the_limit(schema: Schema) -> None:
    targets, uses = (
        _edge_collection(schema, "targets"),
        _edge_collection(schema, "uses"),
    )
    wo, press, line = (
        _vertex("work_order", "WO1"),
        _vertex("machine", "press"),
        _vertex("line", "stamping"),
    )
    rows = [
        {
            "far": line,
            "edges": [_walked(targets, 1, wo, press), _walked(uses, 2, line, press)],
        }
    ]
    _, container = _arango_neighbors(schema, rows, hops=2, limit=1)

    assert sum(len(found) for found in container.edges.values()) == 1


# -- FalkorDB -------------------------------------------------------------------


def test_the_neighbour_query_returns_its_columns_in_the_declared_order() -> None:
    query = cypher_neighbors_query(
        anchor_label="node",
        edge_type="links",
        far_label=None,
        direction=EdgeDirection.OUT,
        hops=1,
        limit=None,
    )
    returned = query.split("RETURN ", 1)[1]
    positions = [returned.index(f" AS {column}") for column in NEIGHBOR_COLUMNS]
    assert positions == sorted(positions)


class _Result:
    def __init__(self, result_set: list[list[Any]]) -> None:
        self.result_set = result_set


class _Falkor:
    flavor = DBType.FALKORDB
    vertex_address = Connection.vertex_address

    def __init__(self, result_set: list[list[Any]]) -> None:
        self.result_set = result_set

    def execute(self, query: str, **params: Any) -> _Result:
        return _Result(self.result_set)


def test_falkordb_reads_positional_rows_by_column(schema: Schema) -> None:
    """Labels from the second column, edges from the third."""
    relationship = {
        "type": "targets",
        "properties": {"w": 1},
        "start": {"key": "WO1"},
        "start_labels": ["work_order"],
        "end": {"key": "press"},
        "end_labels": ["machine"],
    }
    fake = _Falkor([[{"key": "press"}, ["machine"], [relationship]]])
    container = FalkordbConnection._graph_neighbors(
        cast(Any, fake), "work_order", "WO1", schema=schema
    )

    assert container.vertices == {"machine": [{"key": "press"}]}
    assert container.edges == {
        ("work_order", "machine", "targets"): [
            {"w": 1, "source": "WO1", "target": "press"}
        ]
    }
