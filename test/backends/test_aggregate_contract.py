"""Every backend's ``aggregate`` returns the shape :mod:`graflo.db.aggregation` documents.

Each backend runs against a fake driver that answers with canned rows in the
columns its query names, so the test checks both the query it sends and the
shape it makes of the answer.
"""

from __future__ import annotations

from typing import Any, cast

import pytest

from graflo.db.aggregation import (
    aggregation_result,
    check_aggregation,
    sort_values,
)
from graflo.db.arango import conn as arango_module
from graflo.db.arango.conn import ArangoConnection
from graflo.db.falkordb.conn import FalkordbConnection
from graflo.db.memgraph.conn import MemgraphConnection
from graflo.db.nebula.conn import NebulaConnection
from graflo.db.neo4j.conn import Neo4jConnection
from graflo.db.postgres.conn import PostgresConnection
from graflo.db.tigergraph.data_ops import TigerGraphDataOps
from graflo.onto import AggregationType, ExpressionFlavor

COUNT = AggregationType.COUNT
MAX = AggregationType.MAX
AVERAGE = AggregationType.AVERAGE
SORTED_UNIQUE = AggregationType.SORTED_UNIQUE


# -- the shared contract --------------------------------------------------------


def test_an_ungrouped_result_is_one_value():
    assert aggregation_result(COUNT, [(None, 3)], grouped=False) == 3
    assert aggregation_result(MAX, [(None, "z")], grouped=False) == "z"
    assert aggregation_result(AVERAGE, [(None, 2)], grouped=False) == 2.0
    assert aggregation_result(SORTED_UNIQUE, [(None, [2, 1])], grouped=False) == [
        1,
        2,
    ]


def test_an_empty_answer_is_the_empty_aggregation():
    assert aggregation_result(COUNT, [], grouped=False) == 0
    assert aggregation_result(MAX, [], grouped=False) is None
    assert aggregation_result(AVERAGE, [(None, None)], grouped=False) is None
    assert aggregation_result(SORTED_UNIQUE, [(None, None)], grouped=False) == []
    assert aggregation_result(COUNT, [], grouped=True) == {}


def test_a_grouped_result_maps_each_group_to_its_value():
    rows = [("a", [3, 1, None]), (None, []), (["x", "y"], [2])]
    assert aggregation_result(SORTED_UNIQUE, rows, grouped=True) == {
        "a": [1, 3],
        None: [],
        ("x", "y"): [2],
    }


def test_sorted_values_order_mixed_types_the_same_way_everywhere():
    assert sort_values(["b", 2, None, 1.5, "a", (1,)]) == [1.5, 2, "a", "b", (1,)]


def test_a_field_is_required_for_everything_but_count():
    assert check_aggregation(COUNT, None) is COUNT
    assert check_aggregation(cast(Any, "MAX"), "price") is MAX
    with pytest.raises(ValueError, match="aggregated_field is required"):
        check_aggregation(SORTED_UNIQUE, None)


# -- the backends ----------------------------------------------------------------


class _Cypher:
    """A Neo4j / FalkorDB stand-in: records the query, answers with *rows*."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.sent: list[str] = []

    def expression_flavor(self) -> ExpressionFlavor:
        return ExpressionFlavor.CYPHER

    def execute(self, query: str, **_: Any) -> Any:
        self.sent.append(query)
        rows = self.rows
        return type(
            "R",
            (),
            {
                "data": lambda _self: rows,
                "result_set": [list(row.values()) for row in rows],
            },
        )()


@pytest.mark.parametrize("connection", [Neo4jConnection, FalkordbConnection])
def test_a_cypher_backend_groups_every_function(connection) -> None:
    fake = _Cypher(
        [{"group_key": "a", "agg_value": 3}, {"group_key": None, "agg_value": None}]
    )

    result = connection.aggregate(
        cast(Any, fake), "Item", MAX, discriminant="kind", aggregated_field="price"
    )

    assert result == {"a": 3, None: None}
    (query,) = fake.sent
    assert "AS group_key" in query and "max(n." in query


@pytest.mark.parametrize("connection", [Neo4jConnection, FalkordbConnection])
def test_a_cypher_backend_returns_sorted_unique_values(connection) -> None:
    fake = _Cypher([{"agg_value": ["b", "a"]}])

    result = connection.aggregate(
        cast(Any, fake), "Item", SORTED_UNIQUE, aggregated_field="name"
    )

    assert result == ["a", "b"]
    assert "collect(DISTINCT n." in fake.sent[0]


def test_memgraph_groups_every_function() -> None:
    sent: list[str] = []

    class _Cursor:
        def execute(self, query: str, params: Any = None) -> None:
            sent.append(query)

        def fetchall(self) -> list:
            return [("a", 1.5), ("b", 4)]

        def close(self) -> None:
            return None

    fake = _Cypher([])
    cast(Any, fake).conn = type("C", (), {"cursor": lambda _self: _Cursor()})()

    result = MemgraphConnection.aggregate(
        cast(Any, fake), "Item", AVERAGE, discriminant="kind", aggregated_field="p"
    )

    assert result == {"a": 1.5, "b": 4.0}
    assert "avg(n." in sent[0]


def test_nebula_counts_into_an_int() -> None:
    rows = [{"agg_value": 7}]
    fake = type(
        "N",
        (),
        {
            "config": type("C", (), {"is_v3": True})(),
            "_render_filter": lambda _self, filters, doc_name: "",
            "_execute": lambda _self, q: type(
                "RS", (), {"rows_as_dicts": lambda _s: rows}
            )(),
        },
    )()

    assert NebulaConnection.aggregate(cast(Any, fake), "Person", COUNT) == 7


def test_arango_groups_into_a_mapping(monkeypatch) -> None:
    sent: list[tuple[str, dict[str, Any]]] = []

    def rows(_conn: Any, query: str, bind_vars: Any, _timeout: Any) -> list:
        sent.append((query, dict(bind_vars or {})))
        return [["a", 2], ["b", 4.5]]

    monkeypatch.setattr(arango_module, "_aql_rows", rows)

    result = ArangoConnection.aggregate(
        cast(Any, object()),
        "samples",
        AVERAGE,
        discriminant="class",
        aggregated_field="value",
    )

    assert result == {"a": 2.0, "b": 4.5}
    ((query, bound),) = sent
    assert "COLLECT group_key = doc[@" in query
    assert "AGGREGATE agg_value = AVERAGE(doc[@" in query
    assert {"class", "value"} <= set(bound.values())


def test_arango_counts_into_an_int(monkeypatch) -> None:
    monkeypatch.setattr(arango_module, "_aql_rows", lambda *_: [[None, 5]])

    result = ArangoConnection.aggregate(cast(Any, object()), "samples", COUNT)

    assert result == 5


class _Pg:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.sent: list[str] = []
        self.config = type("C", (), {"schema_name": "g"})()

    def read(self, query: str, params: Any = None, **_: Any) -> list:
        self.sent.append(query)
        return self.rows


def test_postgres_returns_sorted_unique_values() -> None:
    fake = _Pg([{"agg_value": ["b", None, "a"]}])

    result = PostgresConnection.aggregate(
        cast(Any, fake), "person", SORTED_UNIQUE, aggregated_field="name"
    )

    assert result == ["a", "b"]
    assert 'ARRAY_AGG(DISTINCT "name")' in fake.sent[0]


def test_postgres_groups_into_a_mapping() -> None:
    fake = _Pg([{"group_key": "x", "agg_value": 2}])

    result = PostgresConnection.aggregate(
        cast(Any, fake), "person", COUNT, discriminant="city"
    )

    assert result == {"x": 2}
    assert "COUNT(*)" in fake.sent[0] and 'GROUP BY "city"' in fake.sent[0]


def test_tigergraph_counts_and_refuses_the_rest() -> None:
    ops = cast(Any, object.__new__(TigerGraphDataOps))
    ops._conn = type("C", (), {"_get_vertex_count": lambda _self, name: 4})()

    assert TigerGraphDataOps.aggregate(ops, "Person", COUNT) == 4
    with pytest.raises(NotImplementedError):
        TigerGraphDataOps.aggregate(ops, "Person", MAX, aggregated_field="age")
    with pytest.raises(NotImplementedError):
        TigerGraphDataOps.aggregate(ops, "Person", COUNT, discriminant="city")
