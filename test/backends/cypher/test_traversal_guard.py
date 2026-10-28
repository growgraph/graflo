"""Cypher traversals: an edge filter and a read guard hold on every element of a path.

No database: a recording ``run`` stands in for the driver, so these pin the
predicates a traversal sends and that their values travel as parameters.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, cast

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.graph_types import EdgeDirection
from graflo.architecture.query import ReservedFieldError, TraverseQuery
from graflo.architecture.schema import Schema
from graflo.db.arango.conn import ArangoConnection
from graflo.db.cypher.traversal import ANCHOR_PARAM, cypher_graph_neighbors
from graflo.db.falkordb.conn import FalkordbConnection
from graflo.db.memgraph.conn import MemgraphConnection
from graflo.db.neo4j.conn import Neo4jConnection
from graflo.db.postgres.conn import PostgresConnection
from graflo.filter.onto import ComparisonOperator, FilterExpression
from graflo.onto import DBType


def zones_guard(zones: set[int]) -> FilterExpression:
    """A read guard: the element shares a zone with *zones*."""
    return FilterExpression(
        kind="leaf",
        field="access_zones",
        cmp_operator=ComparisonOperator.ANY_IN,
        value=[sorted(zones)],
    )


SCHEMA: dict[str, Any] = {
    "metadata": {"name": "support", "version": "1.0.0"},
    "graph": {
        "vertex_config": {
            "vertices": [
                {
                    "name": name,
                    "properties": [
                        {"name": "key", "type": "STRING"},
                        {
                            "name": "access_zones",
                            "type": "LIST",
                            "item_type": "INT",
                            "reserved": True,
                        },
                    ],
                    "identity": ["key"],
                }
                for name in ("Ticket", "Product")
            ]
        },
        "edge_config": {
            "edges": [
                {
                    "source": "Ticket",
                    "target": "Product",
                    "relation": "about",
                    "properties": [{"name": "weight", "type": "INT"}],
                }
            ]
        },
    },
}


class _Conn:
    flavor = DBType.NEO4J

    def vertex_address(
        self, doc: dict[str, Any], identity_fields: Sequence[str]
    ) -> str | None:
        for field in identity_fields:
            if doc.get(field) is not None:
                return str(doc[field])
        return None


class _Recorder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def __call__(self, query: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        self.calls.append((query, params))
        return []


@pytest.fixture
def schema() -> Schema:
    manifest = GraphManifest.model_validate({"schema": SCHEMA})
    manifest.finish_init()
    return manifest.require_schema()


def _walk(schema: Schema, run: _Recorder, **kwargs: Any) -> None:
    params: dict[str, Any] = {
        "vertex_type": "Ticket",
        "key": "SUP-42",
        "hops": 2,
        "direction": EdgeDirection.ANY,
        "edge_types": None,
        "limit": None,
    }
    params.update(kwargs)
    cypher_graph_neighbors(cast(Any, _Conn()), schema=schema, run=run, **params)


def test_a_guard_holds_on_every_node_and_relationship_of_the_path(
    schema: Schema,
) -> None:
    run = _Recorder()
    _walk(schema, run, guard=zones_guard({11, 12}))

    assert run.calls
    for query, params in run.calls:
        assert (
            "all(_gn IN nodes(path) WHERE any(_any_in IN coalesce(_gn.access_zones, [])"
            in query
        )
        assert (
            "all(_gr IN relationships(path) WHERE any(_any_in IN "
            "coalesce(_gr.access_zones, [])" in query
        )
        assert "[11, 12]" not in query
        assert params[ANCHOR_PARAM] == "SUP-42"
        assert [11, 12] in params.values()


def test_an_edge_filter_holds_on_every_relationship(schema: Schema) -> None:
    run = _Recorder()
    _walk(schema, run, filters=[">", 3, "weight"])

    for query, params in run.calls:
        assert "all(_gr IN relationships(path) WHERE _gr.weight > $f0)" in query
        assert params["f0"] == 3


def test_one_hop_patterns_carry_the_guard_too(schema: Schema) -> None:
    run = _Recorder()
    _walk(schema, run, hops=1, guard=zones_guard({1}))

    assert run.calls
    assert all("all(_gn IN nodes(path)" in query for query, _ in run.calls)


def test_an_unrenderable_guard_fails_the_read(schema: Schema) -> None:
    run = _Recorder()
    bad = zones_guard({1}).model_copy(update={"field": None})

    with pytest.raises(ValueError, match="ANY_IN needs a field"):
        _walk(schema, run, guard=bad)
    assert run.calls == []


@pytest.mark.parametrize(
    "conn_cls", [ArangoConnection, PostgresConnection, MemgraphConnection]
)
def test_a_backend_without_guard_support_refuses_a_guard(
    schema: Schema, conn_cls: type
) -> None:
    conn = cast(Any, object.__new__(conn_cls))

    with pytest.raises(NotImplementedError, match="read guard"):
        conn.graph_neighbors("Ticket", "SUP-42", schema=schema, guard=zones_guard({1}))
    with pytest.raises(NotImplementedError, match="read guard"):
        conn.resolve_vertices(
            "Ticket", [{"key": "SUP-42"}], ("key",), ("key",), guard=zones_guard({1})
        )


def test_neo4j_declares_the_guard_and_the_union() -> None:
    assert Neo4jConnection.supports_read_guard
    assert Neo4jConnection.supports_union_merge
    assert not MemgraphConnection.supports_read_guard


def test_falkordb_declares_the_guard_and_the_union() -> None:
    assert FalkordbConnection.supports_read_guard
    assert FalkordbConnection.supports_graph_export
    assert FalkordbConnection.supports_union_merge


class _Falkor(FalkordbConnection):
    """A FalkorDB connection with no server: statements are recorded, rows empty."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, dict[str, Any]]] = []

    def execute(self, query: str, **params: Any) -> Any:
        self.sent.append((query, params))
        return type("R", (), {"result_set": []})()


def test_falkordb_guards_every_node_and_relationship_of_a_walk(
    schema: Schema,
) -> None:
    conn = _Falkor()

    conn.graph_neighbors(
        "Ticket", "SUP-42", hops=2, schema=schema, guard=zones_guard({11, 12})
    )

    walks = [(q, p) for q, p in conn.sent if "path" in q]
    assert walks
    for query, params in walks:
        assert "all(_gn IN nodes(path) WHERE any(_any_in IN" in query
        assert "all(_gr IN relationships(path) WHERE any(_any_in IN" in query
        assert "[11, 12]" not in query
        assert [11, 12] in params.values()


def test_falkordb_guards_a_lookup_by_key(schema: Schema) -> None:
    conn = _Falkor()

    conn.resolve_vertices(
        "Ticket", [{"key": "SUP-42"}], ("key",), ("key",), guard=zones_guard({3})
    )

    ((query, params),) = conn.sent
    assert "any(_any_in IN coalesce(n.access_zones, [])" in query
    assert [3] in params.values()
    assert "SUP-42" in params.values()


def test_falkordb_unions_a_list_field_instead_of_replacing_it() -> None:
    conn = _Falkor()

    conn.upsert_docs_batch(
        [{"key": "SUP-42", "title": "t", "access_zones": [3, 4]}],
        "Ticket",
        ["key"],
        union_fields=("access_zones",),
    )

    ((query, params),) = conn.sent
    assert "UNWIND $data AS row" in query
    assert "MERGE (n:Ticket { key: row.p.key })" in query
    assert "SET n += row.p" in query
    assert "n.`access_zones` = reduce(acc = [], x IN [y IN" in query
    assert (
        "coalesce(n.`access_zones`, []) + coalesce(row.u.`access_zones`, [])" in query
    )
    assert params == {
        "data": [{"p": {"key": "SUP-42", "title": "t"}, "u": {"access_zones": [3, 4]}}]
    }


def test_a_traversal_filter_on_a_reserved_field_is_refused(schema: Schema) -> None:
    conn = cast(Any, Neo4jConnection.__new__(Neo4jConnection))

    with pytest.raises(ReservedFieldError):
        conn.graph_neighbors(
            "Ticket",
            "SUP-42",
            schema=schema,
            filters={"field": "access_zones", "cmp_operator": "==", "value": 1},
        )
    with pytest.raises(ReservedFieldError):
        conn.traverse(
            TraverseQuery(
                seeds=[{"vertex_type": "Ticket", "key": "SUP-42"}],
                filters={"field": "access_zones", "cmp_operator": "==", "value": 1},
            ),
            schema=schema,
        )


def test_a_failed_guarded_read_raises_instead_of_answering_less(
    schema: Schema,
) -> None:
    def failing(query: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        raise RuntimeError("type mismatch")

    with pytest.raises(RuntimeError, match="type mismatch"):
        cypher_graph_neighbors(
            cast(Any, _Conn()),
            schema=schema,
            run=failing,
            vertex_type="Ticket",
            key="SUP-42",
            hops=1,
            direction=EdgeDirection.ANY,
            edge_types=None,
            limit=None,
            guard=zones_guard({1}),
        )


def test_a_mapping_anchor_key_on_a_reserved_field_is_refused(schema: Schema) -> None:
    conn = cast(Any, object.__new__(Neo4jConnection))

    with pytest.raises(ReservedFieldError):
        conn.graph_neighbors("Ticket", {"access_zones": 1}, schema=schema)


def test_tigergraph_refuses_a_guarded_lookup() -> None:
    from graflo.db.tigergraph.conn import TigerGraphConnection

    conn = cast(Any, object.__new__(TigerGraphConnection))

    with pytest.raises(NotImplementedError, match="read guard"):
        conn.resolve_vertices(
            "Ticket", [{"key": "x"}], ("key",), ("key",), guard=zones_guard({1})
        )
