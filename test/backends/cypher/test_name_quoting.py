"""Schema names reach Cypher text only as single tokens.

A vertex storage name, a relationship type or a property key is interpolated
into the query, so a hostile one must render as one quoted token on every
Cypher path. No database: recording stand-ins capture the text sent.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any, cast

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.graph_types import EdgeDirection, Index
from graflo.architecture.schema import Schema
from graflo.db.cypher import cypher_name, cypher_rel_pattern
from graflo.db.cypher.delete import delete_nodes_query, delete_relationships_query
from graflo.db.cypher.introspection import _edge_property_query
from graflo.db.cypher.rel_merge import rel_absent_upsert_clause
from graflo.db.cypher.traversal import cypher_neighbors_query
from graflo.db.falkordb.conn import FalkordbConnection
from graflo.db.memgraph.conn import MemgraphConnection, edge_property_index_query
from graflo.db.neo4j.conn import Neo4jConnection
from graflo.filter.onto import ComparisonOperator, FilterExpression
from graflo.onto import AggregationType

#: Closes the label and adds an unguarded branch, if written unquoted.
LABEL = "Ticket) RETURN n UNION MATCH (n:Ticket"
#: Closes a backtick-quoted relationship type and adds a write.
RELATION = "about`]->() SET pwned.x = 1 //"
#: Closes a property access and adds a branch.
FIELD = "key} UNION MATCH (m) RETURN m //"

MARKERS = ("UNION", "pwned")


def quoted(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


#: A backtick-quoted token or a single-quoted string literal.
_QUOTED = re.compile(r"`(?:[^`]|``)*`|'(?:[^'\\]|\\.)*'")


def assert_tokens(query: str, *names: str) -> None:
    """Every *name* is one quoted token, and no hostile text is left outside one."""
    for name in names:
        assert quoted(name) in query, (name, query)
    rest = _QUOTED.sub("", query)
    for marker in MARKERS:
        assert marker not in rest, query


def assert_sent_safely(sent: list[str], minimum: int) -> None:
    """Each statement naming a hostile name holds it only as quoted tokens."""
    naming = [q for q in sent if any(quoted(n) in q for n in (LABEL, RELATION, FIELD))]
    assert len(naming) >= minimum, sent
    for query in sent:
        assert_tokens(
            query, *[n for n in (LABEL, RELATION, FIELD) if quoted(n) in query]
        )


def zones_guard() -> FilterExpression:
    return FilterExpression(
        kind="leaf",
        field="access_zones",
        cmp_operator=ComparisonOperator.ANY_IN,
        value=[[1]],
    )


# --- the helper ------------------------------------------------------------


def test_an_identifier_stays_bare() -> None:
    assert cypher_name("Ticket") == "Ticket"
    assert cypher_name("_k9") == "_k9"


@pytest.mark.parametrize(
    ("name", "rendered"),
    [
        ("work order", "`work order`"),
        ("9lives", "`9lives`"),
        ("a`b", "`a``b`"),
        ("Größe", "`Größe`"),
    ],
)
def test_any_other_name_is_one_quoted_token(name: str, rendered: str) -> None:
    assert cypher_name(name) == rendered


def test_quoted_forces_backticks() -> None:
    assert cypher_name("Ticket", quoted=True) == "`Ticket`"


def test_an_empty_name_is_refused() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        cypher_name("")


# --- shared builders -------------------------------------------------------


def test_relationship_pattern() -> None:
    assert cypher_rel_pattern(RELATION) == f"-[r:{quoted(RELATION)}]->"
    assert (
        cypher_rel_pattern(["knows", RELATION], max_hops=2)
        == f"-[r:knows|{quoted(RELATION)}*1..2]->"
    )


def test_neighbors_query() -> None:
    query = cypher_neighbors_query(
        anchor_label=LABEL,
        anchor_key_field=FIELD,
        edge_type=(RELATION, "plain"),
        far_label=LABEL,
        direction=EdgeDirection.ANY,
        hops=2,
        limit=None,
    )
    assert_tokens(query, LABEL, RELATION, FIELD)
    assert f"(far:{quoted(LABEL)})" in query


def test_delete_queries() -> None:
    assert_tokens(delete_nodes_query(LABEL, [FIELD]), LABEL, FIELD)
    assert_tokens(
        delete_relationships_query(LABEL, LABEL, RELATION, [FIELD], ["id"]),
        LABEL,
        RELATION,
        FIELD,
    )


def test_absent_upsert_clause() -> None:
    clause = rel_absent_upsert_clause(
        RELATION, ["since"], ["since"], props_expr="row[2]"
    )
    assert_tokens(clause, RELATION)
    assert clause.count(quoted(RELATION)) == 2


def test_introspection_query() -> None:
    assert_tokens(_edge_property_query(LABEL, LABEL, RELATION, 5), LABEL, RELATION)


# --- a schema with hostile storage names -----------------------------------


@pytest.fixture
def schema() -> Schema:
    vertex = {
        "properties": [
            {"name": "key", "type": "STRING"},
            {"name": "access_zones", "type": "LIST", "item_type": "INT"},
        ],
        "identity": ["key"],
    }
    manifest = GraphManifest.model_validate(
        {
            "schema": {
                "metadata": {"name": "support", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {"name": "Ticket", **vertex},
                            {"name": "Product", **vertex},
                        ]
                    },
                    "edge_config": {
                        "edges": [
                            {
                                "source": "Ticket",
                                "target": "Product",
                                "relation": RELATION,
                            }
                        ]
                    },
                },
                "db_profile": {"vertex_storage_names": {"Ticket": LABEL}},
            }
        }
    )
    manifest.finish_init()
    return manifest.require_schema()


# --- Neo4j -----------------------------------------------------------------


class _Cursor:
    def __init__(self, rows: list[Any] | None = None) -> None:
        self.rows = rows or []

    def data(self) -> list[Any]:
        return self.rows

    def consume(self) -> None:
        return None


class _Tx:
    def __init__(self, sent: list[str]) -> None:
        self.sent = sent

    def run(self, query: str, **_: Any) -> _Cursor:
        self.sent.append(query)
        return _Cursor()


class _Session:
    def __init__(self, sent: list[str]) -> None:
        self.sent = sent

    def run(self, query: str, **_: Any) -> _Cursor:
        self.sent.append(query)
        return _Cursor()

    def execute_write(self, fn: Callable[[_Tx], Any]) -> Any:
        return fn(_Tx(self.sent))


@pytest.fixture
def neo4j() -> tuple[Neo4jConnection, list[str]]:
    sent: list[str] = []
    conn = cast(Any, object.__new__(Neo4jConnection))
    conn.conn = _Session(sent)
    return conn, sent


def test_neo4j_paths(neo4j: tuple[Neo4jConnection, list[str]]) -> None:
    conn, sent = neo4j
    conn.fetch_docs(LABEL, filters=["==", "x", "key"], return_keys=[FIELD])
    conn.upsert_docs_batch([{"key": "a"}], LABEL, [FIELD])
    conn.upsert_docs_batch([{"key": "a"}], LABEL, [FIELD], union_fields=[FIELD])
    for merge in ((), (FIELD,)):
        conn.insert_edges_batch(
            [[{"key": "a"}, {"key": "b"}, {FIELD: 1}]],
            LABEL,
            LABEL,
            RELATION,
            (FIELD,),
            (FIELD,),
            relationship_merge_properties=merge,
        )
    conn.insert_edges_batch(
        [[{"key": "a"}, {"key": "b"}, {}]],
        LABEL,
        LABEL,
        RELATION,
        ("key",),
        ("key",),
        relationship_merge_properties=("since",),
    )
    conn.fetch_edges(LABEL, "a", edge_type=RELATION, to_type=LABEL, return_keys=[FIELD])
    conn.delete_graph_structure(vertex_types=[LABEL])
    conn.delete_vertices(LABEL, [{FIELD: "a"}], (FIELD,))
    conn.delete_edges(LABEL, LABEL, RELATION, [({"k": 1}, {"k": 2})], ("k",), ("k",))
    conn.aggregate(LABEL, AggregationType.COUNT, discriminant=FIELD)
    conn.aggregate(LABEL, AggregationType.MAX, aggregated_field=FIELD)
    conn.fetch_all_docs(LABEL)
    conn.fetch_all_edges(LABEL, LABEL, RELATION)
    conn._add_index(LABEL, Index(fields=[FIELD]))
    conn._add_index(RELATION, Index(fields=[FIELD], unique=True), is_vertex_index=False)

    assert_sent_safely(sent, 16)


def test_neo4j_guarded_lookup_quotes_the_label(
    neo4j: tuple[Neo4jConnection, list[str]],
) -> None:
    conn, sent = neo4j
    conn.resolve_vertices(
        LABEL, [{"key": "SUP-1"}], ("key",), ("key",), guard=zones_guard()
    )

    (query,) = sent
    assert_tokens(query, LABEL)
    assert f"MATCH (n:{quoted(LABEL)})" in query
    assert "access_zones" in query


def test_neo4j_guarded_traversal_quotes_storage_names(
    neo4j: tuple[Neo4jConnection, list[str]], schema: Schema
) -> None:
    conn, sent = neo4j
    for hops in (1, 2):
        conn.graph_neighbors(
            "Ticket",
            "SUP-1",
            hops=hops,
            direction=EdgeDirection.ANY,
            schema=schema,
            guard=zones_guard(),
        )

    assert len(sent) == 2
    for query in sent:
        assert_tokens(query, LABEL, RELATION)
        assert "all(_gn IN nodes(path)" in query


def test_neo4j_schema_indexes_quote_storage_names(
    neo4j: tuple[Neo4jConnection, list[str]], schema: Schema
) -> None:
    conn, sent = neo4j
    conn.define_vertex_indexes(schema.core_schema.vertex_config, schema)

    assert any(quoted(LABEL) in query for query in sent)
    for query in sent:
        if "Ticket" in query:
            assert_tokens(query, LABEL)


def test_neo4j_create_database_quotes_the_name(
    neo4j: tuple[Neo4jConnection, list[str]],
) -> None:
    conn, sent = neo4j
    conn.create_database("db` DROP DATABASE neo4j //")

    assert sent == ["CREATE DATABASE `db`` DROP DATABASE neo4j //`"]


# --- Memgraph --------------------------------------------------------------


class _MgCursor:
    description: list[Any] = []

    def __init__(self, sent: list[str]) -> None:
        self.sent = sent

    def execute(self, query: str, params: Any = None) -> None:
        self.sent.append(query)

    def fetchall(self) -> list[Any]:
        return []

    def close(self) -> None:
        return None


class _MgConn:
    def __init__(self, sent: list[str]) -> None:
        self.sent = sent

    def cursor(self) -> _MgCursor:
        return _MgCursor(self.sent)


def test_memgraph_paths() -> None:
    sent: list[str] = []
    conn = cast(Any, object.__new__(MemgraphConnection))
    conn.conn = _MgConn(sent)
    conn._merge_indexed = set()

    conn.fetch_docs(LABEL, filters=["==", "x", "key"], return_keys=[FIELD])
    conn.upsert_docs_batch([{"key": "a", "k2": 1}], LABEL, ["key"])
    for merge in ((), ("since",)):
        conn.insert_edges_batch(
            [[{"key": "a"}, {"key": "b"}, {"since": 1}]],
            LABEL,
            LABEL,
            RELATION,
            ("key",),
            ("key",),
            relationship_merge_properties=merge,
        )
    conn.insert_edges_batch(
        [[{"key": "a"}, {"key": "b"}, {}]],
        LABEL,
        LABEL,
        RELATION,
        ("key",),
        ("key",),
        relationship_merge_properties=("since",),
    )
    conn.fetch_edges(
        LABEL, "a", edge_type=RELATION, to_type=LABEL, return_keys=[FIELD, "from_x"]
    )
    conn.delete_graph_structure(vertex_types=[LABEL])
    conn.aggregate(LABEL, AggregationType.COUNT, discriminant=FIELD)
    conn.aggregate(LABEL, AggregationType.AVERAGE, aggregated_field=FIELD)

    assert_sent_safely(sent, 9)

    assert_tokens(edge_property_index_query(RELATION, FIELD), RELATION, FIELD)


def test_memgraph_aggregate_without_a_field_is_refused() -> None:
    conn = cast(Any, object.__new__(MemgraphConnection))
    conn.conn = _MgConn([])

    with pytest.raises(ValueError, match="aggregated_field is required"):
        conn.aggregate(LABEL, AggregationType.MAX)


# --- FalkorDB --------------------------------------------------------------


class _FalkorResult:
    result_set: list[Any] = []


def test_falkordb_paths() -> None:
    sent: list[str] = []
    conn = cast(Any, object.__new__(FalkordbConnection))

    def execute(query: str, **_: Any) -> _FalkorResult:
        sent.append(query)
        return _FalkorResult()

    conn.execute = execute
    conn.client = object()

    conn.fetch_docs(LABEL, filters=["==", "x", "key"], return_keys=[FIELD])
    conn.upsert_docs_batch([{"key": "a"}], LABEL, ["key"])
    for merge in ((), ("since",)):
        conn.insert_edges_batch(
            [[{"key": "a"}, {"key": "b"}, {"since": 1}]],
            LABEL,
            LABEL,
            RELATION,
            ("key",),
            ("key",),
            relationship_merge_properties=merge,
        )
    conn.insert_edges_batch(
        [[{"key": "a"}, {"key": "b"}, {}]],
        LABEL,
        LABEL,
        RELATION,
        ("key",),
        ("key",),
        relationship_merge_properties=("since",),
    )
    conn.fetch_edges(LABEL, "a", edge_type=RELATION, to_type=LABEL, return_keys=[FIELD])
    conn.delete_graph_structure(vertex_types=[LABEL])
    conn.aggregate(LABEL, AggregationType.COUNT, discriminant=FIELD)
    conn.aggregate(LABEL, AggregationType.SORTED_UNIQUE, aggregated_field=FIELD)
    conn._add_index(LABEL, Index(fields=[FIELD]))
    conn._add_index(RELATION, Index(fields=[FIELD]), is_vertex_index=False)

    assert_sent_safely(sent, 10)


def test_memgraph_schema_indexes_quote_storage_names(schema: Schema) -> None:
    sent: list[str] = []
    conn = cast(Any, object.__new__(MemgraphConnection))
    conn.conn = _MgConn(sent)
    conn.define_vertex_indexes(schema.core_schema.vertex_config, schema)

    assert f"CREATE INDEX ON :{quoted(LABEL)}(key)" in sent
    assert f"CREATE CONSTRAINT ON (n:{quoted(LABEL)}) ASSERT n.key IS UNIQUE" in sent
    assert_sent_safely(sent, 2)


@pytest.mark.parametrize("word", ["Order", "end", "MATCH", "Return", "null"])
def test_a_reserved_word_is_quoted_although_it_is_an_identifier(word: str) -> None:
    assert cypher_name(word) == f"`{word}`"


def test_an_ordinary_identifier_stays_bare() -> None:
    assert cypher_name("Ticket") == "Ticket"
    assert cypher_name("ordered_at") == "ordered_at"
