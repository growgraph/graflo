"""Time limits on bounded reads: what each backend sends, and what it raises.

No database: each backend gets a fake driver that records the call, so these pin
that a limit reaches the driver as the database's own timeout, that no limit
sends nothing, and that the driver's timeout error becomes ``QueryTimeoutError``.
"""

from __future__ import annotations

import pickle
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any, Self, cast

import pytest
from arango.exceptions import AQLQueryExecuteError
from neo4j import Query
from neo4j.exceptions import Neo4jError
from psycopg2.extensions import QueryCanceledError
from redis.exceptions import ResponseError

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.graph_types import EdgeDirection
from graflo.architecture.query import Deadline, QueryTimeoutError
from graflo.architecture.query import timeout as timeout_module
from graflo.architecture.schema import Schema
from graflo.db.arango.conn import ArangoConnection
from graflo.db.conn import Connection, ConnectionCapability
from graflo.db.falkordb.conn import FalkordbConnection
from graflo.db.manager import ConnectionManager
from graflo.db.neo4j.conn import Neo4jConnection
from graflo.db.postgres.conn import PostgresConnection
from graflo.db.traversal import bfs_neighbors
from graflo.onto import AggregationType, DBType

# -- the contract ---------------------------------------------------------------


def test_the_backends_whose_database_enforces_the_limit() -> None:
    assert set(
        ConnectionManager.flavors_supporting(ConnectionCapability.QUERY_TIMEOUT)
    ) == {DBType.ARANGO, DBType.NEO4J, DBType.FALKORDB, DBType.POSTGRES}
    assert ConnectionCapability.QUERY_TIMEOUT.value == "supports_query_timeout"
    assert ConnectionCapability.QUERY_TIMEOUT.label == (
        "a time limit on each read, enforced by the database"
    )


def test_the_error_is_a_timeout_naming_its_limit() -> None:
    error = QueryTimeoutError(2.5)

    assert isinstance(error, TimeoutError)
    assert error.timeout_s == 2.5
    assert "2.5 s" in str(error)
    assert pickle.loads(pickle.dumps(error)).timeout_s == 2.5


@pytest.mark.parametrize("bad", [0, -1.0, float("inf"), float("nan")])
def test_a_limit_that_is_no_limit_is_refused(bad: float) -> None:
    # Several drivers read zero as "run forever".
    with pytest.raises(ValueError, match="timeout_s"):
        Deadline(bad)


class _Clock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def test_a_deadline_hands_each_read_what_is_left() -> None:
    clock = _Clock()
    deadline = Deadline(10.0, clock=clock)

    assert deadline.remaining() == 10.0
    clock.now += 4.0
    assert deadline.kwargs() == {"timeout_s": 6.0}
    clock.now += 6.0
    with pytest.raises(QueryTimeoutError) as raised:
        deadline.remaining()
    assert raised.value.timeout_s == 10.0


def test_no_deadline_is_no_limit() -> None:
    deadline = Deadline(None)

    assert not deadline.bounded
    assert deadline.remaining() is None
    assert deadline.kwargs() == {}


def test_a_read_timing_out_on_the_remainder_names_the_whole_limit() -> None:
    deadline = Deadline(10.0)

    with pytest.raises(QueryTimeoutError) as raised, deadline.spent():
        raise QueryTimeoutError(3.0)
    assert raised.value.timeout_s == 10.0
    assert isinstance(raised.value.__cause__, QueryTimeoutError)


def test_a_backend_without_enforcement_gets_no_deadline() -> None:
    fake = SimpleNamespace(supports_query_timeout=False)

    assert not Connection.query_deadline(cast(Any, fake), 5.0).bounded
    fake.supports_query_timeout = True
    assert Connection.query_deadline(cast(Any, fake), 5.0).timeout_s == 5.0


# -- Neo4j: a transaction timeout -----------------------------------------------


class _Result:
    def data(self) -> list[dict[str, Any]]:
        return []


class _Session:
    def __init__(self, error: Exception | None = None) -> None:
        self.calls: list[tuple[Any, Any, dict[str, Any]]] = []
        self.error = error

    def run(self, query: Any, parameters: Any = None, **kwargs: Any) -> _Result:
        self.calls.append((query, parameters, kwargs))
        if self.error is not None:
            raise self.error
        return _Result()


def _neo4j(session: _Session) -> Any:
    conn = cast(Any, object.__new__(Neo4jConnection))
    conn.conn = session
    return conn


_NEO4J_READS: list[tuple[str, dict[str, Any]]] = [
    ("fetch_docs", {"class_name": "Person"}),
    ("fetch_edges", {"from_type": "Person", "from_id": "p1", "edge_type": "KNOWS"}),
    ("fetch_all_docs", {"class_name": "Person"}),
    (
        "aggregate",
        {"class_name": "Person", "aggregation_function": AggregationType.COUNT},
    ),
]


@pytest.mark.parametrize(("method", "args"), _NEO4J_READS)
def test_neo4j_runs_a_read_as_a_transaction_with_a_timeout(
    method: str, args: dict[str, Any]
) -> None:
    session = _Session()

    getattr(Neo4jConnection, method)(_neo4j(session), **args, timeout_s=2.5)

    (query, _parameters, _kwargs) = session.calls[0]
    assert isinstance(query, Query)
    assert query.timeout == 2.5


@pytest.mark.parametrize(("method", "args"), _NEO4J_READS)
def test_neo4j_sends_no_timeout_without_a_limit(
    method: str, args: dict[str, Any]
) -> None:
    session = _Session()

    getattr(Neo4jConnection, method)(_neo4j(session), **args)

    (query, _parameters, _kwargs) = session.calls[0]
    assert isinstance(query, str)


@pytest.mark.parametrize(
    "code",
    [
        "Neo.ClientError.Transaction.TransactionTimedOutClientConfiguration",
        "Neo.ClientError.Transaction.TransactionTimedOut",
    ],
)
def test_neo4j_reports_a_timed_out_transaction_as_a_query_timeout(code: str) -> None:
    error = Neo4jError._hydrate_neo4j(code=code, message="terminated")
    conn = _neo4j(_Session(error))

    with pytest.raises(QueryTimeoutError) as raised:
        Neo4jConnection.fetch_docs(conn, "Person", timeout_s=2.5)
    assert raised.value.timeout_s == 2.5
    assert raised.value.__cause__ is error


def test_neo4j_leaves_other_errors_alone() -> None:
    error = Neo4jError._hydrate_neo4j(
        code="Neo.ClientError.Statement.SyntaxError", message="bad"
    )
    conn = _neo4j(_Session(error))

    with pytest.raises(Neo4jError):
        Neo4jConnection.fetch_docs(conn, "Person", timeout_s=2.5)


# -- FalkorDB: GRAPH.QUERY ... TIMEOUT --------------------------------------------


class _FalkorGraph:
    def __init__(self, error: Exception | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self.error = error

    def query(self, query: str, params: Any = None, **kwargs: Any) -> Any:
        self.calls.append({"query": query, "params": params, **kwargs})
        if self.error is not None:
            raise self.error
        return SimpleNamespace(result_set=[])


def _falkordb(graph: _FalkorGraph) -> Any:
    conn = cast(Any, object.__new__(FalkordbConnection))
    conn.graph = graph
    return conn


_FALKOR_READS: list[tuple[str, dict[str, Any]]] = [
    ("fetch_docs", {"class_name": "Person"}),
    ("fetch_edges", {"from_type": "Person", "from_id": "p1", "edge_type": "KNOWS"}),
    ("fetch_all_docs", {"class_name": "Person"}),
    (
        "aggregate",
        {"class_name": "Person", "aggregation_function": AggregationType.COUNT},
    ),
]


@pytest.mark.parametrize(("method", "args"), _FALKOR_READS)
def test_falkordb_passes_the_limit_in_milliseconds(
    method: str, args: dict[str, Any]
) -> None:
    graph = _FalkorGraph()

    getattr(FalkordbConnection, method)(_falkordb(graph), **args, timeout_s=2.5)

    assert graph.calls[0]["timeout"] == 2500


@pytest.mark.parametrize(("method", "args"), _FALKOR_READS)
def test_falkordb_sends_no_timeout_without_a_limit(
    method: str, args: dict[str, Any]
) -> None:
    graph = _FalkorGraph()

    getattr(FalkordbConnection, method)(_falkordb(graph), **args)

    assert "timeout" not in graph.calls[0]


def test_falkordb_rounds_a_sub_millisecond_limit_up() -> None:
    graph = _FalkorGraph()

    FalkordbConnection.fetch_docs(_falkordb(graph), "Person", timeout_s=0.0001)

    assert graph.calls[0]["timeout"] == 1


def test_falkordb_reports_a_timed_out_query_as_a_query_timeout() -> None:
    conn = _falkordb(_FalkorGraph(ResponseError("Query timed out")))

    with pytest.raises(QueryTimeoutError) as raised:
        FalkordbConnection.fetch_docs(conn, "Person", timeout_s=2.5)
    assert raised.value.timeout_s == 2.5


# -- ArangoDB: AQL maxRuntime ---------------------------------------------------


class _Aql:
    def __init__(self, error: Exception | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self.error = error

    def execute(self, query: str, bind_vars: Any = None, **kwargs: Any) -> Iterator:
        self.calls.append({"query": query, "bind_vars": bind_vars, **kwargs})
        if self.error is not None:
            raise self.error
        return iter([])


def _arango(aql: _Aql) -> Any:
    conn = cast(Any, object.__new__(ArangoConnection))
    conn.conn = SimpleNamespace(aql=aql)
    return conn


_ARANGO_READS: list[tuple[str, dict[str, Any]]] = [
    ("fetch_docs", {"class_name": "Person"}),
    ("fetch_edges", {"from_type": "Person", "from_id": "p1", "edge_type": "knows"}),
    ("fetch_all_docs", {"class_name": "Person"}),
    (
        "aggregate",
        {"class_name": "Person", "aggregation_function": AggregationType.COUNT},
    ),
]


@pytest.mark.parametrize(("method", "args"), _ARANGO_READS)
def test_arango_passes_the_limit_as_max_runtime(
    method: str, args: dict[str, Any]
) -> None:
    aql = _Aql()

    getattr(ArangoConnection, method)(_arango(aql), **args, timeout_s=2.5)

    assert aql.calls[0]["max_runtime"] == 2.5


@pytest.mark.parametrize(("method", "args"), _ARANGO_READS)
def test_arango_sends_no_max_runtime_without_a_limit(
    method: str, args: dict[str, Any]
) -> None:
    aql = _Aql()

    getattr(ArangoConnection, method)(_arango(aql), **args)

    assert "max_runtime" not in aql.calls[0]


def _killed() -> AQLQueryExecuteError:
    error = AQLQueryExecuteError.__new__(AQLQueryExecuteError)
    error.error_code = 1500
    return error


def test_arango_reports_a_killed_query_as_a_query_timeout() -> None:
    conn = _arango(_Aql(_killed()))

    with pytest.raises(QueryTimeoutError) as raised:
        ArangoConnection.fetch_docs(conn, "Person", timeout_s=2.5)
    assert raised.value.timeout_s == 2.5


# -- PostgreSQL: SET LOCAL statement_timeout ----------------------------------


class _Cursor:
    def __init__(self, owner: _Pg) -> None:
        self.owner = owner

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, query: str, params: Any = None) -> None:
        self.owner.statements.append((query, params))
        if query == self.owner.fails_on:
            raise QueryCanceledError("canceling statement due to statement timeout")

    def fetchone(self) -> dict[str, Any]:
        return {"statement_timeout": "30s"}

    def fetchall(self) -> list[dict[str, Any]]:
        return []


class _Pg:
    def __init__(self, fails_on: str | None = None) -> None:
        self.statements: list[tuple[str, Any]] = []
        self.fails_on = fails_on
        self.rolled_back = False

    def cursor(self, **kwargs: Any) -> _Cursor:
        return _Cursor(self)

    def rollback(self) -> None:
        self.rolled_back = True


def _postgres(pg: _Pg) -> Any:
    conn = cast(Any, object.__new__(PostgresConnection))
    conn.conn = pg
    return conn


def test_postgres_sets_the_statement_timeout_for_the_read_and_restores_it() -> None:
    pg = _Pg()

    PostgresConnection.read(_postgres(pg), "SELECT 1", timeout_s=2.5)

    assert pg.statements == [
        ("SHOW statement_timeout", None),
        ("SET LOCAL statement_timeout = 2500", None),
        ("SELECT 1", None),
        ("SELECT set_config('statement_timeout', %s, true)", ("30s",)),
    ]


def test_postgres_sets_nothing_without_a_limit() -> None:
    pg = _Pg()

    PostgresConnection.read(_postgres(pg), "SELECT 1")

    assert pg.statements == [("SELECT 1", None)]


def test_postgres_reports_a_cancelled_statement_and_ends_the_transaction() -> None:
    pg = _Pg(fails_on="SELECT 1")

    with pytest.raises(QueryTimeoutError) as raised:
        PostgresConnection.read(_postgres(pg), "SELECT 1", timeout_s=2.5)
    assert raised.value.timeout_s == 2.5
    assert pg.rolled_back


@pytest.mark.parametrize(
    ("method", "args"),
    [
        ("fetch_docs", {"class_name": "Person"}),
        (
            "fetch_edges",
            {
                "from_type": "Person",
                "from_id": "p1",
                "edge_type": "person_person_knows_edges",
            },
        ),
        ("fetch_all_docs", {"class_name": "Person"}),
        (
            "aggregate",
            {"class_name": "Person", "aggregation_function": AggregationType.COUNT},
        ),
    ],
)
def test_postgres_reads_hand_their_limit_to_read(
    method: str, args: dict[str, Any]
) -> None:
    seen: list[dict[str, Any]] = []
    conn = _postgres(_Pg())
    conn.config = SimpleNamespace(schema_name=None)
    conn.read = lambda query, params=None, **kwargs: seen.append(kwargs) or []
    # The edge table's columns come from the catalogue; none means "unknown".
    conn.get_table_columns = lambda table, schema_name=None: []

    getattr(PostgresConnection, method)(conn, **args, timeout_s=2.5)
    getattr(PostgresConnection, method)(conn, **args)

    assert seen == [{"timeout_s": 2.5}, {}]


# -- the generic breadth-first walk: one deadline -------------------------------

SCHEMA: dict[str, Any] = {
    "metadata": {"name": "chain", "version": "1.0.0"},
    "graph": {
        "vertex_config": {
            "vertices": [
                {
                    "name": "node",
                    "properties": [{"name": "id", "type": "STRING"}],
                    "identity": ["id"],
                }
            ]
        },
        "edge_config": {
            "edges": [{"source": "node", "target": "node", "relation": "next"}]
        },
    },
}


@pytest.fixture
def schema() -> Schema:
    manifest = GraphManifest.model_validate({"schema": SCHEMA})
    manifest.finish_init()
    return manifest.require_schema()


class _Chain:
    """A chain ``n0 -> n1 -> ...``; every read costs *cost* seconds of *clock*."""

    flavor = DBType.POSTGRES
    supports_query_timeout = True
    query_deadline = Connection.query_deadline
    vertex_address = Connection.vertex_address

    def __init__(self, clock: _Clock, cost: float) -> None:
        self.clock = clock
        self.cost = cost
        self.limits: list[float | None] = []

    def _read(self, timeout_s: float | None) -> None:
        self.limits.append(timeout_s)
        self.clock.now += self.cost

    def fetch_edges(
        self, from_type: str, from_id: str, **kwargs: Any
    ) -> list[dict[str, Any]]:
        self._read(kwargs.get("timeout_s"))
        step = int(from_id[1:])
        return [{"source_id": from_id, "target_id": f"n{step + 1}"}]

    def fetch_docs(
        self, class_name: str, filters: Any = None, **kwargs: Any
    ) -> list[dict[str, Any]]:
        self._read(kwargs.get("timeout_s"))
        return [{"id": filters["value"]}]


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> _Clock:
    clock = _Clock()
    monkeypatch.setattr(timeout_module, "time", SimpleNamespace(monotonic=clock))
    return clock


def test_the_walk_spends_one_deadline_across_its_reads(
    schema: Schema, clock: _Clock
) -> None:
    conn = _Chain(clock, cost=1.0)

    bfs_neighbors(
        cast(Any, conn),
        anchor_type="node",
        anchor_key="n0",
        hops=3,
        direction=EdgeDirection.OUT,
        schema=schema,
        timeout_s=10.0,
    )

    # Two reads a hop (the edges, then the far ends), each given what is left.
    assert conn.limits == [10.0, 9.0, 8.0, 7.0, 6.0, 5.0]


def test_the_walk_stops_when_the_deadline_is_spent(
    schema: Schema, clock: _Clock
) -> None:
    conn = _Chain(clock, cost=2.0)

    with pytest.raises(QueryTimeoutError) as raised:
        bfs_neighbors(
            cast(Any, conn),
            anchor_type="node",
            anchor_key="n0",
            hops=5,
            direction=EdgeDirection.OUT,
            schema=schema,
            timeout_s=5.0,
        )
    assert raised.value.timeout_s == 5.0
    # Three reads fit in five seconds; no fourth one starts.
    assert conn.limits == [5.0, 3.0, 1.0]


def test_a_read_timing_out_inside_the_walk_is_not_an_empty_hop(
    schema: Schema, clock: _Clock
) -> None:
    conn = _Chain(clock, cost=1.0)

    def fetch_edges(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        raise QueryTimeoutError(kwargs["timeout_s"])

    conn.fetch_edges = fetch_edges
    with pytest.raises(QueryTimeoutError) as raised:
        bfs_neighbors(
            cast(Any, conn),
            anchor_type="node",
            anchor_key="n0",
            hops=2,
            direction=EdgeDirection.OUT,
            schema=schema,
            timeout_s=10.0,
        )
    assert raised.value.timeout_s == 10.0


def test_the_walk_sends_no_limit_without_one(schema: Schema, clock: _Clock) -> None:
    conn = _Chain(clock, cost=1.0)

    bfs_neighbors(
        cast(Any, conn),
        anchor_type="node",
        anchor_key="n0",
        hops=2,
        direction=EdgeDirection.OUT,
        schema=schema,
    )

    assert conn.limits == [None] * 4


def test_the_walk_ignores_the_limit_where_the_backend_cannot_enforce_it(
    schema: Schema, clock: _Clock
) -> None:
    conn = _Chain(clock, cost=2.0)
    conn.supports_query_timeout = False

    bfs_neighbors(
        cast(Any, conn),
        anchor_type="node",
        anchor_key="n0",
        hops=3,
        direction=EdgeDirection.OUT,
        schema=schema,
        timeout_s=1.0,
    )

    assert conn.limits == [None] * 6


class _Lookup:
    """``Connection.resolve_vertices`` over a ``fetch_docs`` that costs time."""

    supports_query_timeout = True
    supports_read_guard = False
    query_deadline = Connection.query_deadline
    require_read_guard = Connection.require_read_guard
    # Typed `Any`: called on this stand-in, not on a `Connection`.
    resolve_vertices: Any = Connection.resolve_vertices

    def __init__(self, clock: _Clock) -> None:
        self.clock = clock
        self.limits: list[float | None] = []

    def fetch_docs(self, class_name: str, **kwargs: Any) -> list[dict[str, Any]]:
        self.limits.append(kwargs.get("timeout_s"))
        self.clock.now += 1.0
        return []


def test_a_chunked_lookup_spends_one_deadline_across_its_chunks(
    clock: _Clock,
) -> None:
    conn = _Lookup(clock)

    conn.resolve_vertices(
        "node",
        [{"id": f"n{i}"} for i in range(3)],
        ("id",),
        ("id",),
        chunk_size=1,
        timeout_s=10.0,
    )

    assert conn.limits == [10.0, 9.0, 8.0]
