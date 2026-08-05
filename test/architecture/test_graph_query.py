"""The read contract's caps must hold without a database anywhere in sight.

Every test here runs against the models alone. That is the point: if enforcement
needed a live connection, "enforced in core" would mean "enforced wherever a
driver happens to be imported", and a route could opt out by not importing one.
"""

from __future__ import annotations

import pytest

from graflo.architecture.graph_types import EdgeDirection, GraphContainer
from graflo.architecture.query import (
    HARD_CAPS,
    AggregateQuery,
    CapExceededError,
    NeighborQuery,
    NodeQuery,
    QueryCaps,
    QueryResult,
    TraverseQuery,
)
from graflo.onto import AggregationType

# ── caps are not addressable from a request body ────────────────────────────


@pytest.mark.parametrize(
    "model",
    [NodeQuery, NeighborQuery, TraverseQuery, AggregateQuery],
)
def test_no_query_model_accepts_a_caps_field(model) -> None:
    """The security property of the whole stage.

    If a request body could carry `caps`, `{"caps": {"max_hops": 99}}` would
    make "no route handler can opt out" false by construction.
    """
    assert "caps" not in model.model_fields
    with pytest.raises(Exception):
        model.model_validate({"caps": {"max_hops": 99}})


# ── violations raise, and name the cap ──────────────────────────────────────


def test_hops_beyond_the_cap_raises_naming_it() -> None:
    """Naming the cap is what lets an agent retry.

    Told "max_hops exceeded, maximum is 3" it can ask again; told "invalid
    request" it cannot.
    """
    with pytest.raises(CapExceededError) as exc:
        NeighborQuery(vertex_type="person", key="a", hops=99).finish_init()
    assert exc.value.cap == "max_hops"
    assert "3" in str(exc.value)


def test_hops_are_not_silently_clamped() -> None:
    """Clamping hands back a partial answer the caller believes is complete.

    The same failure mode this codebase already rejected for TigerGraph edge
    direction, where under-reporting was ruled worse than failing loudly.
    """
    query = NeighborQuery(vertex_type="person", key="a", hops=99)
    with pytest.raises(CapExceededError):
        query.finish_init()
    assert query.hops == 99, "finish_init must not mutate the query"


def test_limit_beyond_max_rows_raises() -> None:
    with pytest.raises(CapExceededError) as exc:
        NodeQuery(vertex_type="person", limit=100_000).finish_init()
    assert exc.value.cap == "max_rows"


def test_timeout_beyond_the_cap_raises() -> None:
    with pytest.raises(CapExceededError) as exc:
        NodeQuery(vertex_type="person", timeout_s=600.0).finish_init()
    assert exc.value.cap == "timeout_s"


def test_too_many_seeds_raises() -> None:
    """A traversal fans out per seed, so seed count multiplies cost."""
    seeds = [{"vertex_type": "person", "key": str(i)} for i in range(50)]
    with pytest.raises(CapExceededError) as exc:
        TraverseQuery(seeds=seeds).finish_init()
    assert exc.value.cap == "max_seeds"


def test_too_many_edge_relations_raises() -> None:
    with pytest.raises(CapExceededError) as exc:
        NeighborQuery(
            vertex_type="person",
            key="a",
            edge_relations=[f"r{i}" for i in range(50)],
        ).finish_init()
    assert exc.value.cap == "max_edge_types"


def test_projection_outside_the_allow_list_raises() -> None:
    caps = QueryCaps(projection_allow_list=["id", "name"])
    with pytest.raises(CapExceededError) as exc:
        NodeQuery(vertex_type="person", projection=["id", "ssn"]).finish_init(caps)
    assert exc.value.cap == "projection_allow_list"
    assert "ssn" in str(exc.value)


def test_an_empty_allow_list_permits_nothing() -> None:
    """Distinct from `None`, which permits everything.

    Collapsing the two would turn "this connection exposes no properties" into
    "this connection exposes all of them".
    """
    caps = QueryCaps(projection_allow_list=[])
    NodeQuery(vertex_type="person").finish_init(caps)
    with pytest.raises(CapExceededError):
        NodeQuery(vertex_type="person", projection=["id"]).finish_init(caps)


def test_direction_crosses_the_driver_boundary_as_an_enum() -> None:
    """Regression: `use_enum_values=True` stores the bare string.

    Backends compare against `EdgeDirection` members, and a string matches none
    of them — which each backend then resolves differently, so the same query
    returned a different neighbourhood depending on where it ran. PostgreSQL
    answered `{B, D}` where Neo4j and Arango answered `{B}`.
    """
    query = NeighborQuery(
        vertex_type="person", key="a", direction=EdgeDirection.OUT
    ).finish_init()
    assert isinstance(query.edge_direction, EdgeDirection)
    assert query.edge_direction is EdgeDirection.OUT

    traverse = TraverseQuery(
        seeds=[{"vertex_type": "person", "key": "a"}], direction=EdgeDirection.IN
    ).finish_init()
    assert traverse.edge_direction is EdgeDirection.IN


def test_aggregation_crosses_the_driver_boundary_as_an_enum() -> None:
    query = AggregateQuery(
        vertex_type="person", function=AggregationType.MAX, aggregated_field="age"
    ).finish_init()
    assert isinstance(query.aggregation, AggregationType)
    assert query.aggregation is AggregationType.MAX


def test_a_query_within_the_caps_validates() -> None:
    query = NeighborQuery(
        vertex_type="person", key="a", hops=2, direction=EdgeDirection.ANY
    ).finish_init()
    assert query.hops == 2


# ── narrowing goes one way ──────────────────────────────────────────────────


def test_narrowed_clamps_a_default_the_caller_never_set() -> None:
    """A strict policy must not 422 every request that omitted a limit.

    Clamping the default is what makes `max_rows=5` a usable setting rather
    than one that rejects almost everything.
    """
    query = NodeQuery(vertex_type="person")
    assert "limit" not in query.model_fields_set
    assert query.narrowed(QueryCaps(max_rows=10)).limit == 10


def test_narrowed_raises_on_a_limit_the_caller_asked_for() -> None:
    """Explicitly asking for 500 and silently getting 10 is a wrong answer.

    The caller believes it saw everything. Distinguishing this from the
    clamped-default case is exactly what `model_fields_set` is for.
    """
    query = NodeQuery(vertex_type="person", limit=500)
    with pytest.raises(CapExceededError) as exc:
        query.narrowed(QueryCaps(max_rows=10))
    assert exc.value.cap == "max_rows"


def test_narrowed_raises_on_hops_the_caller_asked_for() -> None:
    query = NeighborQuery(vertex_type="person", key="a", hops=3)
    with pytest.raises(CapExceededError) as exc:
        query.narrowed(QueryCaps(max_hops=1))
    assert exc.value.cap == "max_hops"


def test_narrowed_never_widens_past_the_hard_caps() -> None:
    """A policy that tries to raise a ceiling becomes a no-op, not an escalation."""
    query = NeighborQuery(vertex_type="person", key="a", hops=3).finish_init()
    widened = query.narrowed(QueryCaps(max_hops=99, max_rows=1_000_000))
    assert widened.hops <= HARD_CAPS.max_hops
    assert widened.limit <= HARD_CAPS.max_rows


def test_narrowed_leaves_a_conforming_query_alone() -> None:
    query = NodeQuery(vertex_type="person", limit=10).finish_init()
    assert query.narrowed(QueryCaps(max_rows=1000)).limit == 10


def test_narrowed_does_not_mutate_the_original() -> None:
    """Ordering in the enforcement path depends on this.

    The service narrows before any I/O; if narrowing mutated in place, a shared
    or retried query object would carry the previous connection's policy.
    """
    query = NodeQuery(vertex_type="person", limit=500).finish_init()
    query.narrowed(QueryCaps(max_rows=500))
    assert query.limit == 500


def test_narrowed_intersects_projection_allow_lists() -> None:
    """Projection narrows silently, unlike every other cap.

    An allow-list exists to *hide* properties; raising would confirm to the
    caller that the forbidden name they guessed is a real one.
    """
    query = NodeQuery(vertex_type="person", projection=["id", "name", "email"])
    narrowed = query.narrowed(QueryCaps(projection_allow_list=["id", "name"]))
    assert narrowed.projection == ["id", "name"]


def test_narrowed_never_drops_a_seed() -> None:
    """There is no default set of anchors, so every seed was asked for.

    Trimming them would answer a different question than the one posed.
    """
    seeds = [{"vertex_type": "person", "key": str(i)} for i in range(8)]
    query = TraverseQuery(seeds=seeds, max_hops=3).finish_init()
    with pytest.raises(CapExceededError) as exc:
        query.narrowed(QueryCaps(max_seeds=2))
    assert exc.value.cap == "max_seeds"


def test_caps_narrow_takes_the_stricter_of_each() -> None:
    combined = QueryCaps(max_hops=5, max_rows=10).narrow(
        QueryCaps(max_hops=2, max_rows=100)
    )
    assert combined.max_hops == 2
    assert combined.max_rows == 10


# ── per-kind validation ─────────────────────────────────────────────────────


def test_aggregate_without_a_field_raises_for_non_count() -> None:
    with pytest.raises(ValueError, match="aggregated_field"):
        AggregateQuery(vertex_type="person", function=AggregationType.MAX).finish_init()


def test_count_needs_no_aggregated_field() -> None:
    AggregateQuery(vertex_type="person", function=AggregationType.COUNT).finish_init()


def test_group_by_is_rejected_for_non_count() -> None:
    """No backend groups a MAX the way it groups a COUNT here; saying so beats
    letting each one interpret it differently."""
    with pytest.raises(ValueError, match="group_by"):
        AggregateQuery(
            vertex_type="person",
            function=AggregationType.MAX,
            aggregated_field="age",
            group_by="city",
        ).finish_init()


def test_a_seed_missing_its_key_raises() -> None:
    with pytest.raises(ValueError, match="key"):
        TraverseQuery(seeds=[{"vertex_type": "person"}]).finish_init()


def test_traverse_requires_at_least_one_seed() -> None:
    with pytest.raises(Exception):
        TraverseQuery(seeds=[])


def test_zero_hops_is_rejected_at_the_model() -> None:
    with pytest.raises(Exception):
        NeighborQuery(vertex_type="person", key="a", hops=0)


# ── round-trip and result shape ─────────────────────────────────────────────


@pytest.mark.parametrize(
    "query",
    [
        NodeQuery(vertex_type="person", limit=5),
        NeighborQuery(vertex_type="person", key="a", hops=2),
        TraverseQuery(seeds=[{"vertex_type": "person", "key": "a"}]),
        AggregateQuery(vertex_type="person"),
    ],
)
def test_queries_round_trip(query) -> None:
    assert type(query).model_validate(query.model_dump()) == query


def test_result_derives_its_element_count() -> None:
    container = GraphContainer()
    container.vertices["person"] = [{"id": "a"}, {"id": "b"}]
    result = QueryResult.of(container)
    assert result.element_count == 2
    assert result.truncated is False
    assert result.caps_hit == []


def test_a_result_reports_which_cap_bound_it() -> None:
    """`len(rows) == limit` is wrong in both directions as a truncation signal.

    Exactly `limit` rows may be the complete answer, and fewer than `limit` may
    still have been cut by an element or timeout bound.
    """
    result = QueryResult.of(GraphContainer(), caps_hit=["max_rows"])
    assert result.truncated is True
    assert result.caps_hit == ["max_rows"]


# ── no raw-query passthrough ────────────────────────────────────────────────


@pytest.mark.parametrize(
    "model", [NodeQuery, NeighborQuery, TraverseQuery, AggregateQuery]
)
def test_no_query_model_carries_a_raw_query_string(model) -> None:
    """`Connection.execute` must not be reachable from an agent path.

    A `query` / `aql` / `cypher` field on a request model would be exactly that
    reachability, however carefully the handler treated it.
    """
    forbidden = {"query", "statement", "aql", "cypher", "gsql", "ngql", "sql", "raw"}
    assert not (forbidden & set(model.model_fields))
