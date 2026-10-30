"""FalkorDB parameters are checked before writes and preserve valid numbers."""

from __future__ import annotations

from typing import Any, cast

import pytest
from redis.exceptions import ResponseError

from graflo.db.falkordb.conn import FalkordbConnection


class _Graph:
    """Record query parameters or raise the supplied driver error."""

    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls: list[dict[str, Any] | None] = []
        self.result = object()

    def query(self, query: str, params: dict[str, Any] | None = None) -> object:
        """Record one query attempt, retaining the original numeric types."""
        self.calls.append(params)
        if self.error is not None:
            raise self.error
        return self.result


def _connection(error: Exception | None = None) -> tuple[FalkordbConnection, _Graph]:
    """Build a connection without contacting a server."""
    graph = _Graph(error)
    conn = cast(Any, object.__new__(FalkordbConnection))
    conn.graph = graph
    return conn, graph


@pytest.mark.parametrize("value", [-(2**63), 2**63 - 1, 2**53 + 1, 0, True, False])
def test_valid_integer_parameters_keep_their_value_and_type(value: int) -> None:
    """Large integers stay exact, and booleans remain booleans."""
    db, graph = _connection()

    assert db.execute("RETURN $value", value=value) is graph.result
    assert graph.calls == [{"value": value}]
    params = graph.calls[0]
    assert params is not None
    assert type(params["value"]) is type(value)


@pytest.mark.parametrize("value", [2**63, -(2**63) - 1, 2**64 - 1])
@pytest.mark.parametrize("container", ["scalar", "list", "tuple", "map"])
def test_overflowing_parameters_are_refused_before_querying(
    value: int, container: str
) -> None:
    """Neither engine gets an integer it could clamp or reject differently."""
    nested = {"scalar": value, "list": [value], "tuple": (value,), "map": {"x": value}}
    db, graph = _connection()

    with pytest.raises(OverflowError, match="signed 64-bit"):
        db.execute("RETURN $value", value=nested[container])

    assert graph.calls == []


def test_an_overflowing_batch_writes_none_of_its_documents() -> None:
    """A bad value in the last document cannot leave earlier documents written."""
    db, graph = _connection()

    with pytest.raises(OverflowError, match="signed 64-bit"):
        db.upsert_docs_batch(
            [{"id": "valid", "value": 1}, {"id": "invalid", "value": 2**63}],
            "number",
            match_keys=["id"],
        )

    assert graph.calls == []


def _edge_rows(value: int, *, null_code: bool = False) -> list[list[dict[str, Any]]]:
    """Build one property-MERGE group followed by one absent-property group."""
    props: dict[str, Any] = {"weight": value}
    if null_code:
        props["code"] = None
    return [
        [{"id": "s1"}, {"id": "t1"}, {"code": "valid", "weight": 1}],
        [{"id": "s2"}, {"id": "t2"}, props],
    ]


@pytest.mark.parametrize("value", [2**63, -(2**63) - 1, 2**64 - 1])
@pytest.mark.parametrize("null_code", [False, True])
def test_an_overflowing_edge_group_sends_no_query(value: int, null_code: bool) -> None:
    """A later group must be checked before the first group can be written."""
    db, graph = _connection()

    with pytest.raises(OverflowError, match="signed 64-bit"):
        db.insert_edges_batch(
            _edge_rows(value, null_code=null_code),
            "number",
            "number",
            "LINK",
            ("id",),
            ("id",),
            relationship_merge_properties=("code",),
        )

    assert graph.calls == []


def test_valid_edge_groups_are_both_sent() -> None:
    """The property-MERGE and absent-property groups both remain writable."""
    db, graph = _connection()
    db.insert_edges_batch(
        _edge_rows(2),
        "number",
        "number",
        "LINK",
        ("id",),
        ("id",),
        relationship_merge_properties=("code",),
    )

    assert len(graph.calls) == 2
    groups = [call["data"] for call in graph.calls if call is not None]
    assert [group[0][2]["weight"] for group in groups] == [1, 2]


def test_edge_head_excludes_an_overflowing_later_group() -> None:
    """Only the selected prefix is validated and sent."""
    db, graph = _connection()
    db.insert_edges_batch(
        _edge_rows(2**63),
        "number",
        "number",
        "LINK",
        ("id",),
        ("id",),
        relationship_merge_properties=("code",),
        head=1,
    )

    assert len(graph.calls) == 1
    assert graph.calls[0] is not None
    assert graph.calls[0]["data"][0][2] == {"code": "valid", "weight": 1}


def test_dry_edge_groups_are_not_numerically_validated_or_sent() -> None:
    """A dry run retains its existing behaviour without contacting the graph."""
    db, graph = _connection()
    db.insert_edges_batch(
        _edge_rows(2**63),
        "number",
        "number",
        "LINK",
        ("id",),
        ("id",),
        relationship_merge_properties=("code",),
        dry=True,
    )

    assert graph.calls == []


def test_a_server_parameter_refusal_is_a_value_error() -> None:
    """Expose a rejected parameter as invalid input and preserve its cause."""
    error = ResponseError("Failed to parse the value of parameter 'value'")
    db, _ = _connection(error)

    with pytest.raises(ValueError, match="query parameter") as caught:
        db.execute("RETURN $value", value=1e-308)

    assert caught.value.__cause__ is error


@pytest.mark.parametrize("message", ["unique constraint violation", "Invalid input"])
def test_other_server_errors_keep_their_driver_type(message: str) -> None:
    """Constraint and Cypher errors retain their original error semantics."""
    error = ResponseError(message)
    db, _ = _connection(error)

    with pytest.raises(ResponseError) as caught:
        db.execute("RETURN $value", value=1)

    assert caught.value is error
