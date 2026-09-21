"""What the backend-neutral traversal tells a backend about direction.

No database: a recording connection stands in for the driver. These pin the
half of the contract that is invisible from query text -- that an undirected
edge and a database-maintained inverse travel with the ``fetch_edges`` request,
and that a direction the backend cannot follow is a refusal, never an empty
neighbourhood.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.graph_types import EdgeDirection
from graflo.architecture.schema import Schema
from graflo.db.edge_direction_support import (
    UnsupportedEdgeDirectionError,
    assert_direction_supported,
)
from graflo.db.tigergraph.data_ops import _as_forward_rows
from graflo.db.traversal import bfs_neighbors
from graflo.onto import DBType


def _schema(*, native: bool) -> Schema:
    payload: dict[str, Any] = {
        "metadata": {"name": "direction", "version": "1.0.0"},
        "graph": {
            "vertex_config": {
                "vertices": [
                    {
                        "name": name,
                        "properties": [{"name": "id", "type": "STRING"}],
                        "identity": ["id"],
                    }
                    for name in ("person", "company")
                ]
            },
            "edge_config": {
                "edges": [
                    {
                        "source": "person",
                        "target": "person",
                        "relation": "knows",
                        "directed": False,
                    },
                    {
                        "source": "person",
                        "target": "company",
                        "relation": "employed_by",
                    },
                ],
                "inverses": [{"relation": "employed_by", "inverse": "employs"}],
                "symmetric": ["knows"],
            },
        },
        "db_profile": {
            "db_flavor": "tigergraph",
            "native_inverses": ["employed_by"] if native else [],
        },
    }
    manifest = GraphManifest.model_validate({"schema": payload})
    manifest.finish_init()
    return manifest.require_schema()


class _TigerGraphLike:
    """Enforces direction the way the TigerGraph read path does, and records calls."""

    flavor = DBType.TIGERGRAPH

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def vertex_address(
        self, doc: dict[str, Any], identity_fields: Sequence[str]
    ) -> str | None:
        for field in identity_fields:
            if doc.get(field) is not None:
                return str(doc[field])
        return None

    def fetch_docs(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        return []

    def fetch_edges(
        self,
        from_type: str,
        from_id: str,
        *,
        direction: EdgeDirection = EdgeDirection.OUT,
        native_inverse_type: str | None = None,
        edge_is_undirected: bool = False,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        assert_direction_supported(
            DBType.TIGERGRAPH,
            direction,
            has_native_inverse=native_inverse_type is not None,
            edge_is_undirected=edge_is_undirected,
        )
        self.calls.append(
            {
                "edge_type": kwargs.get("edge_type"),
                "direction": direction,
                "native_inverse_type": native_inverse_type,
                "edge_is_undirected": edge_is_undirected,
            }
        )
        return []


def _walk(conn: _TigerGraphLike, schema: Schema, **kwargs: Any):
    params: dict[str, Any] = {
        "anchor_type": "person",
        "anchor_key": "p1",
        "direction": EdgeDirection.OUT,
    }
    params.update(kwargs)
    return bfs_neighbors(conn, schema=schema, **params)  # ty: ignore[invalid-argument-type]


def test_an_undirected_edge_is_announced_to_the_backend() -> None:
    """Without it a schema-time backend refuses the read it could have answered."""
    conn = _TigerGraphLike()
    _walk(conn, _schema(native=False), edge_types=["knows"])

    (call,) = conn.calls
    assert call["direction"] is EdgeDirection.ANY
    assert call["edge_is_undirected"] is True


def test_a_native_inverse_makes_the_reverse_read_answerable() -> None:
    conn = _TigerGraphLike()
    _walk(
        conn,
        _schema(native=True),
        anchor_type="company",
        anchor_key="c1",
        direction=EdgeDirection.IN,
        edge_types=["employed_by"],
    )

    (call,) = conn.calls
    assert call["direction"] is EdgeDirection.IN
    assert call["native_inverse_type"] == "employs"


def test_a_direction_the_backend_cannot_follow_is_refused_not_dropped() -> None:
    conn = _TigerGraphLike()
    with pytest.raises(UnsupportedEdgeDirectionError):
        _walk(
            conn,
            _schema(native=False),
            anchor_type="company",
            anchor_key="c1",
            direction=EdgeDirection.IN,
            edge_types=["employed_by"],
        )
    assert conn.calls == []


def test_a_refusal_raised_by_the_backend_itself_is_not_swallowed() -> None:
    class _Refusing(_TigerGraphLike):
        def fetch_edges(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
            raise UnsupportedEdgeDirectionError("no reverse type")

    with pytest.raises(UnsupportedEdgeDirectionError):
        _walk(_Refusing(), _schema(native=False), edge_types=["knows"])


def test_rows_from_a_reverse_type_are_restated_in_the_declared_orientation() -> None:
    """An inbound read is an outgoing query on the paired type; callers see one shape."""
    rows = [
        {
            "e_type": "employs",
            "from_id": "c1",
            "from_type": "company",
            "to_id": "p1",
            "to_type": "person",
            "attributes": {"since": 2020},
        }
    ]

    (row,) = _as_forward_rows(rows, "employed_by")

    assert row == {
        "e_type": "employed_by",
        "from_id": "p1",
        "from_type": "person",
        "to_id": "c1",
        "to_type": "company",
        "attributes": {"since": 2020},
    }
    assert rows[0]["from_id"] == "c1"


def test_restating_keeps_a_missing_endpoint_missing() -> None:
    (row,) = _as_forward_rows([{"from_id": "c1"}], None)
    assert row == {"to_id": "c1"}


def test_an_edge_type_is_followed_only_from_the_end_the_direction_names() -> None:
    """``OUT`` leaves the source type, ``IN`` arrives at the target type; never the reverse."""
    schema = _schema(native=True)
    for anchor, direction, expected in [
        ("person", EdgeDirection.OUT, [EdgeDirection.OUT]),
        ("person", EdgeDirection.IN, []),
        ("company", EdgeDirection.OUT, []),
        ("company", EdgeDirection.IN, [EdgeDirection.IN]),
        # From one end of a two-type edge, ANY narrows to the orientation that exists.
        ("company", EdgeDirection.ANY, [EdgeDirection.IN]),
        ("person", EdgeDirection.ANY, [EdgeDirection.OUT]),
    ]:
        conn = _TigerGraphLike()
        _walk(
            conn,
            schema,
            anchor_type=anchor,
            anchor_key="x",
            direction=direction,
            edge_types=["employed_by"],
        )
        assert [call["direction"] for call in conn.calls] == expected, (
            anchor,
            direction,
        )


class _Stored(_TigerGraphLike):
    """Answers every read of ``employed_by`` with one stored edge, p1 -> c1."""

    flavor = DBType.NEO4J

    def fetch_edges(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        self.calls.append({"direction": kwargs.get("direction")})
        return [{"source_id": "p1", "target_id": "c1", "since": 2020}]


def test_a_declared_inverse_that_stores_nothing_reads_the_forward_edge_backwards() -> (
    None
):
    conn = _Stored()
    container = _walk(
        conn,
        _schema(native=False),
        anchor_type="company",
        anchor_key="c1",
        direction=EdgeDirection.OUT,
        edge_types=["employs"],
    )

    # Outgoing `employs` from a company is the stored `employed_by` arriving at it.
    assert [call["direction"] for call in conn.calls] == [EdgeDirection.IN]
    # Reported as the reading that was asked for, endpoints in that order.
    assert container.edges == {
        ("company", "person", "employs"): [
            {"source_id": "p1", "target_id": "c1", "since": 2020}
            | {"source": "c1", "target": "p1"}
        ]
    }


def test_a_name_that_is_neither_stored_nor_a_declared_inverse_selects_nothing() -> None:
    conn = _Stored()
    container = _walk(conn, _schema(native=False), edge_types=["manages"])
    assert conn.calls == []
    assert container.edges == {}


def test_a_walk_is_not_refused_for_an_orientation_it_never_needs() -> None:
    """``ANY`` from the source end of a directed type only ever reads outward."""
    conn = _TigerGraphLike()
    _walk(
        conn,
        _schema(native=False),
        direction=EdgeDirection.ANY,
        edge_types=["employed_by"],
    )
    assert [call["direction"] for call in conn.calls] == [EdgeDirection.OUT]
