"""Query-shape assertions for traversal, with no database involved.

Mirrors ``test_edge_direction_queries.py``: the per-backend builders are pure
``(params) -> str`` functions, so the dialect-specific half of traversal is
testable in plain CI. The live cross-backend agreement test is the e2e file.
"""

import pytest

from graflo.architecture.graph_types import EdgeDirection
from graflo.db.cypher.direction import cypher_rel_pattern
from graflo.db.edge_direction_support import (
    UnsupportedEdgeDirectionError,
    assert_direction_supported,
)
from graflo.onto import DBType


@pytest.mark.parametrize(
    "direction,expected",
    [
        (EdgeDirection.OUT, "-[r:KNOWS*1..3]->"),
        (EdgeDirection.IN, "<-[r:KNOWS*1..3]-"),
        (EdgeDirection.ANY, "-[r:KNOWS*1..3]-"),
    ],
)
def test_cypher_variable_length_pattern(direction, expected):
    assert cypher_rel_pattern("KNOWS", direction, min_hops=1, max_hops=3) == expected


def test_cypher_single_hop_is_unchanged_by_default():
    """Existing callers pass no hop bounds and must keep the single-hop form."""
    assert cypher_rel_pattern("KNOWS", EdgeDirection.OUT) == "-[r:KNOWS]->"
    assert cypher_rel_pattern(None, EdgeDirection.ANY) == "-[r]-"


@pytest.mark.parametrize("hops", [1, 2, 5])
def test_cypher_max_hops_alone_implies_min_one(hops):
    assert cypher_rel_pattern("E", max_hops=hops) == f"-[r:E*1..{hops}]->"


def test_cypher_untyped_variable_length():
    assert cypher_rel_pattern(None, EdgeDirection.ANY, max_hops=2) == "-[r*1..2]-"


def test_cypher_rejects_inverted_bounds():
    with pytest.raises(ValueError, match="max_hops"):
        cypher_rel_pattern("E", min_hops=3, max_hops=1)


def test_cypher_rejects_zero_hops():
    """An unbounded or zero-length pattern is a full scan; never emitted."""
    with pytest.raises(ValueError, match="min_hops"):
        cypher_rel_pattern("E", min_hops=0, max_hops=2)


@pytest.mark.parametrize(
    "db_type",
    [DBType.ARANGO, DBType.NEO4J, DBType.MEMGRAPH, DBType.FALKORDB, DBType.NEBULA],
)
@pytest.mark.parametrize(
    "direction", [EdgeDirection.OUT, EdgeDirection.IN, EdgeDirection.ANY]
)
def test_reverse_traversal_is_allowed_where_it_is_answerable(db_type, direction):
    assert_direction_supported(db_type, direction)


@pytest.mark.parametrize("direction", [EdgeDirection.IN, EdgeDirection.ANY])
def test_tigergraph_reverse_traversal_raises(direction):
    """The one case where failing beats answering: a partial neighbourhood.

    Reverse reachability on TigerGraph is fixed when the edge type is created,
    so returning only the outgoing half would look like a complete answer.
    """
    with pytest.raises(UnsupportedEdgeDirectionError):
        assert_direction_supported(DBType.TIGERGRAPH, direction)


def test_tigergraph_reverse_traversal_allowed_with_an_escape_hatch():
    assert_direction_supported(
        DBType.TIGERGRAPH, EdgeDirection.ANY, edge_is_undirected=True
    )
    assert_direction_supported(
        DBType.TIGERGRAPH, EdgeDirection.IN, has_reverse_edge=True
    )


def test_tigergraph_outbound_is_always_fine():
    assert_direction_supported(DBType.TIGERGRAPH, EdgeDirection.OUT)
