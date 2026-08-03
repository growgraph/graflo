"""Direction rendering in the read path, per query language.

These assert the *generated query*, not a live database, so they run without any
container. Behaviour against real backends is covered by the per-DB suites.
"""

from __future__ import annotations

import pytest

from graflo.architecture.graph_types import EdgeDirection
from graflo.architecture.schema.edge import Edge
from graflo.db.arango.conn import _arango_edge_anchor_clause
from graflo.db.cypher import cypher_rel_pattern
from graflo.db.edge_direction_support import (
    UnsupportedEdgeDirectionError,
    assert_direction_supported,
    default_direction_for_edge,
)
from graflo.db.nebula.query import fetch_edges_ngql
from graflo.onto import DBType

# --------------------------------------------------------------------------
# Where `Edge.directed` turns into a query decision
# --------------------------------------------------------------------------


def test_undirected_edge_defaults_to_any() -> None:
    undirected = Edge(source="a", target="a", relation="knows", directed=False)
    assert default_direction_for_edge(undirected) is EdgeDirection.ANY


def test_directed_edge_defaults_to_out() -> None:
    directed = Edge(source="a", target="b", relation="works_at")
    assert default_direction_for_edge(directed) is EdgeDirection.OUT


# --------------------------------------------------------------------------
# Cypher family (Neo4j, Memgraph, FalkorDB)
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("direction", "expected"),
    [
        (EdgeDirection.OUT, "-[r:KNOWS]->"),
        (EdgeDirection.IN, "<-[r:KNOWS]-"),
        (EdgeDirection.ANY, "-[r:KNOWS]-"),
    ],
)
def test_cypher_rel_pattern(direction: EdgeDirection, expected: str) -> None:
    assert cypher_rel_pattern("KNOWS", direction) == expected


def test_cypher_rel_pattern_without_edge_type() -> None:
    assert cypher_rel_pattern(None, EdgeDirection.ANY) == "-[r]-"


def test_cypher_rel_pattern_honours_variable_name() -> None:
    assert (
        cypher_rel_pattern("KNOWS", EdgeDirection.OUT, variable="e") == "-[e:KNOWS]->"
    )


# --------------------------------------------------------------------------
# ArangoDB — the edge index covers both endpoints, so no branch is special
# --------------------------------------------------------------------------


def test_arango_out_anchors_on_from() -> None:
    clause = _arango_edge_anchor_clause("person/1", EdgeDirection.OUT, None, None)
    assert clause == "e._from == 'person/1'"


def test_arango_in_anchors_on_to() -> None:
    clause = _arango_edge_anchor_clause("person/1", EdgeDirection.IN, None, None)
    assert clause == "e._to == 'person/1'"


def test_arango_any_matches_either_orientation() -> None:
    clause = _arango_edge_anchor_clause("person/1", EdgeDirection.ANY, None, None)
    assert clause == "(e._from == 'person/1') || (e._to == 'person/1')"


def test_arango_endpoint_filters_follow_the_anchor() -> None:
    """Under ANY, `to_id` constrains whichever end is not the anchor."""
    clause = _arango_edge_anchor_clause(
        "person/1", EdgeDirection.ANY, "company", "company/9"
    )
    assert clause == (
        "(e._from == 'person/1' && e._to LIKE 'company/%' && e._to == 'company/9')"
        " || "
        "(e._to == 'person/1' && e._from LIKE 'company/%' && e._from == 'company/9')"
    )


def test_arango_in_puts_endpoint_filter_on_from() -> None:
    clause = _arango_edge_anchor_clause(
        "person/1", EdgeDirection.IN, "company", "company/9"
    )
    assert clause == (
        "e._to == 'person/1' && e._from LIKE 'company/%' && e._from == 'company/9'"
    )


# --------------------------------------------------------------------------
# Nebula — the clause that had zero occurrences before
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("direction", "clause"),
    [
        (EdgeDirection.OUT, ""),
        (EdgeDirection.IN, "REVERSELY"),
        (EdgeDirection.ANY, "BIDIRECT"),
    ],
)
def test_nebula_go_direction_clause(direction: EdgeDirection, clause: str) -> None:
    q = fetch_edges_ngql("person", "v1", edge_type="knows", direction=direction)
    assert q.startswith('GO FROM "v1" OVER `knows`')
    if clause:
        assert f"OVER `knows` {clause}" in q
    else:
        assert "REVERSELY" not in q and "BIDIRECT" not in q


def test_nebula_other_endpoint_predicate_survives_direction() -> None:
    """``$$`` is the far end of whichever orientation GO walked."""
    q = fetch_edges_ngql(
        "person", "v1", edge_type="knows", to_vid="v2", direction=EdgeDirection.ANY
    )
    assert "BIDIRECT" in q
    assert 'id($$) == "v2"' in q


# --------------------------------------------------------------------------
# TigerGraph — reverse reachability is decided at DDL time
# --------------------------------------------------------------------------


@pytest.mark.parametrize("direction", [EdgeDirection.IN, EdgeDirection.ANY])
def test_tigergraph_rejects_reverse_read_without_reverse_edge(
    direction: EdgeDirection,
) -> None:
    with pytest.raises(UnsupportedEdgeDirectionError, match="fixed when the edge type"):
        assert_direction_supported(DBType.TIGERGRAPH, direction)


@pytest.mark.parametrize("direction", [EdgeDirection.IN, EdgeDirection.ANY])
def test_tigergraph_accepts_reverse_read_with_reverse_edge(
    direction: EdgeDirection,
) -> None:
    assert_direction_supported(DBType.TIGERGRAPH, direction, has_reverse_edge=True)


@pytest.mark.parametrize("direction", [EdgeDirection.IN, EdgeDirection.ANY])
def test_tigergraph_accepts_reverse_read_on_undirected_type(
    direction: EdgeDirection,
) -> None:
    """An ``UNDIRECTED EDGE`` already answers both orientations natively."""
    assert_direction_supported(DBType.TIGERGRAPH, direction, edge_is_undirected=True)


def test_tigergraph_out_is_always_fine() -> None:
    assert_direction_supported(DBType.TIGERGRAPH, EdgeDirection.OUT)


# --------------------------------------------------------------------------
# Every other backend can answer any direction by query alone
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "db_type",
    [
        DBType.ARANGO,
        DBType.NEO4J,
        DBType.MEMGRAPH,
        DBType.FALKORDB,
        DBType.NEBULA,
        DBType.POSTGRES,
        DBType.GRAFLO_BACKEND,
    ],
)
@pytest.mark.parametrize(
    "direction", [EdgeDirection.OUT, EdgeDirection.IN, EdgeDirection.ANY]
)
def test_non_schema_time_backends_accept_every_direction(
    db_type: DBType, direction: EdgeDirection
) -> None:
    assert_direction_supported(db_type, direction)
