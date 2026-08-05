"""Adjacency index and schema-plane navigation."""

import pytest

from graflo.architecture.graph_types import EdgeDirection
from graflo.architecture.schema.context import SchemaGraph


@pytest.fixture()
def graph(context_schema):
    return SchemaGraph.from_schema(context_schema)


def test_vertex_and_edge_inventory(graph):
    assert graph.vertex_types == frozenset(
        {"person", "company", "city", "doc", "orphan"}
    )
    assert len(graph.edge_ids) == 6
    assert graph.isolated_types() == ["orphan"]
    assert graph.relation_vocabulary() == [
        "founded",
        "hq_in",
        "knows",
        "lives_in",
        "works_at",
    ]


def test_edge_ids_sort_with_null_relation(graph):
    """A relation-less edge must not break ordering against named relations."""
    assert graph.edge_ids == sorted(
        graph.edge_ids, key=lambda e: (e[0], e[1], e[2] or "")
    )
    assert ("doc", "person", None) in graph.edge_ids


def test_degree_counts_self_loop_twice(graph):
    # person: knows (self-loop, both ends), works_at, founded, lives_in, doc->person
    assert graph.degree("person") == 6
    assert graph.degree("orphan") == 0


def test_out_and_in_edges_are_indexed(graph):
    assert ("person", "company", "works_at") in graph.out_edges("person")
    assert ("person", "company", "works_at") in graph.in_edges("company")
    assert graph.out_edges("orphan") == []


@pytest.mark.parametrize(
    "hops,expected", [(0, {"person"}), (1, {"person", "company", "city", "doc"})]
)
def test_schema_neighbors_hop_bounds(graph, hops, expected):
    result = graph.schema_neighbors("person", hops=hops)
    assert set(result.distances) == expected
    assert result.distances["person"] == 0


def test_schema_neighbors_direction_out_excludes_inbound(graph):
    """OUT from person must not reach doc, which only points *at* person."""
    result = graph.schema_neighbors("person", hops=1, direction=EdgeDirection.OUT)
    assert "doc" not in result.distances
    assert {"company", "city"} <= set(result.distances)


def test_schema_neighbors_direction_in(graph):
    result = graph.schema_neighbors("person", hops=1, direction=EdgeDirection.IN)
    assert "doc" in result.distances
    assert "city" not in result.distances


def test_undirected_edge_traversable_from_target_under_out(graph):
    """``company -hq_in-> city`` is declared undirected, so OUT from city reaches company.

    Direction is a property of the edge first and the request second; an
    undirected edge is followable both ways whatever the caller asked for.
    """
    result = graph.schema_neighbors("city", hops=1, direction=EdgeDirection.OUT)
    assert "company" in result.distances


def test_directed_edge_not_traversable_backwards_under_out(graph):
    """The contrast case: lives_in is directed, so OUT from city must not reach person."""
    result = graph.schema_neighbors("city", hops=1, direction=EdgeDirection.OUT)
    assert result.distances.get("person") is None


def test_schema_neighbors_edge_relation_filter(graph):
    result = graph.schema_neighbors("person", hops=1, edge_relations={"works_at"})
    assert set(result.distances) == {"person", "company"}
    assert result.edges == [("person", "company", "works_at")]


def test_schema_neighbors_rejects_unknown_type(graph):
    with pytest.raises(KeyError, match="nope"):
        graph.schema_neighbors("nope")


def test_schema_neighbors_rejects_negative_hops(graph):
    with pytest.raises(ValueError, match="hops"):
        graph.schema_neighbors("person", hops=-1)


def test_neighborhood_vertex_types_ordered_by_distance(graph):
    result = graph.schema_neighbors("doc", hops=2)
    assert result.vertex_types[0] == "doc"
    assert result.distances["person"] == 1


def test_relations_between_finds_parallel_edges(graph):
    paths = graph.relations_between("person", "company", max_len=1)
    assert [p.edges[0][2] for p in paths] == ["founded", "works_at"]
    assert all(p.length == 1 for p in paths)


def test_relations_between_disconnected_is_empty(graph):
    assert graph.relations_between("orphan", "person") == []


def test_relations_between_self_returns_self_loop(graph):
    paths = graph.relations_between("person", "person", max_len=1)
    assert [p.edges for p in paths] == [[("person", "person", "knows")]]


def test_relations_between_terminates_on_cycles(graph):
    """A cycle must bound out at max_len rather than looping forever."""
    paths = graph.relations_between("doc", "city", max_len=3)
    assert paths
    assert all(p.length <= 3 for p in paths)


def test_relations_between_respects_max_paths(graph):
    paths = graph.relations_between("person", "company", max_len=3, max_paths=1)
    assert len(paths) == 1


def test_relations_between_shortest_first(graph):
    paths = graph.relations_between("doc", "company", max_len=3)
    assert [p.length for p in paths] == sorted(p.length for p in paths)


def test_relations_between_rejects_unknown_endpoint(graph):
    with pytest.raises(KeyError):
        graph.relations_between("person", "nope")


def test_graph_does_not_mutate_schema(context_schema):
    before = context_schema.to_dict()
    graph = SchemaGraph.from_schema(context_schema)
    graph.schema_neighbors("person", hops=3)
    graph.relations_between("doc", "city", max_len=3)
    assert context_schema.to_dict() == before
