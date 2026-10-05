"""DotGraph: what reaches the DOT text, and unflatten against Graphviz's own tool.

The ``unflatten`` expectations were produced by Graphviz's ``unflatten``
binary on the same graphs and are pinned here, so the port stays faithful.
"""

from __future__ import annotations

import networkx as nx
import pytest

from graflo.plot.dot import DotGraph, quote


def test_values_are_quoted_and_only_quotes_are_escaped():
    assert quote('say "hi"') == '"say \\"hi\\""'
    assert quote("left\\l") == '"left\\l"', "label escapes reach Graphviz intact"
    assert quote("ends\\") == '"ends\\\\"', (
        "a lone trailing backslash cannot eat the quote"
    )


def test_networkx_defaults_and_attributes_are_carried_over():
    graph = nx.DiGraph()
    graph.graph["graph"] = {"rankdir": "LR"}
    graph.graph["node"] = {"fontname": "Helvetica"}
    graph.graph["edge"] = {"color": "grey"}
    graph.graph["label"] = "title"
    graph.add_node("a", shape="box", forcelabel=True)
    graph.add_edge("a", "b", tailport="p1")

    dot = DotGraph.from_networkx(graph)
    source = dot.to_string()

    assert dot.strict, "a simple graph without self-loops is written strict"
    assert source.startswith('strict digraph "" {')
    assert 'graph ["label"="title", "rankdir"="LR"];' in source
    assert 'node ["fontname"="Helvetica"];' in source
    assert 'edge ["color"="grey"];' in source
    assert '"a" ["shape"="box", "forcelabel"="True"];' in source
    assert '"a" -> "b" ["tailport"="p1"];' in source


def test_parallel_edges_keep_their_own_attributes():
    graph = nx.MultiDiGraph()
    graph.add_edge("a", "b", label="first")
    graph.add_edge("a", "b", label="second")

    dot = DotGraph.from_networkx(graph)

    assert not dot.strict
    assert [attrs["label"] for _, _, attrs in dot.edges] == ["first", "second"]


def test_subgraph_members_are_declared_inside_it_with_their_attributes():
    graph = nx.DiGraph()
    graph.add_node("a", label="A")
    graph.add_node("b", label="B")
    dot = DotGraph.from_networkx(graph)
    cluster = dot.add_subgraph(["a"], name="cluster_x", label="X", rank="same")
    cluster.node_attr["style"] = "filled"

    source = dot.to_string()

    inside = source[source.index('subgraph "cluster_x"') : source.index("  }")]
    assert 'graph ["label"="X", "rank"="same"];' in inside
    assert 'node ["style"="filled"];' in inside
    assert '"a" ["label"="A"];' in inside
    assert source.count('"a" [') == 1, "declared once, inside its subgraph"
    assert dot.node_order() == ["a", "b"]


def _hub() -> nx.DiGraph:
    graph = nx.DiGraph()
    graph.add_edge("x", "h")
    for leaf in "abcdefg":
        graph.add_edge("h", leaf)
    graph.add_edge("h", "m")
    graph.add_edge("m", "n")
    graph.add_edge("h", "p", minlen="4")
    for index in range(5):
        graph.add_node(f"i{index}")
    graph.add_edge("s", "s")
    return graph


def _edges(dot: DotGraph) -> set[tuple[str, str, str | None, str | None]]:
    return {(t, h, a.get("minlen"), a.get("style")) for t, h, a in dot.edges}


@pytest.mark.parametrize(
    ("levels", "fans", "chain", "expected"),
    [
        (
            3,
            True,
            2,
            {
                ("x", "h", "1", None),
                ("h", "a", "1", None),
                ("h", "b", "2", None),
                ("h", "c", "3", None),
                ("h", "d", "1", None),
                ("h", "e", "2", None),
                ("h", "f", "3", None),
                ("h", "g", "1", None),
                ("h", "m", "2", None),
                ("m", "n", "1", None),
                ("h", "p", "4", None),
                ("s", "s", None, None),
                ("i0", "i1", None, "invis"),
                ("i1", "i2", None, "invis"),
                ("i3", "i4", None, "invis"),
            },
        ),
        (
            5,
            False,
            3,
            {
                ("x", "h", "1", None),
                ("h", "a", "1", None),
                ("h", "b", "2", None),
                ("h", "c", "3", None),
                ("h", "d", "4", None),
                ("h", "e", "5", None),
                ("h", "f", "1", None),
                ("h", "g", "2", None),
                ("h", "m", None, None),
                ("m", "n", "1", None),
                ("h", "p", "4", None),
                ("s", "s", None, None),
                ("i0", "i1", None, "invis"),
                ("i1", "i2", None, "invis"),
                ("i2", "i3", None, "invis"),
            },
        ),
    ],
)
def test_unflatten_matches_the_graphviz_tool(levels, fans, chain, expected):
    """Leaves stagger over 1..levels, fans include chain nodes, isolated nodes chain."""
    dot = DotGraph.from_networkx(_hub()).unflatten(
        levels=levels, fans=fans, chain=chain
    )

    assert _edges(dot) == expected
