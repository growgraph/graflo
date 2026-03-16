from types import MethodType, SimpleNamespace

import networkx as nx

from graflo.architecture.edge import Edge
from graflo.plot.plotter import ManifestPlotter


class _EdgeConfigStub:
    def __init__(self, edges: dict):
        self._edges = edges

    def edges_items(self):
        return self._edges.items()


class _AgraphStub:
    def __init__(self, graph):
        self.graph = graph
        self.graph_attr = {}
        self.subgraphs: list[dict[str, object]] = []

    def add_subgraph(self, nodes, name, rank, label=None):
        self.subgraphs.append(
            {
                "nodes": list(nodes),
                "name": name,
                "rank": rank,
                "label": label,
            }
        )
        return SimpleNamespace(node_attr={})


def _build_plotter(
    configured_edges: dict,
    vertex_set: set[str],
) -> ManifestPlotter:
    plotter = ManifestPlotter.__new__(ManifestPlotter)
    plotter.output_format = "pdf"
    plotter.output_dpi = None
    plotter.fig_path = "."
    plotter.name = "test_schema"
    plotter.prefix = "test_schema"
    plotter.schema = SimpleNamespace(
        metadata=SimpleNamespace(name="test_schema"),
        graph=SimpleNamespace(
            edge_config=_EdgeConfigStub(configured_edges),
            vertex_config=SimpleNamespace(vertex_set=vertex_set),
        ),
    )
    plotter.ingestion_model = SimpleNamespace(resources=[])
    plotter._draw = MethodType(lambda self, ag, stem, prog="dot": None, plotter)
    return plotter


def test_plot_vc2vc_filters_unknown_endpoints_and_logs_error(monkeypatch, caplog):
    configured_edge = Edge.from_dict({"source": "a", "target": "b", "relation": "ab"})
    discovered_invalid_edge = Edge.from_dict({"source": "ghost", "target": "a"})

    plotter = _build_plotter(
        configured_edges={configured_edge.edge_id: configured_edge},
        vertex_set={"a", "b"},
    )
    monkeypatch.setattr(
        plotter,
        "_discover_edges_from_resources",
        lambda: {discovered_invalid_edge.edge_id: discovered_invalid_edge},
    )

    captured = {}

    def _fake_to_agraph(graph):
        captured["ag"] = _AgraphStub(graph)
        return captured["ag"]

    monkeypatch.setattr(nx.nx_agraph, "to_agraph", _fake_to_agraph)

    with caplog.at_level("ERROR"):
        plotter.plot_vc2vc(include_all_vertices=False)

    graph = captured["ag"].graph
    assert "vertex:ghost" not in graph.nodes
    assert ("vertex:a", "vertex:b", 0) in graph.edges
    assert "ignored 1 edge(s) with unknown vertices" in caplog.text


def test_plot_vc2vc_preserves_labels_and_partition_grouping(monkeypatch):
    edge_relation_field = Edge.from_dict(
        {"source": "a", "target": "b", "relation_field": "edge_kind"}
    )
    edge_relation_key = Edge.from_dict(
        {"source": "b", "target": "c", "relation_from_key": True}
    )

    plotter = _build_plotter(
        configured_edges={
            edge_relation_field.edge_id: edge_relation_field,
            edge_relation_key.edge_id: edge_relation_key,
        },
        vertex_set={"a", "b", "c"},
    )
    monkeypatch.setattr(plotter, "_discover_edges_from_resources", lambda: {})

    captured = {}

    def _fake_to_agraph(graph):
        captured["ag"] = _AgraphStub(graph)
        return captured["ag"]

    monkeypatch.setattr(nx.nx_agraph, "to_agraph", _fake_to_agraph)

    plotter.plot_vc2vc(
        include_all_vertices=False,
        partition={"a": "left", "b": "left", "c": "right"},
        color_by_partition=True,
        group_by_partition=True,
    )

    graph = captured["ag"].graph
    assert graph.edges["vertex:a", "vertex:b", 0]["label"] == "[edge_kind]"
    assert graph.edges["vertex:b", "vertex:c", 0]["label"] == "[key]"
    assert graph.nodes["vertex:a"]["fillcolor"]
    assert graph.nodes["vertex:c"]["fillcolor"]

    subgraph_names = {entry["name"] for entry in captured["ag"].subgraphs}
    assert "cluster_partition_left" in subgraph_names
    assert "cluster_partition_right" in subgraph_names
