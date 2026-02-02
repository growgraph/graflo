import logging

from graflo.architecture.edge import Edge, EdgeConfig
from graflo.architecture.onto import Index, Weight
from graflo.architecture.vertex import VertexConfig

logger = logging.getLogger(__name__)


def test_weight_config_b(vertex_helper_b):
    wc = Weight.from_dict(vertex_helper_b)
    assert len(wc.fields) == 2


def test_init_edge(edge_with_weights):
    vc = Edge.from_dict(edge_with_weights)
    assert vc.weights is not None and len(vc.weights.vertices) == 2
    assert len(vc.indexes) == 0


def test_index_a(index_a):
    ci = Index.from_dict(index_a)
    assert len(ci.fields) == 2


def test_init_edge_indexes(edge_indexes):
    e = Edge.from_dict(edge_indexes)
    assert len(e.indexes) == 2
    assert e.purpose == "aux"


def test_complement_edge_init(edge_indexes, vertex_config_kg):
    vertex_config = VertexConfig.from_dict(vertex_config_kg)
    e = Edge.from_dict(edge_indexes)
    e.finish_init(vertex_config)
    assert len(e.indexes) == 2


def test_edge_with_vertex_index_init(edge_with_vertex_indexes, vertex_config_kg):
    vertex_config = VertexConfig.from_dict(vertex_config_kg)
    e = Edge.from_dict(edge_with_vertex_indexes)
    e.finish_init(vertex_config)
    assert e.indexes[0].fields == [
        "_from",
        "_to",
        "publication@arxiv",
        "publication@doi",
    ]
    assert e.indexes[1].fields == ["publication@_key"]


def test_edge_config(vertex_config_kg, edge_config_kg):
    vertex_config = VertexConfig.from_dict(vertex_config_kg)
    e = EdgeConfig.from_dict(edge_config_kg)
    e.finish_init(vertex_config)
    assert True


def test_edge_key(vertex_config_kg, edge_config_kg):
    vertex_config = VertexConfig.from_dict(vertex_config_kg)
    e = EdgeConfig.from_dict(edge_config_kg)
    e.finish_init(vertex_config)
    assert True
