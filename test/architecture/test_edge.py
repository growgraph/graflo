import logging

import pytest

from graflo.architecture.edge import Edge, EdgeConfig
from graflo.architecture.database_features import DatabaseFeatures
from graflo.architecture.db_aware import EdgeConfigDBAware, VertexConfigDBAware
from graflo.architecture.onto import Weight
from graflo.architecture.vertex import VertexConfig
from graflo.onto import DBType

logger = logging.getLogger(__name__)


def test_weight_config_b(vertex_helper_b):
    wc = Weight.from_dict(vertex_helper_b)
    assert len(wc.fields) == 2


def test_init_edge(edge_with_weights):
    edge = Edge.from_dict(edge_with_weights)
    assert edge.weights is not None and len(edge.weights.vertices) == 2
    assert edge.identities == []


def test_init_edge_with_explicit_identities():
    edge = Edge.from_dict(
        {
            "source": "entity",
            "target": "entity",
            "identities": [["source", "target", "relation", "pub_id"]],
            "weights": {"direct": ["pub_id"]},
        }
    )
    assert edge.identities == [["source", "target", "relation", "pub_id"]]


def test_edge_rejects_legacy_indexes_field():
    with pytest.raises(ValueError):
        Edge.from_dict(
            {
                "source": "entity",
                "target": "entity",
                "indexes": [{"fields": ["pub_id"]}],
            }
        )


def test_edge_identities_require_declared_direct_fields(vertex_config_kg):
    vertex_config = VertexConfig.from_dict(vertex_config_kg)
    edge = Edge.from_dict(
        {
            "source": "entity",
            "target": "entity",
            "identities": [["source", "target", "relation", "pub_id"]],
        }
    )
    with pytest.raises(ValueError, match="unknown identity fields"):
        edge.finish_init(vertex_config)


def test_edge_config(vertex_config_kg, edge_config_kg):
    vertex_config = VertexConfig.from_dict(vertex_config_kg)
    e = EdgeConfig.from_dict(edge_config_kg)
    e.finish_init(vertex_config)
    assert True


def test_edge_key(vertex_config_kg, edge_config_kg):
    vertex_config = VertexConfig.from_dict(vertex_config_kg)
    e = EdgeConfig.from_dict(edge_config_kg)
    e.finish_init(vertex_config)
    first_edge = next(iter(e.edges_list(include_aux=True)))
    assert first_edge.edge_id in e


def test_edge_finish_init_is_idempotent(vertex_config_kg):
    vertex_config = VertexConfig.from_dict(vertex_config_kg)
    edge = Edge.from_dict(
        {
            "source": "entity",
            "target": "entity",
            "identities": [["source", "target", "relation", "pub_id"]],
            "weights": {"direct": ["pub_id"]},
        }
    )
    edge.finish_init(vertex_config)
    first_identities = [list(key) for key in edge.identities]

    edge.finish_init(vertex_config)
    second_identities = [list(key) for key in edge.identities]

    assert second_identities == first_identities


def test_edge_finish_init_tigergraph_relation_artifacts_are_not_duplicated(
    vertex_config_kg,
):
    vertex_config = VertexConfig.from_dict(vertex_config_kg)
    e = Edge.from_dict(
        {
            "source": "entity",
            "target": "entity",
            "relation_from_key": True,
        }
    )

    db_features = DatabaseFeatures(db_flavor=DBType.TIGERGRAPH)
    vc_db = VertexConfigDBAware(vertex_config, db_features)
    ec_db = EdgeConfigDBAware(EdgeConfig(edges=[e]), vc_db, db_features)
    first_weights = ec_db.effective_weights(e)
    second_weights = ec_db.effective_weights(e)
    first_direct_names = list(
        first_weights.direct_names if first_weights is not None else []
    )
    second_direct_names = list(
        second_weights.direct_names if second_weights is not None else []
    )

    assert second_direct_names == first_direct_names
