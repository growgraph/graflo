import logging

import pytest
from pydantic import ValidationError

from graflo.architecture.schema.edge import (
    DEFAULT_TIGERGRAPH_RELATION_WEIGHTNAME,
    Edge,
    EdgeConfig,
)
from graflo.architecture.database_features import DatabaseProfile
from graflo.architecture.schema import EdgeConfigDBAware, VertexConfigDBAware
from graflo.architecture.graph_types import Weight
from graflo.architecture.schema.vertex import VertexConfig
from graflo.architecture.contract.declarations.edge_derivation_registry import (
    EdgeDerivationRegistry,
)
from graflo.onto import DBType

logger = logging.getLogger(__name__)


def test_weight_config_b(vertex_helper_b):
    wc = Weight.from_dict(vertex_helper_b)
    assert len(wc.fields) == 2


def test_schema_edge_rejects_weights_key():
    with pytest.raises(ValidationError):
        Edge.from_dict(
            {
                "source": "analyst",
                "target": "agency",
                "weights": {"direct": ["x"]},
            }
        )


def test_init_edge_with_explicit_identities():
    edge = Edge.from_dict(
        {
            "source": "entity",
            "target": "entity",
            "identities": [["source", "target", "relation", "pub_id"]],
            "properties": ["pub_id"],
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


def test_edge_identities_merge_undeclared_tokens_into_properties(vertex_config_kg):
    """Identity fields not listed under ``properties`` are added like vertex identity."""
    vertex_config = VertexConfig.from_dict(vertex_config_kg)
    edge = Edge.from_dict(
        {
            "source": "entity",
            "target": "entity",
            "identities": [["source", "target", "relation", "pub_id"]],
        }
    )
    edge.finish_init(vertex_config)
    assert "pub_id" in edge.property_names
    assert "relation" in edge.property_names


def test_compile_identity_indexes_registers_each_identity_key(vertex_config_kg):
    vertex_config = VertexConfig.from_dict(vertex_config_kg)
    edge = Edge.from_dict(
        {
            "source": "entity",
            "target": "entity",
            "identities": [
                ["source", "target", "pub_id"],
                ["source", "target", "kind"],
            ],
        }
    )
    edge.finish_init(vertex_config)
    profile = DatabaseProfile(db_flavor=DBType.ARANGO)
    vc_db = VertexConfigDBAware(vertex_config, profile)
    ec_db = EdgeConfigDBAware(EdgeConfig(edges=[edge]), vc_db, profile)
    ec_db.compile_identity_indexes()
    indexes = profile.edge_secondary_indexes(edge.edge_id)
    field_sets = {tuple(ix.fields) for ix in indexes}
    assert field_sets == {("_from", "_to", "pub_id"), ("_from", "_to", "kind")}
    assert all(ix.unique for ix in indexes)


def test_compile_identity_indexes_arango_prepends_from_to_when_identity_omits_endpoints(
    vertex_config_kg,
):
    """Relationship-only identity tokens still get _from/_to on Arango unique indexes."""
    vertex_config = VertexConfig.from_dict(vertex_config_kg)
    edge = Edge.from_dict(
        {
            "source": "mention",
            "target": "mention",
            "identities": [["_role"]],
        }
    )
    edge.finish_init(vertex_config)
    profile = DatabaseProfile(db_flavor=DBType.ARANGO)
    vc_db = VertexConfigDBAware(vertex_config, profile)
    ec_db = EdgeConfigDBAware(EdgeConfig(edges=[edge]), vc_db, profile)
    ec_db.compile_identity_indexes()
    indexes = profile.edge_secondary_indexes(edge.edge_id)
    assert len(indexes) == 1
    assert tuple(indexes[0].fields) == ("_from", "_to", "_role")
    assert indexes[0].unique is True


def test_compile_identity_indexes_neo4j_property_indexes_not_globally_unique(
    vertex_config_kg,
):
    """LPG backends cannot encode endpoints in rel property constraints; no bogus UNIQUE."""
    vertex_config = VertexConfig.from_dict(vertex_config_kg)
    edge = Edge.from_dict(
        {
            "source": "mention",
            "target": "mention",
            "identities": [["_role"]],
        }
    )
    edge.finish_init(vertex_config)
    profile = DatabaseProfile(db_flavor=DBType.NEO4J)
    vc_db = VertexConfigDBAware(vertex_config, profile)
    ec_db = EdgeConfigDBAware(EdgeConfig(edges=[edge]), vc_db, profile)
    ec_db.compile_identity_indexes()
    indexes = profile.edge_secondary_indexes(edge.edge_id)
    assert len(indexes) == 1
    assert tuple(indexes[0].fields) == ("_role",)
    assert indexes[0].unique is False


def test_relationship_merge_property_names_uses_first_identity_only(vertex_config_kg):
    vertex_config = VertexConfig.from_dict(vertex_config_kg)
    edge = Edge.from_dict(
        {
            "source": "entity",
            "target": "entity",
            "identities": [
                ["source", "target", "relation", "pub_id"],
                ["source", "target", "kind"],
            ],
        }
    )
    edge.finish_init(vertex_config)
    profile = DatabaseProfile(db_flavor=DBType.NEO4J)
    vc_db = VertexConfigDBAware(vertex_config, profile)
    ec_db = EdgeConfigDBAware(EdgeConfig(edges=[edge]), vc_db, profile)
    assert ec_db.relationship_merge_property_names(edge) == ["relation", "pub_id"]


def test_edge_config(vertex_config_kg, edge_config_kg):
    vertex_config = VertexConfig.from_dict(vertex_config_kg)
    e = EdgeConfig.from_dict(edge_config_kg)
    e.finish_init(vertex_config)
    assert True


def test_edge_key(vertex_config_kg, edge_config_kg):
    vertex_config = VertexConfig.from_dict(vertex_config_kg)
    e = EdgeConfig.from_dict(edge_config_kg)
    e.finish_init(vertex_config)
    first_edge = next(e.values())
    assert first_edge.edge_id in e


def test_edge_finish_init_is_idempotent(vertex_config_kg):
    vertex_config = VertexConfig.from_dict(vertex_config_kg)
    edge = Edge.from_dict(
        {
            "source": "entity",
            "target": "entity",
            "identities": [["source", "target", "relation", "pub_id"]],
            "properties": ["pub_id"],
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
        }
    )
    e.finish_init(vertex_config)
    ec = EdgeConfig(edges=[e])

    db_features = DatabaseProfile(db_flavor=DBType.TIGERGRAPH)
    vc_db = VertexConfigDBAware(vertex_config, db_features)
    overlay = EdgeDerivationRegistry()
    overlay.mark_relation_from_key(e.edge_id)
    ec_db = EdgeConfigDBAware(ec, vc_db, db_features, ingestion_overlay=overlay)
    first_weights = ec_db.effective_weights(e)
    second_weights = ec_db.effective_weights(e)
    first_direct_names = list(
        first_weights.direct_names if first_weights is not None else []
    )
    second_direct_names = list(
        second_weights.direct_names if second_weights is not None else []
    )

    assert second_direct_names == first_direct_names


def test_schema_edge_rejects_relation_field():
    with pytest.raises(ValidationError):
        Edge.from_dict(
            {
                "source": "entity",
                "target": "entity",
                "relation_field": "rel",
            }
        )


def test_tigergraph_effective_weights_adds_default_relation_attr(vertex_config_kg):
    vertex_config = VertexConfig.from_dict(vertex_config_kg)
    edge = Edge.from_dict(
        {
            "source": "entity",
            "target": "entity",
            "properties": ["date"],
        }
    )
    edge.finish_init(vertex_config)
    db_features = DatabaseProfile(db_flavor=DBType.TIGERGRAPH)
    vc_db = VertexConfigDBAware(vertex_config, db_features)
    ec_db = EdgeConfigDBAware(EdgeConfig(edges=[edge]), vc_db, db_features)
    w = ec_db.effective_weights(edge)
    assert w is not None
    assert DEFAULT_TIGERGRAPH_RELATION_WEIGHTNAME in w.direct_names
    assert "date" in w.direct_names
    rt = ec_db.runtime(edge)
    assert rt.effective_relation_field == DEFAULT_TIGERGRAPH_RELATION_WEIGHTNAME
    assert rt.store_extracted_relation_as_weight is True


def test_tigergraph_runtime_fixed_relation_has_no_relation_attr(vertex_config_kg):
    vertex_config = VertexConfig.from_dict(vertex_config_kg)
    edge = Edge.from_dict(
        {
            "source": "entity",
            "target": "entity",
            "relation": "KNOWS",
            "properties": ["date"],
        }
    )
    edge.finish_init(vertex_config)
    db_features = DatabaseProfile(db_flavor=DBType.TIGERGRAPH)
    vc_db = VertexConfigDBAware(vertex_config, db_features)
    ec_db = EdgeConfigDBAware(EdgeConfig(edges=[edge]), vc_db, db_features)
    rt = ec_db.runtime(edge)
    assert rt.effective_relation_field is None
    assert rt.store_extracted_relation_as_weight is False
    w = ec_db.effective_weights(edge)
    assert w is not None and w.direct_names == ["date"]


def test_relationship_merge_property_names_defaults_to_direct_weights(
    vertex_config_kg,
):
    vertex_config = VertexConfig.from_dict(vertex_config_kg)
    edge = Edge.from_dict(
        {
            "source": "entity",
            "target": "entity",
            "properties": ["date", "relation"],
        }
    )
    edge.finish_init(vertex_config)
    profile = DatabaseProfile(db_flavor=DBType.NEO4J)
    vc_db = VertexConfigDBAware(vertex_config, profile)
    ec_db = EdgeConfigDBAware(EdgeConfig(edges=[edge]), vc_db, profile)
    assert ec_db.relationship_merge_property_names(edge) == ["date", "relation"]


def test_relationship_merge_property_names_prefers_first_identity(
    vertex_config_kg,
):
    vertex_config = VertexConfig.from_dict(vertex_config_kg)
    edge = Edge.from_dict(
        {
            "source": "entity",
            "target": "entity",
            "identities": [["source", "target", "relation", "pub_id"]],
            "properties": ["pub_id"],
        }
    )
    edge.finish_init(vertex_config)
    profile = DatabaseProfile(db_flavor=DBType.NEO4J)
    vc_db = VertexConfigDBAware(vertex_config, profile)
    ec_db = EdgeConfigDBAware(EdgeConfig(edges=[edge]), vc_db, profile)
    assert ec_db.relationship_merge_property_names(edge) == ["relation", "pub_id"]
