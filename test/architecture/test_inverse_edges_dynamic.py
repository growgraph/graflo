from __future__ import annotations

from graflo.architecture.contract import GraphManifest
from graflo.architecture.evolution import AddInverseEdgesOp, apply_evolution


def _dynamic_manifest() -> dict:
    return {
        "schema": {
            "metadata": {"name": "demo", "version": "1.0.0"},
            "graph": {
                "vertex_config": {
                    "vertices": [
                        {"name": "person", "identity": ["id"], "properties": ["id"]},
                        {
                            "name": "institution",
                            "identity": ["id"],
                            "properties": ["id"],
                        },
                    ]
                },
                "edge_config": {
                    "edges": [
                        {
                            "source": "person",
                            "target": "institution",
                            "relation": "employed_by",
                        }
                    ]
                },
            },
        },
        "ingestion_model": {
            "resources": [
                {
                    "name": "relations",
                    "pipeline": [
                        {
                            "vertex_router": {
                                "type_field": "source_type",
                                "role": "source",
                            }
                        },
                        {
                            "vertex_router": {
                                "type_field": "target_type",
                                "role": "target",
                            }
                        },
                        {
                            "edge": {
                                "source_role": "source",
                                "target_role": "target",
                                "relation_field": "relation_type",
                                "relation_map": {"EMPLOYED_BY": "employed_by"},
                            }
                        },
                    ],
                }
            ]
        },
    }


def test_inverse_edges_dynamic_edge_actor() -> None:
    manifest = GraphManifest.from_dict(_dynamic_manifest())
    out = apply_evolution(
        manifest,
        [AddInverseEdgesOp(relations={"employed_by": "employs"})],
        bump_version=False,
    )
    assert out.ingestion_model is not None
    resource = out.ingestion_model.resources[0]
    edge_steps = [s["edge"] for s in resource.pipeline if "edge" in s]
    assert len(edge_steps) == 2
    inverse = edge_steps[1]
    assert inverse["source_role"] == "target"
    assert inverse["target_role"] == "source"
    assert inverse["relation_field"] == "relation_type"
    assert inverse["relation_map"] == {"EMPLOYED_BY": "employs"}

    assert out.graph_schema is not None
    edge_ids = [e.edge_id for e in out.graph_schema.core_schema.edge_config.edges]
    assert ("institution", "person", "employs") in edge_ids


def test_inverse_edges_skips_undirected() -> None:
    payload = _dynamic_manifest()
    payload["schema"]["graph"]["edge_config"]["edges"][0]["directed"] = False
    manifest = GraphManifest.from_dict(payload)
    out = apply_evolution(
        manifest,
        [AddInverseEdgesOp(relations={"employed_by": "employs"})],
        bump_version=False,
    )
    assert out.graph_schema is not None
    edge_ids = [e.edge_id for e in out.graph_schema.core_schema.edge_config.edges]
    assert ("institution", "person", "employs") not in edge_ids
    assert out.ingestion_model is not None
    edge_steps = [s for s in out.ingestion_model.resources[0].pipeline if "edge" in s]
    assert len(edge_steps) == 1


def test_inverse_edges_skips_tigergraph_reverse_edge_spec() -> None:
    payload = _dynamic_manifest()
    payload["schema"]["db_profile"] = {
        "db_flavor": "tigergraph",
        "edge_specs": [
            {
                "source": "person",
                "target": "institution",
                "relation": "employed_by",
                "reverse_edge": "employs",
            }
        ],
    }
    manifest = GraphManifest.from_dict(payload)
    out = apply_evolution(
        manifest,
        [AddInverseEdgesOp(relations={"employed_by": "employs"})],
        bump_version=False,
    )
    assert out.graph_schema is not None
    edge_ids = [e.edge_id for e in out.graph_schema.core_schema.edge_config.edges]
    assert ("institution", "person", "employs") not in edge_ids
