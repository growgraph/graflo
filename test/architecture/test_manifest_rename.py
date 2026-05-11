import pytest

from graflo.architecture.contract import GraphManifest
from graflo.architecture.evolution import (
    AddEdgePropertiesOp,
    AddVertexPropertiesOp,
    MergeEdgesOp,
    RemoveEdgesOp,
    RemoveEdgePropertiesOp,
    RenameEdgePropertiesOp,
    RenameRelationsOp,
    RenameResourcesOp,
    RenameVerticesOp,
    apply_evolution,
)


def _sample_manifest_payload() -> dict:
    return {
        "schema": {
            "metadata": {"name": "demo", "version": "1.0.0"},
            "graph": {
                "vertex_config": {
                    "vertices": [
                        {"name": "person", "identity": ["id"], "properties": ["id"]},
                        {"name": "company", "identity": ["id"], "properties": ["id"]},
                    ],
                    "blank_vertices": ["company"],
                    "force_types": {"person": ["STRING"]},
                },
                "edge_config": {
                    "edges": [
                        {
                            "source": "person",
                            "target": "company",
                            "relation": "works_at",
                            "properties": ["since", "weight"],
                            "identities": [["source", "target", "since"]],
                        },
                        {
                            "source": "person",
                            "target": "company",
                            "relation": "employee_of",
                            "properties": ["since"],
                        },
                    ]
                },
            },
            "db_profile": {
                "db_flavor": "tigergraph",
                "edge_specs": [
                    {
                        "source": "person",
                        "target": "company",
                        "relation": "works_at",
                        "indexes": [{"fields": ["since", "weight"]}],
                    },
                    {
                        "source": "person",
                        "target": "company",
                        "relation": "employee_of",
                        "indexes": [{"fields": ["since"]}],
                    },
                ],
                "default_property_values": {
                    "vertices": {"person": {"id": "unknown"}},
                    "edges": [
                        {
                            "source": "person",
                            "target": "company",
                            "relation": "works_at",
                            "values": {"since": "2000", "weight": 1},
                        }
                    ],
                },
            },
        },
        "ingestion_model": {
            "resources": [
                {
                    "name": "employees",
                    "pipeline": [
                        {"vertex": "person"},
                        {
                            "vertex_router": {
                                "type_field": "kind",
                                "type_map": {"org": "company"},
                                "vertex_from_map": {"company": {"id": "company_id"}},
                            }
                        },
                        {
                            "edge": {
                                "from": "person",
                                "to": "company",
                                "relation": "works_at",
                                "properties": [{"name": "since"}, {"name": "weight"}],
                            }
                        },
                    ],
                    "infer_edge_only": [
                        {
                            "source": "person",
                            "target": "company",
                            "relation": "works_at",
                        }
                    ],
                    "extra_weights": [
                        {
                            "edge": {
                                "source": "person",
                                "target": "company",
                                "relation": "works_at",
                            }
                        }
                    ],
                }
            ]
        },
        "bindings": {
            "connectors": [
                {
                    "name": "employees_file",
                    "regex": "employees.csv",
                    "resource_name": "employees",
                }
            ],
            "resource_connector": [
                {"resource": "employees", "connector": "employees_file"}
            ],
        },
    }


def test_graph_manifest_rename_entities_updates_schema_ingestion_and_bindings() -> None:
    manifest = GraphManifest.from_dict(_sample_manifest_payload())

    renamed = apply_evolution(
        manifest,
        [
            RenameVerticesOp(vertices={"person": "author", "company": "institution"}),
            RenameRelationsOp(relations={"works_at": "rel_works_at"}),
            RenameResourcesOp(resources={"employees": "staff"}),
        ],
        bump_version=False,
    )

    assert renamed.graph_schema is not None
    assert renamed.ingestion_model is not None
    assert renamed.bindings is not None

    assert renamed.graph_schema.core_schema.vertex_config.vertex_set == {
        "author",
        "institution",
    }
    assert renamed.graph_schema.core_schema.edge_config.edges[0].edge_id == (
        "author",
        "institution",
        "rel_works_at",
    )

    resource = renamed.ingestion_model.resources[0]
    assert resource.name == "staff"
    assert resource.pipeline[0]["vertex"] == "author"
    edge_step = resource.pipeline[2]["edge"]
    assert edge_step["from"] == "author"
    assert edge_step["to"] == "institution"
    assert edge_step["relation"] == "rel_works_at"
    assert resource.infer_edge_only[0].edge_id == (
        "author",
        "institution",
        "rel_works_at",
    )
    assert resource.extra_weights[0].edge.edge_id == (
        "author",
        "institution",
        "rel_works_at",
    )

    assert renamed.bindings.connectors[0].resource_name == "staff"
    resource_binding = renamed.bindings.resource_connector[0]
    if isinstance(resource_binding, dict):
        assert resource_binding["resource"] == "staff"
    else:
        assert resource_binding.resource == "staff"


def test_apply_rename_relations_updates_schema_vertices_and_edge_relations() -> None:
    manifest = GraphManifest.from_dict(_sample_manifest_payload())
    renamed = apply_evolution(
        manifest,
        [
            RenameVerticesOp(vertices={"person": "author", "company": "institution"}),
            RenameRelationsOp(relations={"works_at": "affiliated_with"}),
        ],
        bump_version=False,
    )

    assert renamed.graph_schema is not None
    assert renamed.graph_schema.core_schema.vertex_config.vertex_set == {
        "author",
        "institution",
    }
    assert renamed.graph_schema.core_schema.edge_config.edges[0].edge_id == (
        "author",
        "institution",
        "affiliated_with",
    )


def test_remove_edges_prunes_schema_ingestion_and_profile() -> None:
    manifest = GraphManifest.from_dict(_sample_manifest_payload())
    out = apply_evolution(
        manifest, [RemoveEdgesOp(relations=["works_at"])], bump_version=False
    )
    assert out.graph_schema is not None
    assert {
        edge.relation for edge in out.graph_schema.core_schema.edge_config.edges
    } == {"employee_of"}
    assert out.ingestion_model is not None
    resource = out.ingestion_model.resources[0]
    assert resource.infer_edge_only == []
    assert resource.extra_weights == []
    assert out.graph_schema.db_profile.edge_specs[0].relation == "employee_of"


def test_merge_edges_canonicalizes_relations() -> None:
    manifest = GraphManifest.from_dict(_sample_manifest_payload())
    out = apply_evolution(
        manifest,
        [MergeEdgesOp(sources=["works_at", "employee_of"], into="employed_by")],
        bump_version=False,
    )
    assert out.graph_schema is not None
    assert [
        edge.relation for edge in out.graph_schema.core_schema.edge_config.edges
    ] == ["employed_by"]
    assert out.ingestion_model is not None
    relation = out.ingestion_model.resources[0].infer_edge_only[0].relation
    assert relation == "employed_by"


def test_rename_and_remove_edge_properties_updates_schema_ingestion_and_profile() -> (
    None
):
    manifest = GraphManifest.from_dict(_sample_manifest_payload())
    out = apply_evolution(
        manifest,
        [
            RenameEdgePropertiesOp(renames={"works_at": {"since": "started_at"}}),
            RemoveEdgePropertiesOp(removals={"works_at": ["weight"]}),
        ],
        bump_version=False,
    )
    assert out.graph_schema is not None
    edge = next(
        edge
        for edge in out.graph_schema.core_schema.edge_config.edges
        if edge.relation == "works_at"
    )
    assert [field.name for field in edge.properties] == ["started_at"]
    assert edge.identities == [["source", "target", "started_at"]]
    assert out.graph_schema.db_profile.edge_specs[0].indexes[0].fields == ["started_at"]
    assert out.ingestion_model is not None
    edge_step = out.ingestion_model.resources[0].pipeline[2]["edge"]
    assert edge_step["properties"] == [{"name": "started_at"}]


def test_add_vertex_and_edge_properties() -> None:
    manifest = GraphManifest.from_dict(_sample_manifest_payload())
    out = apply_evolution(
        manifest,
        [
            AddVertexPropertiesOp(additions={"person": ["canonical_id"]}),
            AddEdgePropertiesOp(additions={"works_at": ["confidence"]}),
        ],
        bump_version=False,
    )
    assert out.graph_schema is not None
    person = next(
        vertex
        for vertex in out.graph_schema.core_schema.vertex_config.vertices
        if vertex.name == "person"
    )
    assert "canonical_id" in {field.name for field in person.properties}
    works_at = next(
        edge
        for edge in out.graph_schema.core_schema.edge_config.edges
        if edge.relation == "works_at"
    )
    assert "confidence" in {field.name for field in works_at.properties}


def test_remove_edge_properties_rejects_identity_fields() -> None:
    manifest = GraphManifest.from_dict(_sample_manifest_payload())
    with pytest.raises(ValueError):
        apply_evolution(
            manifest,
            [RemoveEdgePropertiesOp(removals={"works_at": ["since"]})],
            bump_version=False,
        )
