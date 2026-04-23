from graflo.architecture.contract import GraphManifest
from graflo.architecture.schema import Schema


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
                        }
                    ]
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

    renamed = manifest.rename_entities(
        vertices={"person": "author", "company": "institution"},
        edges=lambda relation: f"rel_{relation}",
        resources={"employees": "staff"},
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


def test_schema_rename_entities_updates_vertices_and_edge_relations() -> None:
    schema = Schema.from_dict(_sample_manifest_payload()["schema"])

    renamed = schema.rename_entities(
        vertices={"person": "author", "company": "institution"},
        edges={"works_at": "affiliated_with"},
    )

    assert renamed.core_schema.vertex_config.vertex_set == {"author", "institution"}
    assert renamed.core_schema.edge_config.edges[0].edge_id == (
        "author",
        "institution",
        "affiliated_with",
    )
