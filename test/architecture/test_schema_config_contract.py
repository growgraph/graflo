import pytest

from graflo.architecture.pipeline.runtime.actor.config import EdgeRouterActorConfig
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.schema import Schema


def _minimal_graph() -> dict:
    return {
        "vertex_config": {
            "vertices": [{"name": "person", "properties": ["id"], "identity": ["id"]}]
        },
        "edge_config": {"edges": []},
    }


def test_manifest_requires_nested_schema_block():
    cfg = {
        "metadata": {"name": "kg"},
        "core_schema": _minimal_graph(),
    }
    with pytest.raises(ValueError):
        GraphManifest.from_config(cfg)


def test_manifest_requires_at_least_one_block():
    with pytest.raises(ValueError, match="GraphManifest requires at least one block"):
        GraphManifest.from_config({})


def test_manifest_accepts_nested_schema_and_ingestion():
    cfg = {
        "schema": {"metadata": {"name": "kg"}, "core_schema": _minimal_graph()},
        "ingestion_model": {"resources": []},
    }
    manifest = GraphManifest.from_config(cfg)
    assert manifest.graph_schema is not None
    assert manifest.ingestion_model is not None


def test_manifest_accepts_ingestion_transform_list():
    cfg = {
        "schema": {"metadata": {"name": "kg"}, "core_schema": _minimal_graph()},
        "ingestion_model": {
            "resources": [],
            "transforms": [
                {
                    "name": "normalize_id",
                    "foo": "split_keep_part",
                    "module": "graflo.util.transform",
                    "input": ["id"],
                    "output": ["_key"],
                    "params": {"sep": "/", "keep": -1},
                }
            ],
        },
    }
    manifest = GraphManifest.from_config(cfg)
    assert manifest.ingestion_model is not None
    assert len(manifest.ingestion_model.transforms) == 1


def test_manifest_rejects_ingestion_transform_without_name():
    cfg = {
        "schema": {"metadata": {"name": "kg"}, "core_schema": _minimal_graph()},
        "ingestion_model": {
            "resources": [],
            "transforms": [
                {
                    "foo": "split_keep_part",
                    "module": "graflo.util.transform",
                    "input": ["id"],
                    "output": ["_key"],
                }
            ],
        },
    }
    with pytest.raises(ValueError, match="must define a non-empty name"):
        GraphManifest.from_config(cfg)


def test_manifest_rejects_duplicate_ingestion_transform_names():
    cfg = {
        "schema": {"metadata": {"name": "kg"}, "core_schema": _minimal_graph()},
        "ingestion_model": {
            "resources": [],
            "transforms": [
                {
                    "name": "normalize_id",
                    "foo": "split_keep_part",
                    "module": "graflo.util.transform",
                    "input": ["id"],
                    "output": ["_key"],
                },
                {
                    "name": "normalize_id",
                    "foo": "split_keep_part",
                    "module": "graflo.util.transform",
                    "input": ["other_id"],
                    "output": ["_key"],
                },
            ],
        },
    }
    with pytest.raises(ValueError, match="Duplicate ingestion transform names found"):
        GraphManifest.from_config(cfg)


def test_schema_rejects_edges_with_undefined_vertices():
    with pytest.raises(
        ValueError,
        match=r"edge_config references undefined vertices: \['company', 'person'\]",
    ) as exc_info:
        Schema.model_validate(
            {
                "metadata": {"name": "kg"},
                "core_schema": {
                    "vertex_config": {
                        "vertices": [
                            {"name": "user", "properties": ["id"], "identity": ["id"]},
                        ]
                    },
                    "edge_config": {
                        "edges": [
                            {"source": "user", "target": "person"},
                            {"source": "company", "target": "user"},
                        ]
                    },
                },
            }
        )

    # Error details should include currently declared vertices for easier debugging.
    assert "Declared vertices: ['user']" in str(exc_info.value)


def test_edge_router_requires_at_least_one_dynamic_type_side():
    with pytest.raises(
        ValueError,
        match=(
            "edge_router requires at least one of "
            "source_type_field or target_type_field"
        ),
    ):
        EdgeRouterActorConfig.model_validate(
            {
                "source": "person",
                "target": "institution",
                "source_fields": {"id": "source_id"},
                "target_fields": {"id": "target_id"},
            }
        )
