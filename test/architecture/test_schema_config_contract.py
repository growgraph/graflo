import pytest

from graflo.architecture.schema import IngestionModel, Schema


def _minimal_graph() -> dict:
    return {
        "vertex_config": {
            "vertices": [{"name": "person", "fields": ["id"], "identity": ["id"]}]
        },
        "edge_config": {"edges": []},
    }


def test_schema_from_config_rejects_top_level_resources():
    cfg = {
        "metadata": {"name": "kg"},
        "graph": _minimal_graph(),
        "resources": [{"resource_name": "people", "pipeline": [{"vertex": "person"}]}],
    }
    with pytest.raises(ValueError, match="Unknown top-level keys"):
        Schema.from_config(cfg)


def test_ingestion_from_config_requires_nested_ingestion_model():
    cfg = {
        "metadata": {"name": "kg"},
        "graph": _minimal_graph(),
    }
    with pytest.raises(ValueError, match="Missing required 'ingestion_model'"):
        IngestionModel.from_config(cfg)


def test_ingestion_from_config_rejects_unknown_ingestion_keys():
    cfg = {
        "metadata": {"name": "kg"},
        "graph": _minimal_graph(),
        "ingestion_model": {"resources": [], "legacy": True},
    }
    with pytest.raises(ValueError, match="Unknown keys under ingestion_model"):
        IngestionModel.from_config(cfg)
