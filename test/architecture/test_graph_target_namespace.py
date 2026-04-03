"""Tests for GraphEngine target LPG namespace resolution."""

from graflo.architecture.contract.manifest import GraphManifest
from graflo.db import ArangoConfig, MemgraphConfig, NebulaConfig, TigergraphConfig
from graflo.hq.graph_engine import (
    _ensure_graph_target_namespace,
    _resolve_graph_target_namespace,
)


def _minimal_manifest(metadata_name: str = "logical_schema") -> GraphManifest:
    return GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": metadata_name},
                "core_schema": {
                    "vertex_config": {
                        "vertices": [
                            {"name": "v", "properties": ["id"], "identity": ["id"]}
                        ]
                    },
                    "edge_config": {"edges": []},
                },
            },
            "ingestion_model": {"resources": []},
        }
    )


def test_resolve_prefers_call_arg_then_profile_then_metadata() -> None:
    manifest = _minimal_manifest("meta_only")
    schema = manifest.require_schema()
    assert _resolve_graph_target_namespace(schema, "call") == "call"
    schema.db_profile.target_namespace = "profile"
    assert _resolve_graph_target_namespace(schema, "call2") == "call2"
    assert _resolve_graph_target_namespace(schema, None) == "profile"
    schema.db_profile.target_namespace = None
    assert _resolve_graph_target_namespace(schema, None) == "meta_only"


def test_ensure_sets_arango_database_when_unset() -> None:
    manifest = _minimal_manifest("mygraph")
    schema = manifest.require_schema()
    cfg = ArangoConfig(uri="http://localhost:8529", username="u", password="p")
    assert cfg.database is None
    _ensure_graph_target_namespace(schema, cfg, None)
    assert cfg.database == "mygraph"


def test_ensure_sets_tigergraph_schema_name_when_unset() -> None:
    manifest = _minimal_manifest("tg_graph")
    schema = manifest.require_schema()
    cfg = TigergraphConfig(uri="http://localhost:14240")
    assert cfg.schema_name is None
    _ensure_graph_target_namespace(schema, cfg, None)
    assert cfg.schema_name == "tg_graph"
    assert cfg.database is None


def test_ensure_sets_nebula_space_via_schema_name() -> None:
    manifest = _minimal_manifest("space_a")
    schema = manifest.require_schema()
    cfg = NebulaConfig(uri="http://localhost:9669")
    assert cfg.schema_name is None
    _ensure_graph_target_namespace(schema, cfg, "space_explicit")
    assert cfg.schema_name == "space_explicit"


def test_ensure_does_not_overwrite_memgraph_database_when_set() -> None:
    manifest = _minimal_manifest("would_clobber")
    schema = manifest.require_schema()
    cfg = MemgraphConfig(uri="bolt://localhost:7687", database="already_set")
    _ensure_graph_target_namespace(schema, cfg, None)
    assert cfg.database == "already_set"
