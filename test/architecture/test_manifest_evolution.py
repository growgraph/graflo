"""Tests for :mod:`graflo.architecture.evolution`."""

from __future__ import annotations

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import (
    MergeVerticesOp,
    RemoveVerticesOp,
    apply_evolution,
)
from graflo.architecture.schema.core import CoreSchema
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.metadata import GraphMetadata
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig
from graflo.migrate.io import manifest_hash


def _minimal_manifest() -> GraphManifest:
    """Two vertices (a, b), one edge a->b, two resources, version 1.0.0."""
    meta = GraphMetadata(name="test_graph", version="1.0.0")
    vc = VertexConfig(
        vertices=[
            Vertex(name="a", properties=[Field(name="id")], identity=["id"]),
            Vertex(name="b", properties=[Field(name="id")], identity=["id"]),
        ],
        force_types={},
    )
    ec = EdgeConfig(
        edges=[Edge(source="a", target="b", relation=None)],
    )
    core = CoreSchema(vertex_config=vc, edge_config=ec)
    schema = Schema(metadata=meta, core_schema=core)
    ingestion = {
        "resources": [
            {
                "name": "r_a",
                "apply": [{"vertex": "a"}],
            },
            {
                "name": "r_b",
                "apply": [{"vertex": "b"}],
            },
        ],
        "transforms": [],
    }
    return GraphManifest.from_config(
        {"schema": schema.to_dict(skip_defaults=False), "ingestion_model": ingestion}
    )


def test_remove_vertex_cascade_and_hash_changes() -> None:
    m = _minimal_manifest()
    m.finish_init()
    h_before = manifest_hash(m)

    out = apply_evolution(
        m,
        [RemoveVerticesOp(op="remove_vertices", names=["b"])],
        bump_version=False,
    )

    assert out.graph_schema is not None
    assert "b" not in out.graph_schema.core_schema.vertex_config.vertex_set
    assert out.graph_schema.core_schema.edge_config.edges == []
    assert out.ingestion_model is not None
    assert len(out.ingestion_model.resources) == 1
    assert out.ingestion_model.resources[0].name == "r_a"
    assert manifest_hash(out) != h_before


def test_remove_vertices_bumps_minor_version_by_default() -> None:
    m = _minimal_manifest()
    m.finish_init()
    out = apply_evolution(m, [RemoveVerticesOp(op="remove_vertices", names=["b"])])
    assert out.graph_schema is not None
    assert out.graph_schema.metadata.version == "1.1.0"


def test_remove_vertices_unknown_vertex_raises() -> None:
    m = _minimal_manifest()
    m.finish_init()
    with pytest.raises(ValueError, match="Unknown vertices"):
        apply_evolution(
            m,
            [RemoveVerticesOp(op="remove_vertices", names=["nope"])],
            bump_version=False,
        )


def test_remove_vertices_empty_ingestion_raises() -> None:
    m = GraphManifest.from_config(
        {
            "schema": Schema(
                metadata=GraphMetadata(name="g", version="1.0.0"),
                core_schema=CoreSchema(
                    vertex_config=VertexConfig(
                        vertices=[
                            Vertex(
                                name="only",
                                properties=[Field(name="id")],
                                identity=["id"],
                            )
                        ]
                    ),
                    edge_config=EdgeConfig(edges=[]),
                ),
            ).to_dict(skip_defaults=False),
            "ingestion_model": {
                "resources": [{"name": "r1", "apply": [{"vertex": "only"}]}],
                "transforms": [],
            },
        }
    )
    m.finish_init()
    with pytest.raises(ValueError, match="empty"):
        apply_evolution(
            m,
            [RemoveVerticesOp(op="remove_vertices", names=["only"])],
            bump_version=False,
        )


def test_merge_vertices_into_new_name() -> None:
    m = _minimal_manifest()
    m.finish_init()
    h_before = manifest_hash(m)

    out = apply_evolution(
        m,
        [
            MergeVerticesOp(
                op="merge_vertices",
                sources=["a", "b"],
                into="ab",
            )
        ],
        bump_version=False,
    )

    schema = out.require_schema()
    vs = schema.core_schema.vertex_config.vertex_set
    assert vs == {"ab"}
    edges = schema.core_schema.edge_config.edges
    assert len(edges) == 1
    assert edges[0].source == "ab" and edges[0].target == "ab"
    assert out.ingestion_model is not None
    assert len(out.ingestion_model.resources) == 2
    assert manifest_hash(out) != h_before


def test_merge_vertices_into_existing_canonical() -> None:
    m = _minimal_manifest()
    m.finish_init()
    out = apply_evolution(
        m,
        [MergeVerticesOp(op="merge_vertices", sources=["b"], into="a")],
        bump_version=False,
    )
    schema = out.require_schema()
    vs = schema.core_schema.vertex_config.vertex_set
    assert vs == {"a"}
    edges = schema.core_schema.edge_config.edges
    assert len(edges) == 1
    assert edges[0].source == "a" and edges[0].target == "a"


def test_merge_vertices_rejects_into_in_sources() -> None:
    m = _minimal_manifest()
    m.finish_init()
    with pytest.raises(ValueError, match="must not"):
        apply_evolution(
            m,
            [MergeVerticesOp(op="merge_vertices", sources=["a", "into"], into="into")],
            bump_version=False,
        )
