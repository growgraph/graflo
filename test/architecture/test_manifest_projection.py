"""Tests for :class:`~graflo.architecture.evolution.ops.ProjectManifestOp`."""

from __future__ import annotations

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import (
    EdgeSelector,
    ProjectManifestOp,
    apply_evolution,
)
from graflo.architecture.schema.core import CoreSchema
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.metadata import GraphMetadata
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig
from graflo.migrate.io import manifest_hash

from test.architecture.test_manifest_rename import _sample_manifest_payload


def _three_vertex_manifest() -> GraphManifest:
    meta = GraphMetadata(name="three_vertex", version="1.0.0")
    vc = VertexConfig(
        vertices=[
            Vertex(name="a", properties=[Field(name="id")], identity=["id"]),
            Vertex(name="b", properties=[Field(name="id")], identity=["id"]),
            Vertex(name="c", properties=[Field(name="id")], identity=["id"]),
        ],
        force_types={},
    )
    ec = EdgeConfig(
        edges=[
            Edge(source="a", target="b", relation="linked"),
        ]
    )
    schema = Schema(
        metadata=meta, core_schema=CoreSchema(vertex_config=vc, edge_config=ec)
    )
    ingestion = {
        "resources": [
            {"name": "r_a", "apply": [{"vertex": "a"}]},
            {"name": "r_b", "apply": [{"vertex": "b"}]},
            {"name": "r_c", "apply": [{"vertex": "c"}]},
        ],
        "transforms": [],
    }
    return GraphManifest.from_config(
        {"schema": schema.to_dict(skip_defaults=False), "ingestion_model": ingestion}
    )


def _same_relation_two_dyads_manifest() -> GraphManifest:
    meta = GraphMetadata(name="dyads", version="1.0.0")
    vc = VertexConfig(
        vertices=[
            Vertex(name="person", properties=[Field(name="id")], identity=["id"]),
            Vertex(name="company", properties=[Field(name="id")], identity=["id"]),
            Vertex(name="department", properties=[Field(name="id")], identity=["id"]),
        ],
        force_types={},
    )
    ec = EdgeConfig(
        edges=[
            Edge(source="person", target="company", relation="works_at"),
            Edge(source="person", target="department", relation="works_at"),
        ]
    )
    schema = Schema(
        metadata=meta, core_schema=CoreSchema(vertex_config=vc, edge_config=ec)
    )
    ingestion = {
        "resources": [
            {
                "name": "links",
                "pipeline": [
                    {
                        "edge": {
                            "from": "person",
                            "to": "company",
                            "relation": "works_at",
                        }
                    },
                    {
                        "edge": {
                            "from": "person",
                            "to": "department",
                            "relation": "works_at",
                        }
                    },
                ],
            }
        ],
        "transforms": [],
    }
    return GraphManifest.from_config(
        {"schema": schema.to_dict(skip_defaults=False), "ingestion_model": ingestion}
    )


def test_project_keep_edges_by_triple() -> None:
    manifest = GraphManifest.from_dict(_sample_manifest_payload())
    manifest.finish_init()
    h_before = manifest_hash(manifest)

    out = apply_evolution(
        manifest,
        [
            ProjectManifestOp(
                keep_vertices=["person", "company"],
                keep_edges=[
                    EdgeSelector(source="person", target="company", relation="works_at")
                ],
            )
        ],
        bump_version=False,
    )

    schema = out.require_schema()
    assert schema.core_schema.vertex_config.vertex_set == {"person", "company"}
    assert [edge.edge_id for edge in schema.core_schema.edge_config.edges] == [
        ("person", "company", "works_at")
    ]
    assert len(schema.db_profile.edge_specs) == 1
    assert schema.db_profile.edge_specs[0].relation == "works_at"

    resource = out.require_ingestion_model().resources[0]
    assert len(resource.pipeline) == 3
    assert resource.pipeline[2]["edge"]["relation"] == "works_at"
    assert resource.infer_edge_only[0].edge_id == (
        "person",
        "company",
        "works_at",
    )
    assert resource.extra_weights[0].edge.edge_id == (
        "person",
        "company",
        "works_at",
    )
    assert manifest_hash(out) != h_before
    out.finish_init()


def test_project_keep_vertices_induced_prune() -> None:
    manifest = _three_vertex_manifest()
    manifest.finish_init()

    out = apply_evolution(
        manifest,
        [ProjectManifestOp(keep_vertices=["a", "b", "c"])],
        bump_version=False,
    )

    schema = out.require_schema()
    assert schema.core_schema.vertex_config.vertex_set == {"a", "b"}
    assert [edge.relation for edge in schema.core_schema.edge_config.edges] == [
        "linked"
    ]
    resources = out.require_ingestion_model().resources
    assert {resource.name for resource in resources} == {"r_a", "r_b"}


def test_project_same_relation_different_dyads() -> None:
    manifest = _same_relation_two_dyads_manifest()
    manifest.finish_init()

    out = apply_evolution(
        manifest,
        [
            ProjectManifestOp(
                keep_edges=[
                    EdgeSelector(source="person", target="company", relation="works_at")
                ]
            )
        ],
        bump_version=False,
    )

    schema = out.require_schema()
    assert [edge.edge_id for edge in schema.core_schema.edge_config.edges] == [
        ("person", "company", "works_at")
    ]
    pipeline = out.require_ingestion_model().resources[0].pipeline
    assert len(pipeline) == 1
    assert pipeline[0]["edge"]["to"] == "company"


def test_project_strict_unknown_vertex_raises() -> None:
    manifest = _three_vertex_manifest()
    manifest.finish_init()
    with pytest.raises(ValueError, match="Unknown vertices"):
        apply_evolution(
            manifest,
            [ProjectManifestOp(keep_vertices=["a", "nope"])],
            bump_version=False,
        )


def test_project_strict_unknown_edge_raises() -> None:
    manifest = _three_vertex_manifest()
    manifest.finish_init()
    with pytest.raises(ValueError, match="Unknown edges"):
        apply_evolution(
            manifest,
            [
                ProjectManifestOp(
                    keep_edges=[
                        EdgeSelector(source="a", target="b", relation="missing")
                    ]
                )
            ],
            bump_version=False,
        )


def test_project_empty_ingestion_raises() -> None:
    manifest = GraphManifest.from_config(
        {
            "schema": Schema(
                metadata=GraphMetadata(name="solo", version="1.0.0"),
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
    manifest.finish_init()
    with pytest.raises(ValueError, match="empty"):
        apply_evolution(
            manifest,
            [ProjectManifestOp(keep_vertices=["only"])],
            bump_version=False,
        )


def test_project_keep_resources_filters_bindings() -> None:
    manifest = _three_vertex_manifest()
    manifest.finish_init()

    out = apply_evolution(
        manifest,
        [
            ProjectManifestOp(
                keep_vertices=["a", "b", "c"],
                keep_resources=["r_a"],
            )
        ],
        bump_version=False,
    )

    assert len(out.require_ingestion_model().resources) == 1
    assert out.require_ingestion_model().resources[0].name == "r_a"


def test_project_op_requires_at_least_one_selector() -> None:
    with pytest.raises(ValueError, match="at least one"):
        ProjectManifestOp()
