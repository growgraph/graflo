"""Tests for :mod:`graflo.architecture.evolution`."""

from __future__ import annotations

import pytest

from graflo.architecture.contract.ingestion.steps.normalize import (
    normalize_actor_step,
)
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import (
    EdgeSelector,
    MergeVerticesOp,
    ProjectManifestOp,
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
                # a -> b becomes a self-relation on ab; that is the outcome asserted
                # below, so state it rather than tripping the guard.
                allow_self_relations=True,
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
        [
            MergeVerticesOp(
                op="merge_vertices",
                sources=["b"],
                into="a",
                allow_self_relations=True,
            )
        ],
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


# --------------------------------------------------------------------------- #
# Removal over a vertex_router: trim the router, keep the resource.
# --------------------------------------------------------------------------- #

_BARE_ROUTER = {"vertex_router": {"type_field": "kind"}}


def _three_class_manifest(
    pipeline: list[dict], *, extra: list[dict] | None = None
) -> GraphManifest:
    """Classes a, b, c with an edge a->b, fed by ``r_all`` through *pipeline*."""
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "g", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {"name": name, "properties": ["id"], "identity": ["id"]}
                            for name in ("a", "b", "c")
                        ]
                    },
                    "edge_config": {"edges": [{"source": "a", "target": "b"}]},
                },
            },
            "ingestion_model": {
                "resources": [{"name": "r_all", "pipeline": pipeline}]
                + list(extra or []),
                "transforms": [],
            },
        }
    )
    manifest.finish_init()
    return manifest


def _resource_names(manifest: GraphManifest) -> list[str]:
    assert manifest.ingestion_model is not None
    return [r.name for r in manifest.ingestion_model.resources]


def _steps(manifest: GraphManifest, resource: str) -> list[dict]:
    assert manifest.ingestion_model is not None
    pipeline = next(
        r.pipeline for r in manifest.ingestion_model.resources if r.name == resource
    )
    return [normalize_actor_step(dict(s)) for s in pipeline]


def _vertex_set(manifest: GraphManifest) -> set[str]:
    assert manifest.graph_schema is not None
    return set(manifest.graph_schema.core_schema.vertex_config.vertex_set)


def _remove(manifest: GraphManifest, *names: str) -> GraphManifest:
    return apply_evolution(
        manifest, [RemoveVerticesOp(names=list(names))], bump_version=False
    )


def test_remove_vertex_keeps_a_pass_through_router_resource() -> None:
    out = _remove(_three_class_manifest([_BARE_ROUTER]), "b")

    assert _resource_names(out) == ["r_all"]
    (router,) = _steps(out, "r_all")
    assert router["type"] == "vertex_router"
    assert not router.get("type_map")
    assert _vertex_set(out) == {"a", "c"}
    assert out.graph_schema is not None
    assert out.graph_schema.core_schema.edge_config.edges == []


def test_remove_vertex_trims_the_router_table_and_projection() -> None:
    router = {
        "vertex_router": {
            "type_field": "kind",
            "type_map": {"A": "a", "B": "b", "C": "c"},
            "vertex_from_map": {"b": {"bid": "id"}, "c": {"cid": "id"}},
        }
    }

    out = _remove(_three_class_manifest([router]), "b")

    (router_step,) = _steps(out, "r_all")
    assert router_step["type_map"] == {"A": "a", "C": "c"}
    assert router_step["vertex_from_map"] == {"c": {"cid": "id"}}


def test_remove_vertex_keeps_a_router_nested_under_a_descend() -> None:
    pipeline = [{"descend": {"key": "records", "apply": [_BARE_ROUTER]}}]

    out = _remove(_three_class_manifest(pipeline), "b")

    (descend,) = _steps(out, "r_all")
    assert descend["type"] == "descend"
    (router,) = [normalize_actor_step(dict(s)) for s in descend["pipeline"]]
    assert router["type"] == "vertex_router"


def test_remove_vertex_trims_steps_instead_of_dropping_the_resource() -> None:
    pipeline = [
        {"vertex": "a"},
        {"vertex": "b"},
        {"edge": {"source": "a", "target": "b"}},
    ]

    out = _remove(_three_class_manifest(pipeline), "b")

    assert _resource_names(out) == ["r_all"]
    assert [s["type"] for s in _steps(out, "r_all")] == ["vertex"]


def test_remove_vertex_drops_a_resource_left_with_nothing() -> None:
    manifest = _three_class_manifest(
        [{"vertex": "b"}], extra=[{"name": "r_a", "pipeline": [{"vertex": "a"}]}]
    )

    out = _remove(manifest, "b")

    assert _resource_names(out) == ["r_a"]


def test_project_manifest_keeps_the_router_resource() -> None:
    out = apply_evolution(
        _three_class_manifest([_BARE_ROUTER]),
        [
            ProjectManifestOp(
                keep_vertices=["a", "b"],
                keep_edges=[EdgeSelector(source="a", target="b")],
            )
        ],
        bump_version=False,
    )

    assert _vertex_set(out) == {"a", "b"}
    assert _resource_names(out) == ["r_all"]
