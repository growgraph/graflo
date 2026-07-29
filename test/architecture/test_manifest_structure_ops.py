"""Tests for the structure-plane ops: add vertices/edges and retarget edges."""

from typing import Any

import pytest

from graflo.architecture.contract import GraphManifest
from graflo.architecture.evolution import (
    AddEdgesOp,
    AddVerticesOp,
    RetargetEdgesOp,
    apply_evolution,
)


def _manifest(
    *,
    vertices: list[dict] | None = None,
    edges: list[dict] | None = None,
    pipeline: list[dict] | None = None,
    edge_specs: list[dict] | None = None,
) -> GraphManifest:
    schema_block: dict[str, Any] = {
        "metadata": {"name": "structure-demo", "version": "1.0.0"},
        "graph": {
            "vertex_config": {
                "vertices": vertices
                or [
                    {"name": "party", "properties": ["id"], "identity": ["id"]},
                    {"name": "asset", "properties": ["id"], "identity": ["id"]},
                ]
            },
            "edge_config": {"edges": edges or []},
        },
    }
    if edge_specs is not None:
        schema_block["db_profile"] = {"edge_specs": edge_specs}

    payload: dict[str, Any] = {"schema": schema_block}
    if pipeline is not None:
        payload["ingestion_model"] = {
            "resources": [{"name": "src", "pipeline": pipeline}]
        }
    return GraphManifest.model_validate(payload)


def _vertex_names(manifest: GraphManifest) -> list[str]:
    assert manifest.graph_schema is not None
    return [v.name for v in manifest.graph_schema.core_schema.vertex_config.vertices]


def _edge_ids(manifest: GraphManifest) -> list[tuple]:
    assert manifest.graph_schema is not None
    return [e.edge_id for e in manifest.graph_schema.core_schema.edge_config.edges]


class TestAddVertices:
    def test_adds_a_new_vertex_type(self):
        out = apply_evolution(
            _manifest(),
            [
                AddVerticesOp(
                    vertices=[
                        {
                            "name": "custodian",
                            "properties": ["id", "name"],
                            "identity": ["id"],
                        }
                    ]
                )
            ],
        )

        assert "custodian" in _vertex_names(out)

    def test_identity_mode_is_preserved(self):
        out = apply_evolution(
            _manifest(),
            [
                AddVerticesOp(
                    vertices=[
                        {"name": "trace", "properties": ["payload"], "assigned": True}
                    ]
                )
            ],
        )

        assert out.graph_schema is not None
        trace = next(
            v
            for v in out.graph_schema.core_schema.vertex_config.vertices
            if v.name == "trace"
        )
        assert trace.identity_mode == "assigned"

    def test_existing_name_is_rejected(self):
        with pytest.raises(ValueError, match="already exist"):
            apply_evolution(
                _manifest(),
                [
                    AddVerticesOp(
                        vertices=[
                            {"name": "party", "properties": ["id"], "identity": ["id"]}
                        ]
                    )
                ],
            )

    def test_duplicate_names_within_the_op_are_rejected(self):
        with pytest.raises(ValueError, match="unique by name"):
            AddVerticesOp(
                vertices=[
                    {"name": "x", "properties": ["id"], "identity": ["id"]},
                    {"name": "x", "properties": ["id"], "identity": ["id"]},
                ]
            )


class TestAddEdges:
    def test_adds_a_new_relation(self):
        out = apply_evolution(
            _manifest(),
            [
                AddEdgesOp(
                    edges=[{"source": "party", "target": "asset", "relation": "holds"}]
                )
            ],
        )

        assert ("party", "asset", "holds") in _edge_ids(out)

    def test_edge_properties_are_kept(self):
        out = apply_evolution(
            _manifest(),
            [
                AddEdgesOp(
                    edges=[
                        {
                            "source": "party",
                            "target": "asset",
                            "relation": "holds",
                            "properties": ["since"],
                        }
                    ]
                )
            ],
        )

        assert out.graph_schema is not None
        edge = out.graph_schema.core_schema.edge_config.edges[0]
        assert "since" in {field.name for field in edge.properties}

    def test_unknown_endpoint_is_rejected(self):
        with pytest.raises(ValueError, match="unknown endpoint vertex types"):
            apply_evolution(
                _manifest(),
                [AddEdgesOp(edges=[{"source": "party", "target": "nope"}])],
            )

    def test_existing_edge_is_rejected(self):
        manifest = _manifest(
            edges=[{"source": "party", "target": "asset", "relation": "holds"}]
        )

        with pytest.raises(ValueError, match="already exist"):
            apply_evolution(
                manifest,
                [
                    AddEdgesOp(
                        edges=[
                            {"source": "party", "target": "asset", "relation": "holds"}
                        ]
                    )
                ],
            )


class TestRetargetEdges:
    @staticmethod
    def _three_vertex_manifest(**kwargs) -> GraphManifest:
        return _manifest(
            vertices=[
                {"name": "party", "properties": ["id"], "identity": ["id"]},
                {"name": "asset", "properties": ["id"], "identity": ["id"]},
                {"name": "legal_entity", "properties": ["id"], "identity": ["id"]},
            ],
            edges=[
                {
                    "source": "party",
                    "target": "asset",
                    "relation": "holds",
                    "properties": ["since"],
                    "identities": [["source", "target", "since"]],
                    "directed": False,
                }
            ],
            **kwargs,
        )

    def test_source_is_repointed(self):
        out = apply_evolution(
            self._three_vertex_manifest(),
            [
                RetargetEdgesOp(
                    edges=[
                        {
                            "source": "party",
                            "target": "asset",
                            "relation": "holds",
                            "new_source": "legal_entity",
                        }
                    ]
                )
            ],
        )

        assert ("legal_entity", "asset", "holds") in _edge_ids(out)
        assert ("party", "asset", "holds") not in _edge_ids(out)

    def test_edge_payload_survives_the_retarget(self):
        """This is the whole point: remove+add would lose all of these."""
        out = apply_evolution(
            self._three_vertex_manifest(),
            [
                RetargetEdgesOp(
                    edges=[
                        {
                            "source": "party",
                            "target": "asset",
                            "relation": "holds",
                            "new_source": "legal_entity",
                        }
                    ]
                )
            ],
        )

        assert out.graph_schema is not None
        edge = out.graph_schema.core_schema.edge_config.edges[0]
        assert "since" in {field.name for field in edge.properties}
        assert edge.identities == [["source", "target", "since"]]
        assert edge.directed is False

    def test_physical_spec_moves_with_the_edge(self):
        manifest = self._three_vertex_manifest(
            edge_specs=[
                {
                    "source": "party",
                    "target": "asset",
                    "relation": "holds",
                    "indexes": [{"fields": ["since"]}],
                }
            ]
        )

        out = apply_evolution(
            manifest,
            [
                RetargetEdgesOp(
                    edges=[
                        {
                            "source": "party",
                            "target": "asset",
                            "relation": "holds",
                            "new_source": "legal_entity",
                        }
                    ]
                )
            ],
        )

        assert out.graph_schema is not None
        specs = [
            spec
            for spec in out.graph_schema.db_profile.edge_specs
            if spec.indexes and spec.indexes[0].fields == ["since"]
        ]
        assert specs and specs[0].source == "legal_entity"

    def test_pipeline_edge_step_is_repointed(self):
        manifest = self._three_vertex_manifest(
            pipeline=[
                {"vertex": "party"},
                {"vertex": "asset"},
                {"vertex": "legal_entity"},
                {"edge": {"from": "party", "to": "asset", "relation": "holds"}},
            ]
        )

        out = apply_evolution(
            manifest,
            [
                RetargetEdgesOp(
                    edges=[
                        {
                            "source": "party",
                            "target": "asset",
                            "relation": "holds",
                            "new_source": "legal_entity",
                        }
                    ]
                )
            ],
        )

        assert out.ingestion_model is not None
        edge_steps = [
            step["edge"]
            for step in out.ingestion_model.resources[0].pipeline
            if "edge" in step
        ]
        assert edge_steps[0]["from"] == "legal_entity"
        assert edge_steps[0]["to"] == "asset"

    def test_a_different_relation_between_the_same_types_is_left_alone(self):
        manifest = _manifest(
            vertices=[
                {"name": "party", "properties": ["id"], "identity": ["id"]},
                {"name": "asset", "properties": ["id"], "identity": ["id"]},
                {"name": "legal_entity", "properties": ["id"], "identity": ["id"]},
            ],
            edges=[
                {"source": "party", "target": "asset", "relation": "holds"},
                {"source": "party", "target": "asset", "relation": "sold"},
            ],
            pipeline=[
                {"vertex": "party"},
                {"vertex": "asset"},
                {"vertex": "legal_entity"},
                {"edge": {"from": "party", "to": "asset", "relation": "holds"}},
                {"edge": {"from": "party", "to": "asset", "relation": "sold"}},
            ],
        )

        out = apply_evolution(
            manifest,
            [
                RetargetEdgesOp(
                    edges=[
                        {
                            "source": "party",
                            "target": "asset",
                            "relation": "holds",
                            "new_source": "legal_entity",
                        }
                    ]
                )
            ],
        )

        assert ("party", "asset", "sold") in _edge_ids(out)
        assert out.ingestion_model is not None
        by_relation = {
            step["edge"]["relation"]: step["edge"]
            for step in out.ingestion_model.resources[0].pipeline
            if "edge" in step
        }
        assert by_relation["sold"]["from"] == "party"
        assert by_relation["holds"]["from"] == "legal_entity"

    def test_unknown_edge_is_rejected(self):
        with pytest.raises(ValueError, match="unknown edges"):
            apply_evolution(
                self._three_vertex_manifest(),
                [
                    RetargetEdgesOp(
                        edges=[
                            {
                                "source": "party",
                                "target": "asset",
                                "relation": "nope",
                                "new_source": "legal_entity",
                            }
                        ]
                    )
                ],
            )

    def test_unknown_new_endpoint_is_rejected(self):
        with pytest.raises(ValueError, match="unknown endpoint vertex types"):
            apply_evolution(
                self._three_vertex_manifest(),
                [
                    RetargetEdgesOp(
                        edges=[
                            {
                                "source": "party",
                                "target": "asset",
                                "relation": "holds",
                                "new_source": "ghost",
                            }
                        ]
                    )
                ],
            )

    def test_collision_with_an_existing_edge_is_rejected(self):
        manifest = _manifest(
            vertices=[
                {"name": "party", "properties": ["id"], "identity": ["id"]},
                {"name": "asset", "properties": ["id"], "identity": ["id"]},
                {"name": "legal_entity", "properties": ["id"], "identity": ["id"]},
            ],
            edges=[
                {"source": "party", "target": "asset", "relation": "holds"},
                {"source": "legal_entity", "target": "asset", "relation": "holds"},
            ],
        )

        with pytest.raises(ValueError, match="collide with existing"):
            apply_evolution(
                manifest,
                [
                    RetargetEdgesOp(
                        edges=[
                            {
                                "source": "party",
                                "target": "asset",
                                "relation": "holds",
                                "new_source": "legal_entity",
                            }
                        ]
                    )
                ],
            )

    def test_a_no_change_retarget_is_rejected(self):
        with pytest.raises(ValueError, match="would not change endpoints"):
            RetargetEdgesOp(
                edges=[
                    {
                        "source": "party",
                        "target": "asset",
                        "relation": "holds",
                        "new_source": "party",
                    }
                ]
            )

    def test_at_least_one_new_endpoint_is_required(self):
        with pytest.raises(
            ValueError, match="at least one of new_source or new_target"
        ):
            RetargetEdgesOp(
                edges=[{"source": "party", "target": "asset", "relation": "holds"}]
            )
