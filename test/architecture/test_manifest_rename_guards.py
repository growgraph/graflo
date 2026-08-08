"""Guards and propagation for vertex/relation/resource renames and merges.

These cover the ways a rename or merge used to change a manifest *silently*: by
collapsing two types into one, by leaving a reference pointing at a name that no
longer exists, or by changing what ingestion emits without saying so.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import (
    MergeVerticesOp,
    RenameRelationsOp,
    RenameResourcesOp,
    RenameVerticesOp,
    apply_evolution,
)
from graflo.architecture.evolution.autogenerate import RenameHints
from graflo.architecture.evolution.rewrite import rewrite_vertex_names_in_step
from graflo.architecture.schema.core import CoreSchema
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.metadata import GraphMetadata
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig


def _manifest(
    *,
    vertices: list[Vertex] | None = None,
    edges: list[Edge] | None = None,
    resources: list[dict] | None = None,
    db_profile: dict | None = None,
) -> GraphManifest:
    vertices = vertices or [
        Vertex(name="a", properties=[Field(name="a_id")], identity=["a_id"]),
        Vertex(name="b", properties=[Field(name="b_id")], identity=["b_id"]),
    ]
    edges = (
        edges if edges is not None else [Edge(source="a", target="b", relation="ab")]
    )
    resources = resources or [
        {"name": "r", "apply": [{"vertex": "a"}, {"vertex": "b"}]}
    ]
    schema = Schema(
        metadata=GraphMetadata(name="g", version="1.0.0"),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(vertices=vertices, force_types={}),
            edge_config=EdgeConfig(edges=edges),
        ),
        **({"db_profile": db_profile} if db_profile else {}),
    )
    manifest = GraphManifest.from_config(
        {
            "schema": schema.to_dict(skip_defaults=False),
            "ingestion_model": {"resources": resources, "transforms": []},
        }
    )
    manifest.finish_init()
    return manifest


class TestRenameIsNotAMerge:
    """A rename must not change how many types exist."""

    def test_two_vertices_onto_one_name_is_rejected(self) -> None:
        with pytest.raises(ValidationError, match="not injective"):
            RenameVerticesOp(vertices={"a": "c", "b": "c"})

    def test_the_error_names_the_op_that_can_merge(self) -> None:
        with pytest.raises(ValidationError, match="MergeVerticesOp"):
            RenameVerticesOp(vertices={"a": "c", "b": "c"})

    def test_it_reports_every_collision_not_just_the_first(self) -> None:
        with pytest.raises(ValidationError) as excinfo:
            RenameVerticesOp(vertices={"a": "x", "b": "x", "c": "y", "d": "y"})
        message = str(excinfo.value)
        assert "'x' is the target of ['a', 'b']" in message
        assert "'y' is the target of ['c', 'd']" in message

    def test_relations_and_resources_are_guarded_too(self) -> None:
        with pytest.raises(ValidationError, match="MergeEdgesOp"):
            RenameRelationsOp(relations={"r1": "r3", "r2": "r3"})
        with pytest.raises(ValidationError, match="not injective"):
            RenameResourcesOp(resources={"r1": "r3", "r2": "r3"})

    def test_an_injective_map_is_still_accepted(self) -> None:
        assert RenameVerticesOp(vertices={"a": "c", "b": "d"}).vertices == {
            "a": "c",
            "b": "d",
        }

    def test_hints_cannot_smuggle_a_collapse_into_the_differ(self) -> None:
        with pytest.raises(ValidationError, match="not injective"):
            RenameHints(vertices={"a": "c", "b": "c"})
        with pytest.raises(ValidationError, match="not injective"):
            RenameHints(vertex_properties={"a": {"x": "z", "y": "z"}})


class TestRenameTargetsMustExist:
    def test_an_unknown_source_vertex_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="unknown vertices"):
            apply_evolution(
                _manifest(),
                [RenameVerticesOp(vertices={"nope": "c"})],
                bump_version=False,
            )

    def test_renaming_onto_a_surviving_vertex_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="collide"):
            apply_evolution(
                _manifest(),
                [RenameVerticesOp(vertices={"a": "b"})],
                bump_version=False,
            )

    def test_an_unknown_source_relation_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="unknown relations"):
            apply_evolution(
                _manifest(),
                [RenameRelationsOp(relations={"nope": "x"})],
                bump_version=False,
            )

    def test_a_simultaneous_relabel_keeps_schema_and_profile_in_step(self) -> None:
        """{r1: r2, r2: r3} applied once, not composed with itself.

        The db_profile used to be renamed twice — once inside the shared payload
        rewrite and once again afterwards — so the profile reached r3 while the
        schema stopped at r2, and validation failed on an edge that did exist.
        """
        manifest = _manifest(
            edges=[
                Edge(source="a", target="b", relation="r1"),
                Edge(source="b", target="a", relation="r2"),
            ],
            db_profile={
                "edge_specs": [
                    {"source": "a", "target": "b", "relation": "r1", "indexes": []},
                    {"source": "b", "target": "a", "relation": "r2", "indexes": []},
                ]
            },
        )
        out = apply_evolution(
            manifest,
            [RenameRelationsOp(relations={"r1": "r2", "r2": "r3"})],
            bump_version=False,
        )
        schema = out.require_schema()
        assert [e.relation for e in schema.core_schema.edge_config.edges] == [
            "r2",
            "r3",
        ]
        assert [s.relation for s in schema.db_profile.edge_specs] == ["r2", "r3"]


class TestDuplicateNamesAreRejectedAtTheSchema:
    """Defense in depth: whatever produced the duplicate, the schema refuses it."""

    def test_two_vertices_with_one_name(self) -> None:
        with pytest.raises(ValueError, match="duplicate vertex names"):
            VertexConfig(
                vertices=[
                    Vertex(name="a", properties=[Field(name="x")], identity=["x"]),
                    Vertex(name="a", properties=[Field(name="y")], identity=["y"]),
                ],
                force_types={},
            )

    def test_two_edges_with_one_edge_id(self) -> None:
        with pytest.raises(ValueError, match="duplicate edge definitions"):
            EdgeConfig(
                edges=[
                    Edge(source="a", target="b", relation="r"),
                    Edge(source="a", target="b", relation="r"),
                ]
            )


class TestReferencesThatUsedToBeLeftBehind:
    """Every vertex-name reference `collect_vertex_names` counts must be rewritten."""

    @staticmethod
    def _resource_with_all_reference_kinds() -> list[dict]:
        return [
            {
                "name": "r",
                "merge_collections": ["a"],
                "apply": [
                    {"vertex": "a"},
                    {"vertex": "b"},
                    {
                        "edge": {
                            "source": "a",
                            "target": "b",
                            "relation": "ab",
                            "vertex_weights": [{"name": "a", "fields": ["a_id"]}],
                        }
                    },
                ],
            }
        ]

    def test_rename_rewrites_merge_collections_and_vertex_weights(self) -> None:
        out = apply_evolution(
            _manifest(resources=self._resource_with_all_reference_kinds()),
            [RenameVerticesOp(vertices={"a": "agent"})],
            bump_version=False,
        )
        resource = out.ingestion_model.resources[0]
        assert resource.merge_collections == ["agent"]
        assert "a" not in resource.collect_vertex_names()
        assert resource.collect_vertex_names() == {"agent", "b"}

    def test_merge_rewrites_vertex_weights(self) -> None:
        out = apply_evolution(
            _manifest(resources=self._resource_with_all_reference_kinds()),
            [MergeVerticesOp(sources=["a"], into="agent", allow_self_relations=True)],
            bump_version=False,
        )
        resource = out.ingestion_model.resources[0]
        assert resource.collect_vertex_names() == {"agent", "b"}

    def test_strict_references_now_passes_where_it_used_to_raise(self) -> None:
        """The rewrite is complete, so the strict check has nothing to complain about."""
        apply_evolution(
            _manifest(resources=self._resource_with_all_reference_kinds()),
            [RenameVerticesOp(vertices={"a": "agent"})],
            bump_version=False,
            strict_references=True,
        )

    def test_merging_a_router_unions_the_column_maps(self) -> None:
        """Two `from` maps collapsing onto one key must not lose either side."""
        step = {
            "type": "vertex_router",
            "type_field": "kind",
            "type_map": {"alpha": "a", "beta": "b"},
            "vertex_from_map": {"a": {"a_id": "col_a"}, "b": {"b_id": "col_b"}},
        }
        out = rewrite_vertex_names_in_step(step, {"a": "ab", "b": "ab"})
        assert out["vertex_from_map"] == {"ab": {"a_id": "col_a", "b_id": "col_b"}}

    def test_a_genuine_column_conflict_is_reported(self) -> None:
        step = {
            "type": "vertex_router",
            "type_field": "kind",
            "type_map": {"alpha": "a", "beta": "b"},
            "vertex_from_map": {"a": {"id": "col_a"}, "b": {"id": "col_b"}},
        }
        with pytest.raises(ValueError, match="cannot merge vertex_from_map"):
            rewrite_vertex_names_in_step(step, {"a": "ab", "b": "ab"})
