"""Tests for the typing and physical-profile ops."""

import pytest

from graflo.architecture.contract import GraphManifest
from graflo.architecture.evolution import (
    AddEdgeIndexesOp,
    AddVertexIndexesOp,
    ChangeFieldTypesOp,
    RemoveEdgeIndexesOp,
    RemoveVertexIndexesOp,
    SetEdgeDirectedOp,
    apply_evolution,
)


def _manifest(
    *,
    vertices: list[dict] | None = None,
    edges: list[dict] | None = None,
    db_profile: dict | None = None,
) -> GraphManifest:
    payload: dict = {
        "schema": {
            "metadata": {"name": "physical-demo", "version": "1.0.0"},
            "graph": {
                "vertex_config": {
                    "vertices": vertices
                    or [
                        {
                            "name": "party",
                            "properties": ["id", "name", "as_of"],
                            "identity": ["id"],
                        },
                        {"name": "asset", "properties": ["id"], "identity": ["id"]},
                    ]
                },
                "edge_config": {
                    "edges": edges
                    or [
                        {
                            "source": "party",
                            "target": "asset",
                            "relation": "holds",
                            "properties": ["qty", "since"],
                        }
                    ]
                },
            },
        }
    }
    if db_profile is not None:
        payload["schema"]["db_profile"] = db_profile
    return GraphManifest.model_validate(payload)


def _field(manifest: GraphManifest, vertex_name: str, field_name: str):
    assert manifest.graph_schema is not None
    for vertex in manifest.graph_schema.core_schema.vertex_config.vertices:
        if vertex.name == vertex_name:
            for field in vertex.properties:
                if field.name == field_name:
                    return field
    raise AssertionError(f"no field {vertex_name}.{field_name}")


class TestChangeFieldTypes:
    def test_sets_a_vertex_field_type(self):
        out = apply_evolution(
            _manifest(),
            [ChangeFieldTypesOp(vertices={"party": {"as_of": {"type": "DATETIME"}}})],
        )

        assert _field(out, "party", "as_of").type == "DATETIME"

    def test_sets_a_list_type_with_item_type(self):
        out = apply_evolution(
            _manifest(),
            [
                ChangeFieldTypesOp(
                    vertices={
                        "party": {"name": {"type": "LIST", "item_type": "STRING"}}
                    }
                )
            ],
        )

        field = _field(out, "party", "name")
        assert field.type == "LIST"
        assert field.item_type == "STRING"

    def test_sets_an_edge_field_type(self):
        out = apply_evolution(
            _manifest(),
            [ChangeFieldTypesOp(edges={"holds": {"qty": {"type": "FLOAT"}}})],
        )

        assert out.graph_schema is not None
        edge = out.graph_schema.core_schema.edge_config.edges[0]
        qty = next(field for field in edge.properties if field.name == "qty")
        assert qty.type == "FLOAT"

    def test_list_without_item_type_is_rejected(self):
        with pytest.raises(ValueError, match="requires item_type"):
            ChangeFieldTypesOp(vertices={"party": {"name": {"type": "LIST"}}})

    def test_item_type_without_list_is_rejected(self):
        with pytest.raises(ValueError, match="only meaningful for a LIST"):
            ChangeFieldTypesOp(
                vertices={"party": {"name": {"type": "STRING", "item_type": "STRING"}}}
            )

    def test_identity_field_cannot_become_a_list(self):
        with pytest.raises(ValueError, match="participates in the identity"):
            apply_evolution(
                _manifest(),
                [
                    ChangeFieldTypesOp(
                        vertices={
                            "party": {"id": {"type": "LIST", "item_type": "STRING"}}
                        }
                    )
                ],
            )

    def test_undeclared_field_is_rejected(self):
        with pytest.raises(ValueError, match="does not declare"):
            apply_evolution(
                _manifest(),
                [ChangeFieldTypesOp(vertices={"party": {"ghost": {"type": "STRING"}}})],
            )

    def test_unknown_relation_is_rejected(self):
        with pytest.raises(ValueError, match="unknown relations"):
            apply_evolution(
                _manifest(),
                [ChangeFieldTypesOp(edges={"nope": {"qty": {"type": "FLOAT"}}})],
            )

    def test_backend_without_list_support_is_rejected(self):
        """NebulaGraph has no native list storage, so the op must fail here."""
        manifest = _manifest(db_profile={"db_flavor": "nebula"})

        with pytest.raises(ValueError):
            apply_evolution(
                manifest,
                [
                    ChangeFieldTypesOp(
                        vertices={
                            "party": {"name": {"type": "LIST", "item_type": "STRING"}}
                        }
                    )
                ],
            )

    def test_at_least_one_target_is_required(self):
        with pytest.raises(ValueError, match="at least one of vertices or edges"):
            ChangeFieldTypesOp()


class TestVertexIndexes:
    def test_adds_an_index(self):
        out = apply_evolution(
            _manifest(),
            [AddVertexIndexesOp(indexes={"party": [{"fields": ["name"]}]})],
        )

        assert out.graph_schema is not None
        indexes = out.graph_schema.db_profile.vertex_indexes.get("party", [])
        assert any(index.fields == ["name"] for index in indexes)

    def test_undeclared_field_is_rejected(self):
        with pytest.raises(ValueError, match="does not declare"):
            apply_evolution(
                _manifest(),
                [AddVertexIndexesOp(indexes={"party": [{"fields": ["ghost"]}]})],
            )

    def test_removes_an_authored_index(self):
        manifest = _manifest(
            db_profile={"vertex_indexes": {"party": [{"fields": ["name"]}]}}
        )

        out = apply_evolution(
            manifest, [RemoveVertexIndexesOp(indexes={"party": [["name"]]})]
        )

        assert out.graph_schema is not None
        indexes = out.graph_schema.db_profile.vertex_indexes.get("party", [])
        assert all(index.fields != ["name"] for index in indexes)

    def test_removing_a_missing_index_is_rejected(self):
        with pytest.raises(ValueError, match="has no index on"):
            apply_evolution(
                _manifest(), [RemoveVertexIndexesOp(indexes={"party": [["name"]]})]
            )

    def test_derived_secondary_identity_index_cannot_be_removed(self):
        """It would be re-registered by the next finish_init, so refuse up front."""
        manifest = _manifest(
            vertices=[
                {
                    "name": "party",
                    "properties": ["id", "name"],
                    "identity": ["id"],
                    "secondary_identities": [{"name": "by_name", "fields": ["name"]}],
                },
                {"name": "asset", "properties": ["id"], "identity": ["id"]},
            ]
        )

        with pytest.raises(ValueError, match="derived from secondary_identities"):
            apply_evolution(
                manifest, [RemoveVertexIndexesOp(indexes={"party": [["name"]]})]
            )


class TestEdgeIndexes:
    @staticmethod
    def _with_spec(indexes: list[dict] | None = None) -> GraphManifest:
        return _manifest(
            db_profile={
                "edge_specs": [
                    {
                        "source": "party",
                        "target": "asset",
                        "relation": "holds",
                        "indexes": indexes or [],
                    }
                ]
            }
        )

    def test_adds_an_index(self):
        out = apply_evolution(
            self._with_spec(),
            [
                AddEdgeIndexesOp(
                    edges=[
                        {
                            "source": "party",
                            "target": "asset",
                            "relation": "holds",
                            "indexes": [{"fields": ["since"]}],
                        }
                    ]
                )
            ],
        )

        assert out.graph_schema is not None
        spec = out.graph_schema.db_profile.edge_specs[0]
        assert any(index.fields == ["since"] for index in spec.indexes)

    def test_duplicate_index_is_rejected(self):
        with pytest.raises(ValueError, match="already indexes"):
            apply_evolution(
                self._with_spec([{"fields": ["since"]}]),
                [
                    AddEdgeIndexesOp(
                        edges=[
                            {
                                "source": "party",
                                "target": "asset",
                                "relation": "holds",
                                "indexes": [{"fields": ["since"]}],
                            }
                        ]
                    )
                ],
            )

    def test_removes_an_index(self):
        out = apply_evolution(
            self._with_spec([{"fields": ["since"]}]),
            [
                RemoveEdgeIndexesOp(
                    edges=[
                        {
                            "source": "party",
                            "target": "asset",
                            "relation": "holds",
                            "fields": [["since"]],
                        }
                    ]
                )
            ],
        )

        assert out.graph_schema is not None
        spec = out.graph_schema.db_profile.edge_specs[0]
        assert all(index.fields != ["since"] for index in spec.indexes)

    def test_unknown_edge_is_rejected(self):
        with pytest.raises(ValueError, match="unknown edge"):
            apply_evolution(
                self._with_spec(),
                [
                    AddEdgeIndexesOp(
                        edges=[
                            {
                                "source": "party",
                                "target": "asset",
                                "relation": "nope",
                                "indexes": [{"fields": ["since"]}],
                            }
                        ]
                    )
                ],
            )

    def test_removing_a_missing_index_is_rejected(self):
        with pytest.raises(ValueError, match="has no index on"):
            apply_evolution(
                self._with_spec(),
                [
                    RemoveEdgeIndexesOp(
                        edges=[
                            {
                                "source": "party",
                                "target": "asset",
                                "relation": "holds",
                                "fields": [["since"]],
                            }
                        ]
                    )
                ],
            )


class TestSetEdgeDirected:
    def test_flips_the_flag(self):
        out = apply_evolution(
            _manifest(),
            [
                SetEdgeDirectedOp(
                    edges=[{"source": "party", "target": "asset", "relation": "holds"}],
                    directed=False,
                )
            ],
        )

        assert out.graph_schema is not None
        assert out.graph_schema.core_schema.edge_config.edges[0].directed is False

    def test_unknown_edge_is_rejected(self):
        with pytest.raises(ValueError, match="unknown edges"):
            apply_evolution(
                _manifest(),
                [
                    SetEdgeDirectedOp(
                        edges=[
                            {"source": "party", "target": "asset", "relation": "nope"}
                        ],
                        directed=False,
                    )
                ],
            )
