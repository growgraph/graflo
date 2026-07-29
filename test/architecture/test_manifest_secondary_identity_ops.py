"""Tests for the secondary-identity and edge-identity evolution ops."""

import pytest

from graflo.architecture.contract import GraphManifest
from graflo.architecture.evolution import (
    AddSecondaryIdentitiesOp,
    RemoveSecondaryIdentitiesOp,
    ReplaceEdgeIdentitiesOp,
    apply_evolution,
)
from graflo.architecture.schema.vertex import Vertex


def _manifest(
    vertices: list[dict] | None = None,
    *,
    edges: list[dict] | None = None,
    pipeline: list[dict] | None = None,
) -> GraphManifest:
    payload: dict = {
        "schema": {
            "metadata": {"name": "secondary-demo", "version": "1.0.0"},
            "graph": {
                "vertex_config": {
                    "vertices": vertices
                    or [
                        {
                            "name": "instrument",
                            "properties": ["id", "isin", "lei", "venue"],
                            "identity": ["id"],
                        },
                        {"name": "issuer", "properties": ["id"], "identity": ["id"]},
                    ]
                },
                "edge_config": {"edges": edges or []},
            },
        }
    }
    if pipeline is not None:
        payload["ingestion_model"] = {
            "resources": [{"name": "src", "pipeline": pipeline}]
        }
    return GraphManifest.model_validate(payload)


def _vertex(manifest: GraphManifest, name: str) -> Vertex:
    assert manifest.graph_schema is not None
    for vertex in manifest.graph_schema.core_schema.vertex_config.vertices:
        if vertex.name == name:
            return vertex
    raise AssertionError(f"no vertex {name!r}")


class TestAddSecondaryIdentities:
    def test_declares_a_named_lookup_key(self):
        out = apply_evolution(
            _manifest(),
            [
                AddSecondaryIdentitiesOp(
                    additions={"instrument": [{"name": "by_isin", "fields": ["isin"]}]}
                )
            ],
        )

        instrument = _vertex(out, "instrument")
        assert instrument.secondary_identity_names == ["by_isin"]

    def test_bare_field_list_is_accepted_and_auto_named(self):
        out = apply_evolution(
            _manifest(),
            [
                AddSecondaryIdentitiesOp.model_validate(
                    {"additions": {"instrument": [["isin"]]}}
                )
            ],
        )

        instrument = _vertex(out, "instrument")
        assert [entry.fields for entry in instrument.secondary_identities] == [["isin"]]
        assert instrument.secondary_identities[0].name is not None

    def test_composite_sets_are_supported(self):
        out = apply_evolution(
            _manifest(),
            [
                AddSecondaryIdentitiesOp(
                    additions={
                        "instrument": [
                            {"name": "by_venue", "fields": ["venue", "isin"]}
                        ]
                    }
                )
            ],
        )

        instrument = _vertex(out, "instrument")
        assert instrument.secondary_identities[0].fields == ["venue", "isin"]

    def test_a_lookup_index_is_derived(self):
        out = apply_evolution(
            _manifest(),
            [
                AddSecondaryIdentitiesOp(
                    additions={"instrument": [{"name": "by_isin", "fields": ["isin"]}]}
                )
            ],
        )

        assert out.graph_schema is not None
        indexes = out.graph_schema.db_profile.vertex_indexes.get("instrument", [])
        matching = [index for index in indexes if index.fields == ["isin"]]
        assert matching and matching[0].unique is False

    def test_undeclared_field_is_rejected(self):
        with pytest.raises(ValueError, match="does not declare"):
            apply_evolution(
                _manifest(),
                [
                    AddSecondaryIdentitiesOp.model_validate(
                        {"additions": {"instrument": [["cusip"]]}}
                    )
                ],
            )

    def test_duplicate_field_set_is_rejected(self):
        manifest = _manifest(
            [
                {
                    "name": "instrument",
                    "properties": ["id", "isin"],
                    "identity": ["id"],
                    "secondary_identities": [{"name": "by_isin", "fields": ["isin"]}],
                }
            ]
        )

        with pytest.raises(
            ValueError, match="already declares a secondary identity on"
        ):
            apply_evolution(
                manifest,
                [
                    AddSecondaryIdentitiesOp.model_validate(
                        {"additions": {"instrument": [["isin"]]}}
                    )
                ],
            )

    def test_duplicate_name_is_rejected(self):
        manifest = _manifest(
            [
                {
                    "name": "instrument",
                    "properties": ["id", "isin", "lei"],
                    "identity": ["id"],
                    "secondary_identities": [{"name": "by_code", "fields": ["isin"]}],
                }
            ]
        )

        with pytest.raises(
            ValueError, match="already declares a secondary identity named"
        ):
            apply_evolution(
                manifest,
                [
                    AddSecondaryIdentitiesOp(
                        additions={
                            "instrument": [{"name": "by_code", "fields": ["lei"]}]
                        }
                    )
                ],
            )

    def test_blank_vertex_is_rejected(self):
        manifest = _manifest([{"name": "note", "properties": ["text"], "blank": True}])

        with pytest.raises(ValueError, match="is blank"):
            apply_evolution(
                manifest,
                [
                    AddSecondaryIdentitiesOp.model_validate(
                        {"additions": {"note": [["text"]]}}
                    )
                ],
            )

    def test_unknown_vertex_is_rejected(self):
        with pytest.raises(ValueError, match="unknown vertices"):
            apply_evolution(
                _manifest(),
                [
                    AddSecondaryIdentitiesOp.model_validate(
                        {"additions": {"nope": [["isin"]]}}
                    )
                ],
            )


class TestRemoveSecondaryIdentities:
    @staticmethod
    def _with_two() -> GraphManifest:
        return _manifest(
            [
                {
                    "name": "instrument",
                    "properties": ["id", "isin", "lei"],
                    "identity": ["id"],
                    "secondary_identities": [
                        {"name": "by_isin", "fields": ["isin"]},
                        {"name": "by_lei", "fields": ["lei"]},
                    ],
                },
                {"name": "issuer", "properties": ["id"], "identity": ["id"]},
            ]
        )

    def test_removes_by_name(self):
        out = apply_evolution(
            self._with_two(),
            [RemoveSecondaryIdentitiesOp(removals={"instrument": ["by_isin"]})],
        )

        assert _vertex(out, "instrument").secondary_identity_names == ["by_lei"]

    def test_removes_by_field_list(self):
        out = apply_evolution(
            self._with_two(),
            [RemoveSecondaryIdentitiesOp(removals={"instrument": [["isin"]]})],
        )

        assert _vertex(out, "instrument").secondary_identity_names == ["by_lei"]

    def test_derived_index_is_withdrawn(self):
        """finish_init only ever adds derived indexes, so removal must drop them."""
        out = apply_evolution(
            self._with_two(),
            [RemoveSecondaryIdentitiesOp(removals={"instrument": ["by_isin"]})],
        )

        assert out.graph_schema is not None
        indexes = out.graph_schema.db_profile.vertex_indexes.get("instrument", [])
        assert all(index.fields != ["isin"] for index in indexes)

    def test_unknown_selector_lists_what_is_declared(self):
        with pytest.raises(ValueError, match="by_isin"):
            apply_evolution(
                self._with_two(),
                [RemoveSecondaryIdentitiesOp(removals={"instrument": ["by_cusip"]})],
            )

    def test_selector_still_used_by_an_edge_step_is_rejected(self):
        manifest = _manifest(
            [
                {
                    "name": "instrument",
                    "properties": ["id", "isin"],
                    "identity": ["id"],
                    "secondary_identities": [{"name": "by_isin", "fields": ["isin"]}],
                },
                {"name": "issuer", "properties": ["id"], "identity": ["id"]},
            ],
            edges=[{"source": "instrument", "target": "issuer"}],
            pipeline=[
                {"vertex": "instrument"},
                {"vertex": "issuer"},
                {
                    "edge": {
                        "from": "instrument",
                        "to": "issuer",
                        "source_match": "by_isin",
                    }
                },
            ],
        )

        with pytest.raises(ValueError, match="still selected by an edge step"):
            apply_evolution(
                manifest,
                [RemoveSecondaryIdentitiesOp(removals={"instrument": ["by_isin"]})],
            )


class TestReplaceEdgeIdentities:
    @staticmethod
    def _edge_manifest() -> GraphManifest:
        return _manifest(
            edges=[
                {
                    "source": "instrument",
                    "target": "issuer",
                    "relation": "issued_by",
                    "properties": ["as_of", "book"],
                    "identities": [["source", "target"]],
                }
            ]
        )

    @staticmethod
    def _edge(manifest: GraphManifest):
        assert manifest.graph_schema is not None
        return manifest.graph_schema.core_schema.edge_config.edges[0]

    def test_replaces_the_uniqueness_key(self):
        out = apply_evolution(
            self._edge_manifest(),
            [
                ReplaceEdgeIdentitiesOp(
                    edges=[
                        {
                            "source": "instrument",
                            "target": "issuer",
                            "relation": "issued_by",
                            "identities": [["source", "target", "as_of"]],
                        }
                    ]
                )
            ],
        )

        assert self._edge(out).identities == [["source", "target", "as_of"]]

    def test_supports_several_keys(self):
        out = apply_evolution(
            self._edge_manifest(),
            [
                ReplaceEdgeIdentitiesOp(
                    edges=[
                        {
                            "source": "instrument",
                            "target": "issuer",
                            "relation": "issued_by",
                            "identities": [
                                ["source", "target", "as_of"],
                                ["source", "target", "book"],
                            ],
                        }
                    ]
                )
            ],
        )

        assert len(self._edge(out).identities) == 2

    def test_empty_list_clears_the_keys(self):
        out = apply_evolution(
            self._edge_manifest(),
            [
                ReplaceEdgeIdentitiesOp(
                    edges=[
                        {
                            "source": "instrument",
                            "target": "issuer",
                            "relation": "issued_by",
                            "identities": [],
                        }
                    ]
                )
            ],
        )

        assert self._edge(out).identities == []

    def test_non_endpoint_token_is_merged_into_properties(self):
        out = apply_evolution(
            self._edge_manifest(),
            [
                ReplaceEdgeIdentitiesOp(
                    edges=[
                        {
                            "source": "instrument",
                            "target": "issuer",
                            "relation": "issued_by",
                            "identities": [["source", "target", "trade_id"]],
                        }
                    ]
                )
            ],
        )

        assert "trade_id" in {field.name for field in self._edge(out).properties}

    def test_unknown_edge_is_rejected(self):
        with pytest.raises(ValueError, match="unknown edges"):
            apply_evolution(
                self._edge_manifest(),
                [
                    ReplaceEdgeIdentitiesOp(
                        edges=[
                            {
                                "source": "instrument",
                                "target": "issuer",
                                "relation": "nope",
                                "identities": [["source", "target"]],
                            }
                        ]
                    )
                ],
            )

    def test_empty_uniqueness_key_is_rejected(self):
        with pytest.raises(ValueError, match="empty uniqueness key"):
            apply_evolution(
                self._edge_manifest(),
                [
                    ReplaceEdgeIdentitiesOp(
                        edges=[
                            {
                                "source": "instrument",
                                "target": "issuer",
                                "relation": "issued_by",
                                "identities": [[]],
                            }
                        ]
                    )
                ],
            )

    def test_duplicate_edge_selectors_are_rejected(self):
        with pytest.raises(ValueError, match="must be unique"):
            ReplaceEdgeIdentitiesOp(
                edges=[
                    {
                        "source": "instrument",
                        "target": "issuer",
                        "relation": "issued_by",
                        "identities": [["source", "target"]],
                    },
                    {
                        "source": "instrument",
                        "target": "issuer",
                        "relation": "issued_by",
                        "identities": [["source", "target", "as_of"]],
                    },
                ]
            )
