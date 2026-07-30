"""Identity-mode propagation through rename and merge ops.

Regression cover for two fixed defects: rename left `hash_identity_properties` /
`secondary_identities` pointing at pre-rename field names, and merge dropped
every identity-mode field except ``blank``.
"""

import pytest

from graflo.architecture.contract import GraphManifest
from graflo.architecture.evolution import (
    MergeVerticesOp,
    RenameVertexPropertiesOp,
    apply_evolution,
)
from graflo.architecture.evolution.merge_core import merge_vertex_models
from graflo.architecture.schema.vertex import Vertex


def _manifest(vertices: list[dict]) -> GraphManifest:
    return GraphManifest.model_validate(
        {
            "schema": {
                "metadata": {"name": "identity-demo", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {"vertices": vertices},
                    "edge_config": {"edges": []},
                },
            }
        }
    )


def _vertex(manifest: GraphManifest, name: str) -> Vertex:
    assert manifest.graph_schema is not None
    return manifest.graph_schema.core_schema.vertex_config._get_vertex_by_name(name)


class TestRenamePropagation:
    """`RenameVertexPropertiesOp` must rewrite every identity field-set."""

    def test_hash_identity_properties_follow_the_rename(self):
        manifest = _manifest(
            [
                {
                    "name": "party",
                    "properties": ["email", "country"],
                    "hash_identity_properties": ["email", "country"],
                }
            ]
        )

        out = apply_evolution(
            manifest,
            [RenameVertexPropertiesOp(renames={"party": {"email": "email_address"}})],
        )

        party = _vertex(out, "party")
        assert party.hash_identity_properties == ["email_address", "country"]
        assert party.identity_mode == "hash"
        assert "email" not in {f.name for f in party.properties}

    def test_identity_funnel_fields_follow_the_rename(self):
        """Same defect class as `hash_identity_properties`: a rewriter that forgets
        an identity field-set silently rekeys the graph."""
        manifest = _manifest(
            [
                {
                    "name": "party",
                    "properties": ["email", "phone", "country"],
                    "identity_funnel": {
                        "branches": [
                            {"id": "email", "fields": ["email"]},
                            {
                                "id": "phone",
                                "when_all_present": ["phone", "country"],
                                "fields": ["phone", "country"],
                            },
                        ]
                    },
                }
            ]
        )

        out = apply_evolution(
            manifest,
            [
                RenameVertexPropertiesOp(
                    renames={"party": {"email": "email_address", "phone": "phone_no"}}
                )
            ],
        )

        party = _vertex(out, "party")
        assert party.identity_mode == "hash"
        assert party.identity_funnel is not None
        assert party.identity_funnel.branches[0].fields == ["email_address"]
        assert party.identity_funnel.branches[1].fields == ["phone_no", "country"]
        assert party.identity_funnel.branches[1].when_all_present == [
            "phone_no",
            "country",
        ]
        assert "email" not in {f.name for f in party.properties}

    def test_secondary_identity_fields_follow_the_rename(self):
        manifest = _manifest(
            [
                {
                    "name": "instrument",
                    "properties": ["id", "isin", "venue"],
                    "identity": ["id"],
                    "secondary_identities": [
                        {"name": "by_isin", "fields": ["isin"]},
                        {"name": "by_venue_code", "fields": ["venue", "isin"]},
                    ],
                }
            ]
        )

        out = apply_evolution(
            manifest,
            [RenameVertexPropertiesOp(renames={"instrument": {"isin": "isin_code"}})],
        )

        instrument = _vertex(out, "instrument")
        assert [entry.fields for entry in instrument.secondary_identities] == [
            ["isin_code"],
            ["venue", "isin_code"],
        ]
        assert [entry.name for entry in instrument.secondary_identities] == [
            "by_isin",
            "by_venue_code",
        ]
        assert "isin" not in {f.name for f in instrument.properties}

    def test_rename_collapsing_a_composite_set_dedupes(self):
        """Renaming two members of one set onto the same name must not duplicate it."""
        manifest = _manifest(
            [
                {
                    "name": "party",
                    "properties": ["id", "first", "last"],
                    "identity": ["id"],
                    "secondary_identities": [
                        {"name": "by_name", "fields": ["first", "last"]}
                    ],
                }
            ]
        )

        out = apply_evolution(
            manifest,
            [
                RenameVertexPropertiesOp(
                    renames={"party": {"first": "full_name", "last": "full_name"}}
                )
            ],
        )

        party = _vertex(out, "party")
        assert [entry.fields for entry in party.secondary_identities] == [["full_name"]]


class TestMergePropagation:
    """`MergeVerticesOp` must carry identity mode through the merge."""

    def test_assigned_survives_the_merge(self):
        manifest = _manifest(
            [
                {"name": "party_a", "properties": ["id", "name"], "assigned": True},
                {"name": "party_b", "properties": ["id", "email"], "assigned": True},
            ]
        )

        out = apply_evolution(
            manifest, [MergeVerticesOp(sources=["party_b"], into="party_a")]
        )

        party = _vertex(out, "party_a")
        assert party.assigned is True
        assert party.identity_mode == "assigned"

    def test_hash_identity_properties_are_unioned(self):
        manifest = _manifest(
            [
                {
                    "name": "party_a",
                    "properties": ["email"],
                    "hash_identity_properties": ["email"],
                },
                {
                    "name": "party_b",
                    "properties": ["email", "country"],
                    "hash_identity_properties": ["email", "country"],
                },
            ]
        )

        out = apply_evolution(
            manifest, [MergeVerticesOp(sources=["party_b"], into="party_a")]
        )

        party = _vertex(out, "party_a")
        assert party.hash_identity_properties == ["email", "country"]
        assert party.identity_mode == "hash"

    def test_identical_identity_funnels_merge_cleanly(self):
        funnel = {"branches": [{"id": "email", "fields": ["email"]}]}
        manifest = _manifest(
            [
                {"name": "party_a", "properties": ["email"], "identity_funnel": funnel},
                {
                    "name": "party_b",
                    "properties": ["email", "country"],
                    "identity_funnel": funnel,
                },
            ]
        )

        out = apply_evolution(
            manifest, [MergeVerticesOp(sources=["party_b"], into="party_a")]
        )

        party = _vertex(out, "party_a")
        assert party.identity_funnel is not None
        assert party.identity_funnel.branch_ids == ["email"]

    def test_divergent_identity_funnels_refuse_to_merge(self):
        """Branch order and ids decide the key, so unioning them would rekey data."""
        manifest = _manifest(
            [
                {
                    "name": "party_a",
                    "properties": ["email"],
                    "identity_funnel": {
                        "branches": [{"id": "email", "fields": ["email"]}]
                    },
                },
                {
                    "name": "party_b",
                    "properties": ["email", "country"],
                    "identity_funnel": {
                        "branches": [{"id": "country", "fields": ["country"]}]
                    },
                },
            ]
        )

        with pytest.raises(ValueError, match="different identity funnels"):
            apply_evolution(
                manifest, [MergeVerticesOp(sources=["party_b"], into="party_a")]
            )

    def test_funnel_and_flat_hash_refuse_to_merge(self):
        manifest = _manifest(
            [
                {
                    "name": "party_a",
                    "properties": ["email"],
                    "identity_funnel": {
                        "branches": [{"id": "email", "fields": ["email"]}]
                    },
                },
                {
                    "name": "party_b",
                    "properties": ["email"],
                    "hash_identity_properties": ["email"],
                },
            ]
        )

        with pytest.raises(ValueError, match="funnel with flat hash properties"):
            apply_evolution(
                manifest, [MergeVerticesOp(sources=["party_b"], into="party_a")]
            )

    def test_secondary_identities_are_unioned_and_deduped(self):
        manifest = _manifest(
            [
                {
                    "name": "inst_a",
                    "properties": ["id", "isin"],
                    "identity": ["id"],
                    "secondary_identities": [{"name": "by_isin", "fields": ["isin"]}],
                },
                {
                    "name": "inst_b",
                    "properties": ["id", "isin", "lei"],
                    "identity": ["id"],
                    "secondary_identities": [
                        {"name": "by_isin", "fields": ["isin"]},
                        {"name": "by_lei", "fields": ["lei"]},
                    ],
                },
            ]
        )

        out = apply_evolution(
            manifest, [MergeVerticesOp(sources=["inst_b"], into="inst_a")]
        )

        inst = _vertex(out, "inst_a")
        assert [entry.name for entry in inst.secondary_identities] == [
            "by_isin",
            "by_lei",
        ]

    def test_secondary_identity_equal_to_merged_primary_is_dropped(self):
        """A set that becomes a restatement of the primary is dropped, not raised.

        ``Vertex._validated_secondary_identities`` rejects a secondary identity equal
        to the primary, so carrying it through the union would make the merged vertex
        unconstructible.
        """
        merged = merge_vertex_models(
            [
                Vertex.model_validate(
                    {
                        "name": "a",
                        "properties": ["id", "code"],
                        "identity": ["id"],
                        "secondary_identities": [
                            {"name": "by_pair", "fields": ["id", "code"]}
                        ],
                    }
                ),
                Vertex.model_validate(
                    {"name": "b", "properties": ["code"], "identity": ["code"]}
                ),
            ],
            "a",
        )

        assert merged.identity == ["id", "code"]
        assert merged.secondary_identities == []

    def test_narrower_secondary_identity_survives_the_merge(self):
        """A strict subset of the merged primary is a real lookup key, not a restatement."""
        merged = merge_vertex_models(
            [
                Vertex.model_validate(
                    {
                        "name": "a",
                        "properties": ["id", "code"],
                        "identity": ["id"],
                        "secondary_identities": [
                            {"name": "by_code", "fields": ["code"]}
                        ],
                    }
                ),
                Vertex.model_validate(
                    {"name": "b", "properties": ["code"], "identity": ["code"]}
                ),
            ],
            "a",
        )

        assert merged.identity == ["id", "code"]
        assert [entry.fields for entry in merged.secondary_identities] == [["code"]]

    def test_name_reused_for_different_field_sets_is_a_conflict(self):
        with pytest.raises(ValueError, match="refers to"):
            merge_vertex_models(
                [
                    Vertex.model_validate(
                        {
                            "name": "a",
                            "properties": ["id", "isin"],
                            "identity": ["id"],
                            "secondary_identities": [
                                {"name": "by_code", "fields": ["isin"]}
                            ],
                        }
                    ),
                    Vertex.model_validate(
                        {
                            "name": "b",
                            "properties": ["id", "lei"],
                            "identity": ["id"],
                            "secondary_identities": [
                                {"name": "by_code", "fields": ["lei"]}
                            ],
                        }
                    ),
                ],
                "a",
            )

    def test_blank_and_assigned_sources_cannot_merge(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            merge_vertex_models(
                [
                    Vertex.model_validate(
                        {"name": "a", "properties": ["x"], "blank": True}
                    ),
                    Vertex.model_validate(
                        {"name": "b", "properties": ["y"], "assigned": True}
                    ),
                ],
                "a",
            )

    def test_assigned_and_hash_sources_cannot_merge(self):
        with pytest.raises(ValueError, match="assigned source"):
            merge_vertex_models(
                [
                    Vertex.model_validate(
                        {"name": "a", "properties": ["x"], "assigned": True}
                    ),
                    Vertex.model_validate(
                        {
                            "name": "b",
                            "properties": ["y"],
                            "hash_identity_properties": ["y"],
                        }
                    ),
                ],
                "a",
            )

    def test_blank_source_cannot_absorb_secondary_identities(self):
        with pytest.raises(ValueError, match="blank source"):
            merge_vertex_models(
                [
                    Vertex.model_validate(
                        {"name": "a", "properties": ["x"], "blank": True}
                    ),
                    Vertex.model_validate(
                        {
                            "name": "b",
                            "properties": ["id", "lei"],
                            "identity": ["id"],
                            "secondary_identities": [
                                {"name": "by_lei", "fields": ["lei"]}
                            ],
                        }
                    ),
                ],
                "a",
            )
