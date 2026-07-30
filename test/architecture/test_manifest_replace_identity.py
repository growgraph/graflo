"""Tests for :class:`~graflo.architecture.evolution.ops.ReplaceIdentityOp`."""

import pytest

from graflo.architecture.contract import GraphManifest
from graflo.architecture.evolution import (
    ReplaceIdentityOp,
    apply_evolution,
)
from graflo.architecture.schema.vertex import Vertex


def _manifest(
    vertices: list[dict] | None = None,
    *,
    edges: list[dict] | None = None,
    pipeline: list[dict] | None = None,
    vertex_indexes: dict | None = None,
) -> GraphManifest:
    payload: dict = {
        "schema": {
            "metadata": {"name": "identity-demo", "version": "1.0.0"},
            "graph": {
                "vertex_config": {
                    "vertices": vertices
                    or [
                        {
                            "name": "party",
                            "properties": ["legacy_id", "party_uid", "name"],
                            "identity": ["legacy_id"],
                        }
                    ]
                },
                "edge_config": {"edges": edges or []},
            },
        }
    }
    if vertex_indexes is not None:
        payload["schema"]["db_profile"] = {"vertex_indexes": vertex_indexes}
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


def _replace(manifest: GraphManifest, **spec) -> GraphManifest:
    return apply_evolution(manifest, [ReplaceIdentityOp(vertices={"party": spec})])


class TestTargetModes:
    """The `to:` block reaches every shipped identity mode."""

    def test_natural_to_natural_swaps_the_field_set(self):
        out = _replace(_manifest(), to={"mode": "natural", "identity": ["party_uid"]})

        party = _vertex(out, "party")
        assert party.identity == ["party_uid"]
        assert party.identity_mode == "natural"

    def test_natural_to_hash(self):
        out = _replace(
            _manifest(), to={"mode": "hash", "hash_from": ["name", "party_uid"]}
        )

        party = _vertex(out, "party")
        assert party.identity_mode == "hash"
        assert party.hash_identity_properties == ["name", "party_uid"]
        assert party.identity == ["id"]

    def test_natural_to_assigned(self):
        out = _replace(_manifest(), to={"mode": "assigned"})

        party = _vertex(out, "party")
        assert party.identity_mode == "assigned"
        assert party.assigned is True
        assert party.identity == ["id"]

    def test_natural_to_blank_requires_a_non_demote_retire(self):
        out = _replace(_manifest(), to={"mode": "blank"}, retire="keep")

        party = _vertex(out, "party")
        assert party.identity_mode == "blank"
        assert party.blank is True
        assert party.secondary_identities == []

    def test_natural_to_funnel(self):
        out = _replace(
            _manifest(),
            to={
                "mode": "funnel",
                "funnel": {
                    "branches": [
                        {"id": "uid", "fields": ["party_uid"]},
                        {"id": "weak", "fields": ["name"]},
                    ]
                },
            },
        )

        party = _vertex(out, "party")
        assert party.identity_mode == "hash"
        assert party.has_identity_funnel is True
        assert party.identity_funnel is not None
        assert party.identity_funnel.branch_ids == ["uid", "weak"]
        assert party.hash_identity_properties == []
        assert party.identity == ["id"]

    def test_funnel_back_to_natural_clears_the_funnel(self):
        manifest = _manifest(
            [
                {
                    "name": "party",
                    "properties": ["email", "party_uid"],
                    "identity_funnel": {
                        "branches": [{"id": "email", "fields": ["email"]}]
                    },
                }
            ]
        )

        out = _replace(manifest, to={"mode": "natural", "identity": ["party_uid"]})

        party = _vertex(out, "party")
        assert party.identity_mode == "natural"
        assert party.identity_funnel is None

    def test_funnel_requires_its_fields_to_exist(self):
        with pytest.raises(ValueError, match="does not declare"):
            _replace(
                _manifest(),
                to={
                    "mode": "funnel",
                    "funnel": {"branches": [{"id": "a", "fields": ["nope"]}]},
                },
            )

    def test_replacing_a_funnel_with_the_same_funnel_is_a_noop(self):
        funnel = {"branches": [{"id": "email", "fields": ["email"]}]}
        manifest = _manifest(
            [
                {
                    "name": "party",
                    "properties": ["email"],
                    "identity_funnel": funnel,
                }
            ]
        )

        before = _vertex(manifest, "party").to_minimal_canonical_dict()
        out = _replace(manifest, to={"mode": "funnel", "funnel": funnel})

        assert _vertex(out, "party").to_minimal_canonical_dict() == before

    def test_hash_back_to_natural(self):
        manifest = _manifest(
            [
                {
                    "name": "party",
                    "properties": ["email", "party_uid"],
                    "hash_identity_properties": ["email"],
                }
            ]
        )

        out = _replace(manifest, to={"mode": "natural", "identity": ["party_uid"]})

        party = _vertex(out, "party")
        assert party.identity_mode == "natural"
        assert party.identity == ["party_uid"]
        assert party.hash_identity_properties == []


class TestRetirePolicy:
    def test_demote_is_the_default_and_keeps_a_lookup_key(self):
        out = _replace(_manifest(), to={"mode": "natural", "identity": ["party_uid"]})

        party = _vertex(out, "party")
        assert party.identity == ["party_uid"]
        assert [(e.name, e.fields) for e in party.secondary_identities] == [
            ("retired_identity", ["legacy_id"])
        ]

    def test_retire_as_names_the_demoted_set(self):
        out = _replace(
            _manifest(),
            to={"mode": "natural", "identity": ["party_uid"]},
            retire_as="by_legacy",
        )

        party = _vertex(out, "party")
        assert party.secondary_identity_names == ["by_legacy"]

    def test_demotion_registers_a_lookup_index(self):
        """Demotion is cheap precisely because the index is derived, not authored."""
        out = _replace(_manifest(), to={"mode": "natural", "identity": ["party_uid"]})

        assert out.graph_schema is not None
        indexes = out.graph_schema.db_profile.vertex_indexes.get("party", [])
        assert any(index.fields == ["legacy_id"] for index in indexes)

    def test_keep_leaves_the_old_fields_as_plain_properties(self):
        out = _replace(
            _manifest(),
            to={"mode": "natural", "identity": ["party_uid"]},
            retire="keep",
        )

        party = _vertex(out, "party")
        assert party.secondary_identities == []
        assert "legacy_id" in {field.name for field in party.properties}

    def test_drop_removes_the_old_fields(self):
        out = _replace(
            _manifest(),
            to={"mode": "natural", "identity": ["party_uid"]},
            retire="drop",
        )

        party = _vertex(out, "party")
        assert "legacy_id" not in {field.name for field in party.properties}
        assert party.secondary_identities == []

    def test_demote_downgrades_to_keep_for_a_synthetic_old_identity(self):
        """Demoting a hash vertex's synthetic ``id`` would key on a value no source carries."""
        manifest = _manifest(
            [
                {
                    "name": "party",
                    "properties": ["email", "party_uid"],
                    "hash_identity_properties": ["email"],
                }
            ]
        )

        out = _replace(manifest, to={"mode": "natural", "identity": ["party_uid"]})

        party = _vertex(out, "party")
        assert party.secondary_identities == []

    def test_demote_reuses_an_already_declared_secondary_identity(self):
        manifest = _manifest(
            [
                {
                    "name": "party",
                    "properties": ["legacy_id", "party_uid"],
                    "identity": ["legacy_id"],
                    "secondary_identities": [
                        {"name": "by_uid", "fields": ["party_uid"]}
                    ],
                }
            ]
        )

        out = _replace(manifest, to={"mode": "natural", "identity": ["party_uid"]})

        party = _vertex(out, "party")
        # by_uid restated the new primary and is dropped by validation; the retired
        # legacy_id set is what remains.
        assert [e.fields for e in party.secondary_identities] == [["legacy_id"]]

    def test_demote_rejects_a_colliding_retire_as(self):
        manifest = _manifest(
            [
                {
                    "name": "party",
                    "properties": ["legacy_id", "party_uid", "lei"],
                    "identity": ["legacy_id"],
                    "secondary_identities": [{"name": "by_lei", "fields": ["lei"]}],
                }
            ]
        )

        with pytest.raises(ValueError, match="already declares a secondary identity"):
            _replace(
                manifest,
                to={"mode": "natural", "identity": ["party_uid"]},
                retire_as="by_lei",
            )

    def test_blank_target_cannot_demote(self):
        with pytest.raises(ValueError, match="cannot demote"):
            _replace(_manifest(), to={"mode": "blank"})


class TestEndpointCascade:
    """`endpoints` decides whether edge steps follow the new identity or the old one."""

    @staticmethod
    def _edge_manifest() -> GraphManifest:
        return _manifest(
            [
                {
                    "name": "party",
                    "properties": ["legacy_id", "party_uid"],
                    "identity": ["legacy_id"],
                },
                {"name": "asset", "properties": ["id"], "identity": ["id"]},
            ],
            edges=[{"source": "party", "target": "asset", "relation": "holds"}],
            pipeline=[
                {"vertex": "party"},
                {"vertex": "asset"},
                {"edge": {"from": "party", "to": "asset", "relation": "holds"}},
            ],
        )

    @staticmethod
    def _edge_step(manifest: GraphManifest) -> dict:
        assert manifest.ingestion_model is not None
        for step in manifest.ingestion_model.resources[0].pipeline:
            if "edge" in step:
                return step["edge"]
        raise AssertionError("no edge step")

    def test_follow_new_leaves_endpoints_on_the_primary_identity(self):
        out = apply_evolution(
            self._edge_manifest(),
            [
                ReplaceIdentityOp(
                    vertices={
                        "party": {"to": {"mode": "natural", "identity": ["party_uid"]}}
                    }
                )
            ],
        )

        assert self._edge_step(out).get("source_match") in (None, "identity")

    def test_pin_to_retired_repoints_endpoints_at_the_old_key(self):
        out = apply_evolution(
            self._edge_manifest(),
            [
                ReplaceIdentityOp(
                    vertices={
                        "party": {
                            "to": {"mode": "natural", "identity": ["party_uid"]},
                            "retire_as": "by_legacy",
                            "endpoints": "pin_to_retired",
                        }
                    }
                )
            ],
        )

        assert self._edge_step(out)["source_match"] == "by_legacy"
        assert self._edge_step(out).get("target_match") in (None, "identity")

    def test_pin_does_not_override_an_explicit_selector(self):
        manifest = _manifest(
            [
                {
                    "name": "party",
                    "properties": ["legacy_id", "party_uid", "lei"],
                    "identity": ["legacy_id"],
                    "secondary_identities": [{"name": "by_lei", "fields": ["lei"]}],
                },
                {"name": "asset", "properties": ["id"], "identity": ["id"]},
            ],
            edges=[{"source": "party", "target": "asset", "relation": "holds"}],
            pipeline=[
                {"vertex": "party"},
                {"vertex": "asset"},
                {
                    "edge": {
                        "from": "party",
                        "to": "asset",
                        "relation": "holds",
                        "source_match": "by_lei",
                    }
                },
            ],
        )

        out = apply_evolution(
            manifest,
            [
                ReplaceIdentityOp(
                    vertices={
                        "party": {
                            "to": {"mode": "natural", "identity": ["party_uid"]},
                            "retire_as": "by_legacy",
                            "endpoints": "pin_to_retired",
                        }
                    }
                )
            ],
        )

        assert self._edge_step(out)["source_match"] == "by_lei"

    def test_pin_to_retired_requires_demotion(self):
        with pytest.raises(ValueError, match="requires retire: demote"):
            ReplaceIdentityOp(
                vertices={
                    "party": {
                        "to": {"mode": "natural", "identity": ["party_uid"]},
                        "retire": "keep",
                        "endpoints": "pin_to_retired",
                    }
                }
            )


class TestValidation:
    def test_unknown_vertex_is_rejected(self):
        with pytest.raises(ValueError, match="unknown vertices"):
            apply_evolution(
                _manifest(),
                [
                    ReplaceIdentityOp(
                        vertices={
                            "nope": {"to": {"mode": "natural", "identity": ["x"]}}
                        }
                    )
                ],
            )

    def test_identity_field_must_be_declared(self):
        with pytest.raises(ValueError, match="does not declare"):
            _replace(_manifest(), to={"mode": "natural", "identity": ["not_a_field"]})

    def test_hash_source_field_must_be_declared(self):
        with pytest.raises(ValueError, match="does not declare"):
            _replace(_manifest(), to={"mode": "hash", "hash_from": ["not_a_field"]})

    def test_list_typed_field_cannot_become_the_identity(self):
        manifest = _manifest(
            [
                {
                    "name": "party",
                    "properties": [
                        {"name": "legacy_id"},
                        {"name": "tags", "type": "LIST", "item_type": "STRING"},
                    ],
                    "identity": ["legacy_id"],
                }
            ]
        )

        with pytest.raises(ValueError, match="LIST-typed"):
            _replace(manifest, to={"mode": "natural", "identity": ["tags"]})

    def test_retire_as_without_demotion_is_rejected(self):
        with pytest.raises(ValueError, match="only meaningful with retire: demote"):
            ReplaceIdentityOp(
                vertices={
                    "party": {
                        "to": {"mode": "natural", "identity": ["x"]},
                        "retire": "keep",
                        "retire_as": "whatever",
                    }
                }
            )

    def test_replacing_with_the_same_identity_is_a_noop(self):
        manifest = _manifest()
        out = _replace(manifest, to={"mode": "natural", "identity": ["legacy_id"]})

        party = _vertex(out, "party")
        assert party.identity == ["legacy_id"]
        assert party.secondary_identities == []


class TestProfileAndRoundTrip:
    def test_index_encoding_the_old_identity_is_dropped(self):
        manifest = _manifest(vertex_indexes={"party": [{"fields": ["legacy_id"]}]})

        out = _replace(
            manifest, to={"mode": "natural", "identity": ["party_uid"]}, retire="keep"
        )

        assert out.graph_schema is not None
        indexes = out.graph_schema.db_profile.vertex_indexes.get("party", [])
        assert all(index.fields != ["legacy_id"] for index in indexes)

    def test_identity_mode_survives_a_yaml_round_trip(self):
        out = _replace(_manifest(), to={"mode": "hash", "hash_from": ["name"]})

        reloaded = GraphManifest.model_validate(out.to_dict(skip_defaults=False))

        party = _vertex(reloaded, "party")
        assert party.identity_mode == "hash"
        assert party.hash_identity_properties == ["name"]
