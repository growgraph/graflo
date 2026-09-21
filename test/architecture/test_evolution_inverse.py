"""Inverses for the reversible subset of contract operations.

The property under test is the round trip: applying an op and then its inverse
must reproduce the original manifest *by content hash* — not merely look
similar. Anything less makes a "downgrade" a quiet corruption.
"""

from __future__ import annotations

import pytest

from graflo.architecture.contract import GraphManifest
from graflo.architecture.evolution import apply_evolution
from graflo.architecture.evolution.codec import op_from_dict
from graflo.architecture.evolution.hashing import manifest_hash
from graflo.architecture.evolution.inverse import (
    _HANDLERS,
    IRREVERSIBLE,
    invert_op,
    invert_ops,
    irreversible_reason,
    is_reversible,
)
from graflo.architecture.evolution.ops import MergeManifestsOp
from test.architecture.test_evolution_codec import OP_PAYLOADS, _union_members

PARTY = {"name": "party", "properties": ["id", "name", "email"], "identity": ["id"]}
ORDER = {"name": "order", "properties": ["oid", "total"], "identity": ["oid"]}
INVOICE = {"name": "invoice", "properties": ["inv"], "identity": ["inv"]}
PLACES_ID = {"source": "party", "target": "order", "relation": "places"}
PLACES = {**PLACES_ID, "properties": ["when"]}


def _manifest(
    *,
    db_profile: dict | None = None,
    resources: list[dict] | None = None,
    extra_edges: list[dict] | None = None,
    inverses: dict[str, str] | None = None,
):
    payload: dict = {
        "schema": {
            "metadata": {"name": "inverse-demo", "version": "1.0.0"},
            "graph": {
                "vertex_config": {"vertices": [PARTY, ORDER, INVOICE]},
                "edge_config": {
                    "edges": [PLACES, *(extra_edges or [])],
                    "inverses": [
                        {"relation": r, "inverse": i}
                        for r, i in (inverses or {}).items()
                    ],
                },
            },
        }
    }
    if db_profile is not None:
        payload["schema"]["db_profile"] = db_profile
    if resources is not None:
        payload["ingestion_model"] = {"resources": resources}
    return GraphManifest.model_validate(payload)


def _assert_round_trips(op_payload: dict, *, manifest: GraphManifest | None = None):
    """apply(op) then apply(inverse(op)) must reproduce the starting manifest."""
    base = manifest if manifest is not None else _manifest()
    op = op_from_dict(op_payload)
    before = manifest_hash(base)

    inverse = invert_op(op, manifest=base)
    assert inverse is not None, f"{op.op} should be reversible"

    forward = apply_evolution(base, [op], bump_version=False, finish_init=False)
    assert manifest_hash(forward) != before, "the op must actually change something"

    restored = apply_evolution(
        forward, [inverse], bump_version=False, finish_init=False
    )
    assert manifest_hash(restored) == before
    return inverse


REVERSIBLE_CASES = {
    "add_vertices": {
        "op": "add_vertices",
        "vertices": [{"name": "audit", "properties": ["aid"], "identity": ["aid"]}],
    },
    "remove_vertices": {"op": "remove_vertices", "names": ["invoice"]},
    "add_edges": {
        "op": "add_edges",
        "edges": [{"source": "order", "target": "invoice", "relation": "billed"}],
    },
    "remove_edges_by_triple": {"op": "remove_edges", "edges": [PLACES_ID]},
    "remove_edges": {"op": "remove_edges", "relations": ["places"]},
    "add_vertex_properties": {
        "op": "add_vertex_properties",
        "additions": {"party": ["nickname"]},
    },
    "remove_vertex_properties": {
        "op": "remove_vertex_properties",
        "removals": {"party": ["email"]},
    },
    "add_edge_properties": {
        "op": "add_edge_properties",
        "additions": {"places": ["channel"]},
    },
    "remove_edge_properties": {
        "op": "remove_edge_properties",
        "removals": {"places": ["when"]},
    },
    "rename_vertices": {"op": "rename_vertices", "renames": {"party": "person"}},
    "rename_relations": {"op": "rename_relations", "renames": {"places": "ordered"}},
    "rename_vertex_properties": {
        "op": "rename_vertex_properties",
        "renames": {"party": {"name": "label"}},
    },
    "rename_edge_properties": {
        "op": "rename_edge_properties",
        "renames": {"places": {"when": "at"}},
    },
    "set_edge_directed": {
        "op": "set_edge_directed",
        "edges": [{"source": "party", "target": "order", "relation": "places"}],
        "directed": False,
    },
    "add_secondary_identities": {
        "op": "add_secondary_identities",
        "additions": {"party": [{"name": "by_email", "fields": ["email"]}]},
    },
    "retarget_edges": {
        "op": "retarget_edges",
        "edges": [
            {
                "source": "party",
                "target": "order",
                "relation": "places",
                "new_target": "invoice",
            }
        ],
    },
    "declare_edge_inverses": {
        "op": "declare_edge_inverses",
        "inverses": {"places": "placed_by"},
    },
    "replace_identity": {
        "op": "replace_identity",
        "replacements": {
            "party": {
                "to": {"mode": "natural", "identity": ["email"]},
                "retire": "keep",
            }
        },
    },
}


class TestReversibleRoundTrip:
    @pytest.mark.parametrize("name", sorted(REVERSIBLE_CASES))
    def test_op_then_inverse_restores_the_manifest(self, name: str) -> None:
        _assert_round_trips(REVERSIBLE_CASES[name])

    def test_add_inverse_edges_round_trips(self) -> None:
        declared = _manifest(inverses={"places": "placed_by"})
        _assert_round_trips(
            {"op": "add_inverse_edges", "relations": ["places"]}, manifest=declared
        )
        _assert_round_trips({"op": "add_inverse_edges"}, manifest=declared)

    def test_retract_edge_inverses_round_trips(self) -> None:
        _assert_round_trips(
            {"op": "retract_edge_inverses", "relations": ["placed_by"]},
            manifest=_manifest(inverses={"places": "placed_by"}),
        )

    def test_set_native_inverses_round_trips_both_ways(self) -> None:
        tigergraph = {"db_flavor": "tigergraph"}
        declared = _manifest(db_profile=tigergraph, inverses={"places": "placed_by"})
        _assert_round_trips(
            {"op": "set_native_inverses", "relations": ["places"]}, manifest=declared
        )
        native = apply_evolution(
            declared,
            [op_from_dict({"op": "set_native_inverses", "relations": ["places"]})],
            bump_version=False,
        )
        _assert_round_trips(
            {"op": "set_native_inverses", "relations": ["places"], "enabled": False},
            manifest=native,
        )

    def test_declaring_a_pair_in_both_orders_round_trips(self) -> None:
        _assert_round_trips(
            {
                "op": "declare_edge_inverses",
                "inverses": {"places": "placed_by", "placed_by": "places"},
            }
        )

    def test_symmetric_declarations_round_trip(self) -> None:
        linked = {
            "source": "party",
            "target": "party",
            "relation": "knows",
            "directed": False,
        }
        manifest = _manifest(extra_edges=[linked])
        _assert_round_trips(
            {"op": "declare_edge_inverses", "symmetric": ["knows"]}, manifest=manifest
        )
        declared = apply_evolution(
            manifest,
            [op_from_dict({"op": "declare_edge_inverses", "symmetric": ["knows"]})],
            bump_version=False,
        )
        _assert_round_trips(
            {"op": "retract_edge_inverses", "relations": ["knows"]}, manifest=declared
        )

    def test_rename_resources_round_trips(self) -> None:
        manifest = _manifest(
            resources=[{"name": "src", "pipeline": [{"vertex": "party"}]}]
        )
        _assert_round_trips(
            {"op": "rename_resources", "renames": {"src": "crm"}}, manifest=manifest
        )

    def test_add_vertex_indexes_round_trips(self) -> None:
        _assert_round_trips(
            {"op": "add_vertex_indexes", "indexes": {"party": [{"fields": ["email"]}]}}
        )

    def test_remove_vertex_indexes_round_trips(self) -> None:
        manifest = _manifest(
            db_profile={"vertex_indexes": {"party": [{"fields": ["email"]}]}}
        )
        _assert_round_trips(
            {"op": "remove_vertex_indexes", "indexes": {"party": [["email"]]}},
            manifest=manifest,
        )

    def test_set_bindings_round_trips_when_it_adds_the_block(self) -> None:
        """The inverse must be able to say "there was no block", not just "a block"."""
        _assert_round_trips(
            {
                "op": "set_bindings",
                "bindings": {
                    "connectors": [{"regex": "^a\\.csv$", "resource_name": "src"}]
                },
            }
        )

    def test_set_bindings_round_trips_when_it_removes_the_block(self) -> None:
        manifest = GraphManifest.model_validate(
            {
                **_manifest().to_dict(skip_defaults=True),
                "bindings": {
                    "connectors": [{"regex": "^a\\.csv$", "resource_name": "src"}]
                },
            }
        )
        _assert_round_trips({"op": "set_bindings", "bindings": None}, manifest=manifest)

    def test_set_db_profile_round_trips(self) -> None:
        """Storage names are content-hashed, so the restore must be exact."""
        _assert_round_trips(
            {
                "op": "set_db_profile",
                "profile": {"vertex_storage_names": {"party": "p"}},
            }
        )

    def test_remove_secondary_identities_round_trips(self) -> None:
        manifest = apply_evolution(
            _manifest(),
            [
                op_from_dict(
                    {
                        "op": "add_secondary_identities",
                        "additions": {
                            "party": [{"name": "by_email", "fields": ["email"]}]
                        },
                    }
                )
            ],
            bump_version=False,
            finish_init=False,
        )
        _assert_round_trips(
            {"op": "remove_secondary_identities", "removals": {"party": ["by_email"]}},
            manifest=manifest,
        )

    @pytest.mark.parametrize(
        "selector",
        [["email"], "secondary"],
        ids=["field-list", "secondary-shorthand"],
    )
    def test_remove_secondary_identities_round_trips_for_every_selector_shape(
        self, selector: str | list[str]
    ) -> None:
        """The inverse must resolve selectors the way the forward op does."""
        manifest = apply_evolution(
            _manifest(),
            [
                op_from_dict(
                    {
                        "op": "add_secondary_identities",
                        "additions": {
                            "party": [{"name": "by_email", "fields": ["email"]}]
                        },
                    }
                )
            ],
            bump_version=False,
            finish_init=False,
        )
        _assert_round_trips(
            {"op": "remove_secondary_identities", "removals": {"party": [selector]}},
            manifest=manifest,
        )

    def test_an_inverse_restores_the_full_vertex_not_just_its_name(self) -> None:
        """Inverting a removal needs the pre-state; a name alone loses everything."""
        base = _manifest()
        op = op_from_dict({"op": "remove_vertices", "names": ["invoice"]})

        inverse = invert_op(op, manifest=base)

        assert inverse is not None
        assert inverse.op == "add_vertices"
        restored = inverse.vertices[0]
        assert restored.name == "invoice"
        assert restored.identity == ["inv"]


class TestAdditiveInversesUndoOnlyWhatWasAdded:
    """The forward ops skip entries already present; the inverses must too."""

    def test_add_vertex_properties_keeps_a_pre_existing_field(self) -> None:
        inverse = _assert_round_trips(
            {"op": "add_vertex_properties", "additions": {"party": ["email", "nick"]}}
        )
        assert inverse.removals == {"party": ["nick"]}

    def test_add_edge_properties_keeps_a_pre_existing_field(self) -> None:
        inverse = _assert_round_trips(
            {"op": "add_edge_properties", "additions": {"places": ["when", "channel"]}}
        )
        assert inverse.removals == {"places": ["channel"]}

    def test_add_edge_properties_refuses_when_sibling_edges_disagree(self) -> None:
        """A relation-wide removal cannot undo an addition to only some edges."""
        manifest = _manifest(
            extra_edges=[
                {
                    "source": "order",
                    "target": "invoice",
                    "relation": "places",
                    "properties": ["channel"],
                }
            ]
        )
        op = op_from_dict(
            {"op": "add_edge_properties", "additions": {"places": ["channel"]}}
        )
        assert invert_op(op, manifest=manifest) is None

    def test_add_vertex_indexes_keeps_a_pre_existing_index(self) -> None:
        manifest = _manifest(
            db_profile={"vertex_indexes": {"party": [{"fields": ["email"]}]}}
        )
        inverse = _assert_round_trips(
            {
                "op": "add_vertex_indexes",
                "indexes": {"party": [{"fields": ["email"]}, {"fields": ["name"]}]},
            },
            manifest=manifest,
        )
        assert inverse.indexes == {"party": [["name"]]}

    def test_add_edges_removes_only_the_added_triple(self) -> None:
        """Another edge on the same relation must survive the inverse."""
        inverse = _assert_round_trips(
            {
                "op": "add_edges",
                "edges": [
                    {"source": "order", "target": "invoice", "relation": "places"}
                ],
            }
        )
        assert inverse.relations == []
        assert [s.edge_id() for s in inverse.edges] == [("order", "invoice", "places")]

    def test_add_inverse_edges_removes_only_the_created_edges(self) -> None:
        """A pre-existing edge under the inverse relation must survive the inverse."""
        manifest = _manifest(
            extra_edges=[
                {"source": "invoice", "target": "order", "relation": "placed_by"}
            ],
            inverses={"places": "placed_by"},
        )
        inverse = _assert_round_trips(
            {"op": "add_inverse_edges", "relations": ["places"]},
            manifest=manifest,
        )
        assert [s.edge_id() for s in inverse.edges] == [("order", "party", "placed_by")]

    def test_a_forward_op_that_changed_nothing_needs_no_inverse(self) -> None:
        op = op_from_dict(
            {"op": "add_vertex_properties", "additions": {"party": ["email"]}}
        )
        assert invert_ops([op], manifest=_manifest()) == ([], [])


class TestAnInverseIsExactOrAbsent:
    """A candidate that does not land back on the pre-state is never offered."""

    def test_a_removed_property_comes_back_with_its_type(self) -> None:
        typed = {**ORDER, "properties": ["oid", {"name": "total", "type": "FLOAT"}]}
        base = GraphManifest.model_validate(
            {
                "schema": {
                    "metadata": {"name": "inverse-demo"},
                    "graph": {
                        "vertex_config": {"vertices": [typed]},
                        "edge_config": {"edges": []},
                    },
                }
            }
        )
        _assert_round_trips(
            {"op": "remove_vertex_properties", "removals": {"order": ["total"]}},
            manifest=base,
        )

    def test_removing_a_property_the_type_never_had_needs_no_inverse(self) -> None:
        op = op_from_dict(
            {"op": "remove_vertex_properties", "removals": {"party": ["nickname"]}}
        )
        assert invert_ops([op], manifest=_manifest()) == ([], [])

    def test_a_vertex_removal_that_cascaded_over_an_edge_has_no_inverse(self) -> None:
        """Re-adding the vertex does not bring the edge back, and one op cannot."""
        op = op_from_dict({"op": "remove_vertices", "names": ["order"]})

        assert invert_op(op, manifest=_manifest()) is None
        assert invert_ops([op], manifest=_manifest()) == (
            [],
            ["remove_vertices: no inverse could be derived"],
        )

    def test_a_vertex_removal_that_cascaded_nothing_round_trips(self) -> None:
        _assert_round_trips({"op": "remove_vertices", "names": ["invoice"]})

    def test_remove_edge_properties_refuses_when_sibling_edges_disagree(self) -> None:
        """Adding the field back by relation would put it on the edge without it."""
        bare = {"source": "party", "target": "invoice", "relation": "places"}
        op = op_from_dict(
            {"op": "remove_edge_properties", "removals": {"places": ["when"]}}
        )

        assert invert_op(op, manifest=_manifest(extra_edges=[bare])) is None

    def test_a_rename_onto_a_name_already_taken_has_no_inverse(self) -> None:
        """Two fields became one; renaming back cannot make them two again."""
        op = op_from_dict(
            {"op": "rename_vertex_properties", "renames": {"party": {"email": "name"}}}
        )

        assert invert_op(op, manifest=_manifest()) is None


class TestReplaceEdgeIdentities:
    def test_round_trips_when_the_new_key_uses_existing_properties(self) -> None:
        _assert_round_trips(
            {
                "op": "replace_edge_identities",
                "edges": [{**PLACES_ID, "identities": [["when"]]}],
            }
        )

    def test_refuses_when_the_new_key_would_add_a_property(self) -> None:
        """`finish_init` folds identity tokens into properties; one op cannot undo both."""
        op = op_from_dict(
            {
                "op": "replace_edge_identities",
                "edges": [{**PLACES_ID, "identities": [["ref"]]}],
            }
        )
        assert invert_op(op, manifest=_manifest()) is None


class TestIrreversible:
    def test_every_revision_op_is_either_invertible_or_declared_lossy(self) -> None:
        """An op in neither table fails with a generic message instead of a reason."""
        declared = {cls.model_fields["op"].default for cls in _union_members()}
        unclassified = sorted(declared - set(_HANDLERS) - set(IRREVERSIBLE))
        assert not unclassified, (
            f"ops with neither an inverse handler nor an IRREVERSIBLE reason: "
            f"{unclassified}"
        )

    @pytest.mark.parametrize("op_name", sorted(IRREVERSIBLE))
    def test_lossy_ops_report_a_reason_instead_of_guessing(self, op_name: str) -> None:
        assert IRREVERSIBLE[op_name]
        if op_name == "merge_manifests":
            op = MergeManifestsOp()
        else:
            op = op_from_dict({"op": op_name, **OP_PAYLOADS[op_name]})
        assert is_reversible(op) is False
        assert invert_op(op, manifest=_manifest()) is None

    def test_ensure_extracted_fields_is_reported_with_its_reason(self) -> None:
        manifest = _manifest(
            resources=[{"name": "crm", "pipeline": [{"vertex": "party"}]}]
        )
        op = op_from_dict(
            {
                "op": "ensure_extracted_fields",
                "additions": {"crm": [{"vertex": "party", "fields": ["name"]}]},
            }
        )
        _, blockers = invert_ops([op], manifest=manifest)
        assert blockers == [
            f"ensure_extracted_fields: {IRREVERSIBLE['ensure_extracted_fields']}"
        ]

    def test_merge_vertices_has_no_inverse(self) -> None:
        op = op_from_dict(
            {"op": "merge_vertices", "sources": ["order"], "into": "party"}
        )

        assert is_reversible(op) is False
        assert invert_op(op, manifest=_manifest()) is None
        assert "discards" in (irreversible_reason(op) or "")

    def test_canonicalize_inverts_when_it_only_renames(self) -> None:
        op = op_from_dict(
            {
                "op": "canonicalize",
                "vertices": {"order": "purchase"},
                "properties": {"order": {"oid": "purchase_id"}},
                "relations": {"places": "purchases"},
            }
        )

        assert is_reversible(op) is True
        inverse = invert_op(op, manifest=_manifest())
        assert inverse is not None
        assert inverse.op == "canonicalize"
        assert inverse.vertices == {"purchase": "order"}
        # Attribute maps re-key onto the renamed class, which is what the
        # inverse sees when it runs.
        assert inverse.properties == {"purchase": {"purchase_id": "oid"}}
        assert inverse.relations == {"purchases": "places"}

    def test_an_op_the_manifest_refuses_has_no_inverse(self) -> None:
        """Nothing was done, so there is nothing to undo -- and nothing to replay."""
        op = op_from_dict({"op": "rename_relations", "renames": {"buys": "purchases"}})

        assert invert_op(op, manifest=_manifest()) is None

    def test_canonicalize_has_no_inverse_when_it_merges(self) -> None:
        op = op_from_dict(
            {
                "op": "canonicalize",
                "vertices": {"order": "party", "party": "party"},
                "allow_merges": True,
            }
        )

        assert is_reversible(op) is False
        assert invert_op(op, manifest=_manifest()) is None
        assert "discards" in (irreversible_reason(op) or "")

    def test_change_field_types_has_no_inverse(self) -> None:
        op = op_from_dict(
            {
                "op": "change_field_types",
                "vertices": {"party": {"name": {"type": "STRING"}}},
            }
        )

        assert invert_op(op, manifest=_manifest()) is None

    def test_a_demoting_identity_replacement_refuses_to_invert(self) -> None:
        """Demotion also rewrites secondary identities; a partial undo is worse."""
        op = op_from_dict(
            {
                "op": "replace_identity",
                "replacements": {
                    "party": {
                        "to": {"mode": "natural", "identity": ["email"]},
                        "retire": "demote",
                    }
                },
            }
        )

        assert invert_op(op, manifest=_manifest()) is None


class TestInvertSequence:
    def test_a_sequence_inverts_in_reverse_order(self) -> None:
        base = _manifest()
        ops = [
            op_from_dict(
                {"op": "add_vertex_properties", "additions": {"party": ["tag"]}}
            ),
            op_from_dict({"op": "rename_vertices", "renames": {"party": "person"}}),
        ]

        inverses, blockers = invert_ops(ops, manifest=base)

        assert not blockers
        assert [op.op for op in inverses] == [
            "rename_vertices",
            "remove_vertex_properties",
        ]

    def test_inverting_a_sequence_restores_the_manifest(self) -> None:
        base = _manifest()
        ops = [
            op_from_dict(
                {"op": "add_vertex_properties", "additions": {"party": ["tag"]}}
            ),
            op_from_dict({"op": "remove_vertices", "names": ["invoice"]}),
            op_from_dict({"op": "rename_relations", "renames": {"places": "ordered"}}),
        ]
        forward = apply_evolution(base, ops, bump_version=False, finish_init=False)

        inverses, blockers = invert_ops(ops, manifest=base)
        restored = apply_evolution(
            forward, inverses, bump_version=False, finish_init=False
        )

        assert not blockers
        assert manifest_hash(restored) == manifest_hash(base)

    def test_an_irreversible_op_is_reported_not_skipped_silently(self) -> None:
        ops = [
            op_from_dict(
                {"op": "add_vertex_properties", "additions": {"party": ["tag"]}}
            ),
            op_from_dict(
                {
                    "op": "merge_vertices",
                    "sources": ["order"],
                    "into": "party",
                    "allow_self_relations": True,
                }
            ),
        ]

        inverses, blockers = invert_ops(ops, manifest=_manifest())

        assert len(inverses) == 1
        assert any("merge_vertices" in blocker for blocker in blockers)
