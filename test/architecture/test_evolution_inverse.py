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
    IRREVERSIBLE,
    invert_op,
    invert_ops,
    irreversible_reason,
    is_reversible,
)

PARTY = {"name": "party", "properties": ["id", "name", "email"], "identity": ["id"]}
ORDER = {"name": "order", "properties": ["oid", "total"], "identity": ["oid"]}
INVOICE = {"name": "invoice", "properties": ["inv"], "identity": ["inv"]}
PLACES = {
    "source": "party",
    "target": "order",
    "relation": "places",
    "properties": ["when"],
}


def _manifest(*, db_profile: dict | None = None, resources: list[dict] | None = None):
    payload: dict = {
        "schema": {
            "metadata": {"name": "inverse-demo", "version": "1.0.0"},
            "graph": {
                "vertex_config": {"vertices": [PARTY, ORDER, INVOICE]},
                "edge_config": {"edges": [PLACES]},
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
    "rename_vertices": {"op": "rename_vertices", "vertices": {"party": "person"}},
    "rename_relations": {"op": "rename_relations", "relations": {"places": "ordered"}},
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
    "add_inverse_edges": {
        "op": "add_inverse_edges",
        "relations": {"places": "placed_by"},
    },
    "replace_identity": {
        "op": "replace_identity",
        "vertices": {
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

    def test_rename_resources_round_trips(self) -> None:
        manifest = _manifest(
            resources=[{"name": "src", "pipeline": [{"vertex": "party"}]}]
        )
        _assert_round_trips(
            {"op": "rename_resources", "resources": {"src": "crm"}}, manifest=manifest
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


class TestIrreversible:
    @pytest.mark.parametrize("op_name", sorted(IRREVERSIBLE))
    def test_lossy_ops_report_a_reason_instead_of_guessing(self, op_name: str) -> None:
        assert IRREVERSIBLE[op_name]

    def test_merge_vertices_has_no_inverse(self) -> None:
        op = op_from_dict(
            {"op": "merge_vertices", "sources": ["order"], "into": "party"}
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
                "vertices": {
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
            op_from_dict({"op": "rename_vertices", "vertices": {"party": "person"}}),
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
            op_from_dict(
                {"op": "rename_relations", "relations": {"places": "ordered"}}
            ),
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
                {"op": "merge_vertices", "sources": ["order"], "into": "party"}
            ),
        ]

        inverses, blockers = invert_ops(ops, manifest=_manifest())

        assert len(inverses) == 1
        assert any("merge_vertices" in blocker for blocker in blockers)
