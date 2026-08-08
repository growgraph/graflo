"""(De)serialization of contract operations.

No test previously loaded a *heterogeneous* op list from YAML, because there was
no way to: the discriminated union existed only as a type annotation. These
tests are the contract for the revision layer built on top of it.
"""

from __future__ import annotations

import typing
from typing import Any

import pytest

from graflo.architecture.evolution import ops as ops_module
from graflo.architecture.evolution.codec import (
    RevisionOp,
    is_revision_op,
    op_from_dict,
    ops_from_dicts,
    ops_from_yaml,
    ops_to_dicts,
    ops_to_yaml_str,
)
from graflo.architecture.evolution.ops import ComposeManifestsOp, ManifestOp

#: One canonical payload per op, keyed by discriminator.
OP_PAYLOADS: dict[str, dict] = {
    "remove_vertices": {"names": ["ghost"]},
    "add_vertices": {
        "vertices": [{"name": "party", "properties": ["id"], "identity": ["id"]}]
    },
    "add_edges": {"edges": [{"source": "party", "target": "order"}]},
    "retarget_edges": {
        "edges": [{"source": "party", "target": "order", "new_target": "invoice"}]
    },
    "add_secondary_identities": {
        "additions": {"party": [{"name": "by_email", "fields": ["email"]}]}
    },
    "remove_secondary_identities": {"removals": {"party": ["by_email"]}},
    "replace_edge_identities": {
        "edges": [{"source": "party", "target": "order", "identities": [["ref"]]}]
    },
    "change_field_types": {"vertices": {"party": {"amount": {"type": "FLOAT"}}}},
    "add_vertex_indexes": {"indexes": {"party": [{"fields": ["email"]}]}},
    "remove_vertex_indexes": {"indexes": {"party": [["email"]]}},
    "add_edge_indexes": {
        "edges": [
            {
                "source": "party",
                "target": "order",
                "indexes": [{"fields": ["ref"]}],
            }
        ]
    },
    "remove_edge_indexes": {
        "edges": [{"source": "party", "target": "order", "fields": [["ref"]]}]
    },
    "set_edge_directed": {
        "edges": [{"source": "party", "target": "order"}],
        "directed": False,
    },
    "merge_vertices": {"sources": ["person"], "into": "party"},
    "rename_vertex_properties": {"renames": {"party": {"mail": "email"}}},
    "remove_vertex_properties": {"removals": {"party": ["scratch"]}},
    "add_vertex_properties": {"additions": {"party": ["nickname"]}},
    "rename_vertices": {"vertices": {"person": "party"}},
    "rename_relations": {"relations": {"buys": "purchases"}},
    "rename_resources": {"resources": {"src": "crm"}},
    "remove_edges": {"relations": ["obsolete"]},
    "merge_edges": {"sources": ["old"], "into": "new"},
    "rename_edge_properties": {"renames": {"purchases": {"amt": "amount"}}},
    "remove_edge_properties": {"removals": {"purchases": ["scratch"]}},
    "add_edge_properties": {"additions": {"purchases": ["note"]}},
    "add_inverse_edges": {"relations": {"purchases": "purchased_by"}},
    "project_manifest": {"keep_vertices": ["party"]},
    "replace_identity": {
        "vertices": {"party": {"to": {"mode": "natural", "identity": ["email"]}}}
    },
    "sanitize": {"db_flavor": "neo4j"},
}


def _union_members() -> list[Any]:
    return list(typing.get_args(typing.get_args(RevisionOp)[0]))


def _all_ops() -> list:
    return [
        op_from_dict({"op": name, **payload}) for name, payload in OP_PAYLOADS.items()
    ]


class TestUnionCoverage:
    def test_every_manifest_op_except_compose_is_a_revision_op(self) -> None:
        manifest_members = set(typing.get_args(typing.get_args(ManifestOp)[0]))
        revision_members = set(_union_members())

        assert manifest_members - revision_members == {ComposeManifestsOp}, (
            "compose_manifests is binary and must stay out of the revision union; "
            "every other op must be in it"
        )

    def test_the_payload_table_covers_the_whole_union(self) -> None:
        """A new op must arrive with a round-trip payload, not silently uncovered."""
        declared = {cls.model_fields["op"].default for cls in _union_members()}

        assert declared == set(OP_PAYLOADS), (
            f"missing payloads: {sorted(declared - set(OP_PAYLOADS))}; "
            f"stale payloads: {sorted(set(OP_PAYLOADS) - declared)}"
        )

    def test_every_op_is_classified_for_ingestion_reach(self) -> None:
        """A new op must be classified, so schema-only callers cannot silently no-op.

        ``INGESTION_REWRITING_OPS`` drives the schema-artifact guard in graflo-server.
        An op left out of it applies to a schema-only manifest with its ingestion half
        silently dropped, which is how a rename reaches the schema but not the
        resources that reference it.
        """
        declared = {cls.model_fields["op"].default for cls in _union_members()}
        declared.add("compose_manifests")

        stale = sorted(ops_module.INGESTION_REWRITING_OPS - declared)
        assert not stale, f"unknown ops classified as ingestion-rewriting: {stale}"

        # Ops that only ever touch schema/db_profile. Listed explicitly so adding an
        # op forces a decision rather than defaulting to "schema-only".
        schema_only = {
            "add_edge_indexes",
            "add_edge_properties",
            "add_edges",
            "add_secondary_identities",
            "add_vertex_indexes",
            "add_vertex_properties",
            "add_vertices",
            "change_field_types",
            "compose_manifests",
            "remove_edge_indexes",
            "remove_edge_properties",
            "remove_secondary_identities",
            "remove_vertex_indexes",
            "rename_edge_properties",
            "replace_edge_identities",
            "retarget_edges",
            "set_edge_directed",
        }
        unclassified = sorted(
            declared - ops_module.INGESTION_REWRITING_OPS - schema_only
        )
        assert not unclassified, (
            f"ops not classified for ingestion reach: {unclassified}; add each to "
            "INGESTION_REWRITING_OPS or to schema_only here"
        )

    def test_the_vocabulary_is_thirty_ops(self) -> None:
        exported = {
            name
            for name in dir(ops_module)
            if name.endswith("Op")
            and hasattr(getattr(ops_module, name), "model_fields")
        }
        assert len(exported) == 30
        assert len(_union_members()) == 29  # 30 minus the binary compose op


class TestRoundTrip:
    @pytest.mark.parametrize("op_name", sorted(OP_PAYLOADS))
    def test_each_op_round_trips_through_yaml(self, op_name: str) -> None:
        original = op_from_dict({"op": op_name, **OP_PAYLOADS[op_name]})

        restored = ops_from_yaml(ops_to_yaml_str([original]))

        assert len(restored) == 1
        assert type(restored[0]) is type(original)
        assert restored[0] == original

    def test_a_heterogeneous_list_round_trips(self) -> None:
        ops = _all_ops()

        restored = ops_from_yaml(ops_to_yaml_str(ops))

        assert [type(o) for o in restored] == [type(o) for o in ops]
        assert ops_to_dicts(restored) == ops_to_dicts(ops)

    def test_serialized_payload_always_carries_its_discriminator(self) -> None:
        """Without `op` the payload cannot be loaded back at all."""
        for payload in ops_to_dicts(_all_ops()):
            assert "op" in payload

    def test_a_nested_discriminator_with_a_default_survives(self) -> None:
        """`IdentityTarget.mode` defaults, so dropping defaults loses the tag.

        Serialization verifies its own output and falls back to the full form
        rather than emitting a payload that cannot be read back.
        """
        original = op_from_dict(
            {
                "op": "replace_identity",
                "vertices": {
                    "party": {"to": {"mode": "natural", "identity": ["email"]}}
                },
            }
        )

        payload = ops_to_dicts([original])[0]

        assert payload["vertices"]["party"]["to"]["mode"] == "natural"
        assert ops_from_dicts([payload])[0] == original

    def test_a_funnel_identity_target_round_trips(self) -> None:
        original = op_from_dict(
            {
                "op": "replace_identity",
                "vertices": {
                    "party": {
                        "to": {
                            "mode": "funnel",
                            "funnel": {
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
                    }
                },
            }
        )

        restored = ops_from_yaml(ops_to_yaml_str([original]))[0]

        assert restored == original
        assert restored.vertices["party"].to.funnel.branch_ids == ["email", "phone"]

    def test_ops_from_yaml_accepts_a_mapping_with_an_ops_key(self) -> None:
        body = "ops:\n" + "".join(
            f"  {line}\n" for line in ops_to_yaml_str(_all_ops()[:2]).splitlines()
        )

        assert len(ops_from_yaml(body)) == 2


class TestValidation:
    def test_an_unknown_op_is_rejected(self) -> None:
        with pytest.raises(ValueError):
            ops_from_dicts([{"op": "teleport_vertices", "names": ["a"]}])

    def test_a_payload_without_a_discriminator_is_rejected(self) -> None:
        with pytest.raises(ValueError):
            ops_from_dicts([{"names": ["a"]}])

    def test_extra_keys_are_rejected(self) -> None:
        """`extra="forbid"` is what makes a stored change set trustworthy."""
        with pytest.raises(ValueError):
            ops_from_dicts([{"op": "remove_vertices", "names": ["a"], "oops": 1}])

    def test_op_preconditions_still_run_on_load(self) -> None:
        with pytest.raises(ValueError):
            ops_from_dicts([{"op": "change_field_types"}])  # requires a target

    def test_a_non_list_document_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="expected a list of operations"):
            ops_from_yaml("just a string\n")


class TestComposeExclusion:
    def test_compose_manifests_is_not_loadable_as_a_revision_op(self) -> None:
        with pytest.raises(ValueError):
            ops_from_dicts([{"op": "compose_manifests", "onto": "other"}])

    def test_is_revision_op_screens_the_binary_op(self) -> None:
        assert is_revision_op(_all_ops()[0]) is True
        assert is_revision_op(ComposeManifestsOp()) is False
