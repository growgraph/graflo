"""(De)serialization of contract operations.

No test previously loaded a *heterogeneous* op list from YAML, because there was
no way to: the discriminated union existed only as a type annotation. These
tests are the contract for the revision layer built on top of it.
"""

from __future__ import annotations

import typing
from typing import Any

import pytest
from pydantic import ValidationError

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
    "set_vertex_semantics": {
        "semantics": {"party": {"iri": "https://schema.org/Organization"}},
    },
    "set_edge_semantics": {
        "edges": [{"source": "party", "target": "order"}],
        "semantics": {"iri": "https://schema.org/seller"},
    },
    "set_field_semantics": {
        "targets": [
            {
                "vertex": "party",
                "field": "mail",
                "semantics": {"iri": "https://schema.org/email"},
            }
        ],
    },
    "merge_vertices": {"sources": ["person"], "into": "party"},
    "canonicalize": {"vertices": {"person": "party"}},
    "rename_vertex_properties": {"renames": {"party": {"mail": "email"}}},
    "remove_vertex_properties": {"removals": {"party": ["scratch"]}},
    "add_vertex_properties": {"additions": {"party": ["nickname"]}},
    "rename_vertices": {"renames": {"person": "party"}},
    "rename_relations": {"renames": {"buys": "purchases"}},
    "rename_resources": {"renames": {"src": "crm"}},
    "remove_edges": {"relations": ["obsolete"]},
    "merge_edges": {"sources": ["old"], "into": "new"},
    "rename_edge_properties": {"renames": {"purchases": {"amt": "amount"}}},
    "remove_edge_properties": {"removals": {"purchases": ["scratch"]}},
    "add_edge_properties": {"additions": {"purchases": ["note"]}},
    "add_inverse_edges": {"inverses": {"purchases": "purchased_by"}},
    "add_resource_transforms": {
        "additions": {
            "crm": [
                {
                    "transform": {
                        "call": {
                            "module": "graflo.util.transform",
                            "foo": "tagged_key",
                            "params": {"tag": "a"},
                            "input": ["party_id"],
                            "output": ["local_key"],
                        }
                    }
                }
            ]
        },
        "at": {"crm": [1]},
    },
    "ensure_extracted_fields": {
        "additions": {"crm": [{"vertex": "party", "fields": ["local_key"], "at": [1]}]}
    },
    "add_resources": {
        "resources": [{"name": "crm", "pipeline": [{"vertex": "party"}]}]
    },
    "remove_resources": {"names": ["crm"]},
    "project_manifest": {"keep_vertices": ["party"]},
    "replace_identity": {
        "replacements": {"party": {"to": {"mode": "natural", "identity": ["email"]}}}
    },
    "sanitize": {"db_flavor": "neo4j"},
}


#: A manifest carrying an ingestion block, against which every schema-only op
#: in ``SCHEMA_ONLY_PAYLOADS`` applies cleanly.
_PURCHASES = {"source": "party", "target": "order", "relation": "purchases"}
SCHEMA_ONLY_FIXTURE: dict = {
    "schema": {
        "metadata": {"name": "codec-demo", "version": "1.0.0"},
        "graph": {
            "vertex_config": {
                "vertices": [
                    {
                        "name": "party",
                        "properties": ["id", "email", "amount"],
                        "identity": ["id"],
                        "secondary_identities": [
                            {"name": "by_email", "fields": ["email"]}
                        ],
                    },
                    {"name": "order", "properties": ["oid"], "identity": ["oid"]},
                    {"name": "invoice", "properties": ["inv"], "identity": ["inv"]},
                ]
            },
            "edge_config": {
                "edges": [
                    {**_PURCHASES, "properties": ["ref", "note"]},
                    {"source": "order", "target": "invoice", "relation": "billed"},
                ]
            },
        },
        "db_profile": {
            "vertex_indexes": {"party": [{"fields": ["amount"]}]},
            "edge_specs": [{**_PURCHASES, "indexes": [{"fields": ["ref"]}]}],
        },
    },
    "ingestion_model": {
        "resources": [
            {
                "name": "crm",
                "pipeline": [
                    {"vertex": "party"},
                    {"vertex": "order"},
                    {"source": "party", "target": "order", "relation": "purchases"},
                ],
            }
        ]
    },
}

#: One applicable payload per op that ``INGESTION_REWRITING_OPS`` leaves out.
SCHEMA_ONLY_PAYLOADS: dict[str, dict] = {
    "add_edge_indexes": {"edges": [{**_PURCHASES, "indexes": [{"fields": ["note"]}]}]},
    "add_edge_properties": {"additions": {"purchases": ["memo"]}},
    "add_edges": {
        "edges": [{"source": "party", "target": "invoice", "relation": "pays"}]
    },
    "add_secondary_identities": {
        "additions": {"party": [{"name": "by_amount", "fields": ["amount"]}]}
    },
    "add_vertex_indexes": {"indexes": {"party": [{"fields": ["email", "amount"]}]}},
    "add_vertex_properties": {"additions": {"party": ["nickname"]}},
    "add_vertices": {
        "vertices": [{"name": "audit", "properties": ["aid"], "identity": ["aid"]}]
    },
    "change_field_types": {"vertices": {"party": {"amount": {"type": "FLOAT"}}}},
    "remove_edge_indexes": {"edges": [{**_PURCHASES, "fields": [["ref"]]}]},
    "remove_secondary_identities": {"removals": {"party": ["by_email"]}},
    "remove_vertex_indexes": {"indexes": {"party": [["amount"]]}},
    "replace_edge_identities": {"edges": [{**_PURCHASES, "identities": [["ref"]]}]},
    "set_edge_directed": {"edges": [_PURCHASES], "directed": False},
    "set_edge_semantics": {
        "edges": [_PURCHASES],
        "semantics": {"iri": "https://schema.org/seller"},
    },
    "set_field_semantics": {
        "targets": [
            {
                "vertex": "party",
                "field": "email",
                "semantics": {"iri": "https://schema.org/email"},
            }
        ]
    },
    "set_vertex_semantics": {
        "semantics": {"party": {"iri": "https://schema.org/Organization"}}
    },
}


def _union_members() -> list[Any]:
    return list(typing.get_args(typing.get_args(RevisionOp)[0]))


def _all_ops() -> list:
    return [
        op_from_dict({"op": name, **payload}) for name, payload in OP_PAYLOADS.items()
    ]


#: Field names that changed to say what they hold; the old spelling still parses.
LEGACY_FIELD_NAMES: list[tuple[str, str, str, Any]] = [
    ("rename_vertices", "vertices", "renames", {"a": "b"}),
    ("rename_relations", "relations", "renames", {"a": "b"}),
    ("rename_resources", "resources", "renames", {"a": "b"}),
    ("add_inverse_edges", "relations", "inverses", {"a": "b"}),
    (
        "replace_identity",
        "vertices",
        "replacements",
        {"party": {"to": {"mode": "natural", "identity": ["email"]}}},
    ),
]


class TestLegacyFieldNames:
    @pytest.mark.parametrize(("op", "old", "new", "value"), LEGACY_FIELD_NAMES)
    def test_the_old_key_parses_and_the_new_key_serializes(
        self, op: str, old: str, new: str, value: Any
    ) -> None:
        legacy = op_from_dict({"op": op, old: value})
        current = op_from_dict({"op": op, new: value})
        assert legacy == current
        assert new in ops_to_dicts([legacy])[0]
        assert old not in ops_to_dicts([legacy])[0]

    def test_compose_equivalence_lists_accept_their_old_names(self) -> None:
        legacy = ComposeManifestsOp.model_validate(
            {
                "vertices": [{"left": "A", "right": "B", "into": "A"}],
                "relations": [{"left": "r", "right": "s", "into": "r"}],
            }
        )
        assert [v.into for v in legacy.vertex_equivalences] == ["A"]
        assert [r.into for r in legacy.relation_equivalences] == ["r"]
        assert set(legacy.to_dict(skip_defaults=True)) >= {
            "vertex_equivalences",
            "relation_equivalences",
        }


class TestParseTimeValidation:
    """What a serialized change set is refused on read, not on replay."""

    @pytest.mark.parametrize(
        ("name", "field"),
        [
            ("rename_vertex_properties", "renames"),
            ("remove_vertex_properties", "removals"),
            ("add_vertex_properties", "additions"),
            ("rename_edge_properties", "renames"),
            ("remove_edge_properties", "removals"),
            ("add_edge_properties", "additions"),
            ("rename_vertices", "vertices"),
            ("rename_relations", "relations"),
            ("rename_resources", "resources"),
            ("add_inverse_edges", "relations"),
        ],
    )
    def test_an_empty_map_is_rejected(self, name: str, field: str) -> None:
        """An empty op used to reach merge3's catch-all and conflict with everything."""
        with pytest.raises(ValidationError):
            op_from_dict({"op": name, field: {}})

    @pytest.mark.parametrize("name", ["merge_vertices", "merge_edges"])
    def test_a_merge_rejects_into_among_its_sources(self, name: str) -> None:
        with pytest.raises(ValidationError, match="must not appear in `sources`"):
            op_from_dict({"op": name, "sources": ["a", "b"], "into": "a"})
        with pytest.raises(ValidationError, match="more than once"):
            op_from_dict({"op": name, "sources": ["a", "a"], "into": "b"})

    def test_add_inverse_edges_rejects_a_collapsing_or_self_map(self) -> None:
        with pytest.raises(ValidationError, match="not injective"):
            op_from_dict({"op": "add_inverse_edges", "inverses": {"a": "x", "b": "x"}})
        with pytest.raises(ValidationError, match="its own inverse"):
            op_from_dict({"op": "add_inverse_edges", "inverses": {"a": "a"}})

    def test_index_lists_must_be_non_empty(self) -> None:
        with pytest.raises(ValidationError):
            op_from_dict({"op": "add_vertex_indexes", "indexes": {"party": []}})
        with pytest.raises(ValidationError):
            op_from_dict({"op": "remove_vertex_indexes", "indexes": {"party": [[]]}})

    def test_edge_index_entries_are_unique_and_carry_their_field(self) -> None:
        spec = {"source": "party", "target": "order"}
        with pytest.raises(ValidationError, match="unique"):
            op_from_dict(
                {
                    "op": "add_edge_indexes",
                    "edges": [
                        {**spec, "indexes": [{"fields": ["a"]}]},
                        {**spec, "indexes": [{"fields": ["b"]}]},
                    ],
                }
            )
        with pytest.raises(ValidationError, match="list no `indexes`"):
            op_from_dict(
                {"op": "add_edge_indexes", "edges": [{**spec, "fields": [["a"]]}]}
            )
        with pytest.raises(ValidationError, match="list no `fields`"):
            op_from_dict(
                {
                    "op": "remove_edge_indexes",
                    "edges": [{**spec, "indexes": [{"fields": ["a"]}]}],
                }
            )

    def test_edge_selections_are_unique(self) -> None:
        spec = {"source": "party", "target": "order"}
        with pytest.raises(ValidationError, match="unique"):
            op_from_dict(
                {"op": "set_edge_directed", "edges": [spec, spec], "directed": True}
            )
        with pytest.raises(ValidationError, match="unique"):
            op_from_dict({"op": "set_edge_semantics", "edges": [spec, spec]})


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
        # ``test_schema_only_ops_leave_the_ingestion_block_untouched`` checks the
        # claim behaviourally for every entry that unary apply accepts.
        # ``compose_manifests`` unions resources and bindings, but it is binary
        # and rejected by ``apply_evolution``, so it can never reach a
        # schema-only artifact through the guard this set feeds.
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
            "remove_secondary_identities",
            "remove_vertex_indexes",
            "replace_edge_identities",
            "set_edge_directed",
            "set_edge_semantics",
            "set_field_semantics",
            "set_vertex_semantics",
        }
        unclassified = sorted(
            declared - ops_module.INGESTION_REWRITING_OPS - schema_only
        )
        assert not unclassified, (
            f"ops not classified for ingestion reach: {unclassified}; add each to "
            "INGESTION_REWRITING_OPS or to schema_only here"
        )

    def test_schema_only_ops_leave_the_ingestion_block_untouched(self) -> None:
        """The schema-only claim, checked by behaviour rather than by list."""
        from graflo.architecture.contract import GraphManifest
        from graflo.architecture.evolution import apply_evolution
        from graflo.architecture.evolution.hashing import ingestion_hash

        manifest = GraphManifest.model_validate(SCHEMA_ONLY_FIXTURE)
        before = ingestion_hash(manifest.ingestion_model)
        for name, payload in SCHEMA_ONLY_PAYLOADS.items():
            assert name not in ops_module.INGESTION_REWRITING_OPS
            out = apply_evolution(
                manifest,
                [op_from_dict({"op": name, **payload})],
                bump_version=False,
                finish_init=False,
            )
            assert ingestion_hash(out.ingestion_model) == before, (
                f"{name} rewrote the ingestion block but is classified schema-only"
            )

    def test_every_schema_only_op_has_a_behavioural_check(self) -> None:
        declared = {cls.model_fields["op"].default for cls in _union_members()}
        schema_only = declared - ops_module.INGESTION_REWRITING_OPS
        assert schema_only == set(SCHEMA_ONLY_PAYLOADS), (
            "SCHEMA_ONLY_PAYLOADS must cover exactly the ops outside "
            "INGESTION_REWRITING_OPS"
        )

    def test_the_vocabulary_size_is_pinned(self) -> None:
        """Adding an op means updating this on purpose, with the docs table."""
        exported = {
            name
            for name in dir(ops_module)
            if name.endswith("Op")
            and hasattr(getattr(ops_module, name), "model_fields")
        }
        assert len(exported) == 38
        assert len(_union_members()) == 37  # 38 minus the binary compose op


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
                "replacements": {
                    "party": {"to": {"mode": "natural", "identity": ["email"]}}
                },
            }
        )

        payload = ops_to_dicts([original])[0]

        assert payload["replacements"]["party"]["to"]["mode"] == "natural"
        assert ops_from_dicts([payload])[0] == original

    def test_a_funnel_identity_target_round_trips(self) -> None:
        original = op_from_dict(
            {
                "op": "replace_identity",
                "replacements": {
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
        assert restored.replacements["party"].to.funnel.branch_ids == ["email", "phone"]

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


class TestLegacyFieldAliases:
    """Recorded revisions predate the ``row`` → ``observation`` rename.

    ``allow_row_fusion`` named the wrong unit — a pipeline level produces
    *observations* at a ``LocationIndex``, not rows — but a stored change set
    written before the rename must still load, and must round-trip out under
    the new spelling.
    """

    def test_merge_vertices_accepts_allow_row_fusion(self) -> None:
        (op,) = ops_from_dicts(
            [
                {
                    "op": "merge_vertices",
                    "sources": ["shop"],
                    "into": "company",
                    "allow_row_fusion": True,
                }
            ]
        )
        assert op.allow_observation_fusion is True
        payload = ops_to_dicts([op])[0]
        assert payload["allow_observation_fusion"] is True
        assert "allow_row_fusion" not in payload

    def test_compose_manifests_accepts_allow_row_fusion(self) -> None:
        op = ComposeManifestsOp.model_validate(
            {"op": "compose_manifests", "allow_row_fusion": True}
        )
        assert op.allow_observation_fusion is True
        assert "allow_row_fusion" not in op.to_dict()

    def test_the_new_spelling_is_the_one_that_serializes(self) -> None:
        op = ComposeManifestsOp(allow_observation_fusion=True)
        assert op.to_dict()["allow_observation_fusion"] is True
