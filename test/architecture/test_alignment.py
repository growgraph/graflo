"""Tests for :mod:`graflo.architecture.evolution.alignment` — the identity lowering."""

from __future__ import annotations

import copy
import logging

import pytest

from graflo.architecture.contract.ingestion.resource import resolve_pipeline_level
from graflo.architecture.contract.ingestion.steps.models import TransformGuardConfig
from graflo.architecture.contract.ingestion.steps.normalize import (
    normalize_actor_step,
)
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution.alignment import (
    AlignmentConflictError,
    IdentityPlan,
    hosts_member_derivation,
    identity_to_ops,
    validate_identity,
)
from graflo.architecture.evolution.apply import apply_evolution
from graflo.architecture.evolution.ops import (
    AddResourceTransformsOp,
    AddVertexPropertiesOp,
    CanonicalMap,
    DerivationSpec,
    DerivedBranch,
    EnsureExtractedFieldsOp,
    FunnelIdentityTarget,
    IdentityBranchDecl,
    LocalKeyBranch,
    LocalKeySource,
    NaturalIdentityTarget,
    ReplaceIdentityOp,
    VertexEquivalence,
)


def _when(field: str, *values: str) -> TransformGuardConfig:
    return TransformGuardConfig.model_validate({"field": field, "in": list(values)})


def _union_manifest() -> GraphManifest:
    """A merged-union-shaped manifest: one class, two resources feeding it."""
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "u", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "Company",
                                "properties": [
                                    "company_id",
                                    "org_id",
                                    "shared_raw",
                                ],
                                "identity": ["company_id", "org_id"],
                            }
                        ]
                    },
                    "edge_config": {"edges": []},
                },
            },
            "ingestion_model": {
                "resources": [
                    {"name": "r_a", "pipeline": [{"vertex": "Company"}]},
                    {"name": "r_b", "pipeline": [{"vertex": "Company"}]},
                ],
                "transforms": [],
            },
        }
    )
    manifest.finish_init()
    return manifest


def _match_key() -> DerivedBranch:
    return DerivedBranch(
        name="match_key",
        sources={
            "r_a": DerivationSpec(
                input=["secondary_key", "shared_raw"],
                foo="gated_normalized_key",
                params={"prefix": "abc_", "strip_prefix": "ABC-"},
            ),
            "r_b": DerivationSpec(
                input=["org_id", "shared_raw"],
                foo="gated_normalized_key",
                params={"prefix": "", "strip_prefix": "ABC-"},
            ),
        },
    )


def _local_key() -> LocalKeyBranch:
    return LocalKeyBranch(
        local_key={
            "r_a": LocalKeySource(field="firm_id", tag="a"),
            "r_b": LocalKeySource(field="org_id", tag="b"),
        }
    )


def _plan(
    *branches: IdentityBranchDecl,
    vertex: str = "Company",
    at: dict[str, list[int]] | None = None,
) -> IdentityPlan:
    """The union's plan: *branches*, or the default derived + local-key pair."""
    return IdentityPlan(
        vertex=vertex,
        branches=branches or (_match_key(), _local_key()),
        at=at or {},
    )


def _equivalence(identity: list, **kwargs) -> VertexEquivalence:
    return VertexEquivalence.model_validate(
        {"left": "Firm", "right": "Org", "into": "Company", "identity": identity}
        | kwargs
    )


class TestModel:
    """The declaration: ``VertexEquivalence.identity`` as ordered funnel branches."""

    def test_an_empty_identity_is_refused(self) -> None:
        with pytest.raises(ValueError, match="identity lists no branch"):
            _equivalence([])

    def test_the_local_key_must_be_last(self) -> None:
        with pytest.raises(ValueError, match="must be the last one"):
            _equivalence([_local_key(), _match_key()])

    def test_the_local_key_is_unique(self) -> None:
        second = LocalKeyBranch(
            name="other_key",
            local_key={"r_a": LocalKeySource(field="firm_id", tag="a")},
        )
        with pytest.raises(ValueError, match="more than one local_key"):
            _equivalence([_local_key(), second])

    def test_duplicate_branch_ids_are_refused(self) -> None:
        clashing = LocalKeyBranch(
            name="match_key",
            local_key={"r_a": LocalKeySource(field="firm_id", tag="a")},
        )
        with pytest.raises(ValueError, match="repeats the branches"):
            _equivalence([_match_key(), clashing])

    def test_a_composite_id_clashing_with_a_name_is_refused(self) -> None:
        """A composite branch's id is its fields joined by ``_``."""
        with pytest.raises(ValueError, match=r"repeats the branches \['org_id'\]"):
            _equivalence(["org_id", ["org", "id"]])

    def test_a_derived_name_equal_to_a_property_branch_field_is_refused(
        self,
    ) -> None:
        with pytest.raises(ValueError, match="named like properties"):
            _equivalence([["match_key", "org_id"], _match_key()])

    def test_derive_at_needs_a_derived_branch(self) -> None:
        with pytest.raises(ValueError, match="no identity branch derives anything"):
            _equivalence(["company_id"], derive_at={"r_a": [0]})
        with pytest.raises(ValueError, match="no identity branch derives anything"):
            VertexEquivalence(left="Firm", right="Org", derive_at={"r_a": [0]})

    @pytest.mark.parametrize("entry", [{"field": "x"}, 3, ["a", 1]])
    def test_a_bad_branch_shape_names_the_four_shapes(self, entry) -> None:
        with pytest.raises(ValueError, match="no known shape") as excinfo:
            _equivalence([entry])

        message = str(excinfo.value)
        assert "a property name, a list of names, `{name, sources}`" in message
        assert "`{local_key:" in message

    def test_the_dict_form_parses_and_round_trips(self) -> None:
        equivalence = _equivalence(
            [
                "company_id",
                ["org_id", "shared_raw"],
                {
                    "name": "match_key",
                    "sources": {
                        "r_a": {"input": ["shared_raw"]},
                        "r_b": {"Org": {"input": ["org_id"], "foo": "affix_gated_key"}},
                    },
                },
                {"local_key": {"r_a": {"field": "firm_id", "tag": "a"}}},
            ],
            derive_at={"r_a": []},
        )

        assert equivalence.identity is not None
        raw, composite, derived, local = equivalence.identity
        assert raw == "company_id"
        assert composite == ["org_id", "shared_raw"]
        assert isinstance(derived, DerivedBranch)
        assert derived.members_for("r_b") == ["Org"]
        assert derived.specs_for("r_a")[0].foo == "normalized_key"
        assert isinstance(local, LocalKeyBranch)
        assert (local.name, local.sep) == ("local_key", ":")

        reloaded = VertexEquivalence.model_validate(equivalence.to_dict())
        assert reloaded == equivalence


def _parts() -> dict:
    """``derive`` for a key over two parts, each source with its own columns."""
    return {
        "host_key": {
            "r_a": {"input": ["host"]},
            "r_b": {"input": ["hostname"]},
        },
        "group_key": {
            "r_a": {"input": ["group"]},
            "r_b": {"input": ["group_id"]},
        },
    }


class TestDeriveModel:
    """``derive``: attributes each source computes, keyed on by name or composite."""

    def test_a_composite_over_derived_attributes_is_a_funnel(self) -> None:
        equivalence = _equivalence([["host_key", "group_key"]], derive=_parts())

        assert equivalence.has_derivation
        assert not equivalence.is_natural_key
        assert [d.name for d in equivalence.derivations()] == [
            "host_key",
            "group_key",
        ]

    def test_derive_round_trips_and_an_op_without_it_does_not_emit_it(self) -> None:
        equivalence = _equivalence([["host_key", "group_key"]], derive=_parts())
        assert VertexEquivalence.model_validate(equivalence.to_dict()) == equivalence

        plain = _equivalence([_match_key(), _local_key()])
        assert "derive" not in plain.to_dict()

    def test_derive_without_identity_is_refused(self) -> None:
        with pytest.raises(ValueError, match="no identity keys on them"):
            VertexEquivalence.model_validate(
                {"left": "Firm", "right": "Org", "derive": _parts()}
            )

    def test_an_empty_derive_is_refused(self) -> None:
        with pytest.raises(ValueError, match="derive lists no attribute"):
            _equivalence(["company_id"], derive={})

    def test_an_attribute_no_branch_keys_on_is_refused(self) -> None:
        with pytest.raises(ValueError, match=r"no identity branch keys on them"):
            _equivalence(["host_key"], derive=_parts())

    def test_a_composite_mixing_derived_and_property_fields_is_refused(
        self,
    ) -> None:
        parts = _parts()
        del parts["group_key"]
        with pytest.raises(ValueError, match=r"mixes derived attributes"):
            _equivalence([["host_key", "group_id"]], derive=parts)

    def test_an_attribute_named_like_a_derived_branch_is_refused(self) -> None:
        parts = {**_parts(), "match_key": _parts()["host_key"]}
        with pytest.raises(ValueError, match="both name"):
            _equivalence([["host_key", "group_key"], _match_key()], derive=parts)

    def test_a_guard_on_a_member_keyed_attribute_is_refused(self) -> None:
        parts = _parts()
        parts["host_key"]["r_b"] = {
            "Org": {"input": ["hostname"], "when": {"field": "kind", "in": ["x"]}}
        }
        with pytest.raises(ValueError, match="when"):
            _equivalence([["host_key", "group_key"]], derive=parts)

    def test_the_plan_lowers_each_part_and_one_composite_branch(self) -> None:
        equivalence = _equivalence(
            [["host_key", "group_key"], _local_key()], derive=_parts()
        )
        assert equivalence.identity is not None
        plan = IdentityPlan(
            vertex="Company",
            branches=tuple(equivalence.identity),
            derive=tuple(equivalence.derive_attributes()),
        )

        ops = identity_to_ops(plan)

        props = ops[0]
        assert isinstance(props, AddVertexPropertiesOp)
        assert props.additions == {"Company": ["host_key", "group_key", "local_key"]}
        steps = ops[1]
        assert isinstance(steps, AddResourceTransformsOp)
        assert [
            (s["transform"]["call"]["output"], s["transform"]["call"]["input"])
            for s in steps.additions["r_b"]
        ] == [
            (["host_key"], ["hostname"]),
            (["group_key"], ["group_id"]),
            (["local_key"], ["org_id"]),
        ]
        identity = ops[-1]
        assert isinstance(identity, ReplaceIdentityOp)
        target = identity.replacements["Company"].to
        assert isinstance(target, FunnelIdentityTarget)
        assert [(b.id, b.fields) for b in target.funnel.branches] == [
            ("host_key_group_key", ["host_key", "group_key"]),
            ("local_key", ["local_key"]),
        ]
        assert plan.raw == []


class TestComposedOps:
    def test_op_list_shape_and_order(self) -> None:
        ops = identity_to_ops(_plan())
        assert [type(o) for o in ops] == [
            AddVertexPropertiesOp,
            AddResourceTransformsOp,
            ReplaceIdentityOp,
        ]

        props = ops[0]
        assert isinstance(props, AddVertexPropertiesOp)
        assert props.additions == {"Company": ["match_key", "local_key"]}

        derive = ops[1]
        assert isinstance(derive, AddResourceTransformsOp)
        assert set(derive.additions) == {"r_a", "r_b"}
        # Derived-branch steps precede the local_key step per resource.
        r_a_calls = [step["transform"]["call"] for step in derive.additions["r_a"]]
        assert [c["output"] for c in r_a_calls] == [["match_key"], ["local_key"]]
        assert r_a_calls[1]["foo"] == "tagged_key"
        assert r_a_calls[1]["params"] == {"tag": "a", "sep": ":"}

        identity = ops[2]
        assert isinstance(identity, ReplaceIdentityOp)
        replacement = identity.replacements["Company"]
        assert replacement.retire == "keep"
        assert isinstance(replacement.to, FunnelIdentityTarget)
        funnel = replacement.to.funnel
        assert [b.id for b in funnel.branches] == ["match_key", "local_key"]
        assert all(b.fields == [b.id] for b in funnel.branches)

    def test_a_restrictive_router_adds_ensure_before_the_funnel(self) -> None:
        ops = identity_to_ops(
            _routed_plan(), manifest=_routed_manifest(keep_fields=["firm_id"])
        )

        assert [type(o) for o in ops] == [
            AddVertexPropertiesOp,
            AddResourceTransformsOp,
            EnsureExtractedFieldsOp,
            ReplaceIdentityOp,
        ]

    def test_ops_apply_to_the_union(self) -> None:
        manifest = apply_evolution(
            _union_manifest(),
            identity_to_ops(_plan(), manifest=_union_manifest()),
        )
        vc = _vertex_config(manifest)
        assert vc.identity_fields("Company") == ["id"]
        assert {"match_key", "local_key"} <= set(vc.property_names("Company"))
        # Secondaries are merge's job, against the final funnel.
        assert list(vc.secondary_identities("Company")) == []


class TestIdentityPlan:
    """Property, derived and local branches, lowered together in declared order."""

    def test_a_plan_built_directly_checks_its_branches(self) -> None:
        """The rules an equivalence enforces hold for a plan built without one.

        A ``local_key`` first would shadow every later branch: each record
        completes it, so the derived branch would never fire.
        """
        with pytest.raises(AlignmentConflictError, match="must be the last one"):
            _plan(_local_key(), _match_key())
        with pytest.raises(AlignmentConflictError, match="repeats the branches"):
            _plan("company_id", "company_id")

    def test_property_branches_lower_to_no_steps_but_join_the_funnel(self) -> None:
        plan = _plan("company_id", _match_key(), _local_key())

        ops = identity_to_ops(plan, manifest=_union_manifest())

        props = next(op for op in ops if isinstance(op, AddVertexPropertiesOp))
        assert props.additions == {"Company": ["match_key", "local_key"]}
        outputs = {
            call["output"][0]
            for steps in _transforms_op(ops).additions.values()
            for call in (step["transform"]["call"] for step in steps)
        }
        assert outputs == {"match_key", "local_key"}
        assert [b.id for b in _funnel(ops).branches] == [
            "company_id",
            "match_key",
            "local_key",
        ]

    def test_branch_order_is_funnel_order(self) -> None:
        plan = _plan("company_id", _match_key(), ["org_id", "shared_raw"], _local_key())

        funnel = _funnel(identity_to_ops(plan, manifest=_union_manifest()))

        assert [(b.id, b.fields) for b in funnel.branches] == [
            ("company_id", ["company_id"]),
            ("match_key", ["match_key"]),
            ("org_id_shared_raw", ["org_id", "shared_raw"]),
            ("local_key", ["local_key"]),
        ]

    def test_a_single_property_branch_is_a_natural_key(self) -> None:
        ops = identity_to_ops(_plan("company_id"), manifest=_union_manifest())

        assert len(ops) == 1
        replace = ops[0]
        assert isinstance(replace, ReplaceIdentityOp)
        target = replace.replacements["Company"].to
        assert isinstance(target, NaturalIdentityTarget)
        assert target.identity == ["company_id"]

    def test_two_property_branches_are_a_funnel_not_a_composite(self) -> None:
        ops = identity_to_ops(_plan("company_id", "org_id"), manifest=_union_manifest())

        assert [type(o) for o in ops] == [ReplaceIdentityOp]
        assert [b.fields for b in _funnel(ops).branches] == [
            ["company_id"],
            ["org_id"],
        ]


class TestValidation:
    def _validate(self, plan: IdentityPlan, **kwargs) -> None:
        validate_identity(plan, _union_manifest(), **kwargs)

    def test_valid_plan_passes(self) -> None:
        self._validate(_plan())

    def test_unknown_vertex_raises(self) -> None:
        with pytest.raises(AlignmentConflictError, match="unknown vertex"):
            self._validate(_plan(vertex="Ghost"))

    def test_unknown_resource_raises(self) -> None:
        plan = _plan(
            _match_key(),
            LocalKeyBranch(local_key={"ghost": LocalKeySource(field="x", tag="g")}),
        )
        with pytest.raises(AlignmentConflictError, match="unknown resources"):
            self._validate(plan)

    def test_target_colliding_with_current_identity_raises(self) -> None:
        plan = _plan(
            DerivedBranch(
                name="company_id",
                sources={"r_a": DerivationSpec(input=["firm_id"])},
            ),
            _local_key(),
        )
        with pytest.raises(AlignmentConflictError, match="identity collision"):
            self._validate(plan)

    def test_an_undeclared_property_branch_raises(self) -> None:
        plan = _plan("ghost_field", _match_key(), _local_key())
        with pytest.raises(
            AlignmentConflictError, match="undeclared property branch"
        ) as excinfo:
            self._validate(plan)

        assert "['ghost_field']" in str(excinfo.value)

    def test_canonical_name_as_derivation_input_raises(self) -> None:
        cm = CanonicalMap(
            vertices={"Firm": "Company"},
            properties={"Firm": {"firm_id": "company_id"}},
        )
        plan = _plan(
            _match_key(),
            LocalKeyBranch(
                local_key={
                    # WRONG: company_id is the canonical rename target; the
                    # raw docs still carry firm_id.
                    "r_a": LocalKeySource(field="company_id", tag="a"),
                    "r_b": LocalKeySource(field="org_id", tag="b"),
                }
            ),
        )
        with pytest.raises(
            AlignmentConflictError, match="canonical name as derivation input"
        ):
            self._validate(plan, canonical_maps=[cm])

    def test_derived_only_warns_about_missing_local_key(self, caplog) -> None:
        with caplog.at_level(logging.WARNING):
            self._validate(_plan(_match_key()))
        assert any("no local_key" in r.getMessage() for r in caplog.records)


def _routed_manifest(
    *,
    nested: bool = True,
    keep_fields: list[str] | None = None,
    extraction_scope: str = "full",
    sibling_props: list[str] | None = None,
    company_props: list[str] | None = None,
    router_from: dict[str, str] | None = None,
) -> GraphManifest:
    """A union whose left side routes two branches onto ``Company``.

    The shape example 21 is built on: one ``vertex_router`` collapsing ``firm``
    and ``shop`` onto the merged class while ``person`` keeps flowing through
    it, optionally nested under a ``descend``.
    """
    router_config: dict[str, object] = {
        "type_field": "kind",
        "type_map": {"firm": "Company", "shop": "Company", "person": "Person"},
        "extraction_scope": extraction_scope,
    }
    if keep_fields is not None:
        router_config["keep_fields"] = list(keep_fields)
    if router_from is not None:
        router_config["from"] = dict(router_from)
    router = {"vertex_router": router_config}
    pipeline = (
        [{"descend": {"key": "records", "apply": [router]}}] if nested else [router]
    )
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "u", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "Company",
                                "properties": ["company_id", "org_id", "shared_raw"]
                                + list(company_props or []),
                                "identity": ["company_id", "org_id"],
                            },
                            {
                                "name": "Person",
                                "properties": ["person_id"] + list(sibling_props or []),
                                "identity": ["person_id"],
                            },
                        ]
                    },
                    "edge_config": {"edges": []},
                },
            },
            "ingestion_model": {
                "resources": [
                    {"name": "r_view", "pipeline": pipeline},
                    {"name": "r_b", "pipeline": [{"vertex": "Company"}]},
                ],
                "transforms": [],
            },
        }
    )
    manifest.finish_init()
    return manifest


def _routed_match_key() -> DerivedBranch:
    """One unguarded spec per resource: the router supplies the class guard."""
    return DerivedBranch(
        name="match_key",
        sources={
            "r_view": DerivationSpec(
                input=["secondary_key", "company_ref"], foo="gated_normalized_key"
            ),
            "r_b": DerivationSpec(
                input=["org_id", "shared_raw"], foo="gated_normalized_key"
            ),
        },
    )


def _routed_local_key() -> LocalKeyBranch:
    return LocalKeyBranch(
        local_key={
            "r_view": LocalKeySource(field="record_id", tag="view"),
            "r_b": LocalKeySource(field="org_id", tag="b"),
        }
    )


def _routed_plan(
    *branches: IdentityBranchDecl, at: dict[str, list[int]] | None = None
) -> IdentityPlan:
    return IdentityPlan(
        vertex="Company",
        branches=branches or (_routed_match_key(), _routed_local_key()),
        at=at or {},
    )


def _vertex_config(manifest: GraphManifest):
    schema = manifest.graph_schema
    assert schema is not None
    return schema.core_schema.vertex_config


def _transforms_op(ops) -> AddResourceTransformsOp:
    return next(op for op in ops if isinstance(op, AddResourceTransformsOp))


def _funnel(ops):
    replace = next(op for op in ops if isinstance(op, ReplaceIdentityOp))
    target = replace.replacements["Company"].to
    assert isinstance(target, FunnelIdentityTarget)
    return target.funnel


class TestLevelResolution:
    """Where a derivation lands. A root-level step is invisible below a descend."""

    def test_a_root_level_vertex_resolves_to_the_root(self) -> None:
        ops = identity_to_ops(_plan(), manifest=_union_manifest())

        assert _transforms_op(ops).at == {}

    def test_a_nested_router_resolves_to_its_own_level(self) -> None:
        ops = identity_to_ops(_routed_plan(), manifest=_routed_manifest())

        assert _transforms_op(ops).at == {"r_view": [0]}

    def test_a_root_level_router_resolves_to_the_root(self) -> None:
        ops = identity_to_ops(_routed_plan(), manifest=_routed_manifest(nested=False))

        assert _transforms_op(ops).at == {}

    def test_a_nested_plain_vertex_resolves_to_its_own_level(self) -> None:
        manifest = _union_manifest()
        manifest.require_ingestion_model().resources[0].pipeline = [
            {"descend": {"key": "rows", "apply": [{"vertex": "Company"}]}}
        ]
        manifest.finish_init()

        ops = identity_to_ops(_plan(), manifest=manifest)

        assert _transforms_op(ops).at == {"r_a": [0]}

    def test_a_resource_that_never_produces_the_class_is_rejected(self) -> None:
        manifest = _union_manifest()
        manifest.require_ingestion_model().resources[1].pipeline = [
            {"vertex": "Person"}
        ]
        vertex_config = _vertex_config(manifest)
        vertex_config.vertices.append(
            vertex_config["Company"].model_copy(update={"name": "Person"})
        )
        manifest.finish_init()

        with pytest.raises(
            AlignmentConflictError, match="resource does not produce the class"
        ):
            identity_to_ops(_plan(), manifest=manifest)

    def test_several_producing_levels_are_rejected(self) -> None:
        manifest = _union_manifest()
        manifest.require_ingestion_model().resources[0].pipeline = [
            {"vertex": "Company"},
            {"descend": {"key": "rows", "apply": [{"vertex": "Company"}]}},
        ]
        manifest.finish_init()

        with pytest.raises(AlignmentConflictError, match="ambiguous level") as excinfo:
            identity_to_ops(_plan(), manifest=manifest)

        assert "derive_at={'r_a': []}" in str(excinfo.value)

    def test_an_explicit_at_disambiguates(self) -> None:
        manifest = _union_manifest()
        manifest.require_ingestion_model().resources[0].pipeline = [
            {"vertex": "Company"},
            {"descend": {"key": "rows", "apply": [{"vertex": "Company"}]}},
        ]
        manifest.finish_init()

        ops = identity_to_ops(_plan(at={"r_a": [1]}), manifest=manifest)

        assert _transforms_op(ops).at["r_a"] == [1]

    def test_an_at_pointing_at_a_barren_level_is_rejected(self) -> None:
        """The silent failure this resolution exists to prevent."""
        with pytest.raises(AlignmentConflictError, match="level produces nothing"):
            identity_to_ops(
                _routed_plan(at={"r_view": []}), manifest=_routed_manifest()
            )

    def test_an_unresolvable_at_is_rejected(self) -> None:
        with pytest.raises(AlignmentConflictError, match="unresolvable level"):
            identity_to_ops(
                _routed_plan(at={"r_view": [0, 0]}), manifest=_routed_manifest()
            )


class TestSpecLowering:
    """One step per spec, writing the branch attribute directly."""

    def _calls(self, ops, resource: str) -> list[dict]:
        return [
            step["transform"]["call"]
            for step in _transforms_op(ops).additions[resource]
        ]

    @pytest.mark.parametrize("resource", ["r_view", "r_b"])
    def test_a_spec_writes_the_attribute_directly(self, resource: str) -> None:
        calls = self._calls(
            identity_to_ops(_routed_plan(), manifest=_routed_manifest()), resource
        )

        assert [c["output"] for c in calls] == [["match_key"], ["local_key"]]
        assert [c["foo"] for c in calls] == ["gated_normalized_key", "tagged_key"]
        assert all("strategy" not in c for c in calls)

    def test_an_affix_gated_spec_lowers_to_a_single_input_call(self) -> None:
        """The marker idiom reads one field: the value gates itself."""
        plan = _routed_plan(
            DerivedBranch(
                name="match_key",
                sources={
                    "r_view": DerivationSpec(
                        input=["firm_ref"],
                        foo="affix_gated_key",
                        params={"prefix": "ABC-"},
                    ),
                    "r_b": DerivationSpec(
                        input=["shared_raw"],
                        foo="affix_gated_key",
                        params={"prefix": "ABC-"},
                    ),
                },
            ),
            _routed_local_key(),
        )
        calls = self._calls(
            identity_to_ops(plan, manifest=_routed_manifest()), "r_view"
        )
        marker = [c for c in calls if c["foo"] == "affix_gated_key"]

        assert [c["input"] for c in marker] == [["firm_ref"]]
        assert marker[0]["params"] == {"prefix": "ABC-"}
        assert marker[0]["output"] == ["match_key"]


class TestExplicitGuard:
    """``when`` on a spec: an explicit guard replacing the derived class guard."""

    def _steps(self, ops, resource: str) -> list[dict]:
        return [step["transform"] for step in _transforms_op(ops).additions[resource]]

    def test_an_explicit_when_lowers_onto_the_step(self) -> None:
        plan = _plan(
            DerivedBranch(
                name="match_key",
                sources={
                    "r_a": DerivationSpec(
                        input=["shared_raw"], when=_when("kind", "firm")
                    ),
                    "r_b": DerivationSpec(input=["shared_raw"]),
                },
            ),
            LocalKeyBranch(
                local_key={
                    "r_a": LocalKeySource(
                        field="firm_id", tag="a", when=_when("kind", "firm", "shop")
                    ),
                    "r_b": LocalKeySource(field="org_id", tag="b"),
                }
            ),
        )

        ops = identity_to_ops(plan, manifest=_union_manifest())

        assert [s.get("when") for s in self._steps(ops, "r_a")] == [
            {"field": "kind", "in": ["firm"]},
            {"field": "kind", "in": ["firm", "shop"]},
        ]
        assert all("when" not in s for s in self._steps(ops, "r_b"))

    def test_the_dict_form_of_when_parses(self) -> None:
        spec = DerivationSpec.model_validate(
            {"input": ["shared_raw"], "when": {"field": "kind", "in": ["firm"]}}
        )

        assert spec.when == _when("kind", "firm")

    def test_an_explicit_when_wins_over_the_class_guard(self) -> None:
        """The router routes firm and shop onto Company; the spec narrows it."""
        plan = _routed_plan(
            DerivedBranch(
                name="match_key",
                sources={
                    "r_view": DerivationSpec(
                        input=["firm_ref"], when=_when("kind", "firm")
                    ),
                    "r_b": DerivationSpec(input=["shared_raw"]),
                },
            ),
            _routed_local_key(),
        )

        steps = self._steps(
            identity_to_ops(plan, manifest=_routed_manifest()), "r_view"
        )

        by_output = {s["call"]["output"][0]: s["when"] for s in steps}
        assert by_output == {
            "match_key": {"field": "kind", "in": ["firm"]},
            # The unguarded local key still gets the derived class guard.
            "local_key": {"field": "kind", "in": ["Company", "firm", "shop"]},
        }

    def test_a_member_keyed_spec_may_not_carry_when(self) -> None:
        with pytest.raises(ValueError, match="the member already decides"):
            DerivedBranch(
                name="match_key",
                sources={
                    "r_view": {
                        "Shop": DerivationSpec(
                            input=["secondary_key"], when=_when("kind", "shop")
                        )
                    }
                },
            )
        with pytest.raises(ValueError, match="the member already decides"):
            LocalKeyBranch(
                local_key={
                    "r_view": {
                        "Shop": LocalKeySource(
                            field="shop_id", tag="shop", when=_when("kind", "shop")
                        )
                    }
                }
            )

    def test_a_when_field_counts_as_a_raw_input(self) -> None:
        """A guard reads a raw document key, so a canonical rename target is refused."""
        cm = CanonicalMap(vertices={}, properties={"Company": {"kind_raw": "kind"}})

        def plan(when: TransformGuardConfig | None) -> IdentityPlan:
            return _plan(
                DerivedBranch(
                    name="match_key",
                    sources={
                        "r_a": DerivationSpec(input=["shared_raw"], when=when),
                        "r_b": DerivationSpec(input=["shared_raw"]),
                    },
                ),
                _local_key(),
            )

        validate_identity(plan(None), _union_manifest(), canonical_maps=[cm])
        with pytest.raises(
            AlignmentConflictError, match="canonical name as derivation input"
        ) as excinfo:
            validate_identity(
                plan(_when("kind", "firm")), _union_manifest(), canonical_maps=[cm]
            )

        assert "['kind']" in str(excinfo.value)


class TestRouterDelivery:
    """A router's child reads the merged observation, not the transform buffer."""

    def _ensure(self, ops) -> EnsureExtractedFieldsOp | None:
        return next((op for op in ops if isinstance(op, EnsureExtractedFieldsOp)), None)

    def test_a_keep_fields_router_gets_the_canonical_attributes(self) -> None:
        ops = identity_to_ops(
            _routed_plan(),
            manifest=_routed_manifest(keep_fields=["firm_id", "shop_id"]),
        )

        ensure = self._ensure(ops)
        assert ensure is not None
        entries = ensure.additions["r_view"]
        assert [e.vertex for e in entries] == ["Company"]
        assert entries[0].fields == ["match_key", "local_key"]
        assert entries[0].at == [0]

    def test_a_mapped_only_router_gets_the_canonical_attributes(self) -> None:
        ops = identity_to_ops(
            _routed_plan(),
            manifest=_routed_manifest(extraction_scope="mapped_only"),
        )

        assert self._ensure(ops) is not None

    def test_an_unrestricted_router_needs_nothing(self) -> None:
        ops = identity_to_ops(_routed_plan(), manifest=_routed_manifest())

        assert self._ensure(ops) is None

    def test_a_plain_vertex_step_needs_nothing(self) -> None:
        ops = identity_to_ops(_plan(), manifest=_union_manifest())

        assert self._ensure(ops) is None

    def test_a_sibling_class_claiming_a_canonical_name_is_guarded_out(self) -> None:
        ops = identity_to_ops(
            _routed_plan(),
            manifest=_routed_manifest(sibling_props=["match_key"]),
        )

        routed = _transforms_op(ops).additions["r_view"]
        # match_key and local_key: one step each.
        assert len(routed) == 2
        assert all(
            s["transform"]["when"]
            == {"field": "kind", "in": ["Company", "firm", "shop"]}
            for s in routed
        )
        plain = _transforms_op(ops).additions["r_b"]
        assert plain and all("when" not in s["transform"] for s in plain)

    def test_routers_on_two_discriminators_cannot_host_a_derivation(self) -> None:
        # Two routers reading different discriminators at one level share one
        # transform buffer: one derived value would reach both their vertices.
        manifest = _dynamic_union(
            [_nested(_BARE, {"vertex_router": {"type_field": "cls"}})],
            sibling_props=["match_key"],
        )

        with pytest.raises(AlignmentConflictError, match="shared by every router"):
            identity_to_ops(_routed_plan(), manifest=manifest)

    def test_the_sibling_refusal_remains_beside_a_plain_vertex_step(self) -> None:
        # A plain vertex step producing the class beside a router: no guard can
        # be derived, the derivation lowers unguarded, and a sibling the router
        # routes to that declares the attribute is refused.
        manifest = _dynamic_union(
            [
                _nested(
                    {"vertex": "Company"},
                    {
                        "vertex_router": {
                            "type_field": "kind",
                            "type_map": {"firm": "Company", "person": "Person"},
                        }
                    },
                )
            ],
            sibling_props=["match_key"],
        )

        with pytest.raises(AlignmentConflictError, match="claimed by a sibling class"):
            identity_to_ops(_routed_plan(), manifest=manifest)

    def test_guarded_steps_apply_as_valid_pipeline_steps(self) -> None:
        manifest = _routed_manifest(sibling_props=["match_key"])

        out = apply_evolution(
            manifest,
            identity_to_ops(_routed_plan(), manifest=manifest),
            bump_version=False,
        )

        pipeline = out.require_ingestion_model().resources[0].pipeline
        level = resolve_pipeline_level(list(pipeline), [0])
        transforms = [
            normalize_actor_step(dict(s))
            for s in level
            if normalize_actor_step(dict(s)).get("type") == "transform"
        ]
        assert len(transforms) == 2
        assert all(s.get("when") for s in transforms)


class TestEnsureExtractedFieldsApplies:
    """The op's effect on the pipeline, not just its emission."""

    def _router(self, manifest: GraphManifest) -> dict:
        pipeline = manifest.require_ingestion_model().resources[0].pipeline
        descend = normalize_actor_step(dict(pipeline[0]))
        return normalize_actor_step(dict(descend["pipeline"][0]))

    def test_keep_fields_gains_the_canonical_attributes(self) -> None:
        manifest = _routed_manifest(keep_fields=["firm_id", "shop_id"])

        out = apply_evolution(
            manifest,
            identity_to_ops(_routed_plan(), manifest=manifest),
            bump_version=False,
        )

        router = self._router(out)
        assert router["keep_fields"] == [
            "firm_id",
            "shop_id",
            "match_key",
            "local_key",
        ]

    def test_mapped_only_gains_identity_mappings_for_the_class_only(self) -> None:
        manifest = _routed_manifest(extraction_scope="mapped_only")

        out = apply_evolution(
            manifest,
            identity_to_ops(_routed_plan(), manifest=manifest),
            bump_version=False,
        )

        vertex_from_map = self._router(out)["vertex_from_map"]
        assert vertex_from_map["Company"] == {
            "match_key": "match_key",
            "local_key": "local_key",
        }
        # The router keeps serving its other types exactly as before.
        assert "Person" not in vertex_from_map

    def test_an_existing_projection_is_extended_not_replaced(self) -> None:
        """Creating the entry from scratch would drop the router-level ``from``."""
        manifest = _routed_manifest(
            extraction_scope="mapped_only", router_from={"company_id": "firm_id"}
        )

        out = apply_evolution(
            manifest,
            identity_to_ops(_routed_plan(), manifest=manifest),
            bump_version=False,
        )

        assert self._router(out)["vertex_from_map"]["Company"] == {
            "company_id": "firm_id",
            "match_key": "match_key",
            "local_key": "local_key",
        }


# --------------------------------------------------------------------------- #
# Member-keyed sources: the member decides, the side manifest supplies the guard.
# --------------------------------------------------------------------------- #


def _left_side(*, plain_shop: bool = False) -> GraphManifest:
    """The pre-merge left side: one router producing Company, Shop, Person.

    With ``plain_shop`` the resource produces ``Shop`` through a plain
    ``vertex`` step at the same level instead of a router key.
    """
    type_map = {"firm": "Company", "person": "Person"}
    apply: list[dict] = []
    if plain_shop:
        apply.append({"vertex": "Shop"})
    else:
        type_map["shop"] = "Shop"
    apply.append(
        {
            "vertex_router": {
                "type_field": "kind",
                "keep_fields": ["firm_id", "shop_id", "person_id", "secondary_key"],
                "type_map": type_map,
            }
        }
    )
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "left", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "Company",
                                "properties": ["company_id", "secondary_key"],
                                "identity": ["company_id"],
                            },
                            {
                                "name": "Shop",
                                "properties": ["shop_id", "secondary_key"],
                                "identity": ["shop_id"],
                            },
                            {
                                "name": "Person",
                                "properties": ["person_id"],
                                "identity": ["person_id"],
                            },
                        ]
                    },
                    "edge_config": {"edges": []},
                },
            },
            "ingestion_model": {
                "resources": [
                    {
                        "name": "r_view",
                        "pipeline": [{"descend": {"key": "records", "apply": apply}}],
                    }
                ],
                "transforms": [],
            },
        }
    )
    manifest.finish_init()
    return manifest


def _right_side() -> GraphManifest:
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "right", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "Org",
                                "properties": ["org_id", "shared_raw"],
                                "identity": ["org_id"],
                            }
                        ]
                    },
                    "edge_config": {"edges": []},
                },
            },
            "ingestion_model": {
                "resources": [{"name": "r_b", "pipeline": [{"vertex": "Org"}]}],
                "transforms": [],
            },
        }
    )
    manifest.finish_init()
    return manifest


def _sides(**left_kwargs) -> dict[str, GraphManifest]:
    return {"left": _left_side(**left_kwargs), "right": _right_side()}


_CLUSTER = {"left": {"Company", "Shop"}, "right": {"Org"}}


def _member_spec(prefix: str) -> DerivationSpec:
    return DerivationSpec(
        input=["secondary_key"], foo="affix_gated_key", params={"prefix": prefix}
    )


def _member_match_key() -> DerivedBranch:
    return DerivedBranch(
        name="match_key",
        sources={
            "r_view": {
                "Company": _member_spec("abc_"),
                "Shop": _member_spec("def_"),
            },
            "r_b": DerivationSpec(
                input=["shared_raw"],
                foo="affix_gated_key",
                params={"prefix": ""},
            ),
        },
    )


def _member_local_key() -> LocalKeyBranch:
    return LocalKeyBranch(
        local_key={
            "r_view": {
                "Company": LocalKeySource(field="firm_id", tag="firm"),
                "Shop": LocalKeySource(field="shop_id", tag="shop"),
            },
            "r_b": LocalKeySource(field="org_id", tag="b"),
        }
    )


def _member_plan(
    *branches: IdentityBranchDecl, at: dict[str, list[int]] | None = None
) -> IdentityPlan:
    return IdentityPlan(
        vertex="Company",
        branches=branches or (_member_match_key(), _member_local_key()),
        at=at or {},
    )


def _member_local_key_with(r_view: dict[str, LocalKeySource]) -> LocalKeyBranch:
    return LocalKeyBranch(
        local_key={"r_view": r_view, "r_b": LocalKeySource(field="org_id", tag="b")}
    )


def _member_ops(plan: IdentityPlan | None = None, **kwargs):
    return identity_to_ops(
        plan or _member_plan(),
        manifest=_routed_manifest(company_props=["secondary_key"]),
        sides=kwargs.pop("sides", _sides()),
        cluster_members=kwargs.pop("cluster_members", _CLUSTER),
        **kwargs,
    )


class TestMemberKeyedModel:
    def test_a_member_dict_is_told_apart_from_a_spec(self) -> None:
        """A spec is ``extra="forbid"``, so a dict of specs never parses as one."""
        branch = DerivedBranch.model_validate(
            {
                "name": "match_key",
                "sources": {
                    "r_view": {"Shop": {"input": ["secondary_key"]}},
                    "r_b": {"input": ["shared_raw"]},
                },
            }
        )

        assert branch.members_for("r_view") == ["Shop"]
        assert branch.members_for("r_b") is None
        assert [s.input for s in branch.specs_for("r_view")] == [["secondary_key"]]

    def test_the_dict_form_round_trips_through_to_dict(self) -> None:
        derived, local = _member_match_key(), _member_local_key()

        reloaded_derived = DerivedBranch.model_validate(derived.to_dict())
        reloaded_local = LocalKeyBranch.model_validate(local.to_dict())

        assert (reloaded_derived, reloaded_local) == (derived, local)
        assert reloaded_derived.members_for("r_view") == ["Company", "Shop"]
        assert reloaded_local.members_for("r_view") == ["Company", "Shop"]

    def test_tag_none_is_the_empty_tag_and_round_trips(self) -> None:
        """``to_dict`` drops ``None``; the empty tag is what survives."""
        source = LocalKeySource(field="uuid", tag=None)

        assert source.tag == ""
        assert LocalKeySource.model_validate(source.to_dict()) == source
        assert LocalKeySource.model_validate({"field": "uuid", "tag": None}).tag == ""

    def test_an_untagged_local_key_lowers_with_the_empty_tag(self) -> None:
        plan = _member_plan(
            _member_match_key(),
            _member_local_key_with(
                {
                    "Company": LocalKeySource(field="firm_id", tag=None),
                    "Shop": LocalKeySource(field="shop_id", tag="shop"),
                }
            ),
        )
        steps = [
            step["transform"]
            for step in _transforms_op(_member_ops(plan)).additions["r_view"]
        ]
        local = [s["call"] for s in steps if s["call"]["output"] == ["local_key"]]

        assert [c["params"]["tag"] for c in local] == ["", "shop"]

    def test_a_member_keyed_local_key_may_not_set_when(self) -> None:
        with pytest.raises(ValueError, match="member already decides"):
            LocalKeyBranch(
                local_key={
                    "r_view": {
                        "Shop": LocalKeySource(
                            field="shop_id", tag="shop", when=_when("kind", "shop")
                        )
                    }
                }
            )


class TestMemberKeyedLowering:
    def _calls(self, ops, resource: str) -> list[dict]:
        return [step["transform"] for step in _transforms_op(ops).additions[resource]]

    def test_each_member_writes_the_attribute_directly_under_a_guard(self) -> None:
        steps = self._calls(_member_ops(), "r_view")

        match = [s for s in steps if s["call"]["output"] == ["match_key"]]
        assert [s["when"] for s in match] == [
            {"field": "kind", "in": ["Company", "firm"]},
            {"field": "kind", "in": ["Shop", "shop"]},
        ]
        assert [s["call"]["params"]["prefix"] for s in match] == ["abc_", "def_"]
        assert [s["call"]["input"] for s in match] == [["secondary_key"]] * 2
        assert not any(s["call"]["foo"] == "coalesce_fields" for s in steps)

    def test_the_local_key_is_guarded_the_same_way(self) -> None:
        steps = self._calls(_member_ops(), "r_view")

        local = [s for s in steps if s["call"]["output"] == ["local_key"]]
        assert [s["call"]["foo"] for s in local] == ["tagged_key", "tagged_key"]
        assert [s["when"]["in"] for s in local] == [
            ["Company", "firm"],
            ["Shop", "shop"],
        ]

    def test_an_unkeyed_resource_is_lowered_as_before(self) -> None:
        steps = self._calls(_member_ops(), "r_b")

        assert all("when" not in s for s in steps)

    def test_the_level_comes_from_the_member(self) -> None:
        assert _transforms_op(_member_ops()).at == {"r_view": [0]}

    def test_a_plain_vertex_member_needs_no_guard(self) -> None:
        steps = self._calls(_member_ops(sides=_sides(plain_shop=True)), "r_view")

        by_prefix = {
            s["call"]["params"].get("prefix"): s
            for s in steps
            if "prefix" in s["call"]["params"]
        }
        assert by_prefix["abc_"]["when"] == {"field": "kind", "in": ["Company", "firm"]}
        assert "when" not in by_prefix["def_"]

    def test_the_lowered_steps_are_valid_pipeline_steps(self) -> None:
        manifest = apply_evolution(
            _routed_manifest(company_props=["secondary_key"]), _member_ops()
        )

        pipeline = manifest.require_ingestion_model().resources[0].pipeline
        level = resolve_pipeline_level(list(pipeline), [0])
        guarded = [s for s in level if normalize_actor_step(dict(s)).get("when")]
        assert len(guarded) == 4


class TestMemberKeyedValidation:
    def test_member_keyed_sources_need_the_sides(self) -> None:
        with pytest.raises(AlignmentConflictError, match="without sides"):
            identity_to_ops(_member_plan())

    def test_an_unknown_member_is_rejected(self) -> None:
        plan = _member_plan(
            _member_match_key(),
            _member_local_key_with({"Ghost": LocalKeySource(field="x", tag="g")}),
        )
        with pytest.raises(
            AlignmentConflictError, match="resource does not produce the member"
        ) as excinfo:
            _member_ops(plan)

        assert "'Company', 'Person', 'Shop'" in str(excinfo.value)

    def test_a_member_outside_the_cluster_is_rejected(self) -> None:
        """``Person`` is produced by the resource but is not being merged."""
        plan = _member_plan(
            _member_match_key(),
            _member_local_key_with(
                {"Person": LocalKeySource(field="person_id", tag="p")}
            ),
        )
        with pytest.raises(AlignmentConflictError, match="member outside the cluster"):
            _member_ops(plan)

    def test_a_resource_on_no_side_is_rejected(self) -> None:
        with pytest.raises(AlignmentConflictError, match="resource on no side"):
            _member_ops(sides={"right": _right_side()})

    def test_an_at_that_misses_the_member_is_rejected(self) -> None:
        with pytest.raises(AlignmentConflictError, match="level produces nothing"):
            _member_ops(_member_plan(at={"r_view": []}))

    def test_partial_coverage_warns(self, caplog) -> None:
        plan = _member_plan(
            DerivedBranch(
                name="match_key",
                sources={
                    "r_view": {"Company": _member_spec("abc_")},
                    "r_b": DerivationSpec(input=["shared_raw"], foo="affix_gated_key"),
                },
            ),
            _member_local_key(),
        )
        with caplog.at_level(logging.WARNING):
            _member_ops(plan)

        assert any(
            "derives 'match_key' only for ['Company']" in r.getMessage()
            for r in caplog.records
        )

    def test_full_coverage_is_silent(self, caplog) -> None:
        with caplog.at_level(logging.WARNING):
            _member_ops()

        assert not any("only for" in r.getMessage() for r in caplog.records)
        assert not any("one way" in r.getMessage() for r in caplog.records)

    def test_the_discriminator_counts_as_a_raw_input(self) -> None:
        """A canonical map renaming the router's type_field is caught."""
        cm = CanonicalMap(vertices={}, properties={"Company": {"kind_raw": "kind"}})
        with pytest.raises(
            AlignmentConflictError, match="canonical name as derivation input"
        ):
            _member_ops(canonical_maps=[cm])


# --------------------------------------------------------------------------- #
# Dynamic routers: no ``type_map`` — the discriminator value is the class name.
# --------------------------------------------------------------------------- #


_BARE = {"vertex_router": {"type_field": "kind"}}


def _nested(*steps: dict) -> dict:
    return {"descend": {"key": "records", "apply": list(steps)}}


def _side_with(pipeline: list[dict]) -> GraphManifest:
    """A left side declaring three classes, fed by ``r_view`` through *pipeline*."""
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "left", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": name,
                                "properties": [f"{name.lower()}_id", "secondary_key"],
                                "identity": [f"{name.lower()}_id"],
                            }
                            for name in ("Company", "Shop", "Person")
                        ]
                    },
                    "edge_config": {"edges": []},
                },
            },
            "ingestion_model": {
                "resources": [{"name": "r_view", "pipeline": pipeline}],
                "transforms": [],
            },
        }
    )
    manifest.finish_init()
    return manifest


def _dynamic_union(pipeline: list[dict], **kwargs) -> GraphManifest:
    """The routed union with ``r_view`` fed through *pipeline* instead."""
    kwargs.setdefault("company_props", ["secondary_key"])
    manifest = _routed_manifest(**kwargs)
    manifest.require_ingestion_model().resources[0].pipeline = copy.deepcopy(pipeline)
    manifest.finish_init()
    return manifest


def _dynamic_ops(pipeline: list[dict], plan: IdentityPlan | None = None):
    """Member-keyed lowering where the side and the union share *pipeline*."""
    return identity_to_ops(
        plan or _member_plan(),
        manifest=_dynamic_union(pipeline),
        sides={"left": _side_with(pipeline), "right": _right_side()},
        cluster_members=_CLUSTER,
    )


class TestDynamicRouterMembers:
    """A router without a table produces whatever class ``kind`` names."""

    def _match_guards(self, ops) -> list[dict]:
        return [
            step["transform"]["when"]
            for step in _transforms_op(ops).additions["r_view"]
            if step["transform"]["call"]["output"] == ["match_key"]
        ]

    def test_a_member_resolves_to_the_router_and_guards_on_its_own_name(self) -> None:
        ops = _dynamic_ops([_nested(_BARE)])

        assert _transforms_op(ops).at == {"r_view": [0]}
        assert self._match_guards(ops) == [
            {"field": "kind", "in": ["Company"]},
            {"field": "kind", "in": ["Shop"]},
        ]

    def test_an_explicit_table_outranks_a_bare_router_at_another_level(self) -> None:
        mapped = {
            "vertex_router": {
                "type_field": "kind",
                "type_map": {"firm": "Company", "shop": "Shop"},
            }
        }

        ops = _dynamic_ops([_BARE, _nested(mapped)])

        assert _transforms_op(ops).at == {"r_view": [1]}
        assert self._match_guards(ops) == [
            {"field": "kind", "in": ["Company", "firm"]},
            {"field": "kind", "in": ["Shop", "shop"]},
        ]

    def test_bare_routers_at_two_levels_are_ambiguous_until_at_picks_one(self) -> None:
        pipeline = [_BARE, _nested(_BARE)]

        with pytest.raises(AlignmentConflictError, match="ambiguous level"):
            _dynamic_ops(pipeline)

        ops = _dynamic_ops(pipeline, _member_plan(at={"r_view": [1]}))
        assert _transforms_op(ops).at == {"r_view": [1]}
        assert self._match_guards(ops) == [
            {"field": "kind", "in": ["Company"]},
            {"field": "kind", "in": ["Shop"]},
        ]

    def test_a_class_the_side_does_not_declare_is_not_produced(self) -> None:
        plan = _member_plan(
            _member_match_key(),
            _member_local_key_with({"Ghost": LocalKeySource(field="x", tag="g")}),
        )

        with pytest.raises(
            AlignmentConflictError, match="does not produce the member"
        ) as excinfo:
            _dynamic_ops([_nested(_BARE)], plan)

        assert "routes any class" in str(excinfo.value)


class TestDynamicRouterUnion:
    """Validation against the union alone: single specs over a bare router."""

    def _router(self, manifest: GraphManifest) -> dict:
        pipeline = manifest.require_ingestion_model().resources[0].pipeline
        descend = normalize_actor_step(dict(pipeline[0]))
        return normalize_actor_step(dict(descend["pipeline"][0]))

    def test_a_single_spec_lowers_at_the_router_level(self) -> None:
        manifest = _dynamic_union([_nested(_BARE)])

        ops = identity_to_ops(_routed_plan(), manifest=manifest)

        assert _transforms_op(ops).at == {"r_view": [0]}

    def test_every_declared_class_is_guarded_out(self) -> None:
        manifest = _dynamic_union([_nested(_BARE)], sibling_props=["match_key"])

        ops = identity_to_ops(_routed_plan(), manifest=manifest)

        routed = _transforms_op(ops).additions["r_view"]
        assert routed and all(
            s["transform"]["when"] == {"field": "kind", "in": ["Company"]}
            for s in routed
        )

    def test_keep_fields_are_widened(self) -> None:
        router = {
            "vertex_router": {
                "type_field": "kind",
                "keep_fields": ["firm_id", "shop_id"],
            }
        }
        manifest = _dynamic_union([_nested(router)])

        out = apply_evolution(
            manifest,
            identity_to_ops(_routed_plan(), manifest=manifest),
            bump_version=False,
        )

        assert self._router(out)["keep_fields"] == [
            "firm_id",
            "shop_id",
            "match_key",
            "local_key",
        ]


# --------------------------------------------------------------------------- #
# Role routers: two open routers at one level, one transform buffer.
# --------------------------------------------------------------------------- #


_ROLE_ROUTERS = [
    {"vertex_router": {"role": "source", "type_field": "source_type"}},
    {"vertex_router": {"role": "target", "type_field": "target_type"}},
]


class TestRoleRouters:
    """An edge-shaped resource reaches every class through both of its roles."""

    def test_such_a_resource_hosts_no_derivation(self) -> None:
        assert not hosts_member_derivation(
            _side_with(_ROLE_ROUTERS), "r_view", "Company"
        )

    def test_one_router_per_level_does(self) -> None:
        assert hosts_member_derivation(_side_with([_BARE]), "r_view", "Company")
        assert hosts_member_derivation(
            _side_with([_nested(_BARE)]), "r_view", "Company", at=[0]
        )

    def test_a_member_keyed_source_on_it_is_refused(self) -> None:
        with pytest.raises(AlignmentConflictError, match="shared by every router"):
            _dynamic_ops(_ROLE_ROUTERS)


def _one_resource_side(resource: str) -> GraphManifest:
    """A side manifest owning only *resource*, for resolving a resource's side."""
    payload = _union_manifest().to_dict(skip_defaults=True)
    payload["ingestion_model"]["resources"] = [
        r for r in payload["ingestion_model"]["resources"] if r["name"] == resource
    ]
    manifest = GraphManifest.from_config(payload)
    manifest.finish_init()
    return manifest


def _tagged_key_params(ops, resource: str) -> dict:
    (transforms,) = [op for op in ops if isinstance(op, AddResourceTransformsOp)]
    (call,) = [
        step["transform"]["call"]
        for step in transforms.additions[resource]
        if step["transform"]["call"]["foo"] == "tagged_key"
    ]
    return call["params"]


class TestLocalKeyTagDefault:
    """An omitted ``tag`` is the origin of the resource's side; ``null`` is raw."""

    def _ops(self, local_key: LocalKeyBranch, **kwargs):
        return identity_to_ops(
            _plan(_match_key(), local_key),
            sides={
                "left": _one_resource_side("r_a"),
                "right": _one_resource_side("r_b"),
            },
            **kwargs,
        )

    def test_an_omitted_tag_is_the_sides_origin(self) -> None:
        ops = self._ops(
            LocalKeyBranch(
                local_key={
                    "r_a": LocalKeySource(field="firm_id"),
                    "r_b": LocalKeySource(field="org_id"),
                }
            ),
            origins={"left": "acme", "right": "beta"},
        )
        assert _tagged_key_params(ops, "r_a")["tag"] == "acme"
        assert _tagged_key_params(ops, "r_b")["tag"] == "beta"

    def test_an_explicit_null_tag_keeps_raw_values(self) -> None:
        source = LocalKeySource(field="org_id", tag=None)
        assert source.tag == ""
        ops = self._ops(
            LocalKeyBranch(
                local_key={"r_a": LocalKeySource(field="firm_id"), "r_b": source}
            ),
            origins={"left": "acme", "right": "beta"},
        )
        assert _tagged_key_params(ops, "r_b")["tag"] == ""

    def test_an_omitted_tag_round_trips_as_omitted(self) -> None:
        source = LocalKeySource.model_validate({"field": "firm_id"})
        assert source.tag is None
        assert "tag" not in source.to_dict(skip_defaults=True)
        assert LocalKeySource.model_validate({"field": "f", "tag": None}).to_dict(
            skip_defaults=True
        ) == {"field": "f", "tag": ""}

    def test_an_omitted_tag_without_origins_is_refused(self) -> None:
        with pytest.raises(AlignmentConflictError, match="tag"):
            self._ops(
                LocalKeyBranch(local_key={"r_a": LocalKeySource(field="firm_id")})
            )
