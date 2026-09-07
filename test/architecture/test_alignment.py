"""Tests for :mod:`graflo.architecture.evolution.alignment` — the composer."""

from __future__ import annotations

import logging

import pytest

from graflo.architecture.contract.ingestion.resource import resolve_pipeline_level
from graflo.architecture.contract.ingestion.steps.normalize import (
    normalize_actor_step,
)
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import (
    AddResourceTransformsOp,
    AddSecondaryIdentitiesOp,
    AddVertexPropertiesOp,
    AlignmentAttribute,
    AlignmentConflictError,
    CanonicalMap,
    DerivationSpec,
    IdentityAlignment,
    LocalKeySource,
    LocalKeySpec,
    ReplaceIdentityOp,
    alignment_to_ops,
    apply_evolution,
    validate_alignment,
)


def _union_manifest() -> GraphManifest:
    """A composed-union-shaped manifest: one class, two resources feeding it."""
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


def _alignment(**overrides) -> IdentityAlignment:
    base: dict = {
        "vertex": "Company",
        "attributes": [
            AlignmentAttribute(
                into="match_key",
                sources={
                    "r_a": DerivationSpec(
                        input=["secondary_key", "shared_raw"],
                        params={"prefix": "abc_", "strip_prefix": "ABC-"},
                    ),
                    "r_b": DerivationSpec(
                        input=["org_id", "shared_raw"],
                        params={"prefix": "", "strip_prefix": "ABC-"},
                    ),
                },
            )
        ],
        "local_key": LocalKeySpec(
            sources={
                "r_a": LocalKeySource(field="firm_id", tag="a"),
                "r_b": LocalKeySource(field="org_id", tag="b"),
            }
        ),
        "secondary_identities": {
            "by_company_id": ["company_id"],
            "by_org_id": ["org_id"],
        },
    }
    base.update(overrides)
    return IdentityAlignment.model_validate(base)


class TestModel:
    def test_requires_attributes_or_local_key(self) -> None:
        with pytest.raises(ValueError, match="at least one attribute or a local_key"):
            IdentityAlignment(vertex="Company")

    def test_legacy_rows_key_still_loads(self) -> None:
        """``rows`` was the pre-rename spelling; recorded YAML still carries it."""
        alignment = IdentityAlignment.model_validate(
            {
                "vertex": "Company",
                "rows": [
                    {
                        "into": "match_key",
                        "sources": {"r_a": {"input": ["shared_raw"]}},
                    }
                ],
            }
        )
        assert [a.into for a in alignment.attributes] == ["match_key"]
        # Serialization moves to the new spelling.
        assert "attributes" in alignment.to_dict()
        assert "rows" not in alignment.to_dict()

    def test_duplicate_targets_rejected(self) -> None:
        with pytest.raises(ValueError, match="duplicate target attributes"):
            _alignment(
                local_key=LocalKeySpec(
                    into="match_key",
                    sources={"r_a": LocalKeySource(field="firm_id", tag="a")},
                )
            )


class TestComposedOps:
    def test_op_list_shape_and_order(self) -> None:
        ops = alignment_to_ops(_alignment())
        assert [type(o) for o in ops] == [
            AddVertexPropertiesOp,
            AddResourceTransformsOp,
            ReplaceIdentityOp,
            AddSecondaryIdentitiesOp,
        ]

        props = ops[0]
        assert isinstance(props, AddVertexPropertiesOp)
        assert props.additions == {"Company": ["match_key", "local_key"]}

        derive = ops[1]
        assert isinstance(derive, AddResourceTransformsOp)
        assert set(derive.additions) == {"r_a", "r_b"}
        # Row steps precede the local_key step per resource.
        r_a_outputs = [
            step["transform"]["call"]["output"] for step in derive.additions["r_a"]
        ]
        assert r_a_outputs == [["match_key"], ["local_key"]]

        identity = ops[2]
        assert isinstance(identity, ReplaceIdentityOp)
        replacement = identity.vertices["Company"]
        assert replacement.retire == "keep"
        funnel = replacement.to.funnel  # type: ignore[union-attr]
        assert [b.id for b in funnel.branches] == ["match_key", "local_key"]
        assert all(b.fields == [b.id] for b in funnel.branches)

        secondaries = ops[3]
        assert isinstance(secondaries, AddSecondaryIdentitiesOp)
        assert [s.name for s in secondaries.additions["Company"]] == [
            "by_company_id",
            "by_org_id",
        ]

    def test_ops_apply_to_the_union(self) -> None:
        manifest = apply_evolution(
            _union_manifest(),
            alignment_to_ops(_alignment(), manifest=_union_manifest()),
        )
        assert manifest.graph_schema is not None
        vc = manifest.graph_schema.core_schema.vertex_config
        assert vc.identity_fields("Company") == ["id"]
        assert {"match_key", "local_key"} <= set(vc.property_names("Company"))
        assert {s.name for s in vc.secondary_identities("Company")} == {
            "by_company_id",
            "by_org_id",
        }


class TestValidation:
    def _validate(self, alignment: IdentityAlignment, **kwargs) -> None:
        validate_alignment(alignment, _union_manifest(), **kwargs)

    def test_valid_alignment_passes(self) -> None:
        self._validate(_alignment())

    def test_unknown_vertex_raises(self) -> None:
        with pytest.raises(AlignmentConflictError, match="unknown vertex"):
            self._validate(_alignment(vertex="Ghost"))

    def test_unknown_resource_raises(self) -> None:
        alignment = _alignment(
            local_key=LocalKeySpec(
                sources={"ghost": LocalKeySource(field="x", tag="g")}
            )
        )
        with pytest.raises(AlignmentConflictError, match="unknown resources"):
            self._validate(alignment)

    def test_target_colliding_with_current_identity_raises(self) -> None:
        alignment = _alignment(
            attributes=[
                AlignmentAttribute(
                    into="company_id",
                    sources={"r_a": DerivationSpec(input=["firm_id"])},
                )
            ]
        )
        with pytest.raises(AlignmentConflictError, match="identity collision"):
            self._validate(alignment)

    def test_undeclared_secondary_fields_raise(self) -> None:
        alignment = _alignment(secondary_identities={"by_ghost": ["ghost_field"]})
        with pytest.raises(AlignmentConflictError, match="undeclared secondary fields"):
            self._validate(alignment)

    def test_canonical_name_as_derivation_input_raises(self) -> None:
        cm = CanonicalMap(
            vertices={"Firm": "Company"},
            properties={"Firm": {"firm_id": "company_id"}},
        )
        alignment = _alignment(
            local_key=LocalKeySpec(
                sources={
                    # WRONG: company_id is the canonical rename target; the
                    # raw docs still carry firm_id.
                    "r_a": LocalKeySource(field="company_id", tag="a"),
                    "r_b": LocalKeySource(field="org_id", tag="b"),
                }
            )
        )
        with pytest.raises(
            AlignmentConflictError, match="canonical name as derivation input"
        ):
            self._validate(alignment, canonical_maps=[cm])

    def test_rows_only_warns_about_missing_local_key(self, caplog) -> None:
        alignment = _alignment(local_key=None)
        with caplog.at_level(logging.WARNING):
            self._validate(alignment)
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
    and ``shop`` onto the aligned class while ``person`` keeps flowing through
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


def _routed_alignment(**overrides) -> IdentityAlignment:
    base: dict = {
        "vertex": "Company",
        "attributes": [
            AlignmentAttribute(
                into="match_key",
                sources={
                    "r_view": [
                        DerivationSpec(input=["secondary_key", "firm_ref"]),
                        DerivationSpec(input=["secondary_key", "shop_ref"]),
                    ],
                    "r_b": DerivationSpec(input=["org_id", "shared_raw"]),
                },
            )
        ],
        "local_key": LocalKeySpec(
            sources={
                "r_view": [
                    LocalKeySource(
                        field="firm_id", tag="firm", gate="kind", gate_prefix="firm"
                    ),
                    LocalKeySource(
                        field="shop_id", tag="shop", gate="kind", gate_prefix="shop"
                    ),
                ],
                "r_b": LocalKeySource(field="org_id", tag="b"),
            }
        ),
    }
    base.update(overrides)
    return IdentityAlignment.model_validate(base)


def _vertex_config(manifest: GraphManifest):
    schema = manifest.graph_schema
    assert schema is not None
    return schema.core_schema.vertex_config


def _transforms_op(ops) -> AddResourceTransformsOp:
    return next(op for op in ops if isinstance(op, AddResourceTransformsOp))


class TestLevelResolution:
    """Where a derivation lands. A root-level step is invisible below a descend."""

    def test_a_root_level_vertex_resolves_to_the_root(self) -> None:
        ops = alignment_to_ops(_alignment(), manifest=_union_manifest())

        assert _transforms_op(ops).at == {}

    def test_a_nested_router_resolves_to_its_own_level(self) -> None:
        ops = alignment_to_ops(_routed_alignment(), manifest=_routed_manifest())

        assert _transforms_op(ops).at == {"r_view": [0]}

    def test_a_root_level_router_resolves_to_the_root(self) -> None:
        ops = alignment_to_ops(
            _routed_alignment(), manifest=_routed_manifest(nested=False)
        )

        assert _transforms_op(ops).at == {}

    def test_a_nested_plain_vertex_resolves_to_its_own_level(self) -> None:
        manifest = _union_manifest()
        manifest.require_ingestion_model().resources[0].pipeline = [
            {"descend": {"key": "rows", "apply": [{"vertex": "Company"}]}}
        ]
        manifest.finish_init()

        ops = alignment_to_ops(_alignment(), manifest=manifest)

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
            alignment_to_ops(_alignment(), manifest=manifest)

    def test_several_producing_levels_are_rejected(self) -> None:
        manifest = _union_manifest()
        manifest.require_ingestion_model().resources[0].pipeline = [
            {"vertex": "Company"},
            {"descend": {"key": "rows", "apply": [{"vertex": "Company"}]}},
        ]
        manifest.finish_init()

        with pytest.raises(AlignmentConflictError, match="ambiguous level") as excinfo:
            alignment_to_ops(_alignment(), manifest=manifest)

        assert "at={'r_a': []}" in str(excinfo.value)

    def test_an_explicit_at_disambiguates(self) -> None:
        manifest = _union_manifest()
        manifest.require_ingestion_model().resources[0].pipeline = [
            {"vertex": "Company"},
            {"descend": {"key": "rows", "apply": [{"vertex": "Company"}]}},
        ]
        manifest.finish_init()

        ops = alignment_to_ops(_alignment(at={"r_a": [1]}), manifest=manifest)

        assert _transforms_op(ops).at["r_a"] == [1]

    def test_an_at_pointing_at_a_barren_level_is_rejected(self) -> None:
        """The silent failure this resolution exists to prevent."""
        with pytest.raises(AlignmentConflictError, match="level produces nothing"):
            alignment_to_ops(
                _routed_alignment(at={"r_view": []}), manifest=_routed_manifest()
            )

    def test_an_unresolvable_at_is_rejected(self) -> None:
        with pytest.raises(AlignmentConflictError, match="unresolvable level"):
            alignment_to_ops(
                _routed_alignment(at={"r_view": [0, 0]}), manifest=_routed_manifest()
            )


class TestMultiSourceLowering:
    """Several derivations per resource: scratch fields plus one coalesce."""

    def _calls(self, ops, resource: str) -> list[dict]:
        return [
            step["transform"]["call"]
            for step in _transforms_op(ops).additions[resource]
        ]

    def test_one_spec_still_writes_the_attribute_directly(self) -> None:
        calls = self._calls(
            alignment_to_ops(_routed_alignment(), manifest=_routed_manifest()), "r_b"
        )

        assert [c["output"] for c in calls] == [["match_key"], ["local_key"]]
        assert all("strategy" not in c for c in calls)

    def test_several_specs_write_scratch_then_coalesce(self) -> None:
        calls = self._calls(
            alignment_to_ops(_routed_alignment(), manifest=_routed_manifest()),
            "r_view",
        )
        match_calls = calls[:3]

        assert [c["output"] for c in match_calls] == [
            ["_match_key__0"],
            ["_match_key__1"],
            ["match_key"],
        ]
        assert match_calls[-1]["foo"] == "coalesce_fields"
        assert match_calls[-1]["params"]["fields"] == [
            "_match_key__0",
            "_match_key__1",
        ]

    def test_the_coalesce_tolerates_absent_branch_columns(self) -> None:
        """``strategy: all`` empties the missing-input guard."""
        calls = self._calls(
            alignment_to_ops(_routed_alignment(), manifest=_routed_manifest()),
            "r_view",
        )
        coalesce = next(c for c in calls if c["foo"] == "coalesce_fields")

        assert coalesce["strategy"] == "all"
        assert "input" not in coalesce

    def test_a_gated_local_key_reads_the_discriminator(self) -> None:
        calls = self._calls(
            alignment_to_ops(_routed_alignment(), manifest=_routed_manifest()),
            "r_view",
        )
        gated = [c for c in calls if c["foo"] == "gated_tagged_key"]

        assert [c["input"] for c in gated] == [
            ["kind", "firm_id"],
            ["kind", "shop_id"],
        ]
        assert [c["params"]["prefix"] for c in gated] == ["firm", "shop"]

    def test_an_affix_gated_spec_lowers_to_a_single_input_call(self) -> None:
        """The marker idiom reads one field: the value gates itself."""
        alignment = _routed_alignment(
            attributes=[
                AlignmentAttribute(
                    into="match_key",
                    sources={
                        "r_view": [
                            DerivationSpec(
                                input=["firm_ref"],
                                foo="affix_gated_key",
                                params={"prefix": "ABC-"},
                            ),
                            DerivationSpec(
                                input=["shop_ref"],
                                foo="affix_gated_key",
                                params={"prefix": "ABC-"},
                            ),
                        ],
                        "r_b": DerivationSpec(
                            input=["shared_raw"],
                            foo="affix_gated_key",
                            params={"prefix": "ABC-"},
                        ),
                    },
                )
            ]
        )
        calls = self._calls(
            alignment_to_ops(alignment, manifest=_routed_manifest()), "r_view"
        )
        marker = [c for c in calls if c["foo"] == "affix_gated_key"]

        assert [c["input"] for c in marker] == [["firm_ref"], ["shop_ref"]]
        assert all(c["params"] == {"prefix": "ABC-"} for c in marker)
        # Still scratch-then-coalesce: the branch count drives that, not arity.
        assert [c["output"] for c in marker] == [["_match_key__0"], ["_match_key__1"]]
        assert calls[2]["foo"] == "coalesce_fields"

    def test_a_scratch_name_colliding_with_a_property_is_rejected(self) -> None:
        with pytest.raises(AlignmentConflictError, match="scratch name collision"):
            alignment_to_ops(
                _routed_alignment(),
                manifest=_routed_manifest(company_props=["_match_key__0"]),
            )


class TestRouterDelivery:
    """A router's child reads the merged observation, not the transform buffer."""

    def _ensure(self, ops):
        from graflo.architecture.evolution import EnsureExtractedFieldsOp

        return next((op for op in ops if isinstance(op, EnsureExtractedFieldsOp)), None)

    def test_a_keep_fields_router_gets_the_canonical_attributes(self) -> None:
        ops = alignment_to_ops(
            _routed_alignment(),
            manifest=_routed_manifest(keep_fields=["firm_id", "shop_id"]),
        )

        entries = self._ensure(ops).additions["r_view"]
        assert [e.vertex for e in entries] == ["Company"]
        assert entries[0].fields == ["match_key", "local_key"]
        assert entries[0].at == [0]

    def test_a_mapped_only_router_gets_the_canonical_attributes(self) -> None:
        ops = alignment_to_ops(
            _routed_alignment(),
            manifest=_routed_manifest(extraction_scope="mapped_only"),
        )

        assert self._ensure(ops) is not None

    def test_an_unrestricted_router_needs_nothing(self) -> None:
        ops = alignment_to_ops(_routed_alignment(), manifest=_routed_manifest())

        assert self._ensure(ops) is None

    def test_a_plain_vertex_step_needs_nothing(self) -> None:
        ops = alignment_to_ops(_alignment(), manifest=_union_manifest())

        assert self._ensure(ops) is None

    def test_a_sibling_class_claiming_a_canonical_name_is_rejected(self) -> None:
        with pytest.raises(AlignmentConflictError, match="claimed by a sibling class"):
            alignment_to_ops(
                _routed_alignment(),
                manifest=_routed_manifest(sibling_props=["match_key"]),
            )


class TestEnsureExtractedFieldsApplies:
    """The op's effect on the pipeline, not just its emission."""

    def _router(self, manifest: GraphManifest) -> dict:
        from graflo.architecture.contract.ingestion.steps.normalize import (
            normalize_actor_step,
        )

        pipeline = manifest.require_ingestion_model().resources[0].pipeline
        descend = normalize_actor_step(dict(pipeline[0]))
        return normalize_actor_step(dict(descend["pipeline"][0]))

    def test_keep_fields_gains_the_canonical_attributes(self) -> None:
        manifest = _routed_manifest(keep_fields=["firm_id", "shop_id"])

        out = apply_evolution(
            manifest,
            alignment_to_ops(_routed_alignment(), manifest=manifest),
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
            alignment_to_ops(_routed_alignment(), manifest=manifest),
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
            alignment_to_ops(_routed_alignment(), manifest=manifest),
            bump_version=False,
        )

        assert self._router(out)["vertex_from_map"]["Company"] == {
            "company_id": "firm_id",
            "match_key": "match_key",
            "local_key": "local_key",
        }


# --------------------------------------------------------------------------- #
# Member-keyed sources: the member decides, the side manifest supplies the gate.
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


def _member_alignment(**overrides) -> IdentityAlignment:
    base: dict = {
        "vertex": "Company",
        "attributes": [
            AlignmentAttribute(
                into="match_key",
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
        ],
        "local_key": LocalKeySpec(
            sources={
                "r_view": {
                    "Company": LocalKeySource(field="firm_id", tag="firm"),
                    "Shop": LocalKeySource(field="shop_id", tag="shop"),
                },
                "r_b": LocalKeySource(field="org_id", tag="b"),
            }
        ),
    }
    base.update(overrides)
    return IdentityAlignment.model_validate(base)


def _member_ops(alignment: IdentityAlignment | None = None, **kwargs):
    return alignment_to_ops(
        alignment or _member_alignment(),
        manifest=_routed_manifest(company_props=["secondary_key"]),
        sides=kwargs.pop("sides", _sides()),
        cluster_members=kwargs.pop("cluster_members", _CLUSTER),
        **kwargs,
    )


class TestMemberKeyedModel:
    def test_a_member_dict_is_told_apart_from_a_spec(self) -> None:
        """A spec is ``extra="forbid"``, so a dict of specs never parses as one."""
        attribute = AlignmentAttribute.model_validate(
            {
                "into": "match_key",
                "sources": {
                    "r_view": {"Shop": {"input": ["secondary_key"]}},
                    "r_b": {"input": ["shared_raw"]},
                },
            }
        )

        assert attribute.members_for("r_view") == ["Shop"]
        assert attribute.members_for("r_b") is None
        assert [s.input for s in attribute.specs_for("r_view")] == [["secondary_key"]]

    def test_the_dict_form_round_trips_through_to_dict(self) -> None:
        alignment = _member_alignment()

        reloaded = IdentityAlignment.model_validate(alignment.to_dict())

        assert reloaded == alignment
        assert reloaded.attributes[0].members_for("r_view") == ["Company", "Shop"]

    def test_a_member_keyed_local_key_may_not_set_a_gate(self) -> None:
        with pytest.raises(ValueError, match="member already decides"):
            LocalKeySpec(
                sources={
                    "r_view": {
                        "Shop": LocalKeySource(
                            field="shop_id", tag="shop", gate="kind", gate_prefix="shop"
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
            {"field": "kind", "in": ["firm"]},
            {"field": "kind", "in": ["shop"]},
        ]
        assert [s["call"]["params"]["prefix"] for s in match] == ["abc_", "def_"]
        assert [s["call"]["input"] for s in match] == [["secondary_key"]] * 2
        assert not any(s["call"]["foo"] == "coalesce_fields" for s in steps)

    def test_the_local_key_is_guarded_the_same_way(self) -> None:
        steps = self._calls(_member_ops(), "r_view")

        local = [s for s in steps if s["call"]["output"] == ["local_key"]]
        assert [s["call"]["foo"] for s in local] == ["tagged_key", "tagged_key"]
        assert [s["when"]["in"] for s in local] == [["firm"], ["shop"]]

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
        assert by_prefix["abc_"]["when"] == {"field": "kind", "in": ["firm"]}
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
            alignment_to_ops(_member_alignment())

    def test_an_unknown_member_is_rejected(self) -> None:
        alignment = _member_alignment(
            local_key=LocalKeySpec(
                sources={
                    "r_view": {"Ghost": LocalKeySource(field="x", tag="g")},
                    "r_b": LocalKeySource(field="org_id", tag="b"),
                }
            )
        )
        with pytest.raises(
            AlignmentConflictError, match="resource does not produce the member"
        ) as excinfo:
            _member_ops(alignment)

        assert "'Company', 'Person', 'Shop'" in str(excinfo.value)

    def test_a_member_outside_the_cluster_is_rejected(self) -> None:
        """``Person`` is produced by the resource but is not being aligned."""
        alignment = _member_alignment(
            local_key=LocalKeySpec(
                sources={
                    "r_view": {"Person": LocalKeySource(field="person_id", tag="p")},
                    "r_b": LocalKeySource(field="org_id", tag="b"),
                }
            )
        )
        with pytest.raises(AlignmentConflictError, match="member outside the cluster"):
            _member_ops(alignment)

    def test_a_resource_on_no_side_is_rejected(self) -> None:
        with pytest.raises(AlignmentConflictError, match="resource on no side"):
            _member_ops(sides={"right": _right_side()})

    def test_an_at_that_misses_the_member_is_rejected(self) -> None:
        with pytest.raises(AlignmentConflictError, match="level produces nothing"):
            _member_ops(_member_alignment(at={"r_view": []}))

    def test_partial_coverage_warns(self, caplog) -> None:
        alignment = _member_alignment(
            attributes=[
                AlignmentAttribute(
                    into="match_key",
                    sources={
                        "r_view": {"Company": _member_spec("abc_")},
                        "r_b": DerivationSpec(
                            input=["shared_raw"], foo="affix_gated_key"
                        ),
                    },
                )
            ]
        )
        with caplog.at_level(logging.WARNING):
            _member_ops(alignment)

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
