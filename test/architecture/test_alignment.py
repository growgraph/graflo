"""Tests for :mod:`graflo.architecture.evolution.alignment` — the composer."""

from __future__ import annotations

import logging

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import (
    AddResourceTransformsOp,
    AddSecondaryIdentitiesOp,
    AddVertexPropertiesOp,
    AlignmentConflictError,
    AlignmentRow,
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
        "rows": [
            AlignmentRow(
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
    def test_requires_rows_or_local_key(self) -> None:
        with pytest.raises(ValueError, match="at least one row or a local_key"):
            IdentityAlignment(vertex="Company")

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
            rows=[
                AlignmentRow(
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
