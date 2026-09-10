"""Tests for :mod:`graflo.architecture.evolution.canonical`."""

from __future__ import annotations

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import (
    CanonicalizeOp,
    CanonicalMap,
    ComposeCanonicalConflictError,
    ComposeManifestsOp,
    PropertyEquivalence,
    RelationEquivalence,
    VertexEquivalence,
    apply_evolution,
    canonical_map_to_ops,
    compose_manifests,
    merge_canonical_maps,
    validate_and_complete_canonical_map,
)
from graflo.architecture.schema.core import CoreSchema
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.metadata import GraphMetadata
from graflo.architecture.schema.vertex import Field, FieldType, Vertex, VertexConfig


def _manifest(
    *,
    name: str,
    vertices: list[Vertex],
    edges: list[Edge] | None = None,
    resources: list[dict] | None = None,
) -> GraphManifest:
    schema = Schema(
        metadata=GraphMetadata(name=name, version="1.0.0"),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(vertices=vertices, force_types={}),
            edge_config=EdgeConfig(edges=edges or []),
        ),
    )
    payload: dict = {"schema": schema.to_dict(skip_defaults=False)}
    if resources is not None:
        payload["ingestion_model"] = {"resources": resources, "transforms": []}
    m = GraphManifest.from_config(payload)
    m.finish_init()
    return m


def _source_a_manifest() -> GraphManifest:
    """Manifest A speaking its own vocabulary (pre-canonical)."""
    return _manifest(
        name="a",
        vertices=[
            Vertex(
                name="Firm",
                properties=[
                    Field(name="firm_id", type=FieldType.STRING),
                    Field(name="firm_label", type=FieldType.STRING),
                ],
                identity=["firm_id"],
            ),
            Vertex(
                name="Deal",
                properties=[Field(name="id", type=FieldType.STRING)],
                identity=["id"],
            ),
        ],
        edges=[Edge(source="Firm", target="Deal", relation="signs")],
    )


def _right_b_manifest() -> GraphManifest:
    return _manifest(
        name="b",
        vertices=[
            Vertex(
                name="Org",
                properties=[
                    Field(name="org_id", type=FieldType.STRING),
                    Field(name="org_label", type=FieldType.STRING),
                ],
                identity=["org_id"],
            ),
        ],
    )


_CANONICAL = CanonicalMap(
    vertices={"Firm": "Company"},
    properties={"Firm": {"firm_id": "company_id", "firm_label": "label"}},
)


class TestCanonicalMapModel:
    def test_non_injective_vertex_map_rejected(self) -> None:
        with pytest.raises(ValueError, match="not injective"):
            CanonicalMap(vertices={"Firm": "Company", "Shop": "Company"})

    def test_non_injective_vertex_map_allowed_with_flag(self) -> None:
        cm = CanonicalMap(
            vertices={"Firm": "Company", "Shop": "Company"}, allow_merges=True
        )
        assert cm.canonical_class("Firm") == "Company"

    def test_identity_entry_alongside_a_merge_is_not_a_false_collision(self) -> None:
        """A lowered cluster map carries `into: into` for every member.

        That self entry must not read as a second source colliding with the
        real merge -- it is the mechanism that turns a member-equal-to-`into`
        into a `MergeVerticesOp` group instead of a colliding rename.
        """
        cm = CanonicalMap(
            vertices={"Company": "Company", "Shop": "Company"}, allow_merges=True
        )
        assert cm.canonical_class("Shop") == "Company"

    def test_non_injective_relation_map_rejected(self) -> None:
        with pytest.raises(ValueError, match="not injective"):
            CanonicalMap(relations={"signs": "has", "owns": "has"})

    def test_non_injective_relation_map_allowed_with_flag(self) -> None:
        cm = CanonicalMap(relations={"signs": "has", "owns": "has"}, allow_merges=True)
        assert cm.canonical_relation("signs") == "has"

    def test_non_injective_property_map_rejected(self) -> None:
        with pytest.raises(ValueError, match="not injective"):
            CanonicalMap(properties={"Firm": {"a": "x", "b": "x"}})

    def test_canonical_targets_and_attributes(self) -> None:
        assert _CANONICAL.vertex_targets == {"Company"}
        assert _CANONICAL.canonical_property_names("Company") == {"company_id", "label"}
        cm = CanonicalMap(relations={"signs": "has"}, vertices={"Deal": "Deal"})
        assert cm.relation_targets == {"has"}
        assert cm.vertex_targets == set()
        assert cm.canonical_relation("signs") == "has"


class TestMergeCanonicalMaps:
    def test_agreement_unions_cleanly(self) -> None:
        base = CanonicalMap(vertices={"Firm": "Company"})
        extension = CanonicalMap(vertices={"Firm": "Company", "Deal": "Deal"})
        merged = merge_canonical_maps(base, extension)
        assert merged.vertices == {"Firm": "Company", "Deal": "Deal"}

    def test_source_disagreement_raises(self) -> None:
        base = CanonicalMap(vertices={"Firm": "Company"})
        extension = CanonicalMap(vertices={"Firm": "Party"})
        with pytest.raises(
            ComposeCanonicalConflictError, match="canonical vertex clash"
        ):
            merge_canonical_maps(base, extension)

    def test_base_target_is_a_fixed_point(self) -> None:
        base = CanonicalMap(vertices={"Firm": "Company"})
        extension = CanonicalMap(vertices={"Company": "Party"})
        with pytest.raises(
            ComposeCanonicalConflictError, match="canonical vertex re-target"
        ):
            merge_canonical_maps(base, extension)

    def test_relation_fixed_point(self) -> None:
        base = CanonicalMap(relations={"signs": "has"})
        extension = CanonicalMap(relations={"has": "owns"})
        with pytest.raises(
            ComposeCanonicalConflictError, match="canonical relation re-target"
        ):
            merge_canonical_maps(base, extension)

    def test_property_disagreement_raises(self) -> None:
        base = CanonicalMap(properties={"Firm": {"a": "x"}})
        extension = CanonicalMap(properties={"Firm": {"a": "y"}})
        with pytest.raises(
            ComposeCanonicalConflictError, match="canonical property clash"
        ):
            merge_canonical_maps(base, extension)


class TestCanonicalMapToOps:
    def test_the_map_lowers_to_one_op_and_canonicalizes_the_manifest(self) -> None:
        ops = canonical_map_to_ops(_CANONICAL)
        assert [type(o) for o in ops] == [CanonicalizeOp]
        op = ops[0]
        assert isinstance(op, CanonicalizeOp)
        assert op.vertices == {"Firm": "Company"}
        assert op.properties == {
            "Firm": {"firm_id": "company_id", "firm_label": "label"}
        }

        canonical_a = apply_evolution(_source_a_manifest(), ops)
        assert canonical_a.graph_schema is not None
        vc = canonical_a.graph_schema.core_schema.vertex_config
        assert vc.vertex_set == {"Company", "Deal"}
        assert set(vc.property_names("Company")) == {"company_id", "label"}
        assert vc.identity_fields("Company") == ["company_id"]
        edges = canonical_a.graph_schema.core_schema.edge_config.edges
        assert [(e.source, e.target) for e in edges] == [("Company", "Deal")]

    def test_identity_entries_produce_no_ops(self) -> None:
        cm = CanonicalMap(vertices={"Deal": "Deal"}, properties={"Deal": {"id": "id"}})
        assert canonical_map_to_ops(cm) == []

    def test_a_collapsed_group_is_a_merge_group_on_the_op(self) -> None:
        cm = CanonicalMap(
            vertices={"Firm": "Company", "Shop": "Company"}, allow_merges=True
        )
        ops = canonical_map_to_ops(cm)
        assert [type(o) for o in ops] == [CanonicalizeOp]
        op = ops[0]
        assert isinstance(op, CanonicalizeOp)
        assert op.vertex_groups == {"Company": ["Firm", "Shop"]}
        assert op.merges

    def test_collapsed_group_without_allow_merges_raises(self) -> None:
        cm = CanonicalMap(vertices={"Company": "Company", "Shop": "Company"})
        with pytest.raises(ValueError, match="allow_merges"):
            canonical_map_to_ops(cm)

    def test_relations_lower_onto_the_same_op(self) -> None:
        cm = CanonicalMap(relations={"a": "r", "b": "r", "c": "d"}, allow_merges=True)
        ops = canonical_map_to_ops(cm)
        assert [type(o) for o in ops] == [CanonicalizeOp]
        op = ops[0]
        assert isinstance(op, CanonicalizeOp)
        assert op.relation_groups == {"r": ["a", "b"], "d": ["c"]}


class TestCanonicalizeAppliesInOneStep:
    """The map is a function on names applied once; op order cannot leak in."""

    @staticmethod
    def _three() -> GraphManifest:
        def _v(name: str, key: str) -> Vertex:
            return Vertex(
                name=name,
                properties=[Field(name=key, type=FieldType.STRING)],
                identity=[key],
            )

        return _manifest(
            name="t", vertices=[_v("X", "x_id"), _v("X2", "x2_id"), _v("Z", "z_id")]
        )

    def test_a_merge_landing_on_a_name_that_moves_builds_a_fresh_class(self) -> None:
        """{X, X2} -> Z while Z -> Q: Q is the old Z alone, Z is X and X2 alone."""
        cm = CanonicalMap(vertices={"X": "Z", "X2": "Z", "Z": "Q"}, allow_merges=True)
        out = apply_evolution(self._three(), canonical_map_to_ops(cm))
        assert out.graph_schema is not None
        vc = out.graph_schema.core_schema.vertex_config
        assert vc.vertex_set == {"Z", "Q"}
        assert set(vc.property_names("Q")) == {"z_id"}
        assert set(vc.property_names("Z")) == {"x_id", "x2_id"}

    def test_a_rename_onto_a_merge_source_lands_on_a_free_name(self) -> None:
        """A -> X while {X, X2} -> Z: the rename target is vacated by the merge."""
        m = _manifest(
            name="t",
            vertices=[
                Vertex(
                    name=n,
                    properties=[Field(name=f"{n.lower()}_id", type=FieldType.STRING)],
                    identity=[f"{n.lower()}_id"],
                )
                for n in ("A", "X", "X2")
            ],
        )
        cm = CanonicalMap(vertices={"A": "X", "X": "Z", "X2": "Z"}, allow_merges=True)
        out = apply_evolution(m, canonical_map_to_ops(cm))
        assert out.graph_schema is not None
        vc = out.graph_schema.core_schema.vertex_config
        assert vc.vertex_set == {"X", "Z"}
        assert set(vc.property_names("X")) == {"a_id"}
        assert set(vc.property_names("Z")) == {"x_id", "x2_id"}


class TestValidateAndCompleteCanonicalMap:
    def _canonical_a(self) -> GraphManifest:
        return apply_evolution(_source_a_manifest(), canonical_map_to_ops(_CANONICAL))

    def _validate(self, op: ComposeManifestsOp, **kwargs):
        return validate_and_complete_canonical_map(
            op,
            left=self._canonical_a(),
            right=_right_b_manifest(),
            canonical_maps=[("left", _CANONICAL)],
            **kwargs,
        )

    def test_valid_op_passes_and_composes(self) -> None:
        op = ComposeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(
                    left="Company",
                    right="Org",
                    into="Company",
                    properties=[
                        PropertyEquivalence(right="org_id", into="company_id"),
                        PropertyEquivalence(right="org_label", into="label"),
                    ],
                )
            ]
        )
        side_maps = self._validate(op)
        assert side_maps.left.vertices == {"Company": "Company"}
        assert side_maps.right.vertices == {"Org": "Company"}
        out = compose_manifests(
            self._canonical_a(),
            _right_b_manifest(),
            op,
            canonical_maps=[("left", _CANONICAL)],
        )
        assert out.graph_schema is not None
        assert out.graph_schema.core_schema.vertex_config.vertex_set == {
            "Company",
            "Deal",
        }

    def test_a_member_in_the_raw_vocabulary_resolves_through_the_map(self) -> None:
        """E partitions, C names: {Firm} ~ {Org} is called Company because C says so."""
        op = ComposeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(
                    left="Firm",
                    right="Org",
                    properties=[
                        PropertyEquivalence(right="org_id", into="company_id"),
                        PropertyEquivalence(right="org_label", into="label"),
                    ],
                )
            ]
        )
        side_maps = validate_and_complete_canonical_map(
            op,
            left=_source_a_manifest(),
            right=_right_b_manifest(),
            canonical_maps=[("left", _CANONICAL)],
        )
        assert side_maps.left.vertices == {"Firm": "Company"}
        assert side_maps.left.properties == {
            "Firm": {"firm_id": "company_id", "firm_label": "label"}
        }
        assert side_maps.right.vertices == {"Org": "Company"}
        out = compose_manifests(
            _source_a_manifest(),
            _right_b_manifest(),
            op,
            canonical_maps=[("left", _CANONICAL)],
        )
        assert out.graph_schema is not None
        vc = out.graph_schema.core_schema.vertex_config
        assert vc.vertex_set == {"Company", "Deal"}
        assert set(vc.property_names("Company")) >= {"company_id", "label"}

    def test_the_two_authoring_orders_compose_to_the_same_manifest(self) -> None:
        """Canonicalize-then-declare and declare-then-canonicalize are one function."""
        raw_op = ComposeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(left="Firm", right="Org", identity=["company_id"])
            ],
            canonical_maps={"left": _CANONICAL},
        )
        canonical_op = ComposeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(
                    left="Company", right="Org", into="Company", identity=["company_id"]
                )
            ],
            canonical_maps={"left": _CANONICAL},
        )
        via_raw = compose_manifests(
            _source_a_manifest(), _right_b_manifest(), raw_op, bump_version=False
        )
        via_canonical = compose_manifests(
            self._canonical_a(), _right_b_manifest(), canonical_op, bump_version=False
        )
        assert via_raw.graph_schema is not None
        assert via_canonical.graph_schema is not None
        assert via_raw.graph_schema.core_schema.to_dict(
            skip_defaults=False
        ) == via_canonical.graph_schema.core_schema.to_dict(skip_defaults=False)

    def test_a_member_may_be_spelled_by_its_canonical_name(self) -> None:
        """Against a raw manifest, `Company` names `Firm` because C establishes it."""
        op = ComposeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(left="Company", right="Org", into="Company")
            ]
        )
        side_maps = validate_and_complete_canonical_map(
            op,
            left=_source_a_manifest(),
            right=_right_b_manifest(),
            canonical_maps=[("left", _CANONICAL)],
        )
        assert side_maps.left.vertices == {"Firm": "Company"}

    def test_a_disagreeing_into_raises(self) -> None:
        op = ComposeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(left="Firm", right="Org", into="Party")
            ]
        )
        with pytest.raises(ComposeCanonicalConflictError, match="disagreement"):
            validate_and_complete_canonical_map(
                op,
                left=_source_a_manifest(),
                right=_right_b_manifest(),
                canonical_maps=[("left", _CANONICAL)],
            )

    def test_a_retired_name_as_into_is_translated_not_resurrected(self) -> None:
        op = ComposeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(left="Firm", right="Org", into="Firm")
            ]
        )
        side_maps = validate_and_complete_canonical_map(
            op,
            left=_source_a_manifest(),
            right=_right_b_manifest(),
            canonical_maps=[("left", _CANONICAL)],
        )
        assert side_maps.left.vertices == {"Firm": "Company"}
        assert side_maps.right.vertices == {"Org": "Company"}

    def test_a_member_answers_to_its_canonical_name(self) -> None:
        """One alias table serves every per-member map of the declaration."""
        from graflo.architecture.evolution.canonical import resolve_clusters

        op = ComposeManifestsOp(
            vertex_equivalences=[VertexEquivalence(left="Firm", right="Org")]
        )
        index = resolve_clusters(
            op,
            left=_source_a_manifest(),
            right=_right_b_manifest(),
            canonical_maps=[("left", _CANONICAL)],
        ).index
        (cluster,) = index.vertices
        assert cluster.left == ("Firm",)
        assert cluster.resolved("left", "Company") == "Firm"
        assert cluster.resolved("left", "Firm") == "Firm"
        assert cluster.resolved("right", "Company") == "Company"  # unmapped side

    def test_an_unnamed_cluster_raises(self) -> None:
        op = ComposeManifestsOp(
            vertex_equivalences=[VertexEquivalence(left="Deal", right="Org")]
        )
        with pytest.raises(ComposeCanonicalConflictError, match="unnamed"):
            validate_and_complete_canonical_map(
                op,
                left=_source_a_manifest(),
                right=_right_b_manifest(),
                canonical_maps=[("left", _CANONICAL)],
            )

    def test_a_shared_spelling_names_an_unmapped_cluster(self) -> None:
        op = ComposeManifestsOp(
            vertex_equivalences=[VertexEquivalence(left="Deal", right="Deal")]
        )
        right = _manifest(
            name="b3",
            vertices=[
                Vertex(name="Deal", properties=[Field(name="id")], identity=["id"])
            ],
        )
        side_maps = validate_and_complete_canonical_map(
            op, left=_source_a_manifest(), right=right
        )
        assert side_maps.left.vertices == {"Deal": "Deal"}
        assert side_maps.right.vertices == {"Deal": "Deal"}

    def test_a_map_merge_into_a_composed_class_raises(self) -> None:
        cm = CanonicalMap(
            vertices={"Firm": "Company", "Deal": "Company"}, allow_merges=True
        )
        op = ComposeManifestsOp(
            vertex_equivalences=[VertexEquivalence(left="Firm", right="Org")]
        )
        with pytest.raises(
            ComposeCanonicalConflictError, match="merge into a composed class"
        ):
            validate_and_complete_canonical_map(
                op,
                left=_source_a_manifest(),
                right=_right_b_manifest(),
                canonical_maps=[("left", cm)],
            )

    def test_a_dangling_map_entry_raises(self) -> None:
        cm = CanonicalMap(vertices={"Ghost": "Company"})
        op = ComposeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(left="Firm", right="Org", into="Company")
            ]
        )
        with pytest.raises(ComposeCanonicalConflictError, match="dangling"):
            validate_and_complete_canonical_map(
                op,
                left=_source_a_manifest(),
                right=_right_b_manifest(),
                canonical_maps=[("left", cm)],
            )

    def test_a_map_the_caller_already_applied_is_a_no_op(self) -> None:
        """Source absent, target present: the entry is satisfied, not dangling."""
        op = ComposeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(left="Company", right="Org", into="Company")
            ]
        )
        side_maps = self._validate(op)
        assert side_maps.left.vertices == {"Company": "Company"}

    def test_a_raw_relation_member_resolves_through_the_map(self) -> None:
        cm = CanonicalMap(relations={"signs": "has"})
        right = _manifest(
            name="b4",
            vertices=[
                Vertex(name="Org", properties=[Field(name="id")], identity=["id"]),
                Vertex(name="Deal", properties=[Field(name="id")], identity=["id"]),
            ],
            edges=[Edge(source="Org", target="Deal", relation="inks")],
        )
        op = ComposeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(left="Firm", right="Org", into="Company")
            ],
            relation_equivalences=[RelationEquivalence(left="signs", right="inks")],
        )
        side_maps = validate_and_complete_canonical_map(
            op, left=_source_a_manifest(), right=right, canonical_maps=[("left", cm)]
        )
        assert side_maps.left.relations == {"signs": "has"}
        assert side_maps.right.relations == {"inks": "has"}

    def test_a_disagreeing_relation_into_raises(self) -> None:
        cm = CanonicalMap(relations={"signs": "has"})
        right = _manifest(
            name="b4",
            vertices=[
                Vertex(name="Org", properties=[Field(name="id")], identity=["id"]),
                Vertex(name="Deal", properties=[Field(name="id")], identity=["id"]),
            ],
            edges=[Edge(source="Org", target="Deal", relation="inks")],
        )
        op = ComposeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(left="Firm", right="Org", into="Company")
            ],
            relation_equivalences=[
                RelationEquivalence(left="signs", right="inks", into="signed")
            ],
        )
        with pytest.raises(
            ComposeCanonicalConflictError, match="relation disagreement"
        ):
            validate_and_complete_canonical_map(
                op,
                left=_source_a_manifest(),
                right=right,
                canonical_maps=[("left", cm)],
            )

    def test_into_re_targets_canonical_class_raises(self) -> None:
        op = ComposeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(left="Company", right="Org", into="Party")
            ]
        )
        # `Company` is a canonical target, hence a fixed point: a cluster may
        # not rename it, whichever vocabulary the equivalence is written in.
        with pytest.raises(ComposeCanonicalConflictError, match="disagreement"):
            self._validate(op)

    def test_a_property_equivalence_naming_an_absent_field_raises(self) -> None:
        op = ComposeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(
                    left="Company",
                    right="Org",
                    into="Company",
                    properties=[
                        PropertyEquivalence(left="firm_id", into="firm_id"),
                    ],
                )
            ]
        )
        with pytest.raises(ComposeCanonicalConflictError, match="unknown property"):
            self._validate(op)

    def test_property_retarget_raises(self) -> None:
        op = ComposeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(
                    left="Company",
                    right="Org",
                    into="Company",
                    properties=[
                        PropertyEquivalence(
                            left="company_id", right="org_id", into="uid"
                        ),
                    ],
                )
            ]
        )
        with pytest.raises(ComposeCanonicalConflictError, match="property re-target"):
            self._validate(op)

    def _right_two_orgs(self) -> GraphManifest:
        return _manifest(
            name="b2",
            vertices=[
                Vertex(name="Org", properties=[Field(name="id")], identity=["id"]),
                Vertex(name="Branch", properties=[Field(name="id")], identity=["id"]),
            ],
            edges=[Edge(source="Org", target="Branch", relation="owns")],
        )

    def _right_collapse_op(
        self, *, allow_merges: bool = True, allow_self_relations: bool = False
    ) -> ComposeManifestsOp:
        # One right-side n-ary cluster: {Org, Branch} ~ {Company} -> Company.
        # Declares the composed identity explicitly so this fixture isn't
        # also exercising the (separately tested) identity-disagreement check.
        return ComposeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(
                    left="Company",
                    right=["Org", "Branch"],
                    into="Company",
                    identity=["company_id"],
                )
            ],
            allow_merges=allow_merges,
            allow_self_relations=allow_self_relations,
        )

    def test_nary_cluster_without_allow_merges_raises_at_construction(self) -> None:
        with pytest.raises(ValueError, match="allow_merges"):
            self._right_collapse_op(allow_merges=False)

    def test_right_collapse_lowers_and_validates(self) -> None:
        op = self._right_collapse_op()
        side_maps = validate_and_complete_canonical_map(
            op,
            left=self._canonical_a(),
            right=self._right_two_orgs(),
            canonical_maps=[("left", _CANONICAL)],
        )
        assert side_maps.right.vertices == {"Org": "Company", "Branch": "Company"}

    def test_self_relation_merge_raises_without_flag(self) -> None:
        # The right edge Org -> Branch lands on Company at both ends once
        # merged; the unary self-relation guard fires because
        # allow_self_relations was not set on the op.
        op = self._right_collapse_op()
        with pytest.raises(ValueError, match="self-relation"):
            compose_manifests(
                self._canonical_a(),
                self._right_two_orgs(),
                op,
                canonical_maps=[("left", _CANONICAL)],
            )

    def test_self_relation_merge_succeeds_with_flag(self) -> None:
        op = self._right_collapse_op(allow_self_relations=True)
        out = compose_manifests(
            self._canonical_a(),
            self._right_two_orgs(),
            op,
            canonical_maps=[("left", _CANONICAL)],
        )
        assert out.graph_schema is not None
        assert out.graph_schema.core_schema.vertex_config.vertex_set == {
            "Company",
            "Deal",
        }

    def test_left_collapse_completes_with_ack(self) -> None:
        # One n-ary cluster: {Company, Deal} ~ {Org, Branch} -> Company.
        op = ComposeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(
                    left=["Company", "Deal"], right=["Org", "Branch"], into="Company"
                )
            ],
            allow_merges=True,
        )
        side_maps = validate_and_complete_canonical_map(
            op,
            left=self._canonical_a(),
            right=self._right_two_orgs(),
            canonical_maps=[("left", _CANONICAL)],
        )
        assert side_maps.left.vertices == {"Company": "Company", "Deal": "Company"}
        assert side_maps.right.vertices == {"Org": "Company", "Branch": "Company"}

    def test_left_collapse_without_ack_raises_at_construction(self) -> None:
        with pytest.raises(ValueError, match="allow_merges"):
            ComposeManifestsOp(
                vertex_equivalences=[
                    VertexEquivalence(
                        left=["Company", "Deal"],
                        right=["Org", "Branch"],
                        into="Company",
                    )
                ]
            )

    def test_completion_infers_right_peer(self) -> None:
        op = ComposeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(left="Company", right="Org", into="Company")
            ]
        )
        side_maps = validate_and_complete_canonical_map(
            op,
            left=self._canonical_a(),
            right=_right_b_manifest(),
            canonical_maps=[("left", _CANONICAL)],
        )
        assert side_maps.left.vertices == {"Company": "Company"}
        assert side_maps.right.vertices == {"Org": "Company"}

    def test_right_side_canonical_map_and_completion(self) -> None:
        """A right-side map seeds a label; the left peer is completed by the cluster."""
        right_cm = CanonicalMap(vertices={"Org": "Company"})
        # Right already speaks Company; left still has Firm — compose after
        # renaming right, with an equivalence Firm ≡ Company into Company.
        right = apply_evolution(_right_b_manifest(), canonical_map_to_ops(right_cm))
        left = _source_a_manifest()
        op = ComposeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(left="Firm", right="Company", into="Company")
            ]
        )
        side_maps = validate_and_complete_canonical_map(
            op,
            left=left,
            right=right,
            canonical_maps=[("right", right_cm)],
        )
        assert side_maps.left.vertices == {"Firm": "Company"}
        assert side_maps.right.vertices == {"Company": "Company"}

    def test_both_side_canonical_maps(self) -> None:
        """Author maps on both sides are checked together, not last-write-wins."""
        right_cm = CanonicalMap(vertices={"Org": "Company"})
        right = apply_evolution(_right_b_manifest(), canonical_map_to_ops(right_cm))
        left = self._canonical_a()
        op = ComposeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(
                    left="Company",
                    right="Company",
                    into="Company",
                    identity=["company_id"],
                )
            ]
        )
        side_maps = validate_and_complete_canonical_map(
            op,
            left=left,
            right=right,
            canonical_maps=[("left", _CANONICAL), ("right", right_cm)],
        )
        assert side_maps.left.vertices == {"Company": "Company"}
        assert side_maps.right.vertices == {"Company": "Company"}
        out = compose_manifests(
            left, right, op, canonical_maps=[("left", _CANONICAL), ("right", right_cm)]
        )
        assert out.graph_schema is not None
        assert out.graph_schema.core_schema.vertex_config.vertex_set == {
            "Company",
            "Deal",
        }


class TestTheTwoChecksDoNotOverlap:
    """`CanonicalMap` validation and compose's own check are disjoint.

    The validator compares an op against a *declared* map — a disagreement
    over where a name goes, a re-targeted canonical class or attribute.
    Compose's check fires on the residue no equivalence covers. A name that
    trips one cannot trip the other, and this pins that they never
    double-report the same pair.
    """

    @staticmethod
    def _one_vertex(name: str, resource: str) -> GraphManifest:
        schema = Schema(
            metadata=GraphMetadata(name=f"m-{name}", version="1.0.0"),
            core_schema=CoreSchema(
                vertex_config=VertexConfig(
                    vertices=[
                        Vertex(
                            name=name,
                            properties=[Field(name="id", type=FieldType.STRING)],
                            identity=["id"],
                        )
                    ],
                    force_types={},
                ),
                edge_config=EdgeConfig(edges=[]),
            ),
        )
        manifest = GraphManifest.from_config(
            {
                "schema": schema.to_dict(skip_defaults=False),
                "ingestion_model": {
                    "resources": [{"name": resource, "apply": [{"vertex": name}]}],
                    "transforms": [],
                },
            }
        )
        manifest.finish_init()
        return manifest

    def test_an_undeclared_near_collision_raises_only_compose_error(self) -> None:
        from graflo.architecture.evolution import (
            ComposeNameConflictError,
            compose_manifests,
        )

        with pytest.raises(ComposeNameConflictError) as excinfo:
            compose_manifests(
                self._one_vertex("OrderLine", "r_left"),
                self._one_vertex("order_line", "r_right"),
                ComposeManifestsOp(),
            )
        # Not the declared-map error: nothing was declared to contradict.
        assert not isinstance(excinfo.value, ComposeCanonicalConflictError)

    def test_a_mapped_name_keys_differently_so_only_the_validator_sees_it(
        self,
    ) -> None:
        """`Firm` and `Company` do not key alike, so compose's check is silent.

        That is what keeps the two checks from ever reporting the same problem:
        one is about vocabulary the author declared, the other about spellings
        nobody reconciled.
        """
        from graflo.architecture.schema.naming import canonical_slug

        assert canonical_slug("Firm") != canonical_slug("Company")
