"""Tests for :mod:`graflo.architecture.evolution.canonical`."""

from __future__ import annotations

import logging

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import (
    CanonicalMap,
    ComposeCanonicalConflictError,
    ComposeManifestsOp,
    MergeVerticesOp,
    PropertyEquivalence,
    RenameVertexPropertiesOp,
    RenameVerticesOp,
    VertexEquivalence,
    apply_evolution,
    canonical_map_to_ops,
    compose_manifests,
    validate_and_complete_canonical_map,
    validate_compose_against_canonical_map,
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

    def test_non_injective_property_map_rejected(self) -> None:
        with pytest.raises(ValueError, match="not injective"):
            CanonicalMap(properties={"Firm": {"a": "x", "b": "x"}})

    def test_stale_names(self) -> None:
        assert _CANONICAL.stale_class_names == {"Firm"}
        assert _CANONICAL.stale_property_names("Company") == {"firm_id", "firm_label"}
        assert _CANONICAL.canonical_property_names("Company") == {"company_id", "label"}

    def test_identity_mapped_class_is_not_stale(self) -> None:
        cm = CanonicalMap(vertices={"Deal": "Deal", "Firm": "Company"})
        assert cm.stale_class_names == {"Firm"}


class TestCanonicalMapToOps:
    def test_ops_round_trip_canonicalizes_manifest(self) -> None:
        ops = canonical_map_to_ops(_CANONICAL)
        assert [type(o) for o in ops] == [RenameVertexPropertiesOp, RenameVerticesOp]

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

    def test_collapsed_group_becomes_merge(self) -> None:
        cm = CanonicalMap(
            vertices={"Firm": "Company", "Shop": "Company"}, allow_merges=True
        )
        ops = canonical_map_to_ops(cm)
        assert [type(o) for o in ops] == [MergeVerticesOp]
        merge = ops[0]
        assert isinstance(merge, MergeVerticesOp)
        assert merge.sources == ["Firm", "Shop"]
        assert merge.into == "Company"


class TestValidateComposeAgainstCanonicalMap:
    def _canonical_a(self) -> GraphManifest:
        return apply_evolution(_source_a_manifest(), canonical_map_to_ops(_CANONICAL))

    def _validate(self, op: ComposeManifestsOp, **kwargs) -> None:
        validate_compose_against_canonical_map(
            _CANONICAL,
            op,
            left=self._canonical_a(),
            right=_right_b_manifest(),
            **kwargs,
        )

    def test_valid_op_passes_and_composes(self) -> None:
        op = ComposeManifestsOp(
            vertices=[
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
        self._validate(op)
        out = compose_manifests(self._canonical_a(), _right_b_manifest(), op)
        assert out.graph_schema is not None
        assert out.graph_schema.core_schema.vertex_config.vertex_set == {
            "Company",
            "Deal",
        }

    def test_stale_class_name_raises(self) -> None:
        op = ComposeManifestsOp(
            vertices=[VertexEquivalence(left="Firm", right="Org", into="Firm")]
        )
        with pytest.raises(ComposeCanonicalConflictError, match="stale class name"):
            self._validate(op)

    def test_into_differs_from_left_raises(self) -> None:
        op = ComposeManifestsOp(
            vertices=[VertexEquivalence(left="Company", right="Org", into="Party")]
        )
        with pytest.raises(ComposeCanonicalConflictError, match="cluster conflict"):
            self._validate(op)

    def test_stale_property_name_raises(self) -> None:
        op = ComposeManifestsOp(
            vertices=[
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
        with pytest.raises(ComposeCanonicalConflictError, match="stale property name"):
            self._validate(op)

    def test_property_retarget_raises(self) -> None:
        op = ComposeManifestsOp(
            vertices=[
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

    def _right_collapse_op(self) -> ComposeManifestsOp:
        # Two right classes onto one canonical left class = implicit merge.
        return ComposeManifestsOp(
            vertices=[
                VertexEquivalence(left="Company", right="Org", into="Company"),
                VertexEquivalence(left="Company", right="Branch", into="Company"),
            ]
        )

    def test_implicit_merge_requires_ack(self) -> None:
        with pytest.raises(ComposeCanonicalConflictError, match="implicit merge"):
            validate_compose_against_canonical_map(
                _CANONICAL,
                self._right_collapse_op(),
                left=self._canonical_a(),
                right=self._right_two_orgs(),
            )

    def test_implicit_merge_ack_accepted_and_warns_on_self_relation(
        self, caplog
    ) -> None:
        # The right edge Org -> Branch lands on Company at both ends after the
        # collapse; the validator accepts the acknowledged merge but warns.
        with caplog.at_level(logging.WARNING):
            validate_compose_against_canonical_map(
                _CANONICAL,
                self._right_collapse_op(),
                left=self._canonical_a(),
                right=self._right_two_orgs(),
                allow_implicit_merge=True,
            )
        assert any("self-relation" in r.getMessage() for r in caplog.records)

    def test_left_collapse_completes_with_ack(self) -> None:
        # Two left classes in one cluster: Company (canonical target) and Deal
        # (unmapped). With allow_implicit_merge, Deal is completed onto Company.
        op = ComposeManifestsOp(
            vertices=[
                VertexEquivalence(left="Company", right="Org", into="Company"),
                VertexEquivalence(left="Deal", right="Branch", into="Company"),
            ]
        )
        completed = validate_and_complete_canonical_map(
            op,
            left=self._canonical_a(),
            right=self._right_two_orgs(),
            canonical_maps=[("left", _CANONICAL)],
            allow_implicit_merge=True,
        )
        assert completed.vertices["Deal"] == "Company"
        assert completed.vertices["Org"] == "Company"
        assert completed.vertices["Branch"] == "Company"

    def test_left_collapse_without_ack_still_raises(self) -> None:
        op = ComposeManifestsOp(
            vertices=[
                VertexEquivalence(left="Company", right="Org", into="Company"),
                VertexEquivalence(left="Deal", right="Branch", into="Company"),
            ]
        )
        with pytest.raises(ComposeCanonicalConflictError, match="implicit merge"):
            validate_compose_against_canonical_map(
                _CANONICAL,
                op,
                left=self._canonical_a(),
                right=self._right_two_orgs(),
            )

    def test_completion_infers_right_peer(self) -> None:
        op = ComposeManifestsOp(
            vertices=[VertexEquivalence(left="Company", right="Org", into="Company")]
        )
        completed = validate_and_complete_canonical_map(
            op,
            left=self._canonical_a(),
            right=_right_b_manifest(),
            canonical_maps=[("left", _CANONICAL)],
        )
        assert completed.vertices["Firm"] == "Company"
        assert completed.vertices["Org"] == "Company"

    def test_right_side_canonical_map_and_completion(self) -> None:
        """A right-side map seeds labels; the left peer is completed."""
        right_cm = CanonicalMap(vertices={"Org": "Company"})
        # Right already speaks Company; left still has Firm — compose after
        # renaming right, with an equivalence Firm ≡ Company into Company.
        right = apply_evolution(_right_b_manifest(), canonical_map_to_ops(right_cm))
        left = _source_a_manifest()
        op = ComposeManifestsOp(
            vertices=[VertexEquivalence(left="Firm", right="Company", into="Company")]
        )
        completed = validate_and_complete_canonical_map(
            op,
            left=left,
            right=right,
            canonical_maps=[("right", right_cm)],
        )
        assert completed.vertices["Org"] == "Company"
        assert completed.vertices["Firm"] == "Company"


class TestTheTwoChecksDoNotOverlap:
    """`CanonicalMap` validation and compose's own check are disjoint.

    The validator compares an op against a *declared* map — stale names, a
    wrong `into`, retired properties. Compose's check fires on the residue no
    equivalence covers. A name that trips one cannot trip the other, and this
    pins that they never double-report the same pair.
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

    def test_a_stale_declared_name_keys_differently_so_only_the_validator_sees_it(
        self,
    ) -> None:
        """`Firm` and `Company` do not key alike, so compose's check is silent.

        That is what keeps the two checks from ever reporting the same problem:
        one is about vocabulary the author declared, the other about spellings
        nobody reconciled.
        """
        from graflo.architecture.schema.naming import canonical_slug

        assert canonical_slug("Firm") != canonical_slug("Company")
