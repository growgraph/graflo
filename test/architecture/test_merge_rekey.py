"""Re-keying a merged class: how a cluster's identity is replaced, and what survives.

Two classes aligned on a property that is neither side's key -- ``X`` on
``name``, ``Y`` on ``cname`` -- merged into ``Z``. Each test states one rule of
how the merged identity is chosen, which pre-merge keys stay addressable as
secondary identities, and what happens to resources that only reference the
merged class.
"""

from __future__ import annotations

import asyncio

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import (
    AlignmentAttribute,
    AlignmentConflictError,
    CanonicalMap,
    DerivationSpec,
    IdentityAlignment,
    IdentityReplacement,
    LocalKeySource,
    LocalKeySpec,
    MergeIdentityError,
    MergeManifestsOp,
    NaturalIdentityTarget,
    PropertyEquivalence,
    ReplaceIdentityOp,
    VertexEquivalence,
    apply_evolution,
    merge_manifests,
)
from graflo.architecture.evolution.rewrite import collect_endpoint_selectors
from graflo.architecture.schema.identity_funnel import IdentityBranch, IdentityFunnel
from graflo.hq.document_caster import DocumentCaster
from graflo.hq.ingestion_parameters import IngestionParams

NAME_EQUIVALENCE = PropertyEquivalence(left="name", right="cname", into="name")


def _side_a(*, reference_step: dict | None = None) -> GraphManifest:
    """X keyed by ``x_id``; ``Ap`` relates to X through a reference-only resource."""
    x_step = (
        reference_step
        if reference_step is not None
        else {
            "vertex": "X",
            "lookup_only": True,
        }
    )
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "a", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "X",
                                "properties": ["x_id", "name"],
                                "identity": ["x_id"],
                            },
                            {
                                "name": "Ap",
                                "properties": ["ap_id"],
                                "identity": ["ap_id"],
                            },
                        ]
                    },
                    "edge_config": {
                        "edges": [{"source": "Ap", "target": "X", "relation": "owns"}]
                    },
                },
            },
            "ingestion_model": {
                "resources": [
                    {"name": "r_x", "pipeline": [{"vertex": "X"}]},
                    {
                        "name": "r_ap",
                        "pipeline": [
                            {"vertex": "Ap"},
                            x_step,
                            {"source": "Ap", "target": "X", "relation": "owns"},
                        ],
                    },
                ]
            },
        }
    )
    manifest.finish_init()
    return manifest


def _side_b(*, properties: list[str] | None = None) -> GraphManifest:
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "b", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "Y",
                                "properties": properties or ["y_id", "cname"],
                                "identity": ["y_id"],
                            }
                        ]
                    },
                    "edge_config": {"edges": []},
                },
            },
            "ingestion_model": {
                "resources": [{"name": "r_y", "pipeline": [{"vertex": "Y"}]}]
            },
        }
    )
    manifest.finish_init()
    return manifest


def _alignment(name: str = "name_key", **overrides) -> IdentityAlignment:
    base: dict = {
        "vertex": "Z",
        "attributes": [
            AlignmentAttribute(
                name=name,
                sources={
                    "r_x": DerivationSpec(input=["name"], foo="normalized_key"),
                    "r_y": DerivationSpec(input=["cname"], foo="normalized_key"),
                },
            )
        ],
        "local_key": LocalKeySpec(
            sources={
                "r_x": LocalKeySource(field="x_id", tag="a"),
                "r_y": LocalKeySource(field="y_id", tag="b"),
            }
        ),
    }
    base.update(overrides)
    return IdentityAlignment.model_validate(base)


def _merge(
    *equivalences: VertexEquivalence,
    alignments: list[IdentityAlignment] | None = None,
    left: GraphManifest | None = None,
    right: GraphManifest | None = None,
) -> GraphManifest:
    return merge_manifests(
        left if left is not None else _side_a(),
        right if right is not None else _side_b(),
        MergeManifestsOp(
            vertex_equivalences=list(equivalences),
            identity_alignments=alignments or [],
            canonical_maps={"left": CanonicalMap(vertices={"Ap": "Zp"})},
            allow_self_relations=True,
        ),
        bump_version=False,
    )


def _z(manifest: GraphManifest):
    assert manifest.graph_schema is not None
    return manifest.graph_schema.core_schema.vertex_config["Z"]


def _secondaries(manifest: GraphManifest) -> dict[str, list[str]]:
    return {s.name: list(s.fields) for s in _z(manifest).secondary_identities}


def _pipeline(manifest: GraphManifest, resource: str) -> list:
    assert manifest.ingestion_model is not None
    return next(
        r.pipeline for r in manifest.ingestion_model.resources if r.name == resource
    )


class TestFlaggedIdentity:
    def test_flag_over_disagreeing_keys_replaces_them(self) -> None:
        """The flag names the one field-set every member carries, so it is the key.

        Appending it to ``[x_id, y_id]`` would build a key no record carries.
        """
        merged = _merge(
            VertexEquivalence(
                left="X",
                right="Y",
                into="Z",
                properties=[
                    PropertyEquivalence(
                        left="name", right="cname", into="name", identity=True
                    )
                ],
            )
        )
        assert _z(merged).identity == ["name"]
        assert _secondaries(merged) == {"by_x_id": ["x_id"], "by_y_id": ["y_id"]}

    def test_flag_over_disagreeing_keys_honours_retire_keep(self) -> None:
        merged = _merge(
            VertexEquivalence(
                left="X",
                right="Y",
                into="Z",
                retire="keep",
                properties=[
                    PropertyEquivalence(
                        left="name", right="cname", into="name", identity=True
                    )
                ],
            )
        )
        assert _z(merged).identity == ["name"]
        assert _secondaries(merged) == {}

    def test_flag_must_be_carried_by_every_member(self) -> None:
        with pytest.raises(MergeIdentityError, match="coverage|carr"):
            _merge(
                VertexEquivalence(
                    left="X",
                    right="Y",
                    into="Z",
                    properties=[
                        PropertyEquivalence(left="name", into="name", identity=True)
                    ],
                )
            )


class TestDeclaredIdentityCoverage:
    def test_natural_key_a_member_does_not_declare_is_refused(self) -> None:
        """``Y`` has no ``name``: every ``Y`` record would complete no key."""
        with pytest.raises(MergeIdentityError, match="right:Y") as excinfo:
            _merge(VertexEquivalence(left="X", right="Y", into="Z", identity=["name"]))
        assert excinfo.value.check == "identity coverage"

    def test_natural_key_every_member_carries_is_accepted(self) -> None:
        merged = _merge(
            VertexEquivalence(
                left="X",
                right="Y",
                into="Z",
                identity=["name"],
                properties=[NAME_EQUIVALENCE],
            )
        )
        assert _z(merged).identity == ["name"]
        assert _secondaries(merged) == {"by_x_id": ["x_id"], "by_y_id": ["y_id"]}

    def test_funnel_a_member_completes_no_branch_of_is_refused(self) -> None:
        funnel = IdentityFunnel(branches=[IdentityBranch(id="name", fields=["name"])])
        with pytest.raises(MergeIdentityError, match="right:Y"):
            _merge(VertexEquivalence(left="X", right="Y", into="Z", identity=funnel))

    def test_funnel_every_member_completes_a_branch_of_is_accepted(self) -> None:
        funnel = IdentityFunnel(
            branches=[
                IdentityBranch(id="name", fields=["name"]),
                IdentityBranch(id="y_id", fields=["y_id"]),
            ]
        )
        merged = _merge(
            VertexEquivalence(left="X", right="Y", into="Z", identity=funnel)
        )
        assert _z(merged).identity_funnel is not None


class TestAlignmentDemotesMemberKeys:
    def test_member_keys_become_secondaries_without_being_listed(self) -> None:
        merged = _merge(
            VertexEquivalence(left="X", right="Y", into="Z"),
            alignments=[_alignment()],
        )
        assert _z(merged).identity == ["id"]
        assert _secondaries(merged) == {"by_x_id": ["x_id"], "by_y_id": ["y_id"]}

    def test_a_listed_secondary_keeps_its_name(self) -> None:
        merged = _merge(
            VertexEquivalence(left="X", right="Y", into="Z"),
            alignments=[_alignment(secondary_identities={"x_key": ["x_id"]})],
        )
        assert _secondaries(merged) == {"x_key": ["x_id"], "by_y_id": ["y_id"]}

    def test_retire_keep_opts_out(self) -> None:
        merged = _merge(
            VertexEquivalence(left="X", right="Y", into="Z", retire="keep"),
            alignments=[_alignment()],
        )
        assert _secondaries(merged) == {}

    def test_declared_key_the_alignment_replaces_is_still_demoted(self) -> None:
        """Demotion is decided against the final primary, not the intermediate one.

        ``[x_id]`` equals X's own key, so demoting it at the declaration would
        restate the primary; the alignment then replaces that primary, and
        ``x_id`` must still be addressable.
        """
        merged = _merge(
            VertexEquivalence(left="X", right="Y", into="Z", identity=["x_id"]),
            alignments=[_alignment()],
            right=_side_b(properties=["y_id", "cname", "x_id"]),
        )
        assert _secondaries(merged) == {"by_x_id": ["x_id"], "by_y_id": ["y_id"]}

    def test_raw_column_named_like_the_other_sides_rename_target_is_accepted(
        self,
    ) -> None:
        """``r_x`` reads its own raw ``name``; B renaming ``cname`` onto it is B's affair."""
        merged = _merge(
            VertexEquivalence(
                left="X", right="Y", into="Z", properties=[NAME_EQUIVALENCE]
            ),
            alignments=[
                _alignment(
                    attributes=[
                        AlignmentAttribute(
                            name="name_key",
                            sources={
                                "r_x": DerivationSpec(
                                    input=["name"], foo="normalized_key"
                                ),
                                "r_y": DerivationSpec(
                                    input=["cname"], foo="normalized_key"
                                ),
                            },
                        )
                    ]
                )
            ],
        )
        assert _z(merged).identity == ["id"]

    def test_two_alignments_for_one_class_are_refused(self) -> None:
        with pytest.raises(ValueError, match="more than one identity alignment"):
            MergeManifestsOp(
                vertex_equivalences=[VertexEquivalence(left="X", right="Y", into="Z")],
                identity_alignments=[_alignment("k1"), _alignment("k2")],
            )


class TestResourcesOutsideTheAlignment:
    def test_an_upserting_producer_the_alignment_misses_is_refused(self) -> None:
        """``r_ap`` upserts ``X`` records that would derive no funnel attribute."""
        with pytest.raises(AlignmentConflictError, match="r_ap"):
            _merge(
                VertexEquivalence(left="X", right="Y", into="Z"),
                alignments=[_alignment()],
                left=_side_a(reference_step={"vertex": "X"}),
            )

    def test_a_reference_is_pinned_to_the_demoted_member_key(self) -> None:
        merged = _merge(
            VertexEquivalence(left="X", right="Y", into="Z"),
            alignments=[_alignment()],
        )
        assert collect_endpoint_selectors(_pipeline(merged, "r_ap")) == [
            ("Z", "by_x_id")
        ]

    def test_a_pinned_reference_still_emits_its_edge(self) -> None:
        merged = _merge(
            VertexEquivalence(left="X", right="Y", into="Z"),
            alignments=[_alignment()],
        )
        caster = DocumentCaster(merged.require_ingestion_model())
        result = asyncio.run(
            caster.cast_batch(
                [{"ap_id": "p1", "x_id": "x1"}], "r_ap", params=IngestionParams()
            )
        )
        assert sum(len(edges) for edges in result.graph.edges.values()) == 1

    def test_a_declared_identity_change_pins_references_too(self) -> None:
        merged = _merge(
            VertexEquivalence(
                left="X",
                right="Y",
                into="Z",
                identity=["name"],
                properties=[NAME_EQUIVALENCE],
            )
        )
        assert collect_endpoint_selectors(_pipeline(merged, "r_ap")) == [
            ("Z", "by_x_id")
        ]


class TestAlignmentDerivations:
    def test_a_derivation_that_cannot_bind_its_inputs_is_refused(self) -> None:
        """``gated_normalized_key`` takes a gate and a value; one input binds neither."""
        with pytest.raises(AlignmentConflictError, match="derivation signature"):
            _merge(
                VertexEquivalence(left="X", right="Y", into="Z"),
                alignments=[
                    _alignment(
                        attributes=[
                            AlignmentAttribute(
                                name="name_key",
                                sources={
                                    "r_x": DerivationSpec(input=["name"]),
                                    "r_y": DerivationSpec(input=["cname"]),
                                },
                            )
                        ]
                    )
                ],
            )

    def test_aligned_records_fuse_on_the_normalized_name(self) -> None:
        merged = _merge(
            VertexEquivalence(left="X", right="Y", into="Z"),
            alignments=[_alignment()],
        )
        caster = DocumentCaster(merged.require_ingestion_model())
        ids = []
        for resource, row in (
            ("r_x", {"x_id": "x1", "name": "Acme"}),
            ("r_y", {"y_id": "y1", "cname": " ACME "}),
        ):
            result = asyncio.run(
                caster.cast_batch([row], resource, params=IngestionParams())
            )
            ids.extend(doc["id"] for doc in result.graph.vertices["Z"])
        assert len(ids) == 2
        assert ids[0] == ids[1]


def test_pin_to_retired_rewrites_flat_edge_steps() -> None:
    """A flat ``{source, target}`` edge step is an edge step too."""
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "a", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "X",
                                "properties": ["x_id", "name"],
                                "identity": ["x_id"],
                            },
                            {"name": "P", "properties": ["p_id"], "identity": ["p_id"]},
                        ]
                    },
                    "edge_config": {"edges": [{"source": "P", "target": "X"}]},
                },
            },
            "ingestion_model": {
                "resources": [
                    {
                        "name": "flat",
                        "pipeline": [
                            {"vertex": "P"},
                            {"vertex": "X", "lookup_only": True},
                            {"source": "P", "target": "X"},
                        ],
                    }
                ]
            },
        }
    )
    manifest.finish_init()
    out = apply_evolution(
        manifest,
        [
            ReplaceIdentityOp(
                replacements={
                    "X": IdentityReplacement(
                        to=NaturalIdentityTarget(identity=["name"]),
                        retire_as="by_x_id",
                        endpoints="pin_to_retired",
                    )
                }
            )
        ],
    )
    assert out.ingestion_model is not None
    assert collect_endpoint_selectors(out.ingestion_model.resources[0].pipeline) == [
        ("X", "by_x_id")
    ]
