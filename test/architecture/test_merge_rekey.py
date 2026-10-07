"""Re-keying a merged class: how a cluster's identity is replaced, and what survives.

Two classes aligned on a property that is neither side's key -- ``X`` on
``name``, ``Y`` on ``cname`` -- merged into ``Z``. Each test states one rule of
how the merged identity is chosen, which pre-merge keys stay addressable as
secondary identities, and what happens to resources that only reference the
merged class.
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import (
    AlignmentConflictError,
    CanonicalMap,
    DerivationSpec,
    DerivedBranch,
    IdentityBranchDecl,
    IdentityReplacement,
    LocalKeyBranch,
    LocalKeySource,
    MergeIdentityError,
    MergeManifestsOp,
    NaturalIdentityTarget,
    PropertyEquivalence,
    ReplaceIdentityOp,
    VertexEquivalence,
    apply_evolution,
    merge_manifests,
)
from graflo.architecture.evolution.preview import preview_merge
from graflo.architecture.evolution.rewrite import collect_endpoint_selectors
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


def _name_key(name: str = "name_key", **sources: DerivationSpec) -> DerivedBranch:
    """A derived branch each side computes from its own raw name column."""
    return DerivedBranch(
        name=name,
        sources=sources
        or {
            "r_x": DerivationSpec(input=["name"]),
            "r_y": DerivationSpec(input=["cname"]),
        },
    )


def _local_key() -> LocalKeyBranch:
    return LocalKeyBranch(
        local_key={
            "r_x": LocalKeySource(field="x_id", tag="a"),
            "r_y": LocalKeySource(field="y_id", tag="b"),
        }
    )


def _derived_identity(name: str = "name_key") -> list[IdentityBranchDecl]:
    """``Z`` keyed on a normalized name, falling back to each side's tagged key."""
    return [_name_key(name), _local_key()]


def _merge(
    *equivalences: VertexEquivalence,
    left: GraphManifest | None = None,
    right: GraphManifest | None = None,
) -> GraphManifest:
    return merge_manifests(
        left if left is not None else _side_a(),
        right if right is not None else _side_b(),
        MergeManifestsOp(
            vertex_equivalences=[
                e.model_copy(update={"allow": ["self_relations"]}) for e in equivalences
            ],
            canonical_maps={"left": CanonicalMap(vertices={"Ap": "Zp"})},
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


class TestDeclaredIdentityCoverage:
    def test_natural_key_a_member_does_not_declare_is_refused(self) -> None:
        """``Y`` has no ``name``: every ``Y`` record would complete no key."""
        with pytest.raises(MergeIdentityError, match="right:Y") as excinfo:
            _merge(VertexEquivalence(left="X", right="Y", into="Z", identity=["name"]))
        assert excinfo.value.check == "identity coverage"

    def test_natural_key_every_member_carries_is_accepted(self) -> None:
        """The one field-set every member carries replaces their disagreeing keys.

        Appending it to ``[x_id, y_id]`` would build a key no record carries.
        """
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
        assert _secondaries(merged) == {"a": ["a__x_id"], "b": ["b__y_id"]}

    def test_natural_key_honours_retire_keep(self) -> None:
        merged = _merge(
            VertexEquivalence(
                left="X",
                right="Y",
                into="Z",
                retire="keep",
                identity=["name"],
                properties=[NAME_EQUIVALENCE],
            )
        )
        assert _z(merged).identity == ["name"]
        assert _secondaries(merged) == {}

    def test_a_single_property_branch_is_a_natural_key_not_a_funnel(self) -> None:
        merged = _merge(
            VertexEquivalence(
                left="X",
                right="Y",
                into="Z",
                identity=["name"],
                properties=[NAME_EQUIVALENCE],
            )
        )
        assert _z(merged).identity_funnel is None
        assert _z(merged).identity == ["name"]

    def test_funnel_a_member_completes_no_branch_of_is_refused(self) -> None:
        """``Y`` carries neither ``name`` (it has ``cname``) nor ``x_id``."""
        with pytest.raises(MergeIdentityError, match="right:Y") as excinfo:
            _merge(
                VertexEquivalence(
                    left="X", right="Y", into="Z", identity=["name", "x_id"]
                )
            )
        assert excinfo.value.check == "identity coverage"

    def test_funnel_every_member_completes_a_branch_of_is_accepted(self) -> None:
        merged = _merge(
            VertexEquivalence(left="X", right="Y", into="Z", identity=["name", "y_id"])
        )
        funnel = _z(merged).identity_funnel
        assert funnel is not None
        assert [b.id for b in funnel.branches] == ["name", "y_id"]

    def test_a_funnel_over_a_member_declaring_id_is_refused(self) -> None:
        """A funnel's synthetic key is ``id``; a real ``id`` column would be lost."""
        with pytest.raises(MergeIdentityError, match="right:Y") as excinfo:
            _merge(
                VertexEquivalence(
                    left="X", right="Y", into="Z", identity=["x_id", "y_id"]
                ),
                right=_side_b(properties=["y_id", "cname", "id"]),
            )
        assert excinfo.value.check == "identity collision"


class TestDerivedIdentityDemotesMemberKeys:
    def test_member_keys_become_secondaries_without_being_listed(self) -> None:
        merged = _merge(
            VertexEquivalence(
                left="X", right="Y", into="Z", identity=_derived_identity()
            )
        )
        assert _z(merged).identity == ["id"]
        assert _secondaries(merged) == {"a": ["a__x_id"], "b": ["b__y_id"]}

    def test_retire_keep_opts_out(self) -> None:
        merged = _merge(
            VertexEquivalence(
                left="X",
                right="Y",
                into="Z",
                retire="keep",
                identity=_derived_identity(),
            )
        )
        assert _secondaries(merged) == {}

    def test_target_named_like_a_members_own_key_is_a_collision(self) -> None:
        """Deriving ``x_id`` would overwrite the key ``X`` records carry."""
        with pytest.raises(AlignmentConflictError, match="identity collision"):
            _merge(
                VertexEquivalence(
                    left="X",
                    right="Y",
                    into="Z",
                    identity=_derived_identity(name="x_id"),
                )
            )

    def test_a_property_branch_may_name_a_members_own_key(self) -> None:
        """Keying on a member's own key is a property branch, not a collision."""
        merged = _merge(
            VertexEquivalence(
                left="X",
                right="Y",
                into="Z",
                identity=[_name_key("match_key"), "x_id", "y_id"],
            )
        )
        funnel = _z(merged).identity_funnel
        assert funnel is not None
        assert [b.id for b in funnel.branches] == ["match_key", "x_id", "y_id"]

    def test_raw_column_named_like_the_other_sides_rename_target_is_accepted(
        self,
    ) -> None:
        """``r_x`` reads its own raw ``name``; B renaming ``cname`` onto it is B's affair."""
        merged = _merge(
            VertexEquivalence(
                left="X",
                right="Y",
                into="Z",
                properties=[NAME_EQUIVALENCE],
                identity=_derived_identity(),
            )
        )
        assert _z(merged).identity == ["id"]


class TestDigestField:
    """Where a funnel stores its digest: ``id`` by default, or ``digest_field``."""

    def test_a_member_keeps_id_when_the_digest_is_stored_elsewhere(self) -> None:
        merged = _merge(
            VertexEquivalence(
                left="X",
                right="Y",
                into="Z",
                identity=["x_id", "y_id"],
                digest_field="cid",
            ),
            right=_side_b(properties=["y_id", "cname", "id"]),
        )
        assert _z(merged).identity == ["cid"]
        assert _z(merged).identity_funnel is not None
        assert "id" in _z(merged).property_names

    def test_a_derived_identity_stores_its_digest_in_digest_field(self) -> None:
        """A derived branch names the digest's input; ``id`` stays a real column."""
        merged = _merge(
            VertexEquivalence(
                left="X",
                right="Y",
                into="Z",
                identity=_derived_identity(name="new_id"),
                digest_field="cid",
            ),
            right=_side_b(properties=["y_id", "cname", "id"]),
        )
        assert _z(merged).identity == ["cid"]
        caster = DocumentCaster(merged.require_ingestion_model())
        result = asyncio.run(
            caster.cast_batch(
                [{"y_id": "y1", "cname": "Acme", "id": "own-1"}],
                "r_y",
                params=IngestionParams(),
            )
        )
        [doc] = result.graph.vertices["Z"]
        assert doc["id"] == "own-1"
        assert doc["cid"] and doc["cid"] != "own-1"

    def test_a_digest_field_a_member_declares_is_refused(self) -> None:
        with pytest.raises(MergeIdentityError, match="`cname`") as excinfo:
            _merge(
                VertexEquivalence(
                    left="X",
                    right="Y",
                    into="Z",
                    identity=["x_id", "y_id"],
                    digest_field="cname",
                )
            )
        assert excinfo.value.check == "identity collision"

    @pytest.mark.parametrize(
        "identity",
        [[_name_key("cid"), "x_id"], ["cid", "x_id"], [["cid", "name"], "x_id"]],
        ids=["derived", "property", "composite"],
    )
    def test_a_digest_field_named_like_a_branch_is_refused(
        self, identity: list[IdentityBranchDecl]
    ) -> None:
        """The cast drops the digest field from records: that branch could not fire."""
        with pytest.raises(ValueError, match="not where it is stored"):
            VertexEquivalence(
                left="X", right="Y", identity=identity, digest_field="cid"
            )

    @pytest.mark.parametrize(
        "identity", [None, ["name"], [["name", "x_id"]]], ids=["none", "one", "pair"]
    )
    def test_digest_field_without_a_funnel_is_refused(
        self, identity: list[IdentityBranchDecl] | None
    ) -> None:
        with pytest.raises(ValueError, match="digest_field 'cid'"):
            VertexEquivalence(
                left="X", right="Y", identity=identity, digest_field="cid"
            )

    def test_the_default_digest_field_is_id(self) -> None:
        merged = _merge(
            VertexEquivalence(left="X", right="Y", into="Z", identity=["x_id", "y_id"])
        )
        assert _z(merged).identity == ["id"]

    @pytest.mark.parametrize(
        ("digest_field", "colliding"),
        [("id", True), ("cid", False)],
        ids=["default", "elsewhere"],
    )
    def test_the_preview_tests_the_declared_digest_field(
        self, digest_field: str, colliding: bool
    ) -> None:
        preview = preview_merge(
            _side_a(),
            _side_b(properties=["y_id", "cname", "id"]),
            MergeManifestsOp(
                vertex_equivalences=[
                    VertexEquivalence(
                        left="X",
                        right="Y",
                        into="Z",
                        identity=["x_id", "y_id"],
                        digest_field=digest_field,
                        allow=["self_relations"],
                    )
                ],
                canonical_maps={"left": CanonicalMap(vertices={"Ap": "Zp"})},
            ),
        )
        kinds = {f.kind for f in preview.findings}
        assert ("identity_collision" in kinds) is colliding


def _owns_edge(pipeline: list) -> dict:
    return next(s for s in pipeline if s.get("relation") == "owns")


def _cast_graph(merged: GraphManifest, resource: str, rows: list[dict]):
    caster = DocumentCaster(merged.require_ingestion_model())
    return asyncio.run(
        caster.cast_batch(rows, resource, params=IngestionParams())
    ).graph


class TestResourcesOutsideTheDerivedBranches:
    def test_an_upserting_producer_no_derived_branch_names_is_attached(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """``r_ap`` carries ``X``'s own key and nothing of the new one.

        It finds ``Z`` by that key -- the secondary named by its origin -- and
        writes onto the node it finds; its edge follows without a selector.
        """
        with caplog.at_level(logging.INFO):
            merged = _merge(
                VertexEquivalence(
                    left="X", right="Y", into="Z", identity=_derived_identity()
                ),
                left=_side_a(reference_step={"vertex": "X"}),
            )
        pipeline = _pipeline(merged, "r_ap")
        z_step = next(s for s in pipeline if s.get("vertex") == "Z")
        assert z_step["find"] == "a"
        assert not z_step.get("lookup_only")
        assert collect_endpoint_selectors([_owns_edge(pipeline)]) == []
        assert "r_ap" in caplog.text and "'a'" in caplog.text

    def test_a_resource_completing_a_property_branch_is_not_attached(self) -> None:
        """``r_ap`` derives no ``match_key``, but its ``X`` records carry ``x_id``.

        They key on the ``x_id`` branch, so the resource keeps upserting ``Z``.
        """
        merged = _merge(
            VertexEquivalence(
                left="X",
                right="Y",
                into="Z",
                identity=[_name_key("match_key"), "x_id", "y_id"],
            ),
            left=_side_a(reference_step={"vertex": "X"}),
        )
        z_step = next(s for s in _pipeline(merged, "r_ap") if s.get("vertex") == "Z")
        assert not z_step.get("lookup_only")
        assert not z_step.get("find")
        graph = _cast_graph(merged, "r_ap", [{"ap_id": "p1", "x_id": "x1"}])
        assert [doc["x_id"] for doc in graph.vertices["Z"]] == ["x1"]

    def test_an_attached_record_and_its_edge_use_the_prefixed_key(self) -> None:
        merged = _merge(
            VertexEquivalence(
                left="X", right="Y", into="Z", identity=_derived_identity()
            ),
            left=_side_a(reference_step={"vertex": "X"}),
        )
        graph = _cast_graph(merged, "r_ap", [{"ap_id": "p1", "x_id": "x1"}])
        assert not graph.vertices.get("Z")
        assert graph.attached["Z"] == [{"a__x_id": "x1"}]
        targets = [
            dict(target) for docs in graph.edges.values() for _s, target, *_ in docs
        ]
        assert targets == [{"a__x_id": "x1"}]

    def test_a_resource_that_finds_the_class_creates_none_of_it(self) -> None:
        """Re-reading the union: an attached resource is not an uncovered producer."""
        from graflo.architecture.evolution.alignment import (
            IdentityPlan,
            uncovered_producers,
        )

        merged = _merge(
            VertexEquivalence(
                left="X", right="Y", into="Z", identity=_derived_identity()
            ),
            left=_side_a(reference_step={"vertex": "X"}),
        )
        plan = IdentityPlan(vertex="Z", branches=tuple(_derived_identity()))
        assert uncovered_producers(plan, merged) == []

    def test_retire_keep_leaves_an_uncovered_producer_nothing_to_attach_by(
        self,
    ) -> None:
        with pytest.raises(AlignmentConflictError, match="retire: keep"):
            _merge(
                VertexEquivalence(
                    left="X",
                    right="Y",
                    into="Z",
                    retire="keep",
                    identity=_derived_identity(),
                ),
                left=_side_a(reference_step={"vertex": "X"}),
            )

    def test_members_demoted_to_different_secondaries_are_not_attached(
        self,
    ) -> None:
        """``RA`` routes ``X`` (keyed ``x_id``) and ``C`` (keyed ``c_id``).

        Their keys are two secondaries of ``Z``; one ``find`` cannot serve both.
        """
        with pytest.raises(MergeIdentityError, match="RA") as refused:
            merge_manifests(
                _side_a_two_keyed_members(),
                _side_b(),
                MergeManifestsOp(
                    vertex_equivalences=[
                        VertexEquivalence(
                            left=["X", "C"],
                            right="Y",
                            into="Z",
                            identity=[
                                _name_key(
                                    r_x=DerivationSpec(input=["name"]),
                                    r_c=DerivationSpec(input=["name"]),
                                    r_y=DerivationSpec(input=["cname"]),
                                ),
                                LocalKeyBranch(
                                    local_key={
                                        "r_x": LocalKeySource(field="x_id"),
                                        "r_c": LocalKeySource(field="c_id"),
                                        "r_y": LocalKeySource(field="y_id"),
                                    }
                                ),
                            ],
                            allow=["self_relations"],
                        )
                    ]
                ),
                bump_version=False,
            )
        assert refused.value.check == "ambiguous reference"
        assert "find" in str(refused.value)

    def test_a_reference_is_pinned_to_the_demoted_member_key(self) -> None:
        merged = _merge(
            VertexEquivalence(
                left="X", right="Y", into="Z", identity=_derived_identity()
            )
        )
        assert collect_endpoint_selectors(_pipeline(merged, "r_ap")) == [("Z", "a")]

    def test_a_pinned_reference_still_emits_its_edge(self) -> None:
        merged = _merge(
            VertexEquivalence(
                left="X", right="Y", into="Z", identity=_derived_identity()
            )
        )
        graph = _cast_graph(merged, "r_ap", [{"ap_id": "p1", "x_id": "x1"}])
        assert sum(len(edges) for edges in graph.edges.values()) == 1

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
        assert collect_endpoint_selectors(_pipeline(merged, "r_ap")) == [("Z", "a")]


def _side_a_two_keyed_members() -> GraphManifest:
    """``X`` keyed ``x_id`` and ``C`` keyed ``c_id``; ``RA`` routes rows to either."""
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
                                "name": "C",
                                "properties": ["c_id", "name"],
                                "identity": ["c_id"],
                            },
                        ]
                    },
                    "edge_config": {"edges": []},
                },
            },
            "ingestion_model": {
                "resources": [
                    {"name": "r_x", "pipeline": [{"vertex": "X"}]},
                    {"name": "r_c", "pipeline": [{"vertex": "C"}]},
                    {
                        "name": "RA",
                        "pipeline": [
                            {
                                "type": "vertex_router",
                                "type_field": "assetType",
                                "vertex_from_map": {
                                    "X": {"x_id": "blaId"},
                                    "C": {"c_id": "blaId"},
                                },
                            }
                        ],
                    },
                ]
            },
        }
    )
    manifest.finish_init()
    return manifest


# ── owners and attached resources: the shape merge attach is built for ──────


def _left_members(*, local_key: dict | None = None) -> GraphManifest:
    """``L1``, ``L2`` keyed ``lid``; only ``left_resource_A`` carries the match columns.

    ``left_resource_B`` is declared first and writes ``L1`` with a property.
    """
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "left", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "L1",
                                "properties": ["lid", "l1_note"],
                                "identity": ["lid"],
                            },
                            {
                                "name": "L2",
                                "properties": ["lid", "l2_note"],
                                "identity": ["lid"],
                            },
                            {
                                "name": "Site",
                                "properties": ["site_id"],
                                "identity": ["site_id"],
                            },
                        ]
                    },
                    "edge_config": {
                        "edges": [
                            {"source": "L1", "target": "Site", "relation": "located_in"}
                        ]
                    },
                },
            },
            "ingestion_model": {
                "resources": [
                    {
                        "name": "left_resource_B",
                        "pipeline": [
                            {"vertex": "L1"},
                            {"vertex": "Site"},
                            {
                                "source": "L1",
                                "target": "Site",
                                "relation": "located_in",
                            },
                        ],
                    },
                    {
                        "name": "left_resource_A",
                        "pipeline": [
                            {
                                "type": "vertex_router",
                                "type_field": "kind",
                                "vertex_types": ["L1", "L2"],
                            }
                        ],
                    },
                ]
            },
        }
    )
    manifest.finish_init()
    return manifest


def _right_member() -> GraphManifest:
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "right", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "R",
                                "properties": ["rid", "r_note"],
                                "identity": ["rid"],
                            }
                        ]
                    },
                    "edge_config": {"edges": []},
                },
            },
            "ingestion_model": {
                "resources": [
                    {"name": "right_resource_B", "pipeline": [{"vertex": "R"}]},
                    {"name": "right_resource_A", "pipeline": [{"vertex": "R"}]},
                ]
            },
        }
    )
    manifest.finish_init()
    return manifest


def _asset_op(
    local_key: dict[str, LocalKeySource | dict[str, LocalKeySource]] | None = None,
) -> MergeManifestsOp:
    """``[L1, L2] ~ R`` keyed on two match columns only the ``_A`` resources carry."""
    return MergeManifestsOp.model_validate(
        {
            "vertex_equivalences": [
                {
                    "left": ["L1", "L2"],
                    "right": "R",
                    "into": "Asset",
                    "digest_field": "asset_key",
                    "allow": ["observation_fusion"],
                    "derive": {
                        "phys_a": {
                            "left_resource_A": {"input": ["left_attr_a"]},
                            "right_resource_A": {"input": ["r_attr_a"]},
                        },
                        "phys_b": {
                            "left_resource_A": {
                                "L1": {
                                    "foo": "affix_gated_key",
                                    "input": ["left_attr_b"],
                                    "params": {"prefix": "l1_"},
                                },
                                "L2": {
                                    "foo": "affix_gated_key",
                                    "input": ["left_attr_b"],
                                    "params": {"prefix": "l2_"},
                                },
                            },
                            "right_resource_A": {"input": ["r_attr_b"]},
                        },
                    },
                    "identity": [
                        ["phys_a", "phys_b"],
                        {
                            "local_key": local_key
                            or {
                                "left_resource_A": {"field": "lid", "tag": "left"},
                                "right_resource_A": {"field": "rid", "tag": "right"},
                            }
                        },
                    ],
                }
            ]
        }
    )


def _asset_union(**kwargs):
    from graflo.architecture.evolution import merge_manifests_with_report

    return merge_manifests_with_report(
        _left_members(), _right_member(), _asset_op(**kwargs), bump_version=False
    )


class TestOwnersAndAttachedResources:
    def test_other_resources_attach_by_their_sides_origin(self) -> None:
        merged, report = _asset_union()
        left_b = _pipeline(merged, "left_resource_B")
        assert next(s for s in left_b if s.get("vertex") == "Asset")["find"] == "left"
        right_b = _pipeline(merged, "right_resource_B")
        assert next(s for s in right_b if s.get("vertex") == "Asset")["find"] == "right"
        assert (
            collect_endpoint_selectors(
                [s for s in left_b if s.get("relation") == "located_in"]
            )
            == []
        )
        assert {(a.resource, a.side, a.members, a.key) for a in report.attached} == {
            ("left_resource_B", "left", ("L1",), "left"),
            ("right_resource_B", "right", ("R",), "right"),
        }

    def test_key_owners_are_the_resources_the_identity_names(self) -> None:
        _merged, report = _asset_union()
        assert {(o.resource, o.vertex, o.side, o.members) for o in report.owners} == {
            ("left_resource_A", "Asset", "left", ("L1", "L2")),
            ("right_resource_A", "Asset", "right", ("R",)),
        }

    def test_owners_run_before_the_resources_attached_to_them(self) -> None:
        merged, report = _asset_union()
        order = [r.name for r in merged.require_ingestion_model().resources]
        assert order == [
            "left_resource_A",
            "right_resource_A",
            "left_resource_B",
            "right_resource_B",
        ]
        assert report.order_cycles == []

    def test_same_side_members_sharing_a_key_field_share_one_key_space(self) -> None:
        _merged, report = _asset_union()
        assert [
            (s.vertex, s.side, s.members, s.key) for s in report.shared_key_spaces
        ] == [("Asset", "left", ("L1", "L2"), "left")]

    def test_same_side_members_tagged_as_two_key_spaces_are_named_by_their_tags(
        self,
    ) -> None:
        merged, report = _asset_union(
            local_key={
                "left_resource_A": {
                    "L1": LocalKeySource(field="lid", tag="l1"),
                    "L2": LocalKeySource(field="lid", tag="l2"),
                },
                "right_resource_A": LocalKeySource(field="rid", tag="right"),
            }
        )
        asset = merged.require_schema().core_schema.vertex_config["Asset"]
        assert {s.name: list(s.fields) for s in asset.secondary_identities} == {
            "l1": ["l1__lid"],
            "l2": ["l2__lid"],
            "right": ["right__rid"],
        }
        assert report.shared_key_spaces == []
        left_b = _pipeline(merged, "left_resource_B")
        assert next(s for s in left_b if s.get("vertex") == "Asset")["find"] == "l1"

    def test_a_key_space_both_sides_name_is_refused(self) -> None:
        """A left tag naming the right side's origin would join two id spaces."""
        with pytest.raises(MergeIdentityError, match="different sides") as refused:
            _asset_union(
                local_key={
                    "left_resource_A": LocalKeySource(field="lid", tag="right"),
                    "right_resource_A": LocalKeySource(field="rid"),
                }
            )
        assert refused.value.check == "key space"

    def test_an_attached_record_is_cast_onto_its_origin_key(self) -> None:
        merged, _report = _asset_union()
        graph = _cast_graph(
            merged,
            "left_resource_B",
            [{"lid": "7", "l1_note": "n", "site_id": "s1"}],
        )
        assert graph.attached["Asset"] == [{"left__lid": "7", "l1_note": "n"}]
        targets = [dict(source) for docs in graph.edges.values() for source, *_ in docs]
        assert targets == [{"left__lid": "7"}]


def _cyclic_side() -> GraphManifest:
    """``r1`` writes ``P`` and references ``Q``; ``r2`` writes ``Q`` and references ``P``."""
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "cyclic", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {"name": "P", "properties": ["p_id"], "identity": ["p_id"]},
                            {"name": "Q", "properties": ["q_id"], "identity": ["q_id"]},
                        ]
                    },
                    "edge_config": {
                        "edges": [
                            {"source": "P", "target": "Q", "relation": "pq"},
                            {"source": "Q", "target": "P", "relation": "qp"},
                        ]
                    },
                },
            },
            "ingestion_model": {
                "resources": [
                    {
                        "name": "r1",
                        "pipeline": [
                            {"vertex": "P"},
                            {"vertex": "Q", "lookup_only": True},
                            {"source": "P", "target": "Q", "relation": "pq"},
                        ],
                    },
                    {
                        "name": "r2",
                        "pipeline": [
                            {"vertex": "Q"},
                            {"vertex": "P", "lookup_only": True},
                            {"source": "Q", "target": "P", "relation": "qp"},
                        ],
                    },
                ]
            },
        }
    )
    manifest.finish_init()
    return manifest


def test_writers_run_before_the_resources_that_only_reference_their_class() -> None:
    from graflo.architecture.evolution import merge_manifests_with_report

    left = _side_a()
    reordered = GraphManifest.from_config(
        {
            **left.to_dict(skip_defaults=True),
            "ingestion_model": {
                "resources": list(
                    reversed(
                        left.to_dict(skip_defaults=True)["ingestion_model"]["resources"]
                    )
                )
            },
        }
    )
    reordered.finish_init()
    merged, report = merge_manifests_with_report(
        reordered, _side_b(), MergeManifestsOp(), bump_version=False
    )
    assert [r.name for r in merged.require_ingestion_model().resources] == [
        "r_x",
        "r_ap",
        "r_y",
    ]
    assert report.order_cycles == []


def test_a_resource_order_cycle_keeps_the_declared_order() -> None:
    from graflo.architecture.evolution import merge_manifests_with_report

    merged, report = merge_manifests_with_report(
        _cyclic_side(), _side_b(), MergeManifestsOp(), bump_version=False
    )
    assert [r.name for r in merged.require_ingestion_model().resources] == [
        "r1",
        "r2",
        "r_y",
    ]
    assert [(c.resource, c.before, c.vertex) for c in report.order_cycles] == [
        ("r1", "r2", "Q")
    ]


def _side_a_routed() -> GraphManifest:
    """``X`` and ``C`` keyed on ``x_id``; ``RA`` routes work-order rows to either by type."""
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
                            {"name": "C", "properties": ["x_id"], "identity": ["x_id"]},
                            {
                                "name": "WorkOrder",
                                "properties": ["work_order_id"],
                                "identity": ["work_order_id"],
                            },
                        ]
                    },
                    "edge_config": {
                        "edges": [
                            {
                                "source": "WorkOrder",
                                "target": "X",
                                "relation": "targets",
                            },
                            {
                                "source": "WorkOrder",
                                "target": "C",
                                "relation": "targets",
                            },
                        ]
                    },
                },
            },
            "ingestion_model": {
                "resources": [
                    {"name": "r_x", "pipeline": [{"vertex": "X"}]},
                    {
                        "name": "RA",
                        "pipeline": [
                            {
                                "from": {"x_id": "blaId"},
                                "type": "vertex_router",
                                "type_field": "assetType",
                            },
                            {
                                "role": "work_order",
                                "type": "vertex",
                                "vertex": "WorkOrder",
                            },
                            {
                                "relation": "targets",
                                "source": "WorkOrder",
                                "target_role": "assetType",
                                "type": "edge",
                            },
                        ],
                    },
                ]
            },
        }
    )
    manifest.finish_init()
    return manifest


def _merge_routed(
    *, right: GraphManifest | None = None, router_scope: str = "side"
) -> GraphManifest:
    return merge_manifests(
        _side_a_routed(),
        right if right is not None else _side_b(),
        MergeManifestsOp.model_validate(
            {
                "vertex_equivalences": [
                    VertexEquivalence(
                        left="X",
                        right="Y",
                        into="Z",
                        identity=_derived_identity(),
                        allow=["self_relations"],
                    )
                ],
                "router_scope": router_scope,
            }
        ),
        bump_version=False,
    )


def _side_b_with_org() -> GraphManifest:
    """``Y``, plus ``Org``: a class only this side declares, keyed like A's machines."""
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "b", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "Y",
                                "properties": ["y_id", "cname"],
                                "identity": ["y_id"],
                            },
                            {
                                "name": "Org",
                                "properties": ["x_id"],
                                "identity": ["x_id"],
                            },
                        ]
                    },
                    "edge_config": {"edges": []},
                },
            },
            "ingestion_model": {
                "resources": [
                    {"name": "r_y", "pipeline": [{"vertex": "Y"}]},
                    {"name": "r_org", "pipeline": [{"vertex": "Org"}]},
                ]
            },
        }
    )
    manifest.finish_init()
    return manifest


def _cast_ra(merged: GraphManifest, rows: list[dict]):
    caster = DocumentCaster(merged.require_ingestion_model())
    return asyncio.run(caster.cast_batch(rows, "RA", params=IngestionParams())).graph


class TestRoutersKeepToTheirSide:
    """After the union, a value passed through could name the other side's classes."""

    ORG_ROW = [{"blaId": "o7", "assetType": "Org", "work_order_id": "w1"}]

    def test_a_router_is_closed_over_its_own_side(self) -> None:
        """Renamed classes keep their entries; the rest are listed as themselves."""
        router, _work_order, _edge = _pipeline(_merge_routed(), "RA")
        assert router["type_map"] == {"X": "Z", "C": "C", "WorkOrder": "WorkOrder"}
        assert router["type_map_only"] is True

    def test_a_value_only_the_other_side_declares_is_skipped(self) -> None:
        graph = _cast_ra(_merge_routed(right=_side_b_with_org()), self.ORG_ROW)
        assert not graph.vertices.get("Org")
        assert not [edge_id for edge_id, docs in graph.edges.items() if docs]

    def test_router_scope_union_keeps_the_router_open(self) -> None:
        merged = _merge_routed(right=_side_b_with_org(), router_scope="union")
        router, _work_order, _edge = _pipeline(merged, "RA")
        assert not router.get("type_map_only")
        graph = _cast_ra(merged, self.ORG_ROW)
        assert [dict(d) for d in graph.vertices["Org"]] == [{"x_id": "o7"}]


class TestRoutedReferences:
    """A router that references ``X`` among other classes it routes to."""

    def test_only_the_merged_class_is_attached(self) -> None:
        router, _work_order, edge = _pipeline(_merge_routed(), "RA")
        assert router["find"] == {"Z": "a"}
        assert not router.get("lookup_only")
        assert not edge.get("target_match")

    def test_the_merged_class_is_attached_and_the_rest_still_written(self) -> None:
        caster = DocumentCaster(_merge_routed().require_ingestion_model())
        rows = [
            {"blaId": "x1", "assetType": "X", "work_order_id": "w1"},
            {"blaId": "k9", "assetType": "C", "work_order_id": "w1"},
        ]
        graph = asyncio.run(
            caster.cast_batch(rows, "RA", params=IngestionParams())
        ).graph
        assert not graph.vertices.get("Z")
        assert graph.attached["Z"] == [{"a__x_id": "x1"}]
        assert [dict(d) for d in graph.vertices["C"]] == [{"x_id": "k9"}]
        targets = {
            edge_id[1]: [dict(t) for _s, t, *_ in docs]
            for edge_id, docs in graph.edges.items()
        }
        assert targets == {"Z": [{"a__x_id": "x1"}], "C": [{"x_id": "k9"}]}

    def test_members_sharing_a_demoted_key_are_one_reference(self) -> None:
        """A router reaches every member on its side; one key serves them all."""
        left = _side_a_routed()
        side = GraphManifest.from_config(
            {
                **left.to_dict(skip_defaults=True),
                "ingestion_model": {
                    "resources": [
                        {
                            "name": "RA",
                            "pipeline": [
                                {
                                    "from": {"x_id": "blaId"},
                                    "type": "vertex_router",
                                    "type_field": "assetType",
                                    "lookup_only": True,
                                },
                                {"role": "work_order", "vertex": "WorkOrder"},
                                {
                                    "relation": "targets",
                                    "source": "WorkOrder",
                                    "target_role": "assetType",
                                    "type": "edge",
                                },
                            ],
                        }
                    ]
                },
            }
        )
        side.finish_init()
        merged = merge_manifests(
            side,
            _side_b(properties=["y_id", "cname"]),
            MergeManifestsOp(
                vertex_equivalences=[
                    VertexEquivalence(
                        left=["X", "C"],
                        right="Y",
                        into="Z",
                        identity=["x_id", "y_id"],
                        allow=["self_relations"],
                    )
                ],
            ),
            bump_version=False,
        )
        _router, _work_order, edge = _pipeline(merged, "RA")
        assert edge["target_match"] == {"Z": "a"}


def _side_a_linked() -> GraphManifest:
    """``LT`` is a link table: a router per role, each reaching ``X`` and ``C``."""
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
                            {"name": "C", "properties": ["x_id"], "identity": ["x_id"]},
                        ]
                    },
                    "edge_config": {
                        "edges": [{"source": "X", "target": "C", "relation": "linked"}]
                    },
                },
            },
            "ingestion_model": {
                "resources": [
                    {"name": "r_x", "pipeline": [{"vertex": "X"}]},
                    {
                        "name": "LT",
                        "pipeline": [
                            {
                                "type": "vertex_router",
                                "role": "source",
                                "type_field": "source_type",
                                "from": {"x_id": "source_id"},
                            },
                            {
                                "type": "vertex_router",
                                "role": "target",
                                "type_field": "target_type",
                                "from": {"x_id": "target_id"},
                            },
                            {
                                "type": "edge",
                                "source_role": "source",
                                "target_role": "target",
                                "relation": "linked",
                            },
                        ],
                    },
                ]
            },
        }
    )
    manifest.finish_init()
    return manifest


def test_a_link_table_attaches_the_merged_class_in_both_roles() -> None:
    """One derivation cannot serve two roles; attaching needs none."""
    merged = merge_manifests(
        _side_a_linked(),
        _side_b(),
        MergeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(
                    left="X",
                    right="Y",
                    into="Z",
                    identity=_derived_identity(),
                    allow=["self_relations"],
                )
            ]
        ),
        bump_version=False,
    )
    source, target, edge = _pipeline(merged, "LT")
    assert source["find"] == {"Z": "a"}
    assert target["find"] == {"Z": "a"}
    assert not source.get("lookup_only") and not target.get("lookup_only")
    assert collect_endpoint_selectors([edge]) == []


class TestDerivedBranchDerivations:
    def test_a_derivation_that_cannot_bind_its_inputs_is_refused(self) -> None:
        """``normalized_key`` (the default) takes one value; two inputs cannot bind."""
        with pytest.raises(AlignmentConflictError, match="derivation signature"):
            _merge(
                VertexEquivalence(
                    left="X",
                    right="Y",
                    into="Z",
                    identity=[
                        _name_key(
                            r_x=DerivationSpec(input=["name", "x_id"]),
                            r_y=DerivationSpec(input=["cname"]),
                        ),
                        _local_key(),
                    ],
                )
            )

    def test_a_one_input_derivation_binds_the_default_function(self) -> None:
        merged = _merge(
            VertexEquivalence(
                left="X",
                right="Y",
                into="Z",
                identity=[
                    _name_key(
                        r_x=DerivationSpec(input=["name"]),
                        r_y=DerivationSpec(input=["cname"]),
                    ),
                    _local_key(),
                ],
            )
        )
        assert _z(merged).identity_funnel is not None

    def test_derived_records_fuse_on_the_normalized_name(self) -> None:
        merged = _merge(
            VertexEquivalence(
                left="X", right="Y", into="Z", identity=_derived_identity()
            )
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


def _preview(identity: list[IdentityBranchDecl]):
    from graflo.architecture.evolution.preview import preview_merge

    return preview_merge(
        _side_a(),
        _side_b(),
        MergeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(
                    left="X",
                    right="Y",
                    into="Z",
                    identity=identity,
                    allow=["self_relations"],
                )
            ],
            canonical_maps={"left": CanonicalMap(vertices={"Ap": "Zp"})},
        ),
    )


def test_preview_notes_a_demoted_key_is_now_a_lookup() -> None:
    """A member whose records complete two funnel branches loses its dedup key.

    ``X`` records derive ``name_key`` and ``local_key`` alike, so one seen with
    and without a name keys twice; ``x_id`` is only a lookup now. One property
    branch per member leaves each member a single completable branch.
    """
    preview = _preview(_derived_identity())
    assert preview.outcome.status == "merged"
    notes = [f for f in preview.findings if f.kind == "lookup_demotion"]
    assert len(notes) == 2
    assert any("secondary 'a'" in n.message for n in notes)
    assert any("secondary 'b'" in n.message for n in notes)
    assert all(n.severity == "note" and n not in preview.blocking for n in notes)

    per_member = _preview(["x_id", "y_id"])
    assert per_member.outcome.status == "merged"
    assert not [f for f in per_member.findings if f.kind == "lookup_demotion"]


def test_a_merge_that_pins_a_left_reference_is_recordable() -> None:
    """The pinned left pipeline is an edit a merge commit has to express."""
    from graflo.architecture.evolution.merge3 import build_merge_recipe
    from graflo.architecture.evolution.merge_commit import build_merge_commit

    left, right = _side_a(), _side_b()
    op = MergeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(
                left="X",
                right="Y",
                into="Z",
                identity=_derived_identity(),
                allow=["self_relations"],
            )
        ],
        canonical_maps={"left": CanonicalMap(vertices={"Ap": "Zp"})},
    )
    merged = merge_manifests(left, right, op, bump_version=False)
    assert collect_endpoint_selectors(_pipeline(merged, "r_ap")) == [("Z", "a")]

    entry = build_merge_commit(
        left,
        merged,
        parents=["a" * 12, "b" * 12],
        recipe=build_merge_recipe(left, right, op),
        right=right,
    )

    assert "replace_resources" in [o.op for o in entry.ops]


# ── demoted keys are named by their origin ──────────────────────────────────


def _named_side(
    side: GraphManifest, name: str, *, secondaries: list[dict] | None = None
) -> GraphManifest:
    """*side* with its schema renamed, and *secondaries* authored on its first class."""
    payload = side.to_dict(skip_defaults=True)
    payload["schema"]["metadata"]["name"] = name
    if secondaries is not None:
        vertex = payload["schema"]["core_schema"]["vertex_config"]["vertices"][0]
        vertex["secondary_identities"] = secondaries
    manifest = GraphManifest.from_config(payload)
    manifest.finish_init()
    return manifest


def _side_a_two_keys() -> GraphManifest:
    """``X`` keyed ``x_id`` and ``W`` keyed ``w_id``: one origin, two key field sets."""
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
                                "name": "W",
                                "properties": ["w_id", "name"],
                                "identity": ["w_id"],
                            },
                        ]
                    },
                    "edge_config": {"edges": []},
                },
            },
            "ingestion_model": {
                "resources": [
                    {"name": "r_x", "pipeline": [{"vertex": "X"}]},
                    {"name": "r_w", "pipeline": [{"vertex": "W"}]},
                ]
            },
        }
    )
    manifest.finish_init()
    return manifest


def _side_b_composite() -> GraphManifest:
    """``Y`` keyed on the composite ``[y_id, site]``."""
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "b", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "Y",
                                "properties": ["y_id", "site", "cname"],
                                "identity": ["y_id", "site"],
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


def _rekey(**updates) -> VertexEquivalence:
    """``X ~ Y`` re-keyed on ``name``, so both members' own keys are demoted."""
    payload: dict = {
        "left": "X",
        "right": "Y",
        "into": "Z",
        "identity": ["name"],
        "properties": [NAME_EQUIVALENCE],
        **updates,
    }
    return VertexEquivalence(**payload)


def _vertex_step(pipeline: list, vertex: str) -> dict:
    return next(step for step in pipeline if step.get("vertex") == vertex)


class TestOriginNaming:
    """A demoted key is named by the side it came from: property and secondary."""

    def test_a_demoted_key_is_a_secondary_named_by_its_origin(self) -> None:
        merged = _merge(_rekey())
        assert _secondaries(merged) == {"a": ["a__x_id"], "b": ["b__y_id"]}

    def test_a_demoted_key_field_is_prefixed_by_its_origin(self) -> None:
        merged = _merge(_rekey())
        names = set(_z(merged).property_names)
        assert {"a__x_id", "b__y_id"} <= names
        assert not {"x_id", "y_id"} & names

    def test_the_prefixed_property_reads_the_raw_column(self) -> None:
        """The prefix is a property rename: pipelines map it from the raw field."""
        merged = _merge(_rekey())
        assert _vertex_step(_pipeline(merged, "r_x"), "Z")["from"]["a__x_id"] == (
            "x_id"
        )
        caster = DocumentCaster(merged.require_ingestion_model())
        result = asyncio.run(
            caster.cast_batch(
                [{"x_id": "x1", "name": "n1"}], "r_x", params=IngestionParams()
            )
        )
        (doc,) = result.graph.vertices["Z"]
        assert doc["a__x_id"] == "x1"

    def test_a_derived_identity_names_demoted_keys_by_origin(self) -> None:
        merged = _merge(
            VertexEquivalence(
                left="X", right="Y", into="Z", identity=_derived_identity()
            )
        )
        assert _secondaries(merged) == {"a": ["a__x_id"], "b": ["b__y_id"]}

    def test_a_reference_is_pinned_to_the_origin(self) -> None:
        merged = _merge(_rekey())
        assert collect_endpoint_selectors(_pipeline(merged, "r_ap")) == [("Z", "a")]

    def test_every_field_of_a_composite_key_is_prefixed(self) -> None:
        merged = _merge(_rekey(), right=_side_b_composite())
        assert _secondaries(merged)["b"] == ["b__y_id", "b__site"]

    def test_two_key_field_sets_from_one_origin_are_named_by_their_fields(
        self,
    ) -> None:
        merged = merge_manifests(
            _side_a_two_keys(),
            _side_b(),
            MergeManifestsOp(
                vertex_equivalences=[
                    VertexEquivalence(
                        left=["X", "W"],
                        right="Y",
                        into="Z",
                        identity=["name"],
                        properties=[NAME_EQUIVALENCE],
                        allow=["observation_fusion"],
                    )
                ]
            ),
            bump_version=False,
        )
        assert _secondaries(merged) == {
            "a__x_id": ["a__x_id"],
            "a__w_id": ["a__w_id"],
            "b": ["b__y_id"],
        }

    def test_a_key_a_property_branch_names_keeps_its_name(self) -> None:
        """``x_id`` is part of the merged key: renaming it would empty that branch.

        It is still demoted, unprefixed, so a lookup by it finds the node
        whichever branch keyed it.
        """
        merged = _merge(
            VertexEquivalence(left="X", right="Y", into="Z", identity=["x_id", "y_id"])
        )
        assert {"x_id", "y_id"} <= set(_z(merged).property_names)
        assert _secondaries(merged) == {"a": ["x_id"], "b": ["y_id"]}

    def test_a_reference_carrying_a_branch_key_is_pinned_to_its_origin(
        self,
    ) -> None:
        """``r_ap`` carries only ``x_id``; ``X`` owners may key on ``match_key``.

        The primary digest it would compute from the ``x_id`` branch misses an
        owner keyed on the earlier branch; the unprefixed secondary finds it.
        """
        identity: list[IdentityBranchDecl] = [_name_key("match_key"), "x_id", "y_id"]
        merged = _merge(
            VertexEquivalence(left="X", right="Y", into="Z", identity=identity)
        )

        assert _secondaries(merged) == {"a": ["x_id"], "b": ["y_id"]}
        assert collect_endpoint_selectors(_pipeline(merged, "r_ap")) == [("Z", "a")]
        notes = [
            f
            for f in _preview(identity).findings
            if f.kind == "lookup_demotion" and "secondary 'a'" in f.message
        ]
        assert len(notes) == 1

    def test_origins_on_the_op_override_the_schema_names(self) -> None:
        merged = merge_manifests(
            _side_a(),
            _side_b(),
            MergeManifestsOp(
                vertex_equivalences=[_rekey(allow=["self_relations"])],
                origins={"left": "maint", "right": "sens"},
            ),
            bump_version=False,
        )
        assert _secondaries(merged) == {
            "maint": ["maint__x_id"],
            "sens": ["sens__y_id"],
        }

    def test_equal_schema_names_without_origins_are_refused(self) -> None:
        with pytest.raises(MergeIdentityError, match="origins") as excinfo:
            _merge(_rekey(), right=_named_side(_side_b(), "a"))
        assert excinfo.value.check == "origin"

    def test_an_invalid_schema_name_without_origins_is_refused(self) -> None:
        with pytest.raises(MergeIdentityError, match="origins") as excinfo:
            _merge(_rekey(), right=_named_side(_side_b(), "source-b"))
        assert excinfo.value.check == "origin"

    @pytest.mark.parametrize("origin", ["a__x", "identity", "secondary"])
    def test_an_origin_that_cannot_name_a_key_is_refused(self, origin: str) -> None:
        """``__`` would split ambiguously; a selector word names no secondary."""
        with pytest.raises(MergeIdentityError) as excinfo:
            merge_manifests(
                _side_a(),
                _side_b(),
                MergeManifestsOp(
                    vertex_equivalences=[_rekey(allow=["self_relations"])],
                    origins={"left": origin, "right": "b"},
                ),
            )
        assert excinfo.value.check == "origin"

    def test_an_origin_named_like_an_authored_secondary_is_refused(self) -> None:
        left = _named_side(
            _side_a(), "a", secondaries=[{"name": "a", "fields": ["name"]}]
        )
        with pytest.raises(MergeIdentityError, match="'a'") as excinfo:
            _merge(_rekey(), left=left)
        assert excinfo.value.check == "origin"

    def test_a_union_naming_nothing_by_origin_needs_no_valid_origin(self) -> None:
        """The members agree on their key, so none is demoted: any schema names do."""
        merged = merge_manifests(
            _named_side(_side_a(), "same-name"),
            _named_side(_side_b(), "same-name"),
            MergeManifestsOp(
                vertex_equivalences=[
                    VertexEquivalence(
                        left="X",
                        right="Y",
                        into="Z",
                        properties=[
                            NAME_EQUIVALENCE,
                            PropertyEquivalence(left="x_id", right="y_id", into="x_id"),
                        ],
                        allow=["self_relations"],
                    )
                ],
                canonical_maps={"left": CanonicalMap(vertices={"Ap": "Zp"})},
            ),
            bump_version=False,
        )
        assert _z(merged).identity == ["x_id"]
        assert _secondaries(merged) == {}

    def test_an_authored_rename_of_a_demoted_key_is_refused(self) -> None:
        from graflo.architecture.evolution.naming_graph import MergeNamingError

        with pytest.raises(MergeNamingError, match="named by its key space") as excinfo:
            _merge(
                _rekey(
                    properties=[
                        NAME_EQUIVALENCE,
                        PropertyEquivalence(left="x_id", right="y_id", into="key"),
                    ]
                )
            )
        assert "double_home" in {f.kind for f in excinfo.value.findings}

    def test_an_equivalence_keeping_a_demoted_keys_spelling_is_refused(
        self,
    ) -> None:
        """``into: x_id`` would fuse ``Y.ref`` with a key that is renamed away."""
        from graflo.architecture.evolution.naming_graph import MergeNamingError

        with pytest.raises(MergeNamingError, match="named by its key space") as excinfo:
            _merge(
                _rekey(
                    properties=[
                        NAME_EQUIVALENCE,
                        PropertyEquivalence(left="x_id", right="ref", into="x_id"),
                    ]
                ),
                right=_side_b(properties=["y_id", "cname", "ref"]),
            )
        assert "double_home" in {f.kind for f in excinfo.value.findings}
        assert "property branch" in str(excinfo.value)

    def test_a_vocabulary_self_entry_for_a_demoted_key_is_tolerated(self) -> None:
        merged = merge_manifests(
            _side_a(),
            _side_b(),
            MergeManifestsOp(
                vertex_equivalences=[_rekey(allow=["self_relations"])],
                canonical_maps={
                    "left": CanonicalMap(
                        vertices={"Ap": "Zp"}, properties={"X": {"x_id": "x_id"}}
                    )
                },
            ),
            bump_version=False,
        )
        assert _secondaries(merged)["a"] == ["a__x_id"]

    def test_an_explicit_empty_origin_is_refused(self) -> None:
        with pytest.raises(MergeIdentityError) as excinfo:
            merge_manifests(
                _side_a(),
                _side_b(),
                MergeManifestsOp(
                    vertex_equivalences=[_rekey(allow=["self_relations"])],
                    origins={"left": "", "right": "b"},
                ),
            )
        assert excinfo.value.check == "origin"

    def test_a_property_already_named_like_the_prefixed_key_is_refused(
        self,
    ) -> None:
        from graflo.architecture.evolution.naming_graph import MergeNamingError

        with pytest.raises(MergeNamingError) as excinfo:
            _merge(_rekey(), right=_side_b(properties=["y_id", "cname", "b__y_id"]))
        assert "property_collision" in {f.kind for f in excinfo.value.findings}

    def test_an_omitted_local_key_tag_is_the_origin(self) -> None:
        merged = _merge(
            VertexEquivalence(
                left="X",
                right="Y",
                into="Z",
                identity=[
                    _name_key(),
                    LocalKeyBranch(
                        local_key={
                            "r_x": LocalKeySource(field="x_id"),
                            "r_y": LocalKeySource(field="y_id", tag=None),
                        }
                    ),
                ],
            )
        )
        caster = DocumentCaster(merged.require_ingestion_model())
        x = asyncio.run(
            caster.cast_batch(
                [{"x_id": "x1", "name": "n"}], "r_x", params=IngestionParams()
            )
        )
        y = asyncio.run(
            caster.cast_batch(
                [{"y_id": "y1", "cname": "m"}], "r_y", params=IngestionParams()
            )
        )
        assert x.graph.vertices["Z"][0]["local_key"] == "a:x1"
        assert y.graph.vertices["Z"][0]["local_key"] == "y1"
