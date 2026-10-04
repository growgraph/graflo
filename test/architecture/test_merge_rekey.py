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
        assert _secondaries(merged) == {"by_x_id": ["x_id"], "by_y_id": ["y_id"]}

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
        assert _secondaries(merged) == {"by_x_id": ["x_id"], "by_y_id": ["y_id"]}

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


class TestResourcesOutsideTheDerivedBranches:
    def test_an_upserting_producer_no_derived_branch_names_becomes_a_reference(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """``r_ap`` carries ``X``'s own key and nothing of the new one.

        Upserting would write nothing -- no record completes a funnel branch --
        so it looks ``Z`` up by the key it carries, and merge says so.
        """
        with caplog.at_level(logging.WARNING):
            merged = _merge(
                VertexEquivalence(
                    left="X", right="Y", into="Z", identity=_derived_identity()
                ),
                left=_side_a(reference_step={"vertex": "X"}),
            )
        z_step = next(s for s in _pipeline(merged, "r_ap") if s.get("vertex") == "Z")
        assert z_step["lookup_only"] is True
        assert collect_endpoint_selectors(_pipeline(merged, "r_ap")) == [
            ("Z", "by_x_id")
        ]
        assert "r_ap" in caplog.text and "by_x_id" in caplog.text

    def test_a_resource_completing_a_property_branch_is_not_converted(self) -> None:
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
        caster = DocumentCaster(merged.require_ingestion_model())
        result = asyncio.run(
            caster.cast_batch(
                [{"ap_id": "p1", "x_id": "x1"}], "r_ap", params=IngestionParams()
            )
        )
        assert [doc["x_id"] for doc in result.graph.vertices["Z"]] == ["x1"]

    def test_a_converted_reference_still_emits_its_edge(self) -> None:
        merged = _merge(
            VertexEquivalence(
                left="X", right="Y", into="Z", identity=_derived_identity()
            ),
            left=_side_a(reference_step={"vertex": "X"}),
        )
        caster = DocumentCaster(merged.require_ingestion_model())
        result = asyncio.run(
            caster.cast_batch(
                [{"ap_id": "p1", "x_id": "x1"}], "r_ap", params=IngestionParams()
            )
        )
        assert not result.graph.vertices.get("Z")
        assert sum(len(edges) for edges in result.graph.edges.values()) == 1

    def test_retire_keep_leaves_an_uncovered_producer_nothing_to_look_up_by(
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

    def test_a_reference_is_pinned_to_the_demoted_member_key(self) -> None:
        merged = _merge(
            VertexEquivalence(
                left="X", right="Y", into="Z", identity=_derived_identity()
            )
        )
        assert collect_endpoint_selectors(_pipeline(merged, "r_ap")) == [
            ("Z", "by_x_id")
        ]

    def test_a_pinned_reference_still_emits_its_edge(self) -> None:
        merged = _merge(
            VertexEquivalence(
                left="X", right="Y", into="Z", identity=_derived_identity()
            )
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

    def test_only_the_merged_class_becomes_a_lookup(self) -> None:
        router, _work_order, edge = _pipeline(_merge_routed(), "RA")
        assert router["lookup_only"] == ["Z"]
        assert edge["target_match"] == {"Z": "by_x_id"}

    def test_the_merged_class_is_looked_up_and_the_rest_still_written(self) -> None:
        caster = DocumentCaster(_merge_routed().require_ingestion_model())
        rows = [
            {"blaId": "x1", "assetType": "X", "work_order_id": "w1"},
            {"blaId": "k9", "assetType": "C", "work_order_id": "w1"},
        ]
        graph = asyncio.run(
            caster.cast_batch(rows, "RA", params=IngestionParams())
        ).graph
        assert not graph.vertices.get("Z")
        assert [dict(d) for d in graph.vertices["C"]] == [{"x_id": "k9"}]
        targets = {
            edge_id[1]: [dict(t) for _s, t, *_ in docs]
            for edge_id, docs in graph.edges.items()
        }
        assert targets == {"Z": [{"x_id": "x1"}], "C": [{"x_id": "k9"}]}

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
        assert edge["target_match"] == {"Z": "by_x_id"}


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


def test_preview_notes_each_resource_merge_turns_into_a_reference() -> None:
    """Converting a producer drops its writes: the preview says so, without blocking."""
    from graflo.architecture.evolution.preview import preview_merge

    preview = preview_merge(
        _side_a(reference_step={"vertex": "X"}),
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
            ],
            canonical_maps={"left": CanonicalMap(vertices={"Ap": "Zp"})},
        ),
    )
    assert preview.outcome.status == "merged"
    notes = [f for f in preview.findings if f.kind == "reference_conversion"]
    assert len(notes) == 1
    assert notes[0].severity == "note"
    assert "r_ap" in notes[0].message and "by_x_id" in notes[0].message
    assert notes[0] not in preview.blocking


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
    assert any("'by_x_id'" in n.message for n in notes)
    assert any("'by_y_id'" in n.message for n in notes)
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
    assert collect_endpoint_selectors(_pipeline(merged, "r_ap")) == [("Z", "by_x_id")]

    entry = build_merge_commit(
        left,
        merged,
        parents=["a" * 12, "b" * 12],
        recipe=build_merge_recipe(left, right, op),
        right=right,
    )

    assert "replace_resources" in [o.op for o in entry.ops]
