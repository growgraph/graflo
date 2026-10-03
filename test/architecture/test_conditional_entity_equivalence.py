"""End-to-end: union of two manifests with conditional entity equivalence.

The full recipe merged from fundamental ops — canonicalize the left
manifest, then merge with a derived identity declared on the equivalence:
ordered funnel branches over canonical attributes each source derives from
its own columns, ending in a namespaced local key. Merge lowers them into
per-resource derivation transforms and a priority funnel, and demotes each
member's own key to a lookup-only secondary. The class definition stays
side-agnostic; records that pass a derivation gate fuse with the right-hand
entities (same synthetic id), records that fail it keep a namespaced local key.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import (
    AddResourceTransformsOp,
    AddVertexPropertiesOp,
    CanonicalMap,
    DerivationSpec,
    DerivedBranch,
    IdentityBranchDecl,
    LocalKeyBranch,
    LocalKeySource,
    MergeManifestsOp,
    ReplaceIdentityOp,
    VertexEquivalence,
    apply_evolution,
    canonical_map_to_ops,
    merge_manifests,
)
from graflo.architecture.evolution.alignment import (
    AlignmentConflictError,
    IdentityPlan,
    identity_to_ops,
)
from graflo.architecture.evolution.preview import preview_merge
from graflo.hq.document_caster import DocumentCaster
from graflo.hq.ingestion_parameters import IngestionParams


def _manifest_a() -> GraphManifest:
    """Source A, pure: no derived properties, no transforms."""
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "a", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "Firm",
                                "properties": [
                                    "firm_id",
                                    "secondary_key",
                                    "shared_raw",
                                ],
                                "identity": ["firm_id"],
                            }
                        ]
                    },
                    "edge_config": {"edges": []},
                },
            },
            "ingestion_model": {
                "resources": [{"name": "r_a", "pipeline": [{"vertex": "Firm"}]}],
                "transforms": [],
            },
        }
    )
    manifest.finish_init()
    return manifest


def _manifest_b() -> GraphManifest:
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "b", "version": "1.0.0"},
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


_CANONICAL = CanonicalMap(
    vertices={"Firm": "Company"},
    properties={"Firm": {"firm_id": "company_id"}},
)

_LOCAL_KEY = LocalKeyBranch(
    local_key={
        # RAW doc field: renamed documents still carry firm_id.
        "r_a": LocalKeySource(field="firm_id", tag="a"),
        "r_b": LocalKeySource(field="org_id", tag="b"),
    }
)

_IDENTITY: list[IdentityBranchDecl] = [
    DerivedBranch(
        name="match_key",
        sources={
            # Two inputs (gate, value): the gated function, named explicitly.
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
    ),
    _LOCAL_KEY,
]


def _compose_union() -> GraphManifest:
    """A merged union *without* the derived identity applied yet.

    Declares a throwaway identity on the cluster -- each member keyed on its
    own key: `Company` and `Org` disagree on their raw identity field, and
    nothing here promises to resolve it (callers that want the resolved
    identity use :func:`_build_union` instead, which declares the derived
    branches on the same equivalence), so an explicit placeholder is what this
    underived union needs in order to merge at all. Each branch must be one
    some member carries, or merge refuses it.
    """
    canonical_a = apply_evolution(_manifest_a(), canonical_map_to_ops(_CANONICAL))
    right = _manifest_b()
    op = MergeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(
                left="Company",
                right="Org",
                into="Company",
                identity=["company_id", "org_id"],
            )
        ]
    )
    return merge_manifests(
        canonical_a, right, op, canonical_maps=[("left", _CANONICAL)]
    )


def _merge_op(identity: list[IdentityBranchDecl] = _IDENTITY) -> MergeManifestsOp:
    return MergeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(
                left="Company", right="Org", into="Company", identity=list(identity)
            )
        ],
    )


def _build_union(identity: list[IdentityBranchDecl] = _IDENTITY) -> GraphManifest:
    canonical_a = apply_evolution(_manifest_a(), canonical_map_to_ops(_CANONICAL))
    return merge_manifests(
        canonical_a,
        _manifest_b(),
        _merge_op(identity),
        canonical_maps=[("left", _CANONICAL)],
    )


def _cast(manifest: GraphManifest, resource: str, rows: list[dict]) -> list[dict]:
    caster = DocumentCaster(manifest.require_ingestion_model())
    result = asyncio.run(caster.cast_batch(rows, resource, params=IngestionParams()))
    return list(result.graph.vertices.get("Company", []))


class TestComposedSchema:
    def test_union_schema_is_side_agnostic(self) -> None:
        union = _build_union()
        assert union.graph_schema is not None
        vc = union.graph_schema.core_schema.vertex_config
        assert vc.vertex_set == {"Company"}
        assert vc.identity_fields("Company") == ["id"]
        vertex = next(v for v in vc.vertex_list if v.name == "Company")
        assert vertex.identity_funnel is not None
        # The identity references ONLY canonical attributes — no side-specific
        # branches (company_id / org_id do not appear in the funnel).
        assert [b.id for b in vertex.identity_funnel.branches] == [
            "match_key",
            "local_key",
        ]
        funnel_fields = {f for b in vertex.identity_funnel.branches for f in b.fields}
        assert funnel_fields == {"match_key", "local_key"}
        # The retired side keys survive as lookup-only secondary identities,
        # named automatically after their fields.
        assert {s.name for s in vc.secondary_identities("Company")} == {
            "by_company_id",
            "by_org_id",
        }

    def test_identity_validates_against_the_union(self) -> None:
        """The lowering runs validate_identity when handed the manifest."""
        union = _compose_union()
        ops = identity_to_ops(
            IdentityPlan(vertex="Company", branches=tuple(_IDENTITY)),
            manifest=union,
            canonical_maps=[_CANONICAL],
        )
        # No router restricts extraction, so no EnsureExtractedFieldsOp; and
        # secondaries are merge's to demote, not the lowering's.
        assert [type(op) for op in ops] == [
            AddVertexPropertiesOp,
            AddResourceTransformsOp,
            ReplaceIdentityOp,
        ]


class TestConditionalFusion:
    def test_gated_record_fuses_and_non_gated_keys_locally(self) -> None:
        union = _build_union()

        # RAW source vocabulary: r_a rows carry firm_id, not company_id.
        a_rows = [
            {"firm_id": "f1", "secondary_key": "abc_7", "shared_raw": "ABC-Alpha"},
            {"firm_id": "f2", "secondary_key": "zz9", "shared_raw": "ABC-Alpha"},
        ]
        b_rows = [{"org_id": "o1", "shared_raw": "ABC-ALPHA"}]

        a_docs = _cast(union, "r_a", a_rows)
        b_docs = _cast(union, "r_b", b_rows)

        assert len(a_docs) == 2, "non-gated record must still be ingested"
        assert len(b_docs) == 1

        by_company = {doc["company_id"]: doc for doc in a_docs}
        gated, non_gated = by_company["f1"], by_company["f2"]
        b_doc = b_docs[0]

        assert all("id" in doc for doc in (gated, non_gated, b_doc))

        # Conditional equivalence: the gated record and B share the digest...
        assert gated["match_key"] == "alpha"
        assert gated["id"] == b_doc["id"]

        # ...while the non-gated record keys off its namespaced local key,
        # despite carrying the same raw shared value.
        assert non_gated.get("match_key") is None
        assert non_gated["local_key"] == "a:f2"
        assert non_gated["id"] != gated["id"]
        assert non_gated["id"] != b_doc["id"]
        assert b_doc["local_key"] == "b:o1"


class TestPrefixMarkerAdmission:
    """The marker on the value admits it; an unmarked value falls through.

    Contrast the gated idiom above, whose ``strip_prefix`` is a silent no-op
    when the prefix is absent — there, a marked and an unmarked spelling of the
    same name normalize alike and fuse.
    """

    def _marker_identity(self) -> list[IdentityBranchDecl]:
        spec = DerivationSpec(
            input=["shared_raw"], foo="affix_gated_key", params={"prefix": "ABC-"}
        )
        return [
            # Literally the same call on both sides: one normal form.
            DerivedBranch(name="match_key", sources={"r_a": spec, "r_b": spec}),
            _LOCAL_KEY,
        ]

    def test_marked_values_fuse_across_sources(self) -> None:
        union = _build_union(self._marker_identity())

        a = _cast(union, "r_a", [{"firm_id": "f1", "shared_raw": "ABC-Alpha"}])
        b = _cast(union, "r_b", [{"org_id": "o1", "shared_raw": "ABC-ALPHA"}])

        assert a[0]["match_key"] == "alpha"
        assert b[0]["match_key"] == "alpha"
        assert a[0]["id"] == b[0]["id"]

    def test_an_unmarked_value_falls_through_instead_of_fusing(self) -> None:
        """Same business name, no marker: not admitted, and not dropped."""
        union = _build_union(self._marker_identity())

        a = _cast(union, "r_a", [{"firm_id": "f2", "shared_raw": "Alpha"}])
        b = _cast(union, "r_b", [{"org_id": "o1", "shared_raw": "ABC-ALPHA"}])

        assert a[0].get("match_key") is None
        assert a[0]["local_key"] == "a:f2"
        assert a[0]["id"] != b[0]["id"]


class TestPriorityFunnel:
    """Two derived branches: priority semantics, including the known trap."""

    def _two_branch_identity(self) -> list[IdentityBranchDecl]:
        return [
            DerivedBranch(
                name="c1",
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
            ),
            DerivedBranch(
                name="c2",
                sources={
                    "r_a": DerivationSpec(
                        input=["firm_id", "tax_no"],
                        foo="gated_normalized_key",
                        params={"prefix": ""},
                    ),
                    "r_b": DerivationSpec(
                        input=["org_id", "tax_no"],
                        foo="gated_normalized_key",
                        params={"prefix": ""},
                    ),
                },
            ),
            _LOCAL_KEY,
        ]

    def test_same_top_priority_attribute_fuses(self) -> None:
        union = _build_union(self._two_branch_identity())
        a = _cast(
            union,
            "r_a",
            [
                {
                    "firm_id": "f1",
                    "secondary_key": "abc_7",
                    "shared_raw": "ABC-Alpha",
                    "tax_no": "T1",
                }
            ],
        )
        b = _cast(
            union, "r_b", [{"org_id": "o1", "shared_raw": "ABC-ALPHA", "tax_no": "T9"}]
        )
        # Both carry c1 (their strongest evidence) with equal values -> fuse,
        # even though their c2 values differ.
        assert a[0]["id"] == b[0]["id"]

    def test_lower_priority_match_does_not_fuse_when_stronger_present(self) -> None:
        """The documented priority-funnel trap, pinned deliberately.

        X carries {c1, c2}; Y carries only {c2}. They match on c2, but X keys
        by its strongest present attribute (c1) while Y keys by c2 — no
        fusion. Equivalence holds by ONE attribute only when that attribute
        is the strongest evidence both records carry.

        Merge's preview reports this hazard as a ``lookup_demotion`` note: a
        member whose records can complete two or more branches no longer has
        its own key deduplicating them (see
        :meth:`test_preview_notes_the_demoted_key_no_longer_deduplicates`).
        """
        union = _build_union(self._two_branch_identity())
        x = _cast(
            union,
            "r_a",
            [
                {
                    "firm_id": "f1",
                    "secondary_key": "abc_7",  # gate passes -> c1 present
                    "shared_raw": "ABC-Alpha",
                    "tax_no": "T1",  # c2 present too
                }
            ],
        )
        y = _cast(
            union,
            "r_b",
            [
                {
                    "org_id": "o1",
                    # no shared_raw -> c1 absent
                    "tax_no": "T1",  # same c2 value as X
                }
            ],
        )
        assert x[0]["c2"] is not None
        assert x[0]["c2"] == y[0]["c2"]
        assert x[0]["id"] != y[0]["id"]  # no fusion: X keyed by c1, Y by c2

    def test_preview_notes_the_demoted_key_no_longer_deduplicates(self) -> None:
        """A derived branch plus local_key: each member's records reach two
        branches, so its demoted own key is flagged as no longer unique."""
        canonical_a = apply_evolution(_manifest_a(), canonical_map_to_ops(_CANONICAL))

        preview = preview_merge(
            canonical_a,
            _manifest_b(),
            _merge_op(),
            canonical_maps=[("left", _CANONICAL)],
        )

        assert not preview.refused
        notes = [f for f in preview.findings if f.kind == "lookup_demotion"]
        assert {f.severity for f in notes} == {"note"}
        messages = " ".join(f.message for f in notes)
        assert "'by_company_id'" in messages
        assert "'by_org_id'" in messages
        assert not preview.blocking


class TestOrderingRationale:
    def test_identity_cannot_lower_before_compose(self) -> None:
        """The derived branches reference both sides' resources — only the
        union carries them, so lowering against one side fails loudly."""
        canonical_a = apply_evolution(_manifest_a(), canonical_map_to_ops(_CANONICAL))
        with pytest.raises(AlignmentConflictError, match="unknown resources"):
            identity_to_ops(
                IdentityPlan(vertex="Company", branches=tuple(_IDENTITY)),
                manifest=canonical_a,
            )


# --------------------------------------------------------------------------- #
# A routed source: one vertex_router, nested, two branches collapsing onto the
# merged class. This is the shape that silently dropped every routed record
# before derivations became level-aware — the manifest looked right while the
# emitted graph had no identities at all.
# --------------------------------------------------------------------------- #

_ROUTED_CANONICAL = CanonicalMap(
    vertices={"Firm": "Company"},
    properties={"Firm": {"firm_id": "company_id"}},
)

_ROUTED_IDENTITY: list[IdentityBranchDecl] = [
    DerivedBranch(
        name="match_key",
        sources={
            # Each member of the view carries the shared key in its own
            # column; keyed by member, the router decides which one runs. The
            # left member is `Company` because the canonical map renamed
            # `Firm` before the merge.
            "r_view": {
                "Company": DerivationSpec(
                    input=["secondary_key", "firm_ref"],
                    foo="gated_normalized_key",
                    params={"prefix": "abc_", "strip_prefix": "ABC-"},
                ),
                "Shop": DerivationSpec(
                    input=["secondary_key", "shop_ref"],
                    foo="gated_normalized_key",
                    params={"prefix": "abc_", "strip_prefix": "ABC-"},
                ),
            },
            "r_b": DerivationSpec(
                input=["org_id", "shared_raw"],
                foo="gated_normalized_key",
                params={"prefix": "", "strip_prefix": "ABC-"},
            ),
        },
    ),
    LocalKeyBranch(
        local_key={
            "r_view": {
                "Company": LocalKeySource(field="firm_id", tag="firm"),
                "Shop": LocalKeySource(field="shop_id", tag="shop"),
            },
            "r_b": LocalKeySource(field="org_id", tag="b"),
        }
    ),
]


def _routed_manifest_a() -> GraphManifest:
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "a", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "Firm",
                                "properties": ["firm_id", "secondary_key"],
                                "identity": ["firm_id"],
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
                        "pipeline": [
                            {
                                "descend": {
                                    "key": "records",
                                    "apply": [
                                        {
                                            "vertex_router": {
                                                "type_field": "kind",
                                                "keep_fields": [
                                                    "firm_id",
                                                    "shop_id",
                                                    "person_id",
                                                    "secondary_key",
                                                ],
                                                "type_map": {
                                                    "firm": "Firm",
                                                    "shop": "Shop",
                                                    "person": "Person",
                                                },
                                            }
                                        }
                                    ],
                                }
                            }
                        ],
                    }
                ],
                "transforms": [],
            },
        }
    )
    manifest.finish_init()
    return manifest


def _member_equivalence(identity: list[IdentityBranchDecl]) -> VertexEquivalence:
    return VertexEquivalence(
        left=["Company", "Shop"], right="Org", into="Company", identity=list(identity)
    )


def _build_routed_union() -> GraphManifest:
    left = apply_evolution(
        _routed_manifest_a(), canonical_map_to_ops(_ROUTED_CANONICAL)
    )
    op = MergeManifestsOp(
        vertex_equivalences=[_member_equivalence(_ROUTED_IDENTITY)],
    )
    return merge_manifests(
        left, _manifest_b(), op, canonical_maps=[("left", _ROUTED_CANONICAL)]
    )


_VIEW = [
    {
        "records": [
            {
                "kind": "firm",
                "firm_id": "f1",
                "secondary_key": "abc_7",
                "firm_ref": "ABC-Alpha",
            },
            {
                "kind": "firm",
                "firm_id": "f2",
                "secondary_key": "zz9",
                "firm_ref": "ABC-Alpha",
            },
            {
                "kind": "shop",
                "shop_id": "s1",
                "secondary_key": "abc_1",
                "shop_ref": "ABC-Beta",
            },
            {"kind": "person", "person_id": "p1", "secondary_key": "abc_9"},
        ]
    }
]


class TestRoutedSourceFusion:
    def test_both_router_branches_fuse_with_their_b_side_peers(self) -> None:
        union = _build_routed_union()

        view = _cast(union, "r_view", _VIEW)
        b = _cast(union, "r_b", [{"org_id": "o1", "shared_raw": "ABC-ALPHA"}])

        by_local = {doc["local_key"]: doc for doc in view}
        assert set(by_local) == {"firm:f1", "firm:f2", "shop:s1"}
        # The gated firm row and B's row key alike on the shared match_key.
        assert by_local["firm:f1"]["match_key"] == "alpha"
        assert by_local["firm:f1"]["id"] == b[0]["id"]
        # The non-gated firm row carries the same raw value but no match_key,
        # so it falls through to its namespaced local key and stays separate.
        assert by_local["firm:f2"].get("match_key") is None
        assert by_local["firm:f2"]["id"] != b[0]["id"]

    def test_the_shop_branch_derives_from_its_own_column(self) -> None:
        union = _build_routed_union()

        view = _cast(union, "r_view", _VIEW)
        branch = _cast(union, "r_b", [{"org_id": "br1", "shared_raw": "ABC-BETA"}])

        shop = next(d for d in view if d["local_key"] == "shop:s1")
        assert shop["match_key"] == "beta"
        assert shop["id"] == branch[0]["id"]

    def test_the_unaligned_branch_still_flows_through_the_same_router(self) -> None:
        """The router is never split: `person` keeps being routed, unpolluted."""
        union = _build_routed_union()
        caster = DocumentCaster(union.require_ingestion_model())

        result = asyncio.run(
            caster.cast_batch(_VIEW, "r_view", params=IngestionParams())
        )

        people = result.graph.vertices["Person"]
        assert [p["person_id"] for p in people] == ["p1"]
        assert "match_key" not in people[0]
        assert "local_key" not in people[0]

    def test_every_routed_record_gets_an_identity(self) -> None:
        """The regression: root-level derivations left all of these keyless."""
        union = _build_routed_union()

        view = _cast(union, "r_view", _VIEW)

        assert len(view) == 3
        assert all(doc.get("id") for doc in view)


# --------------------------------------------------------------------------- #
# Member-keyed derivations: the members share one key column, each with its
# own marker, and which member a document *is* decides the derivation.
# --------------------------------------------------------------------------- #


def _marker(prefix: str) -> DerivationSpec:
    return DerivationSpec(
        input=["secondary_key"], foo="affix_gated_key", params={"prefix": prefix}
    )


_MEMBER_LOCAL_KEY = LocalKeyBranch(
    local_key={
        "r_view": {
            "Company": LocalKeySource(field="firm_id", tag="firm"),
            "Shop": LocalKeySource(field="shop_id", tag="shop"),
        },
        "r_b": LocalKeySource(field="org_id", tag="b"),
    }
)

_MEMBER_IDENTITY: list[IdentityBranchDecl] = [
    DerivedBranch(
        name="match_key",
        sources={
            # One column for every kind; the member decides which marker
            # admits a value. The left member is `Company` because the
            # canonical map renamed `Firm` before the merge.
            "r_view": {"Company": _marker("abc_"), "Shop": _marker("def_")},
            "r_b": DerivationSpec(
                input=["shared_raw"], foo="affix_gated_key", params={"prefix": ""}
            ),
        },
    ),
    _MEMBER_LOCAL_KEY,
]


def _build_member_union(
    identity: list[IdentityBranchDecl] = _MEMBER_IDENTITY,
) -> GraphManifest:
    left = apply_evolution(
        _routed_manifest_a(), canonical_map_to_ops(_ROUTED_CANONICAL)
    )
    op = MergeManifestsOp(
        vertex_equivalences=[_member_equivalence(identity)],
    )
    return merge_manifests(
        left, _manifest_b(), op, canonical_maps=[("left", _ROUTED_CANONICAL)]
    )


def _map_branches(
    identity: list[IdentityBranchDecl], rewrite: Callable[[dict], dict]
) -> list[IdentityBranchDecl]:
    """*identity* with each stepped branch's per-resource entries rewritten."""
    out: list[IdentityBranchDecl] = []
    for branch in identity:
        if isinstance(branch, DerivedBranch):
            branch = branch.model_copy(
                update={"sources": rewrite(dict(branch.sources))}, deep=True
            )
        elif isinstance(branch, LocalKeyBranch):
            branch = branch.model_copy(
                update={"local_key": rewrite(dict(branch.local_key))}, deep=True
            )
        out.append(branch)
    return out


def _renamed_resource(
    identity: list[IdentityBranchDecl], old: str, new: str
) -> list[IdentityBranchDecl]:
    """*identity* with resource *old* spelled *new* in every stepped branch."""

    def rename(entries: dict) -> dict:
        return {
            (new if name == old else name): entry for name, entry in entries.items()
        }

    return _map_branches(identity, rename)


_SHARED_COLUMN_VIEW = [
    {
        "records": [
            {"kind": "firm", "firm_id": "f1", "secondary_key": "abc_alpha"},
            {"kind": "firm", "firm_id": "f2", "secondary_key": "alpha"},
            {"kind": "shop", "shop_id": "s1", "secondary_key": "def_beta"},
            # A shop row carrying the *firm* marker: same bytes as f1's key.
            {"kind": "shop", "shop_id": "s2", "secondary_key": "abc_alpha"},
            {"kind": "person", "person_id": "p1", "secondary_key": "abc_9"},
        ]
    }
]


class TestMemberKeyedRoutedFusion:
    def test_each_member_fuses_through_its_own_marker(self) -> None:
        union = _build_member_union()

        view = _cast(union, "r_view", _SHARED_COLUMN_VIEW)
        orgs = _cast(
            union,
            "r_b",
            [
                {"org_id": "o1", "shared_raw": "alpha"},
                {"org_id": "o2", "shared_raw": "beta"},
            ],
        )

        by_local = {doc["local_key"]: doc for doc in view}
        assert by_local["firm:f1"]["match_key"] == "alpha"
        assert by_local["firm:f1"]["id"] == orgs[0]["id"]
        assert by_local["shop:s1"]["match_key"] == "beta"
        assert by_local["shop:s1"]["id"] == orgs[1]["id"]

    def test_the_member_not_the_marker_decides(self) -> None:
        """A shop row with the firm marker does not become that firm.

        Under a value-only gate ``abc_alpha`` would admit it and strip the
        marker, fusing s2 with f1 and o1. Keyed by member, the shop derivation
        requires ``def_``, so s2 falls through to its own local key.
        """
        union = _build_member_union()

        view = _cast(union, "r_view", _SHARED_COLUMN_VIEW)

        by_local = {doc["local_key"]: doc for doc in view}
        assert by_local["shop:s2"].get("match_key") is None
        assert by_local["shop:s2"]["id"] != by_local["firm:f1"]["id"]

    def test_an_unmarked_value_still_falls_through(self) -> None:
        union = _build_member_union()

        view = _cast(union, "r_view", _SHARED_COLUMN_VIEW)

        by_local = {doc["local_key"]: doc for doc in view}
        assert by_local["firm:f2"].get("match_key") is None
        assert by_local["firm:f2"]["id"] != by_local["firm:f1"]["id"]

    def test_the_unaligned_member_never_runs_the_derivations(self) -> None:
        union = _build_member_union()
        caster = DocumentCaster(union.require_ingestion_model())

        result = asyncio.run(
            caster.cast_batch(_SHARED_COLUMN_VIEW, "r_view", params=IngestionParams())
        )

        people = result.graph.vertices["Person"]
        assert [p["person_id"] for p in people] == ["p1"]
        assert "match_key" not in people[0]
        assert "local_key" not in people[0]

    def test_records_collapse_to_the_expected_vertices(self) -> None:
        union = _build_member_union()

        view = _cast(union, "r_view", _SHARED_COLUMN_VIEW)
        orgs = _cast(
            union,
            "r_b",
            [
                {"org_id": "o1", "shared_raw": "alpha"},
                {"org_id": "o2", "shared_raw": "beta"},
            ],
        )

        ids = [doc["id"] for doc in [*view, *orgs]]
        assert len(ids) == 6
        assert len(set(ids)) == 4

    def test_an_untagged_local_key_is_the_raw_value(self) -> None:
        """``tag=None``: the author claims the values are globally unique."""
        identity: list[IdentityBranchDecl] = [
            _MEMBER_IDENTITY[0],
            LocalKeyBranch(
                local_key={
                    "r_view": {
                        "Company": LocalKeySource(field="firm_id", tag=None),
                        "Shop": LocalKeySource(field="shop_id", tag="shop"),
                    },
                    "r_b": LocalKeySource(field="org_id", tag="b"),
                }
            ),
        ]
        union = _build_member_union(identity)

        view = _cast(union, "r_view", _SHARED_COLUMN_VIEW)

        assert {doc["local_key"] for doc in view} == {"f1", "f2", "shop:s1", "shop:s2"}

    def test_the_rename_policy_is_honoured_when_resolving_members(self) -> None:
        """Member keys name resources as the union names them."""
        left = apply_evolution(
            _routed_manifest_a(), canonical_map_to_ops(_ROUTED_CANONICAL)
        )
        right = _manifest_b()
        right.require_ingestion_model().resources[0].name = "r_view"
        op = MergeManifestsOp(
            vertex_equivalences=[
                _member_equivalence(
                    _renamed_resource(_MEMBER_IDENTITY, "r_b", "r_orgs")
                )
            ],
            renames={"right": {"resources": {"r_view": "r_orgs"}}},
        )

        union = merge_manifests(
            left, right, op, canonical_maps=[("left", _ROUTED_CANONICAL)]
        )

        names = {r.name for r in union.require_ingestion_model().resources}
        assert names == {"r_view", "r_orgs"}
        view = _cast(union, "r_view", _SHARED_COLUMN_VIEW)
        assert {doc["local_key"] for doc in view} >= {"firm:f1", "shop:s1"}


def _rekeyed(identity: list[IdentityBranchDecl], key: str) -> list[IdentityBranchDecl]:
    """*identity* with its ``Company`` member key spelled *key*."""

    def rekey(entries: dict) -> dict:
        entry = entries["r_view"]
        assert isinstance(entry, dict)
        return {
            **entries,
            "r_view": {
                (key if member == "Company" else member): spec
                for member, spec in entry.items()
            },
        }

    return _map_branches(identity, rekey)


def _raw_member_union(identity: list[IdentityBranchDecl]) -> GraphManifest:
    """The member union authored against the raw left side: `Firm`, no `into`."""
    op = MergeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(
                left=["Firm", "Shop"], right="Org", identity=list(identity)
            )
        ],
        canonical_maps={"left": _ROUTED_CANONICAL},
    )
    return merge_manifests(_routed_manifest_a(), _manifest_b(), op)


class TestMemberKeysResolveThroughTheMap:
    """A member key is the member's own name on its side, or its canonical one."""

    @pytest.mark.parametrize("key", ["Firm", "Company"])
    def test_a_raw_side_takes_the_own_or_the_canonical_key(self, key: str) -> None:
        union = _raw_member_union(_rekeyed(_MEMBER_IDENTITY, key))
        reference = _build_member_union()
        assert _cast(union, "r_view", _SHARED_COLUMN_VIEW) == _cast(
            reference, "r_view", _SHARED_COLUMN_VIEW
        )

    def test_two_spellings_of_one_member_are_refused(self) -> None:
        identity = _rekeyed(_MEMBER_IDENTITY, "Firm")
        derived = identity[0]
        assert isinstance(derived, DerivedBranch)
        entry = derived.sources["r_view"]
        assert isinstance(entry, dict)
        identity[0] = derived.model_copy(
            update={
                "sources": {
                    **derived.sources,
                    "r_view": {**entry, "Company": _marker("abc_")},
                }
            }
        )
        with pytest.raises(AlignmentConflictError, match="keyed twice"):
            _raw_member_union(identity)


def _dynamic_manifest_a() -> GraphManifest:
    """:func:`_routed_manifest_a` with no table: ``kind`` *is* the class name."""
    manifest = _routed_manifest_a()
    manifest.require_ingestion_model().resources[0].pipeline = [
        {
            "descend": {
                "key": "records",
                "apply": [
                    {
                        "vertex_router": {
                            "type_field": "kind",
                            "keep_fields": [
                                "firm_id",
                                "shop_id",
                                "person_id",
                                "secondary_key",
                            ],
                        }
                    }
                ],
            }
        }
    ]
    manifest.finish_init()
    return manifest


def _build_dynamic_member_union() -> GraphManifest:
    left = apply_evolution(
        _dynamic_manifest_a(), canonical_map_to_ops(_ROUTED_CANONICAL)
    )
    op = MergeManifestsOp(
        vertex_equivalences=[_member_equivalence(_MEMBER_IDENTITY)],
    )
    return merge_manifests(
        left, _manifest_b(), op, canonical_maps=[("left", _ROUTED_CANONICAL)]
    )


_DYNAMIC_VIEW = [
    {
        "records": [
            {"kind": "Firm", "firm_id": "f1", "secondary_key": "abc_alpha"},
            {"kind": "Firm", "firm_id": "f2", "secondary_key": "alpha"},
            {"kind": "Shop", "shop_id": "s1", "secondary_key": "def_beta"},
            {"kind": "Shop", "shop_id": "s2", "secondary_key": "abc_alpha"},
            {"kind": "Person", "person_id": "p1", "secondary_key": "abc_9"},
        ]
    }
]


class TestDynamicRouterFusion:
    """The router has no table; the source's own class names are its raw values.

    The canonical map and the merge each rename a class the router passed
    through, and each writes the entry that keeps the raw value routing.
    """

    def _router(self, union: GraphManifest) -> dict:
        from graflo.architecture.contract.ingestion.steps.normalize import (
            normalize_actor_step,
        )

        pipeline = union.require_ingestion_model().resources[0].pipeline
        descend = normalize_actor_step(dict(pipeline[0]))
        return normalize_actor_step(dict(descend["pipeline"][0]))

    def test_the_renames_are_written_into_the_table(self) -> None:
        router = self._router(_build_dynamic_member_union())

        assert router["type_map"] == {
            "Firm": "Company",
            "Shop": "Company",
            # Merge then closes the router over the left side as handed in:
            # its other classes, as themselves.
            "Company": "Company",
            "Person": "Person",
        }
        assert router["type_map_only"] is True

    def test_each_member_fuses_through_its_own_marker(self) -> None:
        union = _build_dynamic_member_union()

        view = _cast(union, "r_view", _DYNAMIC_VIEW)
        orgs = _cast(
            union,
            "r_b",
            [
                {"org_id": "o1", "shared_raw": "alpha"},
                {"org_id": "o2", "shared_raw": "beta"},
            ],
        )

        by_local = {doc["local_key"]: doc for doc in view}
        assert by_local["firm:f1"]["match_key"] == "alpha"
        assert by_local["firm:f1"]["id"] == orgs[0]["id"]
        assert by_local["shop:s1"]["match_key"] == "beta"
        assert by_local["shop:s1"]["id"] == orgs[1]["id"]
        assert by_local["shop:s2"].get("match_key") is None
        assert by_local["firm:f2"].get("match_key") is None

    def test_the_unaligned_class_still_routes_and_derives_nothing(self) -> None:
        union = _build_dynamic_member_union()
        caster = DocumentCaster(union.require_ingestion_model())

        result = asyncio.run(
            caster.cast_batch(_DYNAMIC_VIEW, "r_view", params=IngestionParams())
        )

        people = result.graph.vertices["Person"]
        assert [p["person_id"] for p in people] == ["p1"]
        assert "match_key" not in people[0]
        assert "local_key" not in people[0]
