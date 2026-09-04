"""End-to-end: union of two manifests with conditional entity equivalence.

The full recipe composed from fundamental ops — canonicalize the left
manifest, validate the compose op against the canonical map, compose, then
apply an identity alignment: canonical attribute declarations, per-resource
derivation transforms, a priority funnel over canonical attributes only, and
per-side secondary identities. The class definition stays side-agnostic;
records that pass a derivation gate fuse with the right-hand entities (same
synthetic id), records that fail it keep a namespaced local key.
"""

from __future__ import annotations

import asyncio

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import (
    AlignmentAttribute,
    CanonicalMap,
    ComposeManifestsOp,
    DerivationSpec,
    IdentityAlignment,
    LocalKeySource,
    LocalKeySpec,
    VertexEquivalence,
    alignment_to_ops,
    apply_evolution,
    canonical_map_to_ops,
    compose_manifests,
)
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

_ALIGNMENT = IdentityAlignment(
    vertex="Company",
    attributes=[
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
    local_key=LocalKeySpec(
        sources={
            # RAW doc field: renamed documents still carry firm_id.
            "r_a": LocalKeySource(field="firm_id", tag="a"),
            "r_b": LocalKeySource(field="org_id", tag="b"),
        }
    ),
    secondary_identities={
        "by_company_id": ["company_id"],
        "by_org_id": ["org_id"],
    },
)


def _compose_union() -> GraphManifest:
    """A composed union *without* the identity alignment applied yet.

    Declares a throwaway ``identity=["company_id"]`` on the cluster: `Company`
    and `Org` disagree on their raw identity field, and nothing here promises
    to resolve it (callers that want the resolved identity use
    :func:`_build_union` instead, which folds the alignment into the same
    compose op), so an explicit placeholder is what this unaligned union
    needs in order to compose at all.
    """
    canonical_a = apply_evolution(_manifest_a(), canonical_map_to_ops(_CANONICAL))
    right = _manifest_b()
    op = ComposeManifestsOp(
        vertices=[
            VertexEquivalence(
                left="Company", right="Org", into="Company", identity=["company_id"]
            )
        ]
    )
    return compose_manifests(
        canonical_a, right, op, canonical_maps=[("left", _CANONICAL)]
    )


def _build_union(alignment: IdentityAlignment = _ALIGNMENT) -> GraphManifest:
    canonical_a = apply_evolution(_manifest_a(), canonical_map_to_ops(_CANONICAL))
    right = _manifest_b()
    op = ComposeManifestsOp(
        vertices=[VertexEquivalence(left="Company", right="Org", into="Company")],
        identity_alignments=[alignment],
    )
    return compose_manifests(
        canonical_a, right, op, canonical_maps=[("left", _CANONICAL)]
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
        # The retired side keys survive as lookup-only secondary identities.
        assert {s.name for s in vc.secondary_identities("Company")} == {
            "by_company_id",
            "by_org_id",
        }

    def test_alignment_validates_against_the_union(self) -> None:
        """The composer runs validate_alignment when handed the manifest."""
        union = _compose_union()
        ops = alignment_to_ops(_ALIGNMENT, manifest=union, canonical_maps=[_CANONICAL])
        assert len(ops) == 4


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


class TestPriorityFunnel:
    """Two aligned attributes: priority semantics, including the known trap."""

    def _two_attribute_alignment(self) -> IdentityAlignment:
        return IdentityAlignment(
            vertex="Company",
            attributes=[
                AlignmentAttribute(
                    into="c1",
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
                ),
                AlignmentAttribute(
                    into="c2",
                    sources={
                        "r_a": DerivationSpec(
                            input=["firm_id", "tax_no"], params={"prefix": ""}
                        ),
                        "r_b": DerivationSpec(
                            input=["org_id", "tax_no"], params={"prefix": ""}
                        ),
                    },
                ),
            ],
            local_key=LocalKeySpec(
                sources={
                    "r_a": LocalKeySource(field="firm_id", tag="a"),
                    "r_b": LocalKeySource(field="org_id", tag="b"),
                }
            ),
        )

    def test_same_top_priority_attribute_fuses(self) -> None:
        union = _build_union(self._two_attribute_alignment())
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
        """
        union = _build_union(self._two_attribute_alignment())
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


class TestOrderingRationale:
    def test_alignment_cannot_apply_before_compose(self) -> None:
        """The alignment references both sides' resources — only the union
        carries them, so applying it to one side fails loudly."""
        canonical_a = apply_evolution(_manifest_a(), canonical_map_to_ops(_CANONICAL))
        with pytest.raises(ValueError, match="unknown resources"):
            apply_evolution(
                canonical_a,
                alignment_to_ops(_ALIGNMENT, manifest=canonical_a),
            )


# --------------------------------------------------------------------------- #
# A routed source: one vertex_router, nested, two branches collapsing onto the
# aligned class. This is the shape that silently dropped every routed record
# before derivations became level-aware — the manifest looked right while the
# emitted graph had no identities at all.
# --------------------------------------------------------------------------- #

_ROUTED_CANONICAL = CanonicalMap(
    vertices={"Firm": "Company"},
    properties={"Firm": {"firm_id": "company_id"}},
)

_ROUTED_ALIGNMENT = IdentityAlignment(
    vertex="Company",
    attributes=[
        AlignmentAttribute(
            into="match_key",
            sources={
                # Each branch of the view carries the shared key in its own
                # column; the other is empty, which is what selects.
                "r_view": [
                    DerivationSpec(
                        input=["secondary_key", "firm_ref"],
                        params={"prefix": "abc_", "strip_prefix": "ABC-"},
                    ),
                    DerivationSpec(
                        input=["secondary_key", "shop_ref"],
                        params={"prefix": "abc_", "strip_prefix": "ABC-"},
                    ),
                ],
                "r_b": DerivationSpec(
                    input=["org_id", "shared_raw"],
                    params={"prefix": "", "strip_prefix": "ABC-"},
                ),
            },
        )
    ],
    local_key=LocalKeySpec(
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
    secondary_identities={"by_company_id": ["company_id"], "by_org_id": ["org_id"]},
)


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


def _build_routed_union() -> GraphManifest:
    left = apply_evolution(
        _routed_manifest_a(), canonical_map_to_ops(_ROUTED_CANONICAL)
    )
    op = ComposeManifestsOp(
        vertices=[
            VertexEquivalence(left=["Company", "Shop"], right="Org", into="Company")
        ],
        allow_merges=True,
        identity_alignments=[_ROUTED_ALIGNMENT],
    )
    return compose_manifests(
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
