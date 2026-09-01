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
    AlignmentRow,
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
    validate_compose_against_canonical_map,
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
    rows=[
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
    canonical_a = apply_evolution(_manifest_a(), canonical_map_to_ops(_CANONICAL))
    right = _manifest_b()
    op = ComposeManifestsOp(
        vertices=[VertexEquivalence(left="Company", right="Org", into="Company")]
    )
    validate_compose_against_canonical_map(
        _CANONICAL, op, left=canonical_a, right=right
    )
    return compose_manifests(canonical_a, right, op)


def _build_union(alignment: IdentityAlignment = _ALIGNMENT) -> GraphManifest:
    union = _compose_union()
    return apply_evolution(
        union,
        alignment_to_ops(alignment, manifest=union, canonical_maps=[_CANONICAL]),
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

    def _two_row_alignment(self) -> IdentityAlignment:
        return IdentityAlignment(
            vertex="Company",
            rows=[
                AlignmentRow(
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
                AlignmentRow(
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
        union = _build_union(self._two_row_alignment())
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
        union = _build_union(self._two_row_alignment())
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
