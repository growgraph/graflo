"""End-to-end: union of two manifests with conditional entity equivalence.

The full recipe — canonicalize the left manifest, validate the compose op
against the canonical map, compose, then install an identity funnel whose
shared branch is fed by a gated normalizing transform. Records that pass the
gate fuse with the right-hand entities (same synthetic id); records that fail
it keep a side-local identity and never collide.
"""

from __future__ import annotations

import asyncio

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import (
    CanonicalMap,
    ComposeManifestsOp,
    FunnelIdentityTarget,
    IdentityReplacement,
    ReplaceIdentityOp,
    VertexEquivalence,
    apply_evolution,
    canonical_map_to_ops,
    compose_manifests,
    validate_compose_against_canonical_map,
)
from graflo.architecture.schema.identity_funnel import IdentityBranch, IdentityFunnel
from graflo.hq.document_caster import DocumentCaster
from graflo.hq.ingestion_parameters import IngestionParams

_GATE_TRANSFORM = {
    "name": "gate_match_key",
    "module": "graflo.util.transform",
    "foo": "gated_normalized_key",
    "params": {"prefix": "abc_", "strip_prefix": "ABC-"},
    "input": ["secondary_key", "shared_raw"],
    "output": ["match_key"],
}

_NORMALIZE_TRANSFORM = {
    # Same normalization, always-true gate: the right side has no condition.
    "name": "normalize_match_key",
    "module": "graflo.util.transform",
    "foo": "gated_normalized_key",
    "params": {"prefix": "", "strip_prefix": "ABC-"},
    "input": ["org_id", "shared_raw"],
    "output": ["match_key"],
}


def _manifest_a() -> GraphManifest:
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
                                    "match_key",
                                ],
                                "identity": ["firm_id"],
                            }
                        ]
                    },
                    "edge_config": {"edges": []},
                },
            },
            "ingestion_model": {
                "resources": [
                    {
                        "name": "r_a",
                        "pipeline": [
                            {"transform": {"call": {"use": "gate_match_key"}}},
                            {"vertex": "Firm"},
                        ],
                    }
                ],
                "transforms": [_GATE_TRANSFORM],
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
                                "properties": ["org_id", "shared_raw", "match_key"],
                                "identity": ["org_id"],
                            }
                        ]
                    },
                    "edge_config": {"edges": []},
                },
            },
            "ingestion_model": {
                "resources": [
                    {
                        "name": "r_b",
                        "pipeline": [
                            {"transform": {"call": {"use": "normalize_match_key"}}},
                            {"vertex": "Org"},
                        ],
                    }
                ],
                "transforms": [_NORMALIZE_TRANSFORM],
            },
        }
    )
    manifest.finish_init()
    return manifest


_CANONICAL = CanonicalMap(
    vertices={"Firm": "Company"},
    properties={"Firm": {"firm_id": "company_id"}},
)

_FUNNEL = IdentityFunnel(
    branches=[
        IdentityBranch(id="shared", fields=["match_key"]),
        IdentityBranch(id="a_local", fields=["company_id"]),
        IdentityBranch(id="b_local", fields=["org_id"]),
    ]
)


def _build_union() -> GraphManifest:
    canonical_a = apply_evolution(_manifest_a(), canonical_map_to_ops(_CANONICAL))
    right = _manifest_b()
    op = ComposeManifestsOp(
        vertices=[VertexEquivalence(left="Company", right="Org", into="Company")]
    )
    validate_compose_against_canonical_map(
        _CANONICAL, op, left=canonical_a, right=right
    )
    union = compose_manifests(canonical_a, right, op)
    return apply_evolution(
        union,
        [
            ReplaceIdentityOp(
                vertices={
                    "Company": IdentityReplacement(
                        to=FunnelIdentityTarget(funnel=_FUNNEL),
                        retire="keep",
                    )
                }
            )
        ],
    )


def _cast(manifest: GraphManifest, resource: str, rows: list[dict]) -> list[dict]:
    caster = DocumentCaster(manifest.require_ingestion_model())
    result = asyncio.run(caster.cast_batch(rows, resource, params=IngestionParams()))
    return list(result.graph.vertices.get("Company", []))


class TestComposedSchema:
    def test_union_schema_shape(self) -> None:
        union = _build_union()
        assert union.graph_schema is not None
        vc = union.graph_schema.core_schema.vertex_config
        assert vc.vertex_set == {"Company"}
        assert vc.identity_fields("Company") == ["id"]
        vertex = next(v for v in vc.vertex_list if v.name == "Company")
        assert vertex.identity_funnel is not None
        assert [b.id for b in vertex.identity_funnel.branches] == [
            "shared",
            "a_local",
            "b_local",
        ]
        assert {"company_id", "org_id", "match_key", "shared_raw"} <= set(
            vc.property_names("Company")
        )

    def test_full_funnel_cannot_pre_exist_on_one_side(self) -> None:
        """Locks in the ordering rationale for installing the funnel post-compose.

        The equivalence funnel references both sides' local keys, and
        ``replace_identity`` refuses to key a vertex on properties it does not
        declare — so the three-branch funnel is only expressible on the union.
        """
        canonical_a = apply_evolution(_manifest_a(), canonical_map_to_ops(_CANONICAL))
        with pytest.raises(ValueError, match="does not declare"):
            apply_evolution(
                canonical_a,
                [
                    ReplaceIdentityOp(
                        vertices={
                            "Company": IdentityReplacement(
                                to=FunnelIdentityTarget(funnel=_FUNNEL), retire="keep"
                            )
                        }
                    )
                ],
            )


class TestConditionalFusion:
    def test_abc_record_fuses_with_right_and_non_abc_stays_local(self) -> None:
        union = _build_union()

        a_rows = [
            {"company_id": "f1", "secondary_key": "abc_7", "shared_raw": "ABC-Alpha"},
            {"company_id": "f2", "secondary_key": "zz9", "shared_raw": "ABC-Alpha"},
        ]
        b_rows = [{"org_id": "o1", "shared_raw": "ABC-ALPHA"}]

        a_docs = _cast(union, "r_a", a_rows)
        b_docs = _cast(union, "r_b", b_rows)

        assert len(a_docs) == 2, "non-abc_ record must still be ingested"
        assert len(b_docs) == 1

        by_company = {doc["company_id"]: doc for doc in a_docs}
        abc_doc, non_abc_doc = by_company["f1"], by_company["f2"]
        b_doc = b_docs[0]

        # Every record ends up with a synthetic funnel id.
        assert all("id" in doc for doc in (abc_doc, non_abc_doc, b_doc))

        # Conditional equivalence: abc_-A and B share the shared-branch digest...
        assert abc_doc["match_key"] == "alpha"
        assert abc_doc["id"] == b_doc["id"]

        # ...while the non-abc_ record keys off its local branch, despite
        # carrying the same raw shared value.
        assert non_abc_doc.get("match_key") is None
        assert non_abc_doc["id"] != abc_doc["id"]
        assert non_abc_doc["id"] != b_doc["id"]
