"""Cross-resource vertex identity discovery.

The whole module is proposal-only: nothing here may leak into the write path,
and a key is only ever proven by exact equality after normalization.
"""

from __future__ import annotations

import random

import pytest

from graflo.architecture.onto_sample import (
    ResourceSample,
    SourceSample,
    profile_sample,
)
from graflo.architecture.schema.vertex import Field, FieldType, Vertex
from graflo.db.cross_resource_identity import (
    CrossResourceIdentityConfig,
    CrossResourceIdentityInferencer,
    apply_proposal_to_vertex,
    infer_from_source_sample,
    name_similarity,
    normalize_for_match,
    value_jaccard,
)

N = 120  # comfortably above the default min_sample_size


def _config(**kwargs) -> CrossResourceIdentityConfig:
    payload = {"min_sample_size": 20, "n_boots": 3}
    payload.update(kwargs)
    return CrossResourceIdentityConfig(**payload)


def _inferencer(**kwargs) -> CrossResourceIdentityInferencer:
    return CrossResourceIdentityInferencer(_config(**kwargs), rng=random.Random(17))


def _shared_email() -> dict[str, list[dict]]:
    """Both resources carry the same email column under different names."""
    crm = [
        {"email": f"user{i}@example.com", "full_name": f"User {i}"} for i in range(N)
    ]
    billing = [
        {"email_address": f"user{i}@example.com", "amount": i * 3} for i in range(N)
    ]
    return {"crm": crm, "billing": billing}


# ---------------------------------------------------------------- helpers


class TestNormalization:
    def test_case_and_whitespace_are_folded(self) -> None:
        assert normalize_for_match("  Ada@Example.COM ") == "ada@example.com"

    def test_empty_and_null_are_unusable(self) -> None:
        assert normalize_for_match(None) is None
        assert normalize_for_match("   ") is None

    def test_digits_only_strips_phone_formatting(self) -> None:
        assert normalize_for_match("+44 (163) 296-0001", digits_only=True) == (
            "441632960001"
        )

    def test_uuid_case_is_unified(self) -> None:
        upper = "550E8400-E29B-41D4-A716-446655440000"
        assert normalize_for_match(upper) == upper.lower()


class TestScoring:
    def test_token_overlap_beats_raw_character_ratio(self) -> None:
        assert name_similarity("customer_email", "email_address") > 0.3

    def test_identical_names_score_one(self) -> None:
        assert name_similarity("email", "email") == 1.0

    def test_value_jaccard_counts_normalized_overlap(self) -> None:
        assert value_jaccard(["A@B.C", "d@e.f"], ["a@b.c"]) == pytest.approx(0.5)

    def test_value_jaccard_is_zero_without_usable_values(self) -> None:
        assert value_jaccard([None, ""], ["a"]) == 0.0


# ---------------------------------------------------------------- inference


class TestSharedKeyDiscovery:
    def test_two_resources_sharing_email_propose_a_natural_key(self) -> None:
        proposal = _inferencer().infer(_shared_email(), vertex_name="party")

        assert proposal.strategy == "natural"
        assert proposal.identity == ["email"]
        assert proposal.confidence > 0.0

    def test_aligned_columns_are_renamed_to_one_canonical_name(self) -> None:
        proposal = _inferencer().infer(_shared_email(), vertex_name="party")

        assert proposal.resource_field_maps["billing"] == {"email_address": "email"}
        assert proposal.suggested_transforms == [
            {
                "resource": "billing",
                "transform": {"rename": {"email_address": "email"}},
            }
        ]

    def test_composite_key_when_no_column_is_unique_alone(self) -> None:
        """The deep rule: score tuples, not columns."""
        orgs = ["acme", "globex", "initech"]
        left = [
            {"org": orgs[i % 3], "code": f"C{i // 3}", "label": f"L{i}"}
            for i in range(N)
        ]
        right = [
            {"org": orgs[i % 3], "code": f"C{i // 3}", "tier": i % 5} for i in range(N)
        ]

        proposal = _inferencer().infer({"a": left, "b": right}, vertex_name="thing")

        assert proposal.strategy == "composite"
        assert set(proposal.identity) == {"org", "code"}
        # Neither field keys the rows on its own — that is the point.
        assert len({row["org"] for row in left}) < len(left)
        assert len({row["code"] for row in left}) < len(left)


class TestFallbacks:
    def test_different_strong_keys_per_resource_propose_a_funnel(self) -> None:
        crm = [
            {"email": f"user{i}@example.com", "region": f"r{i % 4}"} for i in range(N)
        ]
        billing = [
            {"email": None, "region": f"r{i % 4}", "acct": f"A{i}"} for i in range(N)
        ]

        proposal = _inferencer().infer(
            {"crm": crm, "billing": billing}, vertex_name="party"
        )

        assert proposal.strategy == "funnel"
        assert proposal.identity == ["id"]
        assert proposal.identity_funnel is not None
        assert len(proposal.identity_funnel.branches) >= 2
        assert proposal.warning is not None

    def test_too_small_sample_is_not_evidence_of_a_key(self) -> None:
        tiny = {
            "a": [{"email": "x@y.z"}],
            "b": [{"email": "x@y.z"}],
        }
        proposal = CrossResourceIdentityInferencer().infer(tiny, vertex_name="party")

        assert proposal.strategy == "no_viable_identity"
        assert proposal.warning is not None
        assert "min_sample_size" in proposal.warning

    def test_a_single_resource_is_not_cross_resource(self) -> None:
        proposal = _inferencer().infer({"only": [{"email": "a@b.c"}] * N})

        assert proposal.strategy == "no_viable_identity"
        assert "at least two" in (proposal.warning or "")

    def test_no_comparable_columns_yields_no_proposal(self) -> None:
        left = [{"alpha": f"a{i}"} for i in range(N)]
        right = [{"zulu": f"z{i}"} for i in range(N)]

        proposal = _inferencer().infer({"a": left, "b": right})

        assert proposal.strategy == "no_viable_identity"


class TestColumnEligibility:
    def test_list_and_long_text_columns_are_ignored(self) -> None:
        blob = "x" * 400
        crm = [
            {"email": f"user{i}@example.com", "tags": ["a", "b"], "notes": blob}
            for i in range(N)
        ]
        billing = [
            {"email": f"user{i}@example.com", "tags": ["c"], "notes": blob}
            for i in range(N)
        ]

        proposal = _inferencer().infer({"crm": crm, "billing": billing})

        assert proposal.identity == ["email"]
        aligned = {a.left_field for a in proposal.alignments}
        assert "tags" not in aligned
        assert "notes" not in aligned


class TestNestedSources:
    def test_flat_docs_makes_a_nested_source_eligible(self) -> None:
        """`ResourceProfile.flat_docs` is the documented bridge for nested sources."""
        nested = [
            {
                "contact": {"email": f"user{i}@example.com"},
                "orders": [{"sku": f"S{i}"}, {"sku": "S-other"}],
            }
            for i in range(N)
        ]
        flat_sample = ResourceSample(resource_name="api", docs=nested)
        profile = profile_sample(flat_sample)
        flat = profile.flat_docs(nested)

        # First occurrence per path, no fan-out over the list.
        assert len(flat) == len(nested)
        assert flat[0]["orders[].sku"] == "S0"

        billing = [
            {"contact.email": f"user{i}@example.com", "amount": i} for i in range(N)
        ]
        proposal = _inferencer().infer({"api": flat, "billing": billing})

        assert proposal.strategy == "natural"
        assert proposal.identity == ["contact.email"]


class TestDeclaredKeys:
    def test_declared_foreign_key_aligns_columns_the_heuristic_would_reject(
        self,
    ) -> None:
        """Declared constraints are ground truth; name similarity is a guess."""
        orders = [{"cust_ref": f"K{i}", "total": i} for i in range(N)]
        customers = [{"pk": f"K{i}", "label": f"n{i}"} for i in range(N)]
        source = SourceSample(
            source_name="erp",
            samples=[
                ResourceSample(
                    resource_name="orders",
                    docs=orders,
                    foreign_keys=[
                        {
                            "field": "cust_ref",
                            "references_resource": "customers",
                            "references_field": "pk",
                        }
                    ],
                ),
                ResourceSample(
                    resource_name="customers", docs=customers, primary_key=["pk"]
                ),
            ],
        )

        proposal = infer_from_source_sample(
            source, vertex_name="customer", config=_config()
        )

        declared = [a for a in proposal.alignments if a.declared]
        assert declared, "a declared foreign key must survive alignment"
        assert declared[0].score == 1.0
        # `cust_ref` and `pk` share no tokens — the heuristic alone would drop them.
        assert name_similarity("cust_ref", "pk") < _config().min_pair_score

    def test_duplicate_resource_names_are_rejected_at_the_source(self) -> None:
        """`samples_by_resource` keys by name, so a duplicate would lose documents."""
        with pytest.raises(ValueError, match="duplicate resource names"):
            SourceSample(
                source_name="dup",
                samples=[
                    ResourceSample(resource_name="a", docs=[{"x": 1}]),
                    ResourceSample(resource_name="a", docs=[{"x": 2}]),
                ],
            )


class TestApplyProposal:
    def test_applying_a_natural_proposal_validates_the_vertex(self) -> None:
        proposal = _inferencer().infer(_shared_email(), vertex_name="party")
        vertex = Vertex(name="party", properties=[Field(name="full_name")])

        updated = apply_proposal_to_vertex(vertex, proposal)

        assert updated.identity == ["email"]
        assert updated.identity_mode == "natural"
        # The identity field must exist as a property after the patch.
        assert "email" in updated.property_names

    def test_applying_a_funnel_proposal_sets_the_funnel(self) -> None:
        crm = [
            {"email": f"user{i}@example.com", "region": f"r{i % 4}"} for i in range(N)
        ]
        billing = [
            {"email": None, "region": f"r{i % 4}", "acct": f"A{i}"} for i in range(N)
        ]
        proposal = _inferencer().infer(
            {"crm": crm, "billing": billing}, vertex_name="party"
        )
        vertex = Vertex(name="party", properties=[Field(name="region")])

        updated = apply_proposal_to_vertex(vertex, proposal)

        assert updated.identity_mode == "hash"
        assert updated.identity_funnel is not None
        assert set(updated.identity_funnel.field_names) <= set(updated.property_names)

    def test_a_no_viable_proposal_refuses_to_apply(self) -> None:
        proposal = _inferencer().infer({"only": [{"email": "a@b.c"}] * N})
        vertex = Vertex(name="party", properties=[Field(name="email")])

        with pytest.raises(ValueError, match="no_viable_identity"):
            apply_proposal_to_vertex(vertex, proposal)

    def test_applied_vertex_survives_a_list_typed_conflict_check(self) -> None:
        """The patch goes through Vertex validation, not a bare field assignment."""
        proposal = _inferencer().infer(_shared_email(), vertex_name="party")
        vertex = Vertex(
            name="party",
            properties=[
                Field(name="email", type=FieldType.LIST, item_type=FieldType.STRING)
            ],
        )

        with pytest.raises(ValueError, match="cannot be used as identity"):
            apply_proposal_to_vertex(vertex, proposal)


class TestDeterminism:
    def test_repeated_runs_agree(self) -> None:
        samples = _shared_email()
        first = _inferencer().infer(samples, vertex_name="party")
        second = _inferencer().infer(samples, vertex_name="party")

        assert first.to_minimal_canonical_dict() == second.to_minimal_canonical_dict()

    def test_resource_order_does_not_change_the_key(self) -> None:
        samples = _shared_email()
        reversed_samples = dict(reversed(list(samples.items())))

        assert (
            _inferencer().infer(samples).identity
            == _inferencer().infer(reversed_samples).identity
        )
