"""A merged type keys its records on fields every member's records carry.

Two members with different natural keys, or a natural key and a digest, have no
such key unless one is declared; the merge refuses rather than keying the merged
type on fields some records lack.
"""

from __future__ import annotations

from typing import Any

import pytest

from graflo.architecture.contract.ingestion.steps import VertexActorConfig
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import (
    MergeIdentityError,
    MergeManifestsOp,
    MergeVerticesOp,
    VertexEquivalence,
    apply_evolution,
    merge_manifests,
)
from graflo.architecture.evolution.merge_core import VertexMergeError
from graflo.architecture.evolution.naming_graph import MergeNamingError

NATURAL = {"name": "X", "properties": ["k", "x", "y"], "identity": ["k"]}
OTHER_NATURAL = {"name": "W", "properties": ["w", "x", "y"], "identity": ["w"]}
HASHED = {"name": "Y", "properties": ["x", "y"], "hash_identity_properties": ["x", "y"]}
SAME_KEY = {"name": "V", "properties": ["k", "v"], "identity": ["k"]}


def _manifest(name: str, *vertices: dict[str, Any]) -> GraphManifest:
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": name, "version": "1.0.0"},
                "graph": {
                    "vertex_config": {"vertices": list(vertices)},
                    "edge_config": {"edges": []},
                },
            },
            "ingestion_model": {
                "resources": [
                    {"name": f"r_{v['name']}", "pipeline": [{"vertex": v["name"]}]}
                    for v in vertices
                ]
            },
        }
    )
    manifest.finish_init()
    return manifest


class TestMergeManifests:
    def test_a_natural_key_and_a_digest_need_a_declared_identity(self) -> None:
        op = MergeManifestsOp(
            vertex_equivalences=[VertexEquivalence(left="X", right="Y", into="Z")]
        )

        with pytest.raises(MergeIdentityError, match="key on a digest"):
            merge_manifests(_manifest("a", NATURAL), _manifest("b", HASHED), op)

    def test_a_declared_identity_settles_it(self) -> None:
        op = MergeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(left="X", right="Y", into="Z", identity=[["x", "y"]])
            ]
        )

        out = merge_manifests(_manifest("a", NATURAL), _manifest("b", HASHED), op)

        assert out.require_schema().core_schema.vertex_config["Z"].identity == [
            "x",
            "y",
        ]


class TestMergeVertices:
    @pytest.mark.parametrize(
        "other",
        [pytest.param(OTHER_NATURAL, id="natural"), pytest.param(HASHED, id="digest")],
    )
    def test_members_keyed_differently_are_refused(self, other: dict) -> None:
        manifest = _manifest("a", NATURAL, other)

        with pytest.raises(VertexMergeError, match="key their records differently"):
            apply_evolution(
                manifest,
                [MergeVerticesOp(sources=[other["name"]], into="X")],
                bump_version=False,
            )

    def test_members_with_one_key_merge(self) -> None:
        out = apply_evolution(
            _manifest("a", NATURAL, SAME_KEY),
            [MergeVerticesOp(sources=["V"], into="X")],
            bump_version=False,
        )

        assert out.require_schema().core_schema.vertex_config["X"].identity == ["k"]


class TestADigestKeyStaysFindable:
    """A member keyed by a digest keeps the fields it was found by, as natural keys do."""

    HASHED_WITH_K = {
        "name": "Y",
        "properties": ["k", "x", "y"],
        "hash_identity_properties": ["x", "y"],
    }

    def _merged(self, member: dict, identity: list) -> Any:
        op = MergeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(left="X", right="Y", into="Z", identity=identity)
            ]
        )
        out = merge_manifests(_manifest("a", NATURAL), _manifest("b", member), op)
        return out.require_schema().core_schema.vertex_config["Z"]

    def test_its_hash_fields_become_a_lookup_key(self) -> None:
        z = self._merged(self.HASHED_WITH_K, ["k"])

        assert [(s.name, s.fields) for s in z.secondary_identities] == [
            ("b", ["b__x", "b__y"])
        ]

    def test_nothing_is_kept_when_the_new_key_is_its_hash_fields(self) -> None:
        z = self._merged(self.HASHED_WITH_K, [["x", "y"]])

        assert z.identity == ["x", "y"]
        assert [s.fields for s in z.secondary_identities] == [["a__k"]]

    def test_a_funnel_of_several_branches_is_refused(self) -> None:
        funnel = {
            "name": "Y",
            "properties": ["k", "x", "y"],
            "identity_funnel": {
                "branches": [
                    {"id": "bx", "fields": ["x"]},
                    {"id": "by", "fields": ["y"]},
                ]
            },
        }

        with pytest.raises(MergeNamingError, match="funnel of 2 branches") as refused:
            self._merged(funnel, ["k"])

        assert refused.value.check == "identity-coverage"


def _with_finds(
    name: str, *steps: tuple[str, str], vertices: list[dict]
) -> GraphManifest:
    """*vertices*, and one resource per ``(vertex, find)`` that finds it by name."""
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": name, "version": "1.0.0"},
                "graph": {
                    "vertex_config": {"vertices": vertices},
                    "edge_config": {"edges": []},
                },
            },
            "ingestion_model": {
                "resources": [
                    {
                        "name": f"r_{vertex}",
                        "pipeline": [{"vertex": vertex, "find": found}],
                    }
                    for vertex, found in steps
                ]
            },
        }
    )
    manifest.finish_init()
    return manifest


def _finds(manifest: GraphManifest) -> dict[str, list[str]]:
    """Per resource, the fields its one ``find`` resolves to on the merged schema."""
    vc = manifest.require_schema().core_schema.vertex_config
    out: dict[str, list[str]] = {}
    for resource in manifest.require_ingestion_model().resources:
        (step,) = resource.pipeline
        assert isinstance(step, VertexActorConfig) and step.find is not None
        entry = vc[step.vertex].secondary_identity(step.find)
        assert entry is not None, (resource.name, step.find)
        out[resource.name] = list(entry.fields)
    return out


EMAIL = {
    "name": "X",
    "properties": ["k", "email"],
    "identity": ["k"],
    "secondary_identities": [{"fields": ["email"]}],
}
PHONE = {
    "name": "Y",
    "properties": ["k", "phone"],
    "identity": ["k"],
    "secondary_identities": [{"fields": ["phone"]}],
}


class TestAFindKeepsItsFieldSet:
    """An unnamed secondary answers to its position; a merge must not re-point it."""

    def test_merge_vertices(self) -> None:
        manifest = _with_finds(
            "a", ("X", "secondary_0"), ("Y", "secondary_0"), vertices=[EMAIL, PHONE]
        )

        out = apply_evolution(
            manifest,
            [MergeVerticesOp(sources=["Y"], into="X", allow_observation_fusion=True)],
            bump_version=False,
        )

        assert _finds(out) == {"r_X": ["email"], "r_Y": ["phone"]}

    def test_merge_manifests(self) -> None:
        left = _with_finds("a", ("X", "secondary_0"), vertices=[EMAIL])
        right = _with_finds("b", ("Y", "secondary_0"), vertices=[PHONE])
        op = MergeManifestsOp(
            vertex_equivalences=[VertexEquivalence(left="X", right="Y", into="Z")]
        )

        out = merge_manifests(left, right, op)

        assert _finds(out) == {"r_X": ["email"], "r_Y": ["phone"]}
