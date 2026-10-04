"""A digest identity keys on a synthetic ``id``; a real ``id`` property would be lost.

The cast discards a record's own ``id`` for the digest, so a declared ``id``
column would never be stored. Each way of arriving at a digest identity over a
declared ``id`` is refused.
"""

from __future__ import annotations

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import (
    IdentityReplacement,
    ReplaceIdentityOp,
    apply_evolution,
)
from graflo.architecture.schema.vertex import VertexConfig

_FUNNEL = {"branches": [{"id": "by_email", "fields": ["email"]}]}


def _manifest(vertex: dict) -> GraphManifest:
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "people", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {"vertices": [vertex]},
                    "edge_config": {"edges": []},
                },
            }
        }
    )
    manifest.finish_init()
    return manifest


def _rekey(manifest: GraphManifest, to: dict) -> GraphManifest:
    return apply_evolution(
        manifest,
        [
            ReplaceIdentityOp(
                replacements={"person": IdentityReplacement.model_validate({"to": to})}
            )
        ],
        bump_version=False,
    )


@pytest.mark.parametrize(
    "to",
    [
        {"mode": "funnel", "funnel": _FUNNEL},
        {"mode": "hash", "hash_from": ["email"]},
    ],
    ids=["funnel", "hash"],
)
def test_re_keying_onto_a_digest_over_a_declared_id_is_refused(to) -> None:
    manifest = _manifest(
        {"name": "person", "properties": ["id", "email"], "identity": ["email"]}
    )
    with pytest.raises(ValueError, match="declares a property `id`"):
        _rekey(manifest, to)


def test_re_keying_onto_a_digest_without_an_id_is_accepted() -> None:
    manifest = _manifest(
        {"name": "person", "properties": ["email", "name"], "identity": ["email"]}
    )
    out = _rekey(manifest, {"mode": "funnel", "funnel": _FUNNEL})
    vertex = out.require_schema().core_schema.vertex_config["person"]
    assert vertex.identity == ["id"]


def test_re_keying_a_digest_vertex_again_is_accepted() -> None:
    """Its ``id`` is the synthetic key itself, not a column records carry."""
    manifest = _manifest(
        {
            "name": "person",
            "properties": ["email", "name"],
            "hash_identity_properties": ["email"],
        }
    )
    out = _rekey(manifest, {"mode": "funnel", "funnel": _FUNNEL})
    assert out.require_schema().core_schema.vertex_config["person"].identity_funnel


@pytest.mark.parametrize(
    "keying",
    [{"identity_funnel": _FUNNEL}, {"hash_identity_properties": ["email"]}],
    ids=["funnel", "hash"],
)
def test_a_digest_vertex_declaring_id_is_refused(keying) -> None:
    with pytest.raises(ValueError, match="declares a property `id`"):
        VertexConfig.model_validate(
            {"vertices": [{"name": "person", "properties": ["id", "email"], **keying}]}
        )


def test_a_normalized_digest_vertex_reloads() -> None:
    config = VertexConfig.model_validate(
        {
            "vertices": [
                {"name": "person", "properties": ["email"], "identity_funnel": _FUNNEL}
            ]
        }
    )
    reloaded = VertexConfig.model_validate(config.to_dict(skip_defaults=False))
    assert reloaded["person"].identity == ["id"]


def test_the_differ_reports_a_re_key_onto_a_digest_over_a_natural_id() -> None:
    """``id`` was the natural key: the transition is refused, not recorded."""
    from graflo.architecture.evolution.autogenerate import diff_manifests_verified

    base = _manifest(
        {"name": "person", "properties": ["id", "email"], "identity": ["id"]}
    )
    target = _manifest(
        {
            "name": "person",
            "properties": ["email"],
            "hash_identity_properties": ["email"],
        }
    )

    _, warnings = diff_manifests_verified(base, target)

    assert any("declares a property `id`" in warning for warning in warnings)
