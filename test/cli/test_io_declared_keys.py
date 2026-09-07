"""Writing a manifest so its declarations survive the round trip.

A conformance profile asks two questions -- is an identity declared, is a
direction declared -- that only the authored document can answer, because
``VertexConfig`` fills an unset identity and ``Edge.directed`` defaults to
``True``. That makes serialization load-bearing rather than incidental: a
generated manifest written with ``exclude_defaults`` loses a ``directed: true``
its generator deliberately chose, and reads afterwards as if nobody decided.

These tests pin both halves -- that the compact dump really does lose it, and
that forcing the declared keys really does bring the profile back to a pass.
"""

from __future__ import annotations

import pathlib

import pytest
import yaml

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.profile import check_manifest_config
from graflo.cli.io import DECLARED_KEYS, dump_manifest, manifest_to_dict

CONFIG: dict = {
    "schema": {
        "metadata": {"name": "t", "version": "1.0.0"},
        "graph": {
            "vertex_config": {
                "vertices": [
                    {"name": "A", "properties": [{"name": "id"}], "identity": ["id"]},
                    {"name": "B", "properties": [{"name": "id"}], "identity": ["id"]},
                ]
            },
            "edge_config": {
                "edges": [
                    {"source": "A", "target": "B", "relation": "r", "directed": True},
                    {"source": "B", "target": "A", "relation": "q", "directed": False},
                ]
            },
        },
    }
}


def _manifest() -> GraphManifest:
    manifest = GraphManifest.from_config(CONFIG)
    manifest.finish_init()
    return manifest


def _edges(payload: dict) -> list[dict]:
    return payload["schema"]["core_schema"]["edge_config"]["edges"]


def _a3(payload: dict) -> str:
    report = check_manifest_config(payload, subject="test")
    return next(
        a.status for a in report.assertions if a.id == "declared-directionality"
    ).lower()


def test_a_compact_dump_drops_a_declared_true() -> None:
    """The failure this exists to fix: ``directed: true`` equals the default."""
    edges = _edges(manifest_to_dict(_manifest()))
    assert "directed" not in edges[0]
    # A non-default value was never at risk.
    assert edges[1]["directed"] is False


def test_forcing_the_declared_keys_keeps_both_values() -> None:
    edges = _edges(manifest_to_dict(_manifest(), declare=DECLARED_KEYS))
    assert edges[0]["directed"] is True
    assert edges[1]["directed"] is False


def test_the_profile_verdict_turns_on_this_and_nothing_else() -> None:
    """Same manifest, same assertion, opposite answers -- from serialization alone."""
    manifest = _manifest()
    assert _a3(manifest_to_dict(manifest)) == "fail"
    assert _a3(manifest_to_dict(manifest, declare=DECLARED_KEYS)) == "pass"


def test_forcing_is_opt_in_so_hand_authored_manifests_are_unaffected() -> None:
    """A round trip must not silently turn every default into a declaration."""
    payload = manifest_to_dict(_manifest())
    vertices = payload["schema"]["core_schema"]["vertex_config"]["vertices"]
    assert all("blank" not in vertex for vertex in vertices)


def test_an_unknown_declared_key_is_refused() -> None:
    with pytest.raises(ValueError, match="unknown declared key"):
        manifest_to_dict(_manifest(), declare=["vertex.nope"])
    with pytest.raises(ValueError, match="unknown declared key"):
        manifest_to_dict(_manifest(), declare=["directed"])


def test_a_schemaless_manifest_is_dumped_unchanged() -> None:
    """Nothing to force, and nothing to trip over."""
    manifest = GraphManifest.from_config(
        {"ingestion_model": {"resources": [{"name": "r", "apply": []}]}}
    )
    manifest.finish_init()
    payload = manifest_to_dict(manifest, declare=DECLARED_KEYS)
    assert "schema" not in payload
    assert payload["ingestion_model"]["resources"][0]["name"] == "r"


def test_dump_manifest_writes_the_forced_document(tmp_path: pathlib.Path) -> None:
    out = tmp_path / "m.yaml"
    dump_manifest(_manifest(), out, declare=DECLARED_KEYS)
    written = yaml.safe_load(out.read_text(encoding="utf-8"))
    assert _edges(written)[0]["directed"] is True
    assert _a3(written) == "pass"
