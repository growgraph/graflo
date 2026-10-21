"""A manifest states the GSTL version it is written in (``metadata.gstl``).

The field is a label, not content: it never enters the content hash. Writers
stamp it; library operations leave it as they found it.
"""

from __future__ import annotations

import copy
import pathlib

import pytest
import yaml
from pydantic import ValidationError

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.contract.provenance import (
    GSTL_VERSION,
    ManifestMetadata,
    stamp_gstl,
    stamp_provenance,
)
from graflo.architecture.evolution.hashing import manifest_hash
from graflo.architecture.evolution.merge import merge_manifests
from graflo.architecture.evolution.ops import MergeManifestsOp
from graflo.cli.io import dump_manifest

BASE: dict = {
    "schema": {
        "metadata": {"name": "shop", "version": "1.0.0"},
        "graph": {
            "vertex_config": {
                "vertices": [
                    {
                        "name": "person",
                        "properties": [{"name": "id"}],
                        "identity": ["id"],
                    }
                ]
            },
            "edge_config": {"edges": []},
        },
    }
}


def _manifest(gstl: str | None = None, vertex: str = "person") -> GraphManifest:
    payload = copy.deepcopy(BASE)
    payload["schema"]["graph"]["vertex_config"]["vertices"][0]["name"] = vertex
    if gstl is not None:
        payload["metadata"] = {"gstl": gstl}
    return GraphManifest.from_dict(payload)


@pytest.mark.parametrize("value", ["1.0", "1.7"])
def test_any_minor_of_a_supported_major_is_accepted(value: str) -> None:
    assert ManifestMetadata(gstl=value).gstl == value


def test_an_unstated_version_is_accepted() -> None:
    assert ManifestMetadata().gstl is None


def test_an_unsupported_major_is_refused_naming_the_supported_ones() -> None:
    with pytest.raises(ValidationError, match=r"major version 2 .*reads major 1"):
        ManifestMetadata(gstl="2.0")


@pytest.mark.parametrize("value", ["1", "v1.0", "1.0.0", ""])
def test_a_malformed_version_fails_the_pattern(value: str) -> None:
    with pytest.raises(ValidationError, match="pattern"):
        ManifestMetadata(gstl=value)


def test_the_version_is_outside_the_content_hash() -> None:
    assert manifest_hash(_manifest()) == manifest_hash(_manifest(gstl="1.0"))


def test_stamp_gstl_creates_the_metadata_block() -> None:
    manifest = _manifest()
    assert manifest.metadata is None
    stamp_gstl(manifest)
    assert manifest.metadata is not None
    assert manifest.metadata.gstl == GSTL_VERSION


def test_stamping_provenance_also_states_the_version() -> None:
    manifest = _manifest()
    stamp_provenance(manifest, content_hash="0" * 64, canon="graflo/canon@5")
    assert manifest.metadata is not None
    assert manifest.metadata.gstl == GSTL_VERSION


def test_a_written_manifest_states_the_version(tmp_path: pathlib.Path) -> None:
    out = tmp_path / "out.yaml"
    dump_manifest(_manifest(), out)
    written = yaml.safe_load(out.read_text())
    assert written["metadata"]["gstl"] == GSTL_VERSION
    metadata = GraphManifest.from_dict(written).metadata
    assert metadata is not None
    assert metadata.gstl == GSTL_VERSION


def test_a_library_merge_leaves_the_version_unstated() -> None:
    merged = merge_manifests(
        _manifest(gstl="1.0"),
        _manifest(gstl="1.0", vertex="order"),
        MergeManifestsOp(),
        bump_version=False,
    )
    assert merged.metadata is None or merged.metadata.gstl is None
