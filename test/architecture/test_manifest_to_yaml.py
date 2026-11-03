"""A manifest written with ``to_yaml`` reads back as the same graph."""

from __future__ import annotations

from pathlib import Path

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import manifest_hash


def manifest() -> GraphManifest:
    built = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "written"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {"name": "A", "properties": ["id"], "identity": ["id"]},
                            {"name": "B", "properties": ["id"], "identity": ["id"]},
                        ]
                    },
                    "edge_config": {
                        "edges": [{"source": "A", "target": "B", "relation": "R"}]
                    },
                },
                "db_profile": {},
            },
            "ingestion_model": {
                "resources": [{"name": "r", "pipeline": [{"vertex": "A"}]}],
                "transforms": [],
            },
        }
    )
    built.finish_init()
    return built


def test_a_manifest_with_edges_is_written_and_reads_back_the_same(
    tmp_path: Path,
) -> None:
    original = manifest()
    path = tmp_path / "out.yaml"

    original.to_yaml(str(path))

    written = GraphManifest.from_yaml(str(path))
    written.finish_init()
    assert manifest_hash(written) == manifest_hash(original)


def test_a_string_and_a_file_hold_the_same_text(tmp_path: Path) -> None:
    path = tmp_path / "out.yaml"
    manifest().to_yaml(str(path))
    assert path.read_text() == manifest().to_yaml_str()


def test_a_failed_write_leaves_the_file_as_it_was(tmp_path: Path) -> None:
    path = tmp_path / "out.yaml"
    path.write_text("before")
    with pytest.raises(TypeError):
        # An option the dumper refuses, standing in for anything that fails to serialise.
        manifest().to_yaml(str(path), indent=object())
    assert path.read_text() == "before"
