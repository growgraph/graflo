from pathlib import Path

import yaml

from graflo.architecture.contract.manifest import GraphManifest


def _load_example4_manifest() -> GraphManifest:
    manifest_path = (
        Path(__file__).resolve().parents[2]
        / "examples"
        / "4-ingest-neo4j"
        / "manifest.yaml"
    )
    with manifest_path.open("r", encoding="utf-8") as fh:
        payload = yaml.safe_load(fh)
    manifest = GraphManifest.from_config(payload)
    manifest.finish_init()
    return manifest


def test_example4_relation_from_key_uses_dependency_key_names() -> None:
    """`relation_from_key` should derive package->package relation per dependency key."""
    manifest = _load_example4_manifest()
    resource = manifest.require_ingestion_model().fetch_resource("package")

    doc = {
        "name": "pkg-a",
        "version": "1.0",
        "dependencies": {
            "depends": [{"name": "pkg-b", "version": ">= 1"}],
            "pre-depends": [{"name": "pkg-c", "version": ">= 1"}],
        },
        "maintainer": {"name": "Maintainer", "email": "maintainer@example.org"},
    }

    entities = resource(doc)
    assert ("package", "package", "depends") in entities
    assert ("package", "package", "pre_depends") in entities
    # relation_from_key should avoid generic relation=None package->package edges
    assert ("package", "package", None) not in entities


def test_example4_any_key_captures_unexpected_dependency_groups() -> None:
    """`any_key: true` should emit edges for keys not explicitly listed in pipeline."""
    manifest = _load_example4_manifest()
    resource = manifest.require_ingestion_model().fetch_resource("package")

    doc = {
        "name": "pkg-a",
        "version": "1.0",
        "dependencies": {
            "custom-link": [{"name": "pkg-z", "version": ">= 0"}],
        },
        "maintainer": {"name": "Maintainer", "email": "maintainer@example.org"},
    }

    entities = resource(doc)
    # Hyphenated key is normalized by relation extraction.
    assert ("package", "package", "custom_link") in entities
