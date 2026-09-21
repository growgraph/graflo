"""Cast one ``registry`` row through the manifest, before and after the repair.

No database: a resource is a callable from a document to the graph entities it
produces, which is all that is needed to see what would be written.

    uv run python mirror.py
"""

from __future__ import annotations

import pathlib

import yaml

from graflo.architecture.contract import GraphManifest

HERE = pathlib.Path(__file__).parent
ROW = {
    "person_id": "p-17",
    "name": "Ada",
    "institution_id": "i-03",
    "title": "Analytical Engines Ltd",
}


def edges_written(manifest_path: pathlib.Path) -> list[str]:
    """The edges the ``registry`` resource writes for ``ROW``, one line each."""
    manifest = GraphManifest.from_config(yaml.safe_load(manifest_path.read_text()))
    manifest.finish_init()
    entities = manifest.require_ingestion_model().fetch_resource("registry")(ROW)
    return [
        f"({source})-[{relation}]->({target})"
        for (source, target, relation), docs in sorted(
            (key, docs) for key, docs in entities.items() if isinstance(key, tuple)
        )
        if docs
    ]


def main() -> None:
    for label, path in [
        ("as assembled", HERE / "manifest.yaml"),
        ("after repair", HERE / "artifacts" / "manifest_repaired.yaml"),
    ]:
        print(f"{label}:")
        for line in edges_written(path):
            print(f"    {line}")


if __name__ == "__main__":
    main()
