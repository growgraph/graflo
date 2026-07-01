"""
Ingest the inferred manifest into a GraFlo file backend.

Run after infer.py:

    cd examples/15-identity-inference
    uv run python infer.py
    uv run python ingest.py
"""

from __future__ import annotations

from pathlib import Path

import click
from suthing import FileHandle

from graflo import DBType, GraphEngine, GraphManifest
from graflo.hq.caster import IngestionParams

from _common import (
    DEFAULT_CSV_BACKEND_DIR,
    DEFAULT_INFERRED_MANIFEST,
    backend_config,
    example_workdir,
)


@click.command()
@click.option(
    "--manifest",
    "manifest_path",
    type=click.Path(exists=True, path_type=Path),
    default=DEFAULT_INFERRED_MANIFEST,
    show_default=True,
    help="Manifest with inferred vertex identities.",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=DEFAULT_CSV_BACKEND_DIR,
    show_default=True,
    help="GraFlo file backend root directory.",
)
def main(manifest_path: Path, output_dir: Path) -> None:
    """Ingest inferred manifest CSV resources into an on-disk backend."""
    manifest = GraphManifest.from_config(FileHandle.load(manifest_path))
    manifest.finish_init()
    backend = backend_config(output_dir)
    engine = GraphEngine(target_db_flavor=DBType.GRAFLO_BACKEND)
    with example_workdir():
        engine.define_and_ingest(
            manifest=manifest,
            target_db_config=backend,
            ingestion_params=IngestionParams(clear_data=True),
            recreate_schema=True,
        )
    click.echo(f"Inferred manifest → GraFlo file backend: {backend.output_dir}")


if __name__ == "__main__":
    main()
