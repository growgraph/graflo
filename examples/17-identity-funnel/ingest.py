"""
Ingest two customer sources that identify the same people by different keys,
using an identity funnel to derive one deterministic synthetic id.

No live graph database required:

    cd examples/17-identity-funnel
    uv run python ingest.py
    uv run python inspect_identities.py
"""

from __future__ import annotations

from pathlib import Path

import click
from _common import (
    DEFAULT_CSV_BACKEND_DIR,
    MANIFEST_PATH,
    backend_config,
    example_workdir,
)
from suthing import FileHandle

from graflo import DBType, GraphEngine, GraphManifest
from graflo.hq.caster import IngestionParams


@click.command()
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=DEFAULT_CSV_BACKEND_DIR,
    show_default=True,
    help="GraFlo file backend root directory.",
)
def main(output_dir: Path) -> None:
    """Define schema and ingest both CSV resources into an on-disk backend."""
    manifest = GraphManifest.from_config(FileHandle.load(MANIFEST_PATH))
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
    click.echo(f"Identity-funnel demo → GraFlo file backend: {backend.output_dir}")


if __name__ == "__main__":
    main()
