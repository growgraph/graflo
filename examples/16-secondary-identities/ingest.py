"""
Ingest instruments, issuers, then an edge-only links CSV matched on secondary
identities (ISIN / LEI) into a GraFlo file backend.

No live graph database required:

    cd examples/16-secondary-identities
    uv run python ingest.py
    uv run python inspect_graph.py
"""

from __future__ import annotations

from pathlib import Path

import click
from suthing import FileHandle

from graflo import DBType, GraphEngine, GraphManifest
from graflo.hq.caster import IngestionParams

from _common import (
    DEFAULT_CSV_BACKEND_DIR,
    MANIFEST_PATH,
    backend_config,
    example_workdir,
)


@click.command()
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=DEFAULT_CSV_BACKEND_DIR,
    show_default=True,
    help="GraFlo file backend root directory.",
)
def main(output_dir: Path) -> None:
    """Define schema and ingest three CSV resources into an on-disk backend."""
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
    click.echo(f"Secondary-identity demo → GraFlo file backend: {backend.output_dir}")


if __name__ == "__main__":
    main()
