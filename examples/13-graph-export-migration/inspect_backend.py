"""
Print GraFlo file backend inventory (schema + INDEX.json summary).

Use after ``ingest.py`` or ``export.py`` to verify the on-disk backend:

    cd examples/13-graph-export-migration
    uv run python ingest.py
    uv run python inspect_backend.py --backend-dir artifacts/csv-backend
"""

from __future__ import annotations

from pathlib import Path

import click

from graflo.architecture.backend import GraFloBackendReader

from _common import DEFAULT_CSV_BACKEND_DIR


@click.command()
@click.option(
    "--backend-dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=DEFAULT_CSV_BACKEND_DIR,
    show_default=True,
    help="GraFlo file backend root directory.",
)
def main(backend_dir: Path) -> None:
    """Summarize vertices, edges, and record counts for a file backend."""
    reader = GraFloBackendReader(backend_dir)
    schema = reader.read_schema()
    index = reader.read_index()

    click.echo(f"Backend: {backend_dir.resolve()}")
    click.echo(f"Schema:  {schema.metadata.name}")
    click.echo("")
    click.echo("Vertices:")
    for name, entry in sorted(index.vertices.items()):
        click.echo(
            f"  {name}: {entry.record_count} records in {len(entry.chunks)} chunk(s)"
        )
    click.echo("")
    click.echo("Edges:")
    if not index.edges:
        click.echo("  (none)")
    for name, entry in sorted(index.edges.items()):
        click.echo(
            f"  {name}: {entry.record_count} records in {len(entry.chunks)} chunk(s)"
        )


if __name__ == "__main__":
    main()
