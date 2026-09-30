"""Show what a file backend directory holds: records and distinct vertices per type.

The file backend appends every record it receives; it does not merge records
that have the same identity, as a database does. This script therefore counts
both the records and the distinct identities of each vertex type. Run it from
this directory, after ``ingest.py`` or ``export.py``:

    uv run python inspect_backend.py
    uv run python inspect_backend.py artifacts/neo4j-backend
"""

from pathlib import Path

import click

from graflo.architecture.backend import GraFloBackendReader


@click.command()
@click.argument(
    "backend_dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default="artifacts/csv-backend",
)
def main(backend_dir: Path) -> None:
    """Print record and identity counts for every vertex and edge type."""
    reader = GraFloBackendReader(backend_dir)
    schema = reader.read_schema()
    vertex_config = schema.core_schema.vertex_config

    click.echo(f"{backend_dir} (schema {schema.metadata.name})")
    click.echo("vertices:")
    for vertex in vertex_config.vertices:
        fields = vertex_config.identity_fields(vertex.name)
        records = [
            doc for batch in reader.iter_vertex_batches(vertex.name) for doc in batch
        ]
        distinct = {tuple(doc.get(field) for field in fields) for doc in records}
        click.echo(
            f"  {vertex.name:<12}{len(records)} records, "
            f"{len(distinct)} distinct identities ({', '.join(fields)})"
        )
    click.echo("edges:")
    for edge in schema.core_schema.edge_config.edges:
        records = [
            row for batch in reader.iter_edge_batches(edge.edge_id) for row in batch
        ]
        label = f"{edge.source} -> {edge.target}"
        click.echo(f"  {label:<24}{len(records)} records")


if __name__ == "__main__":
    main()
