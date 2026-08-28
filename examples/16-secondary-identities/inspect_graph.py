"""
Verify that lookup_only edge rows attached to primary identities.

After ingest.py:

    cd examples/16-secondary-identities
    uv run python inspect_graph.py
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import click
from _common import DEFAULT_CSV_BACKEND_DIR

from graflo.architecture.backend import GraFloBackendReader


def _all_vertices(
    reader: GraFloBackendReader, vertex_type: str
) -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = []
    for batch in reader.iter_vertex_batches(vertex_type):
        docs.extend(batch)
    return docs


def _all_edges(
    reader: GraFloBackendReader, edge_key: tuple[str, str, str | None]
) -> list[list[Any]]:
    docs: list[list[Any]] = []
    for batch in reader.iter_edge_batches(edge_key):
        docs.extend(batch)
    return docs


def _endpoint_label(endpoint: Any) -> str:
    if isinstance(endpoint, dict):
        if len(endpoint) == 1:
            return str(next(iter(endpoint.values())))
        return ", ".join(f"{k}={v}" for k, v in sorted(endpoint.items()))
    return str(endpoint)


@click.command()
@click.option(
    "--backend-dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=DEFAULT_CSV_BACKEND_DIR,
    show_default=True,
    help="GraFlo file backend root directory.",
)
def main(backend_dir: Path) -> None:
    """Print vertices and issuedBy edges resolved to primary keys."""
    reader = GraFloBackendReader(backend_dir)
    schema = reader.read_schema()
    index = reader.read_index()

    instruments = _all_vertices(reader, "instrument")
    issuers = _all_vertices(reader, "issuer")
    edges = _all_edges(reader, ("instrument", "issuer", "issuedBy"))

    click.echo(f"Backend: {backend_dir.resolve()}")
    click.echo(f"Schema:  {schema.metadata.name}")
    click.echo("")
    click.echo("Secondary identities (lookup plane, not upsert keys):")
    for vertex in schema.core_schema.vertex_config.vertices:
        names = ", ".join(
            f"{entry.name}={entry.fields}" for entry in vertex.secondary_identities
        )
        click.echo(f"  {vertex.name}: identity={vertex.identity}; {names}")
    click.echo("")
    click.echo("Vertices (lookup_only must not invent extras from links.csv):")
    for name, entry in sorted(index.vertices.items()):
        click.echo(f"  {name}: {entry.record_count} record(s)")
    click.echo("")
    click.echo("Instruments:")
    for doc in sorted(instruments, key=lambda d: d.get("sid", "")):
        click.echo(f"  {doc.get('sid')}: isin={doc.get('isin')}  {doc.get('name')}")
    click.echo("")
    click.echo("Issuers:")
    for doc in sorted(issuers, key=lambda d: d.get("iid", "")):
        click.echo(f"  {doc.get('iid')}: lei={doc.get('lei')}  {doc.get('name')}")
    click.echo("")
    click.echo("issuedBy edges (source/target are primary identities):")
    if not edges:
        click.echo("  (none — did ingest.py run?)")
        return
    pairs: list[tuple[str, str, Any]] = []
    for edge in edges:
        # File-backend edge rows: [source_id, target_id, relation?, props?]
        source = _endpoint_label(edge[0])
        target = _endpoint_label(edge[1])
        props = edge[-1] if len(edge) > 2 and isinstance(edge[-1], dict) else {}
        pairs.append((source, target, props.get("share")))
    for source, target, share in sorted(pairs):
        click.echo(f"  {source} → {target}  share={share}")


if __name__ == "__main__":
    main()
