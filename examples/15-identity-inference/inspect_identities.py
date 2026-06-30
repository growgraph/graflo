"""
Print identity summary from an inferred manifest.

    cd examples/15-identity-inference
    uv run python inspect_identities.py
"""

from __future__ import annotations

from pathlib import Path

import click
from suthing import FileHandle

from graflo import GraphManifest

from _common import DEFAULT_INFERRED_MANIFEST


@click.command()
@click.option(
    "--manifest",
    "manifest_path",
    type=click.Path(exists=True, path_type=Path),
    default=DEFAULT_INFERRED_MANIFEST,
    show_default=True,
    help="Manifest to inspect (defaults to inferred output).",
)
def main(manifest_path: Path) -> None:
    """Print vertex identity modes and fields from a manifest."""
    manifest = GraphManifest.from_config(FileHandle.load(manifest_path))
    manifest.finish_init()
    vertex_config = manifest.require_schema().core_schema.vertex_config

    click.echo(f"Manifest: {manifest_path}")
    click.echo()
    click.echo(f"{'vertex':<12} {'mode':<10} {'identity':<30} hash_identity_properties")
    click.echo("-" * 80)
    for vertex in vertex_config.vertices:
        identity_repr = ",".join(vertex.identity)
        hash_repr = ",".join(vertex.hash_identity_properties) or "-"
        click.echo(
            f"{vertex.name:<12} {vertex.identity_mode:<10} {identity_repr:<30} {hash_repr}"
        )


if __name__ == "__main__":
    main()
