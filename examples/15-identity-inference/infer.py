"""
Infer vertex identities from bundled CSV samples and write an updated manifest.

No database required:

    cd examples/15-identity-inference
    uv run python infer.py
    uv run python inspect_identities.py
"""

from __future__ import annotations

from pathlib import Path

import click
import yaml
from suthing import FileHandle

from graflo import GraphManifest
from graflo.architecture.schema.vertex import VertexConfig
from graflo.db.identity_inference import (
    IdentityInferenceConfig,
    apply_identity_inference_to_vertices,
)

from _common import (
    DEFAULT_INFERRED_MANIFEST,
    DEFAULT_INFERENCE_CONFIG,
    EXAMPLE_DIR,
    VERTEX_CSV_MAP,
    load_vertex_samples,
)


def _samples_by_vertex() -> dict[str, list[dict]]:
    return {name: load_vertex_samples(name) for name in VERTEX_CSV_MAP}


@click.command()
@click.option(
    "--output",
    "output_path",
    type=click.Path(path_type=Path),
    default=DEFAULT_INFERRED_MANIFEST,
    show_default=True,
    help="Path for the inferred manifest YAML.",
)
@click.option(
    "--min-sample-size",
    type=int,
    default=DEFAULT_INFERENCE_CONFIG.min_sample_size,
    show_default=True,
    help="Minimum rows required per vertex type.",
)
@click.option(
    "--max-sample-size",
    type=int,
    default=None,
    help="Optional cap on rows used for inference (large snapshots).",
)
def main(
    output_path: Path,
    min_sample_size: int,
    max_sample_size: int | None,
) -> None:
    """Run identity inference and write manifest-inferred.yaml."""
    manifest = GraphManifest.from_config(FileHandle.load(EXAMPLE_DIR / "manifest.yaml"))
    manifest.finish_init()
    schema = manifest.require_schema()
    vertex_config = schema.core_schema.vertex_config

    config = IdentityInferenceConfig(
        min_sample_size=min_sample_size,
        max_sample_size=max_sample_size,
    )
    samples_by_name = _samples_by_vertex()
    updated_vertices, results = apply_identity_inference_to_vertices(
        list(vertex_config.vertices),
        samples_by_name,
        config=config,
    )

    updated_vertex_config = VertexConfig(
        vertices=updated_vertices,
        force_types=vertex_config.force_types,
        identity_from_all_properties=False,
    )
    updated_core = schema.core_schema.model_copy(
        update={"vertex_config": updated_vertex_config}
    )
    updated_schema = schema.model_copy(update={"core_schema": updated_core})
    updated_manifest = manifest.model_copy(update={"graph_schema": updated_schema})

    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = updated_manifest.model_dump(
        by_alias=True,
        exclude_none=True,
        mode="json",
    )
    with output_path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(payload, handle, default_flow_style=False, sort_keys=False)

    click.echo(f"Wrote inferred manifest: {output_path}")
    click.echo()
    for vertex in updated_vertex_config.vertices:
        result = results[vertex.name]
        click.echo(
            f"  {vertex.name}: strategy={result.strategy} "
            f"mode={vertex.identity_mode} "
            f"identity={vertex.identity} "
            f"hash_identity_properties={vertex.hash_identity_properties}"
        )


if __name__ == "__main__":
    main()
