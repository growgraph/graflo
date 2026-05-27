"""CLI commands for GraphManifest RDF conversion."""

from __future__ import annotations

import pathlib

import click
import yaml

from graflo import GraphManifest
from graflo.rdf.deserializer import ManifestRdfDeserializer
from graflo.rdf.serializer import ManifestRdfSerializer


def _load_manifest(path: pathlib.Path) -> GraphManifest:
    with path.open(encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    return GraphManifest.from_dict(data)


def _write_manifest(path: pathlib.Path, manifest: GraphManifest) -> None:
    payload = manifest.to_minimal_canonical_dict()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(payload, default_flow_style=False, sort_keys=False),
        encoding="utf-8",
    )


@click.command("manifest-to-rdf")
@click.argument(
    "manifest_path",
    type=click.Path(exists=True, dir_okay=False, path_type=pathlib.Path),
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["turtle", "json-ld", "nt", "xml"], case_sensitive=False),
    default="turtle",
    show_default=True,
    help="RDF output format.",
)
@click.option(
    "--base-uri",
    required=True,
    help="Base URI for manifest resource IRIs (e.g. https://mygraph.dev/manifests/v1).",
)
@click.option(
    "--output",
    "output_path",
    type=click.Path(dir_okay=False, path_type=pathlib.Path),
    default=None,
    help="Output file path. Prints to stdout when omitted.",
)
@click.option(
    "--include-ontology/--no-include-ontology",
    default=True,
    show_default=True,
    help="Embed GraFlo meta-ontology triples in output.",
)
def manifest_to_rdf(
    manifest_path: pathlib.Path,
    output_format: str,
    base_uri: str,
    output_path: pathlib.Path | None,
    include_ontology: bool,
) -> None:
    """Convert a GraphManifest YAML file to RDF."""
    manifest = _load_manifest(manifest_path)
    serializer = ManifestRdfSerializer(include_ontology=include_ontology)
    graph = serializer.to_graph(manifest, base_uri)
    serialized = graph.serialize(format=output_format.lower())

    if output_path is None:
        click.echo(serialized)
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(serialized, encoding="utf-8")


@click.command("rdf-to-manifest")
@click.argument(
    "rdf_path",
    type=click.Path(exists=True, dir_okay=False, path_type=pathlib.Path),
)
@click.option(
    "--manifest-uri",
    required=True,
    help="GraphManifest subject URI inside the RDF document.",
)
@click.option(
    "--input-format",
    type=click.Choice(["turtle", "json-ld", "nt", "xml", "n3"], case_sensitive=False),
    default="turtle",
    show_default=True,
    help="RDF input format.",
)
@click.option(
    "--output",
    "output_path",
    type=click.Path(dir_okay=False, path_type=pathlib.Path),
    default=None,
    help="Output manifest YAML path. Prints to stdout when omitted.",
)
def rdf_to_manifest(
    rdf_path: pathlib.Path,
    manifest_uri: str,
    input_format: str,
    output_path: pathlib.Path | None,
) -> None:
    """Convert RDF (GraFlo meta-ontology) back to GraphManifest YAML."""
    from rdflib import Graph

    graph = Graph()
    graph.parse(str(rdf_path), format=input_format.lower())
    manifest = ManifestRdfDeserializer().from_graph(graph, manifest_uri)

    if output_path is None:
        click.echo(
            yaml.safe_dump(manifest.to_minimal_canonical_dict(), sort_keys=False)
        )
        return

    _write_manifest(output_path, manifest)
