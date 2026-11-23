"""Verbs that write a manifest in formats readable without graflo.

- ``graflo json-schema``: the GSTL JSON Schema a manifest validates against;
- ``graflo schema-to-owl`` / ``graflo schema-to-shacl``: the schema as an
  OWL 2 RL ontology and as SHACL shapes for instance data;
- ``graflo schema-to-ossie``: the manifest as Apache Ossie documents
  (experimental), with the list of what Ossie cannot express;
- ``graflo schema-to-compact`` / ``schema-from-compact``: the schema in the
  compact notation, and a manifest from one;
- ``graflo schema-to-neo4j-graphrag`` / ``schema-from-neo4j-graphrag``: the
  schema as a neo4j-graphrag ``GraphSchema`` document, and a manifest from one,
  each with the list of what was left out.
"""

from __future__ import annotations

import json
from pathlib import Path

import click
import yaml
from pydantic import ValidationError

from graflo.architecture.contract.json_schema import manifest_json_schema
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.contract.provenance import stamp_gstl
from graflo.architecture.schema.document import Schema
from graflo.cli.io import load_manifest, load_mapping, manifest_to_dict
from graflo.interop.compact import CompactUnsupported, from_compact, to_compact
from graflo.interop.neo4j_graphrag import from_graph_schema, to_graph_schema
from graflo.interop.ossie import manifest_to_ossie
from graflo.rdf.tbox import schema_to_owl, schema_to_shacl

_RDF_FORMATS = {"turtle": "turtle", "json-ld": "json-ld"}


def _write(text: str, output: Path | None) -> None:
    if output is None:
        click.echo(text, nl=not text.endswith("\n"))
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(text, encoding="utf-8")


class _NoAliasDumper(yaml.SafeDumper):
    """Writes a repeated list in full; a hand-read document needs no anchors."""

    def ignore_aliases(self, data: object) -> bool:
        return True


def _document(doc: dict, output: Path | None) -> str:
    """*doc* as JSON when *output* ends in ``.json``, else as YAML."""
    if output is not None and output.suffix == ".json":
        return json.dumps(doc, indent=2, ensure_ascii=False) + "\n"
    return yaml.dump(doc, Dumper=_NoAliasDumper, sort_keys=False, allow_unicode=True)


def _manifest_of(schema: Schema) -> dict:
    manifest = GraphManifest(graph_schema=schema)
    manifest.finish_init()
    stamp_gstl(manifest)
    return manifest_to_dict(manifest)


def _report(losses: list[str], path: Path | None) -> None:
    """Name what a conversion left out: on stderr, and in *path* when given."""
    for loss in losses:
        click.echo(f"not expressed: {loss}", err=True)
    if path is not None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("".join(f"- {loss}\n" for loss in losses), encoding="utf-8")


def _schema(manifest: GraphManifest) -> Schema:
    if manifest.graph_schema is None:
        raise click.ClickException("the manifest has no schema")
    return manifest.graph_schema


_manifest_arg = click.argument(
    "manifest_path", type=click.Path(exists=True, dir_okay=False, path_type=Path)
)
_base_iri_opt = click.option(
    "--base-iri",
    default=None,
    help="Base for minted IRIs; default metadata.base_iri, else urn:graflo:<name>:.",
)
_rdf_format_opt = click.option(
    "--format",
    "rdf_format",
    type=click.Choice(sorted(_RDF_FORMATS)),
    default="turtle",
    show_default=True,
)
_output_opt = click.option(
    "--output",
    "-o",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Output file; stdout when omitted.",
)


@click.command("json-schema")
@_output_opt
def json_schema(output: Path | None) -> None:
    """Print the GSTL JSON Schema of a manifest."""
    _write(json.dumps(manifest_json_schema(), indent=2) + "\n", output)


@click.command("schema-to-owl")
@_manifest_arg
@_base_iri_opt
@_rdf_format_opt
@_output_opt
def schema_to_owl_cmd(
    manifest_path: Path, base_iri: str | None, rdf_format: str, output: Path | None
) -> None:
    """Write the manifest's schema as an OWL 2 RL ontology."""
    graph = schema_to_owl(_schema(load_manifest(manifest_path)), base_iri)
    _write(graph.serialize(format=_RDF_FORMATS[rdf_format]), output)


@click.command("schema-to-shacl")
@_manifest_arg
@_base_iri_opt
@_rdf_format_opt
@_output_opt
def schema_to_shacl_cmd(
    manifest_path: Path, base_iri: str | None, rdf_format: str, output: Path | None
) -> None:
    """Write SHACL shapes for instance data of the manifest's schema."""
    graph = schema_to_shacl(_schema(load_manifest(manifest_path)), base_iri)
    _write(graph.serialize(format=_RDF_FORMATS[rdf_format]), output)


@click.command("schema-to-ossie")
@_manifest_arg
@_base_iri_opt
@click.option(
    "--output",
    "-o",
    "output_dir",
    type=click.Path(file_okay=False, path_type=Path),
    required=True,
    help="Directory for the Ossie documents and losses.md.",
)
def schema_to_ossie_cmd(
    manifest_path: Path, base_iri: str | None, output_dir: Path
) -> None:
    """Write the manifest as Apache Ossie documents (experimental)."""
    manifest = load_manifest(manifest_path)
    _schema(manifest)
    export = manifest_to_ossie(manifest, base_iri)
    output_dir.mkdir(parents=True, exist_ok=True)
    for name, text in export.files().items():
        (output_dir / name).write_text(text, encoding="utf-8")
        click.echo(output_dir / name)
    if export.losses:
        click.echo(
            f"{len(export.losses)} item(s) not expressed; see losses.md", err=True
        )


_source_arg = click.argument(
    "source_path", type=click.Path(exists=True, dir_okay=False, path_type=Path)
)
_losses_opt = click.option(
    "--losses",
    "losses_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Also write what was left out to this file.",
)
_document_output_opt = click.option(
    "--output",
    "-o",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Output file, JSON when it ends in .json; YAML on stdout when omitted.",
)


@click.command("schema-to-compact")
@_manifest_arg
@_document_output_opt
def schema_to_compact_cmd(manifest_path: Path, output: Path | None) -> None:
    """Write the manifest's schema in the compact notation.

    Refused, naming each part, when the schema uses more than the notation says.
    """
    try:
        compact = to_compact(_schema(load_manifest(manifest_path)))
    except CompactUnsupported as exc:
        raise click.ClickException(str(exc)) from exc
    _write(_document(compact, output), output)


@click.command("schema-from-compact")
@_source_arg
@_document_output_opt
def schema_from_compact_cmd(source_path: Path, output: Path | None) -> None:
    """Write a manifest holding the schema a compact-notation file stands for."""
    try:
        schema = from_compact(load_mapping(source_path))
    except ValidationError as exc:
        raise click.ClickException(f"{source_path}: {exc}") from exc
    _write(_document(_manifest_of(schema), output), output)


@click.command("schema-to-neo4j-graphrag")
@_manifest_arg
@_document_output_opt
@_losses_opt
def schema_to_neo4j_graphrag_cmd(
    manifest_path: Path, output: Path | None, losses_path: Path | None
) -> None:
    """Write the manifest's schema as a neo4j-graphrag GraphSchema document."""
    document, losses = to_graph_schema(_schema(load_manifest(manifest_path)))
    _write(_document(document, output), output)
    _report(losses, losses_path)


@click.command("schema-from-neo4j-graphrag")
@_source_arg
@click.option(
    "--name",
    default=None,
    help="The schema's name; default the file name without its extension.",
)
@_document_output_opt
@_losses_opt
def schema_from_neo4j_graphrag_cmd(
    source_path: Path,
    name: str | None,
    output: Path | None,
    losses_path: Path | None,
) -> None:
    """Write a manifest holding the schema of a neo4j-graphrag GraphSchema file."""
    try:
        schema, losses = from_graph_schema(
            load_mapping(source_path), name or source_path.stem
        )
    except ValidationError as exc:
        raise click.ClickException(f"{source_path}: {exc}") from exc
    _write(_document(_manifest_of(schema), output), output)
    _report(losses, losses_path)
