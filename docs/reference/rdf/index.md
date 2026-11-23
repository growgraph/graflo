# `graflo.rdf` — manifest ↔ RDF bridge

Bidirectional conversion between **`GraphManifest`** and RDF using the [GraFlo meta-ontology](../../concepts/schema/ontology.md).

## Modules

::: graflo.rdf

## Serializer

::: graflo.rdf.serializer.ManifestRdfSerializer

## Deserializer

::: graflo.rdf.deserializer.ManifestRdfDeserializer

## Namespace constants

::: graflo.rdf.namespace

## Utilities

::: graflo.rdf.utils

## Domain ontology

The serializer describes the manifest in the `gf:` vocabulary. To describe the
domain a schema models instead, as OWL 2 RL classes and properties and as SHACL
shapes, use [`graflo.rdf.tbox`](tbox.md) (`schema_to_owl`, `schema_to_shacl`;
`graflo schema-to-owl`, `graflo schema-to-shacl`).

## CLI

Console entry points (see `pyproject.toml` → `[project.scripts]`):

- **`manifest-to-rdf`** — `graflo.rdf.cli:manifest_to_rdf`
- **`rdf-to-manifest`** — `graflo.rdf.cli:rdf_to_manifest`

## See also

- [`graflo.hq.rdf_inferencer`](../hq/rdf_inferencer.md) — import **user** OWL/RDFS ontologies into a GraFlo schema (opposite direction of concern)
