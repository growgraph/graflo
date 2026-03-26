# Creating a Manifest

This guide explains how to create a GraFlo `GraphManifest`, the canonical config artifact used for ingestion and orchestration.

A full manifest combines three concerns in one file:

- `schema`: logical graph model (metadata, vertices, edges, DB profile)
- `ingestion_model`: resources and transforms
- `bindings`: mapping resources to physical data sources

`GraphManifest` also supports partial payloads (for example, schema-only or
ingestion-only files). At least one block is required.

## Why manifest-first

`GraphManifest` is the top-level contract passed through the runtime (`GraphEngine`, CLI ingest, plotting). Keeping all needed blocks in one document makes validation and execution deterministic.

## Manifest structure

A typical manifest file is named `manifest.yaml` and has this shape:

```yaml
schema:
  metadata:
    name: my_graph
    version: "1.0.0"
  graph:
    vertex_config:
      vertices:
        - name: person
          fields: [id, name, age]
          identity: [id]
        - name: department
          fields: [name]
          identity: [name]
    edge_config:
      edges:
        - source: person
          target: department
  db_profile: {}

ingestion_model:
  resources:
    - name: people
      apply:
        - vertex: person
    - name: departments
      apply:
        - vertex: person
          "from": {id: person_id, name: person}
        - vertex: department
          "from": {name: department}
  transforms: []

bindings: {}
```

## Block-by-block reference

### `schema`

Defines the graph contract.

- `metadata`: human-facing identity (`name`, optional `version`)
- `graph.vertex_config`: vertex types, fields, identity keys
- `graph.edge_config`: source/target relationships, optional relation/weights
- `db_profile`: DB-specific physical behavior (indexes, naming, backend details)

Use `schema` for **what graph exists**.

### `ingestion_model`

Defines ingestion behavior.

- `resources`: named pipelines (`name`) with ordered actor steps
- `transforms`: reusable named transforms as a **list** (each entry must define `name`) and referenced from resources via `transform.call.use`

Use `ingestion_model` for **how source records become vertices/edges**.

### `bindings`

Defines source wiring (`Bindings`).

- **`connectors`**: list of `FileConnector`, `TableConnector`, or `SparqlConnector` entries (where each row points at paths, tables, or RDF/SPARQL sources).
- **`resource_connector`**: list of `{"resource": "<ingestion resource name>", "connector": "<connector name or reference>"}` rows linking `IngestionModel.resources[*].name` to a connector.
- **`connector_connection`** (optional): list of `{"connector": "<name|hash|resource alias>", "conn_proxy": "<label>"}` rows. This keeps manifests **non-secret**: only proxy *names* appear in YAML; runtime code registers each `conn_proxy` on a `ConnectionProvider` with the real `GeneralizedConnConfig` (PostgreSQL, SPARQL, etc.).

Connector references in `resource_connector` / `connector_connection` must match a connector `name` (or resolve via hash / resource alias as documented in `Bindings`). Duplicate connector names and conflicting resource or proxy mappings are rejected at validation time.

The block can be left empty in-file (`bindings: {}`) and supplied at runtime for env-specific deployments.

Use `bindings` for **where data comes from** (and optionally **which proxy label** supplies runtime credentials for each SQL/SPARQL connector).

## Authoring tips

- Keep resource names unique across `ingestion_model.resources`.
- Ensure every `vertex`/`source`/`target` referenced by resources exists in `schema.core_schema`.
- Quote `"from"` in YAML because `from` is a reserved keyword.
- Prefer explicit `relation` names for multi-edge models.
- Keep `ingestion_model.transforms` ordered intentionally; transforms are applied in declaration/appearance order within pipelines.

## Load and validate

```python
from suthing import FileHandle
from graflo import GraphManifest

manifest = GraphManifest.from_config(FileHandle.load("manifest.yaml"))
manifest.finish_init()

schema = manifest.require_schema()
ingestion_model = manifest.require_ingestion_model()
```

`finish_init()` performs runtime wiring and consistency checks across schema and ingestion model.

## Minimal run path

```python
from graflo.hq import GraphEngine
from graflo.hq.caster import IngestionParams

engine = GraphEngine()
engine.define_and_ingest(
    manifest=manifest,
    target_db_config=conn_conf,
    ingestion_params=IngestionParams(clear_data=False),
    recreate_schema=False,
)
```

## See also

- [Quick Start](quickstart.md)
- [Concepts](../concepts/index.md)
- [Examples](../examples/index.md)
- [API Reference](../reference/index.md)
