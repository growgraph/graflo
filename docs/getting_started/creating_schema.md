# Creating a Schema

This guide explains how to define a GraFlo **Schema** — the declarative, database-agnostic specification at the heart of the Graph Schema & Transformation Language (GSTL). A Schema describes the LPG structure (vertices and edges), how data is transformed (Resources and actors), and optional metadata.

## Principles

1. **Schema is the single source of truth** for the LPG: vertex types, edge types, indexes, and the mapping from raw data to vertices/edges.
2. **All schema configs are Pydantic models** (`ConfigBaseModel`). You can load from YAML or dicts; validation runs at load time.
3. **Resources define transformation pipelines**: each `Resource` has a unique `resource_name` and an `apply` (or `pipeline`) list of **actor steps**. `AbstractDataSource` instances (files, APIs, SQL, SPARQL endpoints) bind to Resources by name via the `DataSourceRegistry`.
4. **Order of definition is flexible** in YAML: `general`, `vertex_config`, `edge_config`, `resources`, and `transforms` can appear in any order. References (e.g. vertex names in edges or in `apply`) must refer to names defined in the same schema.

## Schema structure

A Schema has five top-level parts:

| Section          | Required | Description |
|------------------|----------|-------------|
| `general`        | Yes      | Schema name and optional version. |
| `vertex_config`  | Yes      | Vertex types and their fields, indexes, filters. |
| `edge_config`    | Yes      | Edge types (source, target, weights, indexes). |
| `resources`      | No       | List of resources: data pipelines (apply/pipeline) that map data to vertices and edges. |
| `transforms`     | No       | Named transform functions used by resources. |

## `general` (SchemaMetadata)

Identifies the schema. Used for versioning and as fallback graph/schema name when the database config does not set one.

```yaml
general:
  name: my_graph          # required
  version: "1.0"          # optional
```

- **`name`**: Required. Identifier for the schema (e.g. graph or database name).
- **`version`**: Optional. Semantic or custom version string.

## `vertex_config`

Defines **vertex types**: their fields, indexes, and optional filters. Each vertex type has a unique `name` and is referenced by that name in edges and in resources.

### Structure

```yaml
vertex_config:
  vertices:
    - name: person
      fields: [id, name, age]
      indexes:
        - fields: [id]
    - name: department
      fields: [name]
      indexes:
        - fields: [name]
  blank_vertices: []       # optional: vertex names allowed without explicit data
  force_types: {}           # optional: vertex -> list of field type names
  db_flavor: ARANGO         # optional: ARANGO | NEO4J | TIGERGRAPH
```

### Vertex fields

- **`name`**: Required. Vertex type name (e.g. `person`, `department`). Must be unique.
- **`fields`**: List of field definitions. Each item can be:
  - A **string** (field name, type inferred or omitted).
  - A **dict** with `name` and optional `type`: `{"name": "created_at", "type": "DATETIME"}`.
  - For TigerGraph or typed backends, use types: `INT`, `UINT`, `FLOAT`, `DOUBLE`, `BOOL`, `STRING`, `DATETIME`.
- **`indexes`**: List of index definitions. If empty, a single primary index on all fields is created. Each index can specify `fields` and optionally `unique: true/false`.
- **`filters`**: Optional list of filter expressions for querying this vertex.
- **`dbname`**: Optional. Database-specific name (e.g. collection/table). Defaults to `name` if not set.

### VertexConfig-level options

- **`blank_vertices`**: Vertex names that may be created without explicit row data (e.g. placeholders). Each must exist in `vertices`.
- **`force_types`**: Override mapping from vertex name to list of field type names for inference.
- **`db_flavor`**: Database flavor used for schema/index generation: `ARANGO`, `NEO4J`, or `TIGERGRAPH`.

## `edge_config`

Defines **edge types**: source and target vertex types, relation name, weights, and indexes.

### Structure

```yaml
edge_config:
  edges:
    - source: person
      target: department
      # optional: relation, match_source, match_target, weights, indexes, etc.
```

### Edge fields

- **`source`**, **`target`**: Required. Vertex type names (must exist in `vertex_config.vertices`).
- **`relation`**: Optional. Relationship/edge type name (especially for Neo4j). For ArangoDB can be used as weight.
- **`relation_field`**: Optional. Field name that stores or reads the relation type (e.g. for TigerGraph).
- **`relation_from_key`**: Optional. If true, derive relation from the location key during ingestion (e.g. JSON key).
- **`match_source`**, **`match_target`**: Optional. Fields used to match source/target vertices when creating edges.
- **`weights`**: Optional. Weight/attribute configuration:
  - **`direct`**: List of field names or typed fields to attach directly to the edge (e.g. `["date", "weight"]` or `[{"name": "date", "type": "DATETIME"}]`).
  - **`vertices`**: List of vertex-based weight definitions.
- **`indexes`** (or **`index`**): Optional. List of index definitions for the edge.
- **`purpose`**: Optional. Extra label for utility edges between the same vertex types.
- **`type`**: Optional. `DIRECT` (default) or `INDIRECT`.
- **`aux`**: Optional. If true, edge is created in DB but not used by graflo ingestion.
- **`by`**: Optional. For `INDIRECT` edges: vertex type name used to define the edge.

## `resources` (focus)

Resources define **how** each data stream is turned into vertices and edges. Each resource has a unique **`resource_name`** (used by Patterns / DataSourceRegistry to bind files, APIs, or SQL to this pipeline) and an **`apply`** (or **`pipeline`**) list of **actor steps**. Steps are executed in order; the pipeline can branch with **descend** steps.

### Resource-level fields

- **`resource_name`**: Required. Unique identifier (e.g. table or file name). Used when mapping data sources to this resource.
- **`apply`** (or **`pipeline`**): Required. List of actor steps (see below).
- **`encoding`**: Optional. Character encoding (default `UTF_8`).
- **`merge_collections`**: Optional. List of collection names to merge when writing.
- **`extra_weights`**: Optional. Additional edge weight configs for this resource.
- **`types`**: Optional. Field name → Python type expression for casting during ingestion (e.g. `{"age": "int"}`, `{"amount": "float"}`, `{"created_at": "datetime"}`). Useful when input is string-only (CSV, JSON) and you need numeric or date values.
- **`edge_greedy`**: Optional. If true (default), emit edges as soon as source/target exist; if false, wait for explicit targets.

### Actor steps in `apply` / `pipeline`

Each step is a dict. You can write steps in shorthand (e.g. `vertex: person`) or with an explicit **`type`** (`vertex`, `transform`, `edge`, `descend`). The system recognizes:

1. **Vertex step** — create vertices of a given type from the current document level:
   ```yaml
   - vertex: person
   ```
   Optional: `keep_fields: [id, name]`.

2. **Transform step** — rename fields, change shape, or apply a function.
   There are several forms:

   **Field mapping** (pure renaming, no function):
   ```yaml
   - map:
       person: name
       person_id: id
     target_vertex: department   # optional: send result to a vertex
   ```

   **Direct output** (function result maps 1:1 to output fields):
   ```yaml
   - foo: parse_date_yahoo
     module: graflo.util.transform
     input: [Date]
     output: [t_obs]
   ```

   **Dressed output** (function result + input field name packaged as key/value).
   Use `dress` when you need to pivot a wide column into a key/value pair,
   e.g. turning `{Open: "6.43..."}` into `{name: "Open", value: 6.43}`:
   ```yaml
   - foo: round_str
     module: graflo.util.transform
     params:
       ndigits: 3
     input:
     - Open
     dress:
       key: name      # output field for the input field name
       value: value    # output field for the function result
   ```
   The `dress` dict makes the roles explicit: `key` receives the input
   field name (`"Open"`), `value` receives the function result (`6.43`).

   **Named transform** (defined in `transforms`, referenced by name):
   ```yaml
   - name: keep_suffix_id
     params: { sep: "/", keep: -1 }
     input: [id]
     output: [_key]
   ```

3. **Edge step** — create edges between two vertex types:
   ```yaml
   - source: person
     target: department
   ```
   Or:
   ```yaml
   - edge:
       from: person
       to: department
   ```
   You can add edge-specific `weights`, `indexes`, etc. in the step when needed.

4. **Descend step** — go into a nested key and run a sub-pipeline (or process all keys with `any_key`):
   ```yaml
   - key: referenced_works
     apply:
       - vertex: work
       - source: work
         target: work
   ```
   Or with **`any_key`** to iterate over all keys:
   ```yaml
   - any_key: true
     apply: [...]
   ```

### Rules for resources (for agents)

- **Unique names**: Every `resource_name` in the schema must be unique.
- **References**: All vertex names in `apply` (e.g. `vertex: person`, `source`/`target`, `target_vertex`) must exist in `vertex_config.vertices`. All edge relationships implied by `source`/`target` should exist in `edge_config.edges` (or be compatible).
- **Order**: Steps run in sequence. Typically you create vertices before creating edges that reference them; use **transform** to reshape data and **descend** to handle nested structures.
- **Transforms**: If a step uses `name: <transform_name>`, that name must exist in `transforms` (see below).

## `transforms`

Optional dictionary of **named transforms** used by resources. Keys are transform names; values are configs (e.g. `foo`, `module`, `params`, `input`, `output`).

```yaml
transforms:
  keep_suffix_id:
    foo: split_keep_part
    module: graflo.util.transform
    params: { sep: "/", keep: -1 }
    input: [id]
    output: [_key]
```

Resources refer to them with `name: keep_suffix_id` (and optional `params`, `input`, `output` overrides) in a transform step.

## Loading a schema

All schema configs are Pydantic models. You can load a Schema from a dict or YAML:

```python
from graflo import Schema
from suthing import FileHandle

# From dict (e.g. from YAML already parsed)
schema = Schema.model_validate(FileHandle.load("schema.yaml"))
# Or explicit method
schema = Schema.from_dict(FileHandle.load("schema.yaml"))

# From YAML file path (if your root is the schema dict)
data = FileHandle.load("schema.yaml")
schema = Schema.model_validate(data)
```

After loading, the schema runs `finish_init()` (transform names, edge init, resource pipelines, and the internal resource name map). If you modify `resources` programmatically, call `schema.finish_init()` so that `fetch_resource(name)` and ingestion use the updated pipelines.

## Minimal full example

```yaml
general:
  name: hr

vertex_config:
  vertices:
    - name: person
      fields: [id, name, age]
      indexes:
        - fields: [id]
    - name: department
      fields: [name]
      indexes:
        - fields: [name]

edge_config:
  edges:
    - source: person
      target: department

resources:
  - resource_name: people
    apply:
      - vertex: person
  - resource_name: departments
    apply:
      - map:
          person: name
          person_id: id
      - target_vertex: department
        map:
          department: name
```

This defines two vertex types (`person`, `department`), one edge type (`person` → `department`), and two resources: **people** (each row → one `person` vertex) and **departments** (transform + `department` vertices). Data sources are attached to these resources by name (e.g. via `Patterns` or `DataSourceRegistry`) as shown in the [Quick Start](quickstart.md).

## See also

- [Concepts — Schema and constituents](../concepts/index.md#schema) for higher-level overview.
- [Quick Start](quickstart.md) for loading a schema and running ingestion.
- [API Reference — architecture](../reference/architecture/index.md) for Pydantic model details.
