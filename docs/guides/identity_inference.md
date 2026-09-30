# Finding a key for your data

Your records have no obvious key: no `id` column, or several columns that
might be one. This guide takes a sample of your own data, proposes the fields
that identify each vertex type, and writes them into the manifest. At the end
you have a manifest whose vertex types declare an identity you have reviewed,
and you know which proposals to distrust.

## What you need

- GraFlo installed (`pip install graflo`).
- A manifest whose vertex types list their `properties` and leave `identity`
  unset. It loads as it is, because an omitted identity falls back to all
  properties by default; see [Vertex identity](../concepts/schema/vertex_identity.md).
- Records to sample: JSON, JSON Lines, CSV or Parquet files, tables in a
  PostgreSQL schema, or the `bindings` block of a manifest whose connectors
  read such files or PostgreSQL tables.
- At least 100 records per vertex type. Inference refuses smaller samples,
  because on a few records almost any column looks unique. The floor is
  `min_sample_size`.

[The identity inference example (15)](../examples/identity-inference/index.md)
runs these steps on two CSV files.

## Steps

### 1. Sample the source

Sampling reads a bounded number of records from each file or table and keeps
them as they were read.

```python
from graflo import GraphEngine

source = GraphEngine().sample_resources(
    ["data/machines.csv", "data/work_orders.csv"], max_docs=500
)
for sample in source.samples:
    print(sample.resource_name, len(sample.docs), sample.truncated)
```

The result holds one sample per file, named after the file without its
extension (`machines`, `work_orders`). `max_docs` caps the records per sample.
Its default, 100, equals the inference floor, so raise it when you can;
`truncated` tells you whether the cap cut the source short.

`sample_resources` takes other sources too:

- A directory path samples every file in it that GraFlo can read, and skips
  the rest with a warning.
- A `PostgresConfig` samples the tables of a schema: pass `schema_name=` and,
  to sample only some tables, `resources=[...]`. Each sample is named after its
  table and carries the table's declared `primary_key`.
- The `bindings` block of a manifest samples each resource through its own
  connector; a file connector contributes the first file it matches. Pass
  `connection_provider=` when a connector reads a PostgreSQL table. Connectors
  of other kinds (API, SPARQL, Kafka) are skipped with a warning.

[Sampling and profiling](../concepts/schema/sampling_and_profiling.md)
describes the caps and what a sample records.

### 2. Flatten nested records

Inference works on flat records. A CSV row or a table row is already flat; a
JSON document with nested objects is not. Project it onto its
[profile](../concepts/glossary.md#profile) first, which turns paths into field
names such as `machine.serial_number` or `parts[].sku`.

```python
from graflo.architecture.onto_sample import profile_sample

sample = source.get("work_orders")
records = profile_sample(sample).flat_docs(sample.docs)
```

For flat sources, skip this step: `sample.docs` is already the record list.

### 3. Run inference for each vertex type

`apply_identity_inference_to_vertices` takes the vertex types of your manifest
and a dict of records per vertex type, and returns updated copies of the
vertex types plus one result per type. Nothing in the manifest changes yet.

```python
from suthing import FileHandle

from graflo import GraphManifest
from graflo.db.identity_inference import (
    IdentityInferenceConfig,
    apply_identity_inference_to_vertices,
)

manifest = GraphManifest.from_config(FileHandle.load("manifest.yaml"))
manifest.finish_init()
schema = manifest.require_schema()
vertex_config = schema.core_schema.vertex_config

samples_by_vertex = {
    "machine": source.get("machines").docs,
    "work_order": source.get("work_orders").docs,
}
vertices, results = apply_identity_inference_to_vertices(
    list(vertex_config.vertices),
    samples_by_vertex,
    config=IdentityInferenceConfig(min_sample_size=100),
)
```

Only a vertex type's declared `properties` are considered, looked up by name in
the records. If a [resource](../concepts/glossary.md#resource) renames columns
on the way in, rename the keys of the sample records the same way before this
step. Columns that hold lists,
bytes or nested objects, strings longer than 256 characters, or values missing
in more than half the records are excluded.

Among the remaining columns, the inferencer first looks for a single column
whose values are all distinct. Otherwise it adds columns one at a time until
the combination is unique, then drops any column the combination does not
need. A key wider than `max_key_width` columns (default 3) becomes a hash over
those columns instead. Columns whose names end in `id`, `uuid`, `key`, `code`
or `pk` are tried first, and so are columns of integers or UUIDs. A CSV reader
yields every value as a string, so for CSV files only the column name and
UUID-shaped values make a difference.

### 4. Read the results

```python
for name, result in results.items():
    print(name, result.strategy, result.identity, result.confidence, result.warning)
```

| `strategy` | What it means | What to do |
|---|---|---|
| `unary` | One column is unique across the sample | Accept it, unless its values can change over time, as an email address can |
| `composite` | A combination of up to `max_key_width` columns is unique | Check that the combination identifies the thing itself, not a row of this particular export |
| `hash_fallback` | The unique combination is wider than `max_key_width`, or failed the resampling check. `hash_identity_properties` holds its columns and `identity` is `[id]` | Treat it as a warning. Look for a key the sample does not contain |
| `no_viable_identity` | The sample is too small, every column was excluded, or even all columns together repeat | The vertex type is returned unchanged. Get more records, remove duplicate rows, or declare the key yourself |

`confidence` is 1.0 for a key that stayed unique on every resample, and at most
0.5 for a hash fallback. `warning` says why a fallback was chosen.

A vertex type returned unchanged keeps the all-properties identity it received
when the manifest was loaded, and the next step writes that identity out
explicitly. Fix every `no_viable_identity` type by hand before you save.

A PostgreSQL table sample carries the table's declared key as
`sample.primary_key`. Prefer a declared key to an inferred one.

### 5. Write the manifest

Rebuild the vertex configuration with the updated vertex types and write the
manifest to a new file.

```python
from graflo.architecture.schema.vertex import VertexConfig

inferred_config = VertexConfig(
    vertices=vertices,
    force_types=vertex_config.force_types,
    identity_from_all_properties=False,
)
core_schema = schema.core_schema.model_copy(update={"vertex_config": inferred_config})
inferred = manifest.model_copy(
    update={"graph_schema": schema.model_copy(update={"core_schema": core_schema})}
)
FileHandle.dump(inferred.to_minimal_canonical_dict(), "manifest-inferred.yaml")
```

`identity_from_all_properties=False` means that a vertex type added later
without an identity is an error when the manifest is loaded, instead of a type
that keys on all its properties without anyone deciding so.

### 6. Several sources describe one vertex type

When two resources describe the same vertex type under different column
names, run [cross-resource identity
discovery](../concepts/schema/cross_resource_identity.md) instead of step 3.
It aligns columns across the resources, can propose an identity funnel, and
returns `rename` steps that bring each resource to the same column names.
Sample only the resources that describe that vertex type, because every
resource in the sample takes part.

```python
from graflo.db.cross_resource_identity import infer_from_source_sample

source = GraphEngine().sample_resources(
    ["exports/maintenance_assets.csv", "exports/sensor_devices.csv"], max_docs=500
)
proposal = infer_from_source_sample(source, vertex_name="machine")
```

## What you should see

Step 4 prints one line per vertex type: its name, strategy, proposed identity,
confidence and warning. For example:

```text
machine unary ['serial_number'] 1.0 None
```

Load `manifest-inferred.yaml` again and print each vertex type's
`identity_mode`: `natural` for a unary or composite key, `hash` for a fallback.
After you ingest with the new manifest into a database, compare the number of
vertices of each type with the number of records in its source. Fewer vertices
means some records shared a key value: either they describe the same thing, or
the key is not a key.

## Tuning

`IdentityInferenceConfig`:

| Option | Default | Effect |
|---|---|---|
| `min_sample_size` | 100 | Fewer records than this give `no_viable_identity` |
| `max_sample_size` | none | Random subsample cap for large samples |
| `max_key_width` | 3 | Widest composite key before the hash fallback |
| `semantic_weight` | 0.5 | Preference for columns whose name ends in `id`, `uuid`, `key`, `code` or `pk` |
| `type_cost_weight` | 0.2 | Preference for integer and UUID columns over strings, dates and floats |
| `n_boots`, `subsample_ratio` | 5, 0.8 | Number and size of the resamples on which a key must stay unique |

## What to read next

- [Vertex identity](../concepts/schema/vertex_identity.md): what each identity
  declaration does when records are cast.
- [Cross-resource identity discovery](../concepts/schema/cross_resource_identity.md):
  what the alignment looks at, and its limits.
- [Sampling and profiling](../concepts/schema/sampling_and_profiling.md): caps
  on samples, and why `unique` on a profile is not a key.
