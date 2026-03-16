# Example 7: Polymorphic Objects and Relations with `vertex_router` and `edge_router`

This example demonstrates how to ingest polymorphic entity data from a single objects table and dynamic relations from a relations table, using `vertex_router` and `edge_router` to route rows to the correct vertex types and edge types based on type discriminator columns.

## Overview

The dataset contains:

- **objects.csv** — One table with mixed entity types (Person, Vehicle, Institution), distinguished by a `type` column
- **relations.csv** — One table describing relationships between entities, with source/target types and relation names in columns

Instead of separate resources per entity type, this schema uses:

1. **`vertex_router`** — Routes each objects row to the correct vertex type (`person`, `vehicle`, `institution`) via `type_map`
2. **`edge_router`** — Creates edges with dynamic source/target types and relation names via `relation_map`

This connector is ideal for EAV-style or polymorphic data where one table holds multiple entity types and another holds relation tuples.

## Data

### objects.csv

Each row has a `type` column that determines the vertex type. Shared columns (id, name, etc.) plus type-specific columns (salary for Person, license_plate for Vehicle, etc.):

```csv
id,type,name,age,birth_date,email,salary,license_plate,num_wheels,fuel_type,founded_year,num_employees,industry,color,weight_kg,address
ec3cd5f9-8a75-49af-adc8-654eab637ebc,Person,Alice Martin,34.0,1991-02-14,alice@example.com,85000.0,,,,,,,,,12 Rue de Paris
4a4bc6ca-7a1d-49b4-954b-c45a135a4cfa,Vehicle,Toyota Corolla,,,,,AB-123-CD,4.0,Petrol,,,,Blue,1300.0,
eb463fbc-423c-4f82-a532-b21430325f15,Institution,TechNova Labs Europe,,,contact@technova-europe.com,,,,,2012.0,120.0,Biotechnology,,,1 Innovation Drive
```

### relations.csv

Each row describes one edge: source_id, target_id, relation_type, source_type, target_type:

```csv
source_id,target_id,relation_type,source_type,target_type
ec3cd5f9-8a75-49af-adc8-654eab637ebc,eb463fbc-423c-4f82-a532-b21430325f15,EMPLOYED_BY,Person,Institution
0d1c97b4-2be6-4f9c-af45-1e3a18d1513b,4a4bc6ca-7a1d-49b4-954b-c45a135a4cfa,OWNS,Person,Vehicle
b4dc8ede-2875-4d8b-bfd6-3f07d5eddf5e,eb463fbc-423c-4f82-a532-b21430325f15,FUNDS,Institution,Institution
ec3cd5f9-8a75-49af-adc8-654eab637ebc,01dbf082-514a-4c6b-ae05-b0ec66c30f35,COLLEAGUE_OF,Person,Person
```

## Core Schema Ideas

### 1) `vertex_router` with `type_map`

`vertex_router` inspects a type discriminator field and routes each row to the appropriate vertex type:

```yaml
resources:
  - name: objects
    pipeline:
      - vertex_router:
          type_field: type
          type_map:
            Person: person
            Vehicle: vehicle
            Institution: institution
```

- `type_field`: Column whose value selects the vertex type
- `type_map`: Maps raw values (e.g. `Person`) to vertex type names (e.g. `person`)

Each row is dispatched to the correct `VertexActor` for that type. Fields are projected per vertex config; extra columns for other types are ignored.

### 2) `edge_router` with `relation_map`

`edge_router` creates edges with dynamic source/target types and relation names from document fields:

```yaml
  - name: relations
    pipeline:
      - edge_router:
          source_type_field: source_type
          target_type_field: target_type
          source_fields:
            id: source_id
          target_fields:
            id: target_id
          relation_field: relation_type
          type_map:
            Person: person
            Vehicle: vehicle
            Institution: institution
          relation_map:
            EMPLOYED_BY: employed_by
            OWNS: owns
            FUNDS: funds
            COLLEAGUE_OF: colleague_of
            INVESTS_IN: invests_in
```

- `source_type_field` / `target_type_field`: Columns that specify source and target vertex types
- `source_fields` / `target_fields`: Map document columns to vertex identity fields (e.g. `source_id` → `id`)
- `relation_field`: Column with the relation name (e.g. `EMPLOYED_BY`)
- `relation_map`: Maps raw relation values to canonical names (e.g. `EMPLOYED_BY` → `employed_by`)

For database ingestion, pre-declare all edge types in `edge_config` so collections are created during schema definition. The edge_router maps relation names to these edges at runtime.

### 3) Pre-declare edges for database ingestion

When writing to a graph database, declare all edge types in `edge_config` so collections are created during schema definition:

```yaml
edge_config:
  edges:
    - source: person
      target: institution
      relation: employed_by
    - source: person
      target: vehicle
      relation: owns
    - source: institution
      target: institution
      relation: funds
    - source: person
      target: person
      relation: colleague_of
    - source: institution
      target: institution
      relation: invests_in
```

The `relation` names must match the values in `relation_map` (e.g. `employed_by`).

## Graph Structure

The resulting graph has three vertex types and five edge types:

| Vertex   | Count | Example                    |
|----------|-------|----------------------------|
| person   | 4     | Alice Martin, Bob Smith    |
| vehicle  | 3     | Toyota Corolla, Tesla Model 3 |
| institution | 3  | TechNova Labs, FinEdge Capital |

| Edge Type     | Source → Target   | Example                          |
|---------------|-------------------|----------------------------------|
| employed_by   | person → institution | Alice → TechNova Labs         |
| owns          | person → vehicle    | Clara → Toyota Corolla         |
| funds         | institution → institution | GreenCity → TechNova     |
| colleague_of  | person → person     | Alice → Bob                    |
| invests_in    | institution → institution | FinEdge → TechNova      |

## Run the Example

```python
import pathlib

from suthing import FileHandle

from graflo import Bindings, GraphManifest
from graflo.db import ArangoConfig
from graflo.hq import GraphEngine
from graflo.hq.caster import IngestionParams
from graflo.architecture.bindings import FileConnector

manifest = GraphManifest.from_config(FileHandle.load("manifest.yaml"))
manifest.finish_init()
schema = manifest.require_schema()
ingestion_model = manifest.require_ingestion_model()
conn_conf = ArangoConfig.from_docker_env()
db_type = conn_conf.connection_type

bindings = Bindings()
bindings.add_file_connector(
    "objects",
    FileConnector(
        regex=r"^objects\.csv$",
        sub_path=pathlib.Path("."),
        resource_name="objects",
    ),
)
bindings.add_file_connector(
    "relations",
    FileConnector(
        regex=r"^relations\.csv$",
        sub_path=pathlib.Path("."),
        resource_name="relations",
    ),
)

engine = GraphEngine(target_db_flavor=db_type)
ingest_manifest = manifest.model_copy(update={"bindings": bindings})
ingest_manifest.finish_init()
engine.define_and_ingest(
    manifest=ingest_manifest,
    target_db_config=conn_conf,
    ingestion_params=IngestionParams(clear_data=True),
    recreate_schema=True,
)
```

Run from the example directory:

```bash
cd examples/7-objects-relations
uv run python ingest.py
```

## Key Takeaways

1. **`vertex_router`** routes polymorphic rows to the correct vertex type using a type discriminator column and `type_map`.
2. **`edge_router`** creates edges with dynamic source/target types and relation names from relation tables.
3. **`relation_map`** normalizes relation names (e.g. `EMPLOYED_BY` → `employed_by`) for consistent schema.
4. **Single-table polymorphism** — one objects table and one relations table can model a rich graph without separate resources per type.
5. **Order matters** — ingest objects (vertices) before relations (edges) so that edge endpoints exist when edges are created.

## Next Steps

- Explore [vertex_router](../reference/architecture/actor/vertex_router.md) and [edge_router](../reference/architecture/actor/edge_router.md) in the API reference.
- See the [full example code](https://github.com/growgraph/graflo/tree/main/examples/7-objects-relations) for the complete implementation.
