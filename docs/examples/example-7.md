# Example 7: Polymorphic Objects and Relations with `vertex_router` and dynamic `edge`

This example demonstrates how to ingest polymorphic entity data from a single objects table and dynamic relations from a relations table, using `vertex_router` to route rows to the correct vertex types and a dynamic `edge` step to create edges with types and relation names resolved at extraction time.

## Overview

The dataset contains:

- **objects.csv** — One table with mixed entity types (Person, Vehicle, Institution), distinguished by a `type` column
- **relations.csv** — One table describing relationships between entities, with source/target types and relation names in columns

Instead of separate resources per entity type, this schema uses:

1. **`vertex_router`** — Routes each objects row to the correct vertex type (`person`, `vehicle`, `institution`) via `type_map`
2. Two **`vertex_router`** steps on the relations resource — Accumulate source and target vertices into typed slots
3. A dynamic **`edge`** step — Creates edges with relation names resolved from `relation_field` via `relation_map`

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

### 1) `vertex_router` with `type_map` (objects resource)

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

### 2) Two `vertex_router` steps + dynamic `edge` (relations resource)

The relations resource uses two `vertex_router` steps to accumulate both endpoints, then a dynamic `edge` step to resolve the edge type at extraction time:

```yaml
  - name: relations
    pipeline:
      - vertex_router:
          type_field: source_type
          role: source
          from:
            id: source_id
          type_map:
            Person: person
            Vehicle: vehicle
            Institution: institution
      - vertex_router:
          type_field: target_type
          role: target
          from:
            id: target_id
          type_map:
            Person: person
            Vehicle: vehicle
            Institution: institution
      - edge:
          source_role: source
          target_role: target
          relation_field: relation_type
          relation_map:
            EMPLOYED_BY: employed_by
            OWNS: owns
            FUNDS: funds
            COLLEAGUE_OF: colleague_of
            INVESTS_IN: invests_in
```

- `role`: Names the accumulator slot (`lindex.(role, 0)`) for each `vertex_router` endpoint
- `source_role` / `target_role`: Match upstream `vertex_router.role`; the edge actor finds vertex types by scanning those role slots
- `from`: Projects relation-table columns onto vertex fields (e.g. `id: source_id`), same as on a `vertex` step
- `relation_field`: Column with the raw relation name (e.g. `EMPLOYED_BY`)
- `relation_map`: Maps raw relation values to canonical names (e.g. `EMPLOYED_BY` → `employed_by`)

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

**Default (server):** requires ArangoDB, Neo4j, TigerGraph, or FalkorDB. Start the database via Docker and load config from `docker/<db>/.env`:

```bash
cd examples/7-objects-relations
uv run python ingest.py
```

### Grafeo embedded alternative {#grafeo-embedded-alternative}

To run **without** a graph server, swap the connection block in [`ingest.py`](https://github.com/growgraph/graflo/tree/main/examples/7-objects-relations/ingest.py) for [Grafeo](https://github.com/GrafeoDB/grafeo) (in-memory or file-backed). Compare targets in [Graph database targets](../concepts/graph_database_targets.md).

```python
from graflo.db import GrafeoConfig

# conn_conf = GrafeoConfig.in_memory(database="objects_relations")
conn_conf = GrafeoConfig(database="objects_relations", path="objects_relations.grafeo")
```

Then run `uv run python ingest.py` as above. Full `GrafeoConfig` options: [Quick Start → Grafeo](../getting_started/quickstart.md#grafeo-embedded-target).

Expected output:

```
Ingestion complete!
Schema: objects_relations
Vertices: ['person', 'vehicle', 'institution']
```

## Key Takeaways

1. **`vertex_router`** routes polymorphic rows to the correct vertex type using a type discriminator column and `type_map`.
2. Two **`vertex_router`** steps on a relations resource accumulate both endpoint vertices into named role slots.
3. The dynamic **`edge`** step resolves source/target types from those slots and normalizes relation names via `relation_map`.
4. **`source_role` / `target_role`** on the `edge` step should equal the `role` of the corresponding `vertex_router` steps.
5. **Order matters** — ingest objects (vertices) before relations (edges) so that edge endpoints exist when edges are created.

---

## Variant: Flat-row pattern

When source and target vertices appear **in the same row** (e.g. a denormalized CSV where each row encodes a complete relationship tuple), the same `vertex_router` + `edge` pattern works without any changes. See [Example 11](example-11.md) for a fully runnable flat-row example.

## Next Steps

- Explore [vertex_router](../reference/architecture/pipeline/runtime/actor/vertex_router.md) in the API reference.
- See [Example 11](example-11.md) for the flat-row `vertex_router` + dynamic `EdgeActor` pattern.
- See the [full example code](https://github.com/growgraph/graflo/tree/main/examples/7-objects-relations) for the complete implementation.
