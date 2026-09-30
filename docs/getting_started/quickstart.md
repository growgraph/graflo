# Quick start

You have two CSV files. One lists people, the other says which department each
person works in. On this page you write a manifest that describes the graph
you want, run one script, and get one vertex per person, one per department
and an edge from each person to their department. It follows the first example
shipped with the repository.

## What you need

- A clone of the repository with the package installed:

    ```bash
    git clone https://github.com/growgraph/graflo.git
    cd graflo
    uv sync
    ```

- A running ArangoDB. The repository ships a container for it; how to start it
  is described in
  [`docker/README.md`](https://github.com/growgraph/graflo/blob/main/docker/README.md).
  The [file backend example (14)](../examples/file-backend-export/index.md)
  writes a graph to disk and needs no database.

## 1. Look at the data

The example lives in `examples/01-csv-two-resources`. Its `data/` directory
holds `people.csv` and `departments.csv`:

```csv
id,name,age
1,John Hancock,27
2,Mary Arpe,33
3,Sid Mei,45
```

```csv
person_id,person,department
1,John Hancock,Sales
2,Mary Arpe,R&D
3,Sid Mei,Customer Service
```

The same three people appear in both files. In the graph each of them must be
one vertex.

## 2. Write the manifest

The manifest, `manifest.yaml`, has three blocks. The `schema` block says what
the graph looks like: two vertex types and one edge. `identity` names the
properties that make a vertex unique, so two rows with the same `id` become one
`person`.

```yaml
schema:
    metadata:
        name: hr
    graph:
        vertex_config:
            vertices:
            -   name: person
                properties: [id, name, age]
                identity: [id]
            -   name: department
                properties: [name]
                identity: [name]
        edge_config:
            edges:
            -   source: person
                target: department
    db_profile: {}
```

The `ingestion_model` block says how a row becomes vertices. It holds one
resource per kind of file, and a resource is a list of steps that run on every
row. `from` maps a column to a property where their names differ. The
`departments` resource yields a person and a department from each row, and the
schema declares an edge between them, so GraFlo adds the edge.

```yaml
ingestion_model:
    resources:
    -   name: people
        pipeline:
        -   vertex: person
    -   name: departments
        pipeline:
        -   vertex: person
            from: {id: person_id, name: person}
        -   vertex: department
            from: {name: department}
```

The `bindings` block says where the rows come from: one connector per file
name pattern, each naming the resource that reads its files. `sub_path` is
relative to the directory the script runs in.

```yaml
bindings:
    connectors:
    -   regex: "^people.*\\.csv$"
        sub_path: data
        resource_name: people
    -   regex: "^dep.*\\.csv$"
        sub_path: data
        resource_name: departments
```

## 3. Run it

```bash
cd examples/01-csv-two-resources
uv run python ingest.py
```

`ingest.py` loads the manifest, reads the connection settings of the ArangoDB
container, creates the schema in the database and loads the files:

```python
from suthing import FileHandle

from graflo import GraphManifest
from graflo.connections import ArangoConfig
from graflo.hq import GraphEngine
from graflo.hq.caster import IngestionParams

manifest = GraphManifest.from_config(FileHandle.load("manifest.yaml"))
manifest.finish_init()

# Connection settings of the ArangoDB container started from docker/arango.
conn_conf = ArangoConfig.from_docker_env()

engine = GraphEngine(target_db_flavor=conn_conf.connection_type)
engine.define_and_ingest(
    manifest=manifest,
    target_db_config=conn_conf,
    ingestion_params=IngestionParams(clear_data=True),
    recreate_schema=True,
)
```

`finish_init()` connects the resources to the schema. `recreate_schema=True`
and `clear_data=True` drop what an earlier run wrote, so you can run the script
again.

## What you should see

Open the ArangoDB web interface; its address is in `docker/README.md`. When the
connection settings name no database, GraFlo names it after `metadata.name`,
here `hr`.

| | Count | Why |
|---|---|---|
| `person` vertices | 3 | Both files mention each person; rows with the same `id` become one vertex |
| `department` vertices | 3 | One per distinct department name |
| `person` to `department` edges | 3 | One per row of `departments.csv` |

The manifest names no database: with `Neo4jConfig` in place of `ArangoConfig`
in `ingest.py`, the same graph goes to the Neo4j container.

## What to read next

- [Creating a manifest](creating_manifest.md): each block of the manifest one
  level deeper.
- [Database connections](../guides/database_connections.md): how to point
  GraFlo at your own database.
- [Examples](../examples/index.md): the next questions, one example each.
