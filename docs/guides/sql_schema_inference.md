# Inferring a graph from a SQL database

You have a relational database and want its data as a graph without writing
the schema by hand. GraFlo reads the tables and their keys and proposes a draft
manifest: entity tables become vertex types, link tables become edges, and
columns become typed properties. This guide takes a PostgreSQL schema, or any
database SQLAlchemy can read, to a draft manifest, shows what to check and what
to add, and writes the graph. The draft is only as good as the keys the
database declares.

## What you need

- GraFlo installed (`pip install graflo`). It reads PostgreSQL as it is; for
  another engine, install that engine's SQLAlchemy dialect, such as `pymysql`
  for MySQL, `duckdb-engine` for DuckDB or `sqlalchemy-bigquery` for BigQuery.
- Tables that declare primary keys, and foreign keys where rows refer to each
  other.
- A target graph database and its connection settings; see
  [Database connections](database_connections.md).

[The PostgreSQL inference example (09)](../examples/infer-from-postgres/index.md)
runs these steps on a sample database.

## Steps

### 1. Infer a manifest

```python
from graflo import DBType
from graflo.connections import PostgresConfig
from graflo.hq import GraphEngine

# Reads POSTGRES_URI, POSTGRES_USERNAME, POSTGRES_PASSWORD, POSTGRES_DATABASE.
pg_config = PostgresConfig.from_env()

engine = GraphEngine(target_db_flavor=DBType.NEO4J)
manifest = engine.infer_manifest(pg_config, schema_name="public")
```

`infer_manifest` reads the tables of the PostgreSQL schema `public` with their
columns and keys, and returns a complete manifest:

- a `schema` with a vertex type per entity table and an edge per link table;
- an `ingestion_model` with one [resource](../concepts/glossary.md#resource)
  per table;
- `bindings` with one table connector per table, all under the connection
  label `postgres_source`.

The manifest holds no credentials. The engine keeps the PostgreSQL settings for
the label `postgres_source`, so the same engine can read the tables in step 4.

The target flavor does not change the vertex types or edges. It is recorded in
the database profile, and for TigerGraph GraFlo also records storage names that
avoid TigerGraph's reserved words and forbidden characters.

The graph is named after the PostgreSQL schema. When the target connection
settings name no database, that name becomes the target database or graph. To
choose another name, set it before you write anything:

```python
manifest.require_schema().metadata.name = "plant"
```

`infer_manifest` also takes `discard_disconnected_vertices=True`, which drops
vertex types that take part in no edge, with their resources and connectors,
`fuzzy_threshold` (default 0.8), how closely a column name must match a vertex
type name when GraFlo maps a link table's key columns to its endpoints, and
`entity_tables`, described in step 2.

### 2. Check the draft

Save the draft and read it:

```python
from suthing import FileHandle

FileHandle.dump(manifest.to_minimal_canonical_dict(), "plant-manifest.yaml")
```

GraFlo classifies each table by its keys, and a link table also by its name:

| Table | Becomes |
|---|---|
| No primary key | Nothing. The table is skipped |
| Primary key of two or more columns, exactly two foreign keys, or a name starting with `rel_` | An edge. The first foreign key in column order is the source, the second the target. The relation is a word of the table name that names neither endpoint (`maintained` in `machine_maintained_by_technician`). The other non-key columns become edge properties |
| Primary key, not an edge, and at least one column that is neither a primary nor a foreign key | A vertex type. Its identity is the primary key; every column becomes a property |
| Primary key and key columns only, not an edge | Nothing |

A foreign key inside a vertex table that references the primary key of another
vertex table becomes an edge too, from the row to the row it references. A
`work_order` table whose `machine_id` column references `machine` gives an edge
from `work_order` to `machine` named `machine`: the column without a trailing
`_id`, or the referenced table's name for a key of several columns. The
`work_order` resource writes one such edge per row whose `machine_id` is set.

Each skipped table is logged as a warning with the reason. The reasons are
also in the introspection result, from
`engine.introspect(pg_config, schema_name="public").skipped_tables`.

Check the draft for these cases, which inference gets wrong by construction:

- **A table with exactly two foreign keys becomes an edge**, even when it
  describes a thing of its own. A `work_order` table that references both
  `machine` and `technician` becomes an edge from machine to technician; its
  own primary key is dropped and its other columns become edge properties.
  The schema cannot tell the two cases apart. If it should be a vertex type,
  name it when you infer; its foreign keys then become edges from it:

    ```python
    manifest = engine.infer_manifest(
        pg_config, schema_name="public", entity_tables=["work_order"]
    )
    ```

- **A link table whose endpoints cannot be found is skipped**, for example a
  `rel_` table without foreign keys whose name matches no vertex type.
- **A foreign key that references a column other than the primary key is not
  an edge**: a row is found by its primary key, and nothing else identifies it.

Inference sees only what the database declares. That is the main limit on a
denormalized schema: in a star schema, dimension keys are columns like
`customer_key` with no constraint saying what they reference. Warehouses often
declare no foreign keys at all, and some SQLAlchemy dialects cannot report
them, which GraFlo treats as none declared. GraFlo then falls back to matching
table and column names, which finds some link tables and misses the rest, and
it finds no reference from an entity table. A database that declares no
primary keys gives an empty draft, because every table is skipped. For such a
source, add primary keys where you can, and treat the draft as a starting
point: declare the missing vertex types and joins yourself in step 3.

Column types come from a table of type names that covers the spellings of
PostgreSQL, MySQL, SQL Server, SQLite, BigQuery and Snowflake:

| SQL types | GraFlo type |
|---|---|
| `integer`, `bigint`, `smallint`, `serial`, `int64`, `tinyint`, ... | `INT` |
| `real`, `double precision`, `numeric`, `decimal`, `float64`, `number`, ... | `FLOAT` |
| `boolean`, `bit` | `BOOL` |
| `timestamp`, `timestamptz`, `date`, `time`, `datetime2`, ... | `DATETIME` |
| `varchar`, `text`, `json`, `jsonb`, `uuid`, `bytea`, `interval`, ... | `STRING` |
| an array such as `integer[]` | `LIST`, with the element type as `item_type` |

A type name that is not in the table becomes `STRING`, with a warning in the
log, so one unusual column does not stop the whole inference. For the
properties of an edge, GraFlo also reads up to five rows and refines the
declared type from the values: a `TEXT` column of ISO dates becomes
`DATETIME`. Values of `numeric` and `decimal` columns are read as floats, so
they lose exact decimal precision.

To add type names, subclass `SqlTypeMapper` and extend its `TYPE_MAPPING`
dict. Exact names are matched before partial ones, and appended entries come
last in the partial matching, so they do not change how the names already in
the table resolve. `infer_manifest` always uses the default mapper; to use
yours, run inference through the `SQLInferenceManager` described under
[Other databases](#other-databases), which also accepts a `PostgresConnection`
(`graflo.db.postgres.conn`), and replace its mapper:

```python
from graflo.db.sql.types import SqlTypeMapper


class PlantTypeMapper(SqlTypeMapper):
    TYPE_MAPPING = {**SqlTypeMapper.TYPE_MAPPING, "money": "FLOAT"}


manager.inferencer.type_mapper = PlantTypeMapper()
```

### 3. Add what inference cannot see

Edit the saved manifest. When the database declares no foreign key for a
`machine_serial` column of `work_order`, add the edge from a work order to the
machine that column names: declare the edge and let the `work_order` resource
find the machine by that column:

```yaml
schema:
    core_schema:
        edge_config:
            edges:
            -   source: work_order
                target: machine
                relation: concerns
ingestion_model:
    resources:
    -   name: work_order
        pipeline:
        -   vertex: work_order
        -   vertex: machine
            from:
                serial_number: machine_serial
            lookup_only: true
```

The second step reads `machine_serial` as the machine's `serial_number`.
`lookup_only: true` uses it to find the machine for the edge without writing a
machine vertex from the work order row. Keep the edges and resources the draft
already has; the snippet shows only what to add.

Load the edited manifest before you continue:

```python
from graflo import GraphManifest

manifest = GraphManifest.from_config(FileHandle.load("plant-manifest.yaml"))
manifest.finish_init()
```

### 4. Write the graph

```python
from graflo.connections import Neo4jConfig
from graflo.hq import IngestionParams

engine.define_and_ingest(
    manifest=manifest,
    target_db_config=Neo4jConfig.from_env(),
    ingestion_params=IngestionParams(clear_data=True),
    recreate_schema=True,
)
```

`define_and_ingest` creates the schema in the target database and reads every
table through its connector. `recreate_schema=True` drops an existing graph
schema first; without it, the call stops when the schema already exists.

The engine from step 1 keeps the PostgreSQL settings. In a new session,
give them to the ingestion yourself, for the label the bindings use:

```python
from graflo.connections import InMemoryConnectionProvider, PostgresGeneralizedConnConfig

provider = InMemoryConnectionProvider()
provider.bind_single_config_for_bindings(
    bindings=manifest.require_bindings(),
    conn_proxy="postgres_source",
    config=PostgresGeneralizedConnConfig(config=PostgresConfig.from_env()),
)
engine = GraphEngine(target_db_flavor=DBType.NEO4J)
engine.define_and_ingest(
    manifest=manifest,
    target_db_config=Neo4jConfig.from_env(),
    connection_provider=provider,
    recreate_schema=True,
)
```

### Other databases

Any engine SQLAlchemy can reflect goes through `SqlAlchemyMetadataProvider`,
which asks the same questions PostgreSQL answers from its own catalog: tables,
columns, primary keys, single-column unique constraints, foreign keys, row
counts and sample rows.

```python
from sqlalchemy import create_engine

from graflo import DBType
from graflo.db.sql.alchemy import SqlAlchemyMetadataProvider
from graflo.hq.sql_inferencer import SQLInferenceManager

sql_engine = create_engine("sqlite:///plant.db")
provider = SqlAlchemyMetadataProvider(sql_engine)
manager = SQLInferenceManager(provider, target_db_flavor=DBType.NEO4J)
schema, ingestion_model = manager.infer_complete_schema()
```

To read a namespace other than the engine's default, such as a MySQL database
or a BigQuery dataset, pass it as `default_schema`:

```python
provider = SqlAlchemyMetadataProvider(sql_engine, default_schema="analytics")
```

Leave it unset for an engine with a single namespace, such as SQLite.

This path returns a schema and an ingestion model, but no bindings, because
GraFlo's table connectors read from PostgreSQL. To load the data, export the
tables to files and bind each resource to a file connector. It also leaves
storage names as they are; for a TigerGraph target, run
`Sanitizer(DBType.TIGERGRAPH).sanitize_manifest(manifest)` (from
`graflo.hq.sanitizer`) on the manifest you build from them.

The table classification is the same for every engine. It is tested in this
repository against SQLite and PostgreSQL; other engines use the same code but
are not tested here.

## What you should see

After step 1, the saved manifest lists a vertex type for each entity table,
with the table's primary key as its identity, an edge for each link table, and
an edge for each foreign key of an entity table. Every other table is named in
a warning with the reason it was skipped, and so is each unrecognized column
type. After step 4, the target database holds one vertex per row of each
entity table, an edge per row of each link table, an edge per set foreign key
of an entity table, plus the edges you added.

## What to read next

- [Vertex identity](../concepts/schema/vertex_identity.md): how the inferred
  identities decide which rows become the same vertex.
- [Creating a manifest](../getting_started/creating_manifest.md): the manifest
  blocks you edit in step 3.
- [Database connections](database_connections.md): connection settings for the
  source and the target.
