# Database connections

GraFlo needs to know which database to write the graph to: its address, the
credentials, and the name of the graph inside it. None of this is in the
manifest; you pass it in Python as a config object. This guide shows how to
build that object from environment variables, in code or from a YAML file, so
that you can point any script or example at your own database.

## What you need

- GraFlo installed (`pip install graflo`).
- The address and credentials of a running database, or a directory for the
  file backend.

## Steps

### 1. Pick the class for your database

Every class is imported from `graflo.connections`.

| Database | Class | Variable prefix | Typical URI | Field that names the graph |
|---|---|---|---|---|
| ArangoDB | `ArangoConfig` | `ARANGO_` | `http://localhost:8529` | `database` |
| Neo4j | `Neo4jConfig` | `NEO4J_` | `bolt://localhost:7687` | `database` |
| TigerGraph | `TigergraphConfig` | `TIGERGRAPH_` | `http://localhost:14240` | `schema_name` |
| FalkorDB | `FalkordbConfig` | `FALKORDB_` | `redis://localhost:6379` | `database` |
| Memgraph | `MemgraphConfig` | `MEMGRAPH_` | `bolt://localhost:7687` | `database` |
| NebulaGraph | `NebulaConfig` | `NEBULA_` | `nebula://localhost:9669` | `schema_name` (the space) |
| PostgreSQL | `PostgresConfig` | `POSTGRES_` | `postgresql://localhost:5432` | `database` and `schema_name` |
| File backend | `GraFloBackendConfig` | `GRAFLO_BACKEND_` | none | `output_dir` |

If you leave the field that names the graph empty, GraFlo uses the manifest's
`schema.metadata.name`, adjusted to the characters the database accepts; see
[Graph namespace and schema](graph_namespace_and_schema.md). PostgreSQL is
different: `database` must name a database that exists, and GraFlo creates the
tables in the schema `schema_name`, `public` when it is not set.

Write the port in the URI. When it is missing GraFlo adds a default, and for
Neo4j that default is the HTTP port 7474, not the Bolt port 7687.

### 2. Fill in the settings

Four ways lead to the same object. Environment variables suit most programs;
the others follow.

#### From environment variables

```bash
export ARANGO_URI=http://localhost:8529
export ARANGO_USERNAME=root
export ARANGO_PASSWORD=change-me
export ARANGO_DATABASE=plant
```

```python
from graflo.connections import ArangoConfig

conn_conf = ArangoConfig.from_env()
```

`from_env()` reads each field from the variable named by the prefix and the
field name in upper case: `ARANGO_URI`, `ARANGO_USERNAME`, `ARANGO_PASSWORD`,
`ARANGO_DATABASE`. Fields that only one database has are read the same way,
for example `NEO4J_BOLT_PORT`, `TIGERGRAPH_SECRET` (token authentication) and
`NEBULA_VERSION` (`3` or `5`). `schema_name` is the exception: it is not read
from a prefixed variable, so for TigerGraph, NebulaGraph and the PostgreSQL
schema pass it in code, as shown next.

When one program talks to two databases of the same kind, give each set of
variables its own qualifier:

```bash
export SENSORS_ARANGO_URI=http://sensors-db.example:8529
export SENSORS_ARANGO_DATABASE=sensor_feed
```

```python
sensors_conf = ArangoConfig.from_env(prefix="SENSORS")
```

`prefix="SENSORS"` reads `SENSORS_ARANGO_URI`. The two other qualifiers place
the word elsewhere: `profile="DEV"` reads `ARANGO_DEV_URI`, and `suffix="DEV"`
reads `ARANGO_URI_DEV`. Use one qualifier per call.

#### In code

```python
from graflo.connections import TigergraphConfig

conn_conf = TigergraphConfig(
    uri="http://localhost:14240",
    username="tigergraph",
    schema_name="plant",
)
```

A keyword argument wins over the environment, and a field you do not pass is
still read from it. Here the password comes from `TIGERGRAPH_PASSWORD`, while
the name of the graph is fixed in code.

For PostgreSQL, `PostgresConfig.from_dsn("postgresql://user:password@host:5432/plant")`
fills the address, credentials and database from one connection string.

#### In a YAML file, for the command line

`graflo ingest --db-config-path db.yaml` and
`graflo migrate-schema apply --db-config-path db.yaml` read the database from
a file. `db_type` picks the class: `arango`, `neo4j`, `tigergraph`,
`falkordb`, `memgraph`, `nebula`, `postgres` or `graflo_backend`. The other
keys are the fields of that class.

```yaml
db_type: neo4j
uri: bolt://localhost:7687
username: neo4j
database: plant
```

The file is read with `DBConfig.from_dict`, and fields it leaves out are read
from the environment. Keep the password out of the file and set
`NEO4J_PASSWORD` instead.

#### From the containers of the repository

A clone of the repository has a Docker Compose setup per database under
`docker/`;
[`docker/README.md`](https://github.com/growgraph/graflo/blob/main/docker/README.md)
explains how to start them. The examples connect to them with:

```python
conn_conf = ArangoConfig.from_docker_env()
```

`from_docker_env()` reads the settings file of the container in
`docker/arango/` (`docker/neo4j/` for `Neo4jConfig`, and so on). It works only
in a clone, because those files belong to the repository, not to the installed
package; pass `docker_dir=` to read another directory. `GraFloBackendConfig`
has no container: give it `output_dir` instead.

### 3. Pass the config to the engine

Hand the config to `GraphEngine` together with a loaded manifest, as in the
[quick start](../getting_started/quickstart.md):

```python
from graflo.hq import GraphEngine

engine = GraphEngine(target_db_flavor=conn_conf.connection_type)
engine.define_and_ingest(manifest=manifest, target_db_config=conn_conf)
```

`connection_type` is the kind of database the config describes. Passing it to
`GraphEngine` makes the engine prepare the schema for that database.

## What you should see

Print the config before you connect to check what was read. For the
environment variables of step 2:

```python
print(conn_conf.connection_type, conn_conf.uri, conn_conf.database)
```

```text
arango http://localhost:8529 plant
```

A field that shows `None` was found neither in the call nor in the
environment.

## Databases you read from

A PostgreSQL database, a SPARQL endpoint, a REST API or a Kafka topic that
records come from is not configured this way. The manifest names it by a label
(`conn_proxy`), and at run time you register the settings for that label with
a connection provider, so the manifest never holds a password. The
[connection proxy example (11)](../examples/connection-proxy/index.md) shows
it for PostgreSQL, and the
[API environment example (12)](../examples/api-env-config/index.md) for REST
APIs.

## What to read next

- [Graph namespace and schema](graph_namespace_and_schema.md): how the name of
  the graph is chosen, and how to use a graph an administrator created.
- [Credentials outside the manifest](../examples/connection-proxy/index.md):
  the settings of the databases you read from.
- [A graph on disk, without a database](../examples/file-backend-export/index.md):
  the file backend.
