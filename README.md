# GraFlo — Graph Schema & Transformation Language (GSTL) <img src="https://raw.githubusercontent.com/growgraph/graflo/main/docs/assets/favicon.ico" alt="graflo logo" style="height: 32px; width:32px;"/>


![Python](https://img.shields.io/badge/python-3.11%2B-blue.svg) 
[![PyPI version](https://badge.fury.io/py/graflo.svg)](https://badge.fury.io/py/graflo)
[![PyPI Downloads](https://static.pepy.tech/badge/graflo)](https://pepy.tech/projects/graflo)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-green)](https://github.com/growgraph/graflo/blob/main/LICENSE)
[![pre-commit](https://github.com/growgraph/graflo/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/growgraph/graflo/actions/workflows/pre-commit.yml)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.20601698.svg)](https://doi.org/10.5281/zenodo.20601698)


> *Declare, evolve and version a graph world model.*

**GraFlo** makes a graph data model a **versioned artifact**. One `GraphManifest` — YAML or Python —
declares the whole labeled property graph: vertex and edge types, typed properties, explicit
identities, semantic grounding, ingestion pipelines and connector bindings. From that one document
GraFlo validates the model, evolves it under version control, checks it against a conformance
profile, and projects it into any supported backend.

The manifest is the contract, not the database. Validation happens once, in `finish_init`, rather
than at write time — so a manifest that loads is a manifest that will project, and the backend
never becomes the place where the model is really defined.

### What you get

**Declare**

- **Explicit identity, not implicit keys** — every vertex declares how it is keyed: a natural key, a
  deterministic `hash` over chosen properties, an ordered **identity funnel** (fall through several
  keys in priority order), a `blank` node, or an `assigned` UUID. Identity backs upserts, so reloads
  merge on keys instead of duplicating nodes, and `secondary_identities` give edge steps additional
  named lookups.
- **Typed properties** — `INT`, `UINT`, `FLOAT`, `DOUBLE`, `BOOL`, `STRING`, `DATETIME`, `UUID`, and
  `LIST` with a declared `item_type`. Types are optional where a backend does not need them and
  enforced where it does.
- **Semantic grounding** — vertices, edges and properties may carry an `iri`, `exact_match`,
  `synonyms`, and — on properties — a `unit`. Purely descriptive: identity, naming and ingestion
  behave identically with or without it.
- **Reusable ingestion** — `Resource` actor pipelines (*descend*, *transform*, *vertex*,
  *vertex_router*, *edge*) bind to files, SQL, SPARQL/RDF, REST APIs, Kafka topics or in-memory
  batches through `Bindings` and the `DataSourceRegistry`.

**Evolve and version**

- **A change algebra, not hand-edited YAML** — 37 typed operations (`merge_vertices`,
  `rename_relations`, `change_field_types`, `add_secondary_identities`, `project_manifest`,
  `compose_manifests`, …) applied through `apply_evolution`. `diff_manifests` derives them from two
  manifests, so a change set can be generated, reviewed, and replayed.
- **Content-addressed commits** — `graflo commit`, `log`, `checkout`, `verify`, `revert`, `stamp`.
  Every commit records a tree hash; `verify` replays each head and checks it, so a stored history
  that no longer reproduces its manifests is detectable rather than merely suspected.
- **Invertible operations** — most ops carry an inverse computed against the pre-state
  (`invert_ops`), so a change set can be undone exactly rather than approximately. The few genuinely
  irreversible ops (a type change, a property removal) are declared as such instead of failing
  quietly.
- **Three-way merge with slot-level conflicts** — `graflo merge` reconciles two commits against
  their common ancestor. Conflicts are reported per *slot* (a vertex's identity, one field's type,
  one edge's directionality) with the ancestor state attached, and a conflicted merge produces no
  manifest at all rather than a silently chosen side.
- **Compose two models under declared equivalences** — `graflo compose` unions two manifests given
  explicit vertex/relation equivalence clusters and property alignments. It infers nothing: where
  two sides disagree on a property type, a unit, an identity, a storage name or a backend flavor, it
  names the disagreement and refuses. Nothing is silently elected from one side.

**Project and load**

- **One model, eight backends** — the same manifest targets ArangoDB, Neo4j, TigerGraph, FalkorDB,
  Memgraph, NebulaGraph, a chunked **file backend**, or **PostgreSQL** (relational vertex tables plus
  junction edge tables). `DatabaseProfile` and the DB-aware projection absorb naming, type support,
  defaults and indexing differences, so backend specifics never leak into the logical model.
- **Schema migrations** — plan and apply guarded deltas against a live database, with each operation
  classified by risk and backward compatibility (`graflo migrate-schema`; library in `graflo.migrate`).

**Ground and check**

- **The World Model Profile** — `graflo check` scores a manifest against a named conformance level
  (`world-model` v0.1) of six mechanically checkable assertions: types are grounded in an external
  vocabulary, every vertex declares an identity mode, every edge declares directionality, every
  measured property carries a unit, temporal validity is declared or explicitly waived, and
  provenance is expressible and attached. Operator **waivers** are first-class — a waived assertion
  is reported as waived with its reason, never as a pass.
- **`graflo lift`** — plans an existing manifest into a twin-ready schema against a declared
  `LiftSpec`, emitting the planned ops as a reviewable artifact before anything is written.
- **Manifest as linked data** — the [GraFlo ontology](https://growgraph.github.io/graflo/concepts/schema/ontology/)
  (`gf:` at `ontology.growgraph.dev`) round-trips a manifest to RDF for tooling, provenance and
  SPARQL-facing catalogs.

### What's in the manifest

- **`schema`** — `Schema`: metadata, **`core_schema`** (vertices, edges, typed **`properties`**, identities, semantics), and **`db_profile`** (`DatabaseProfile`: target flavor, storage names, secondary indexes, `default_property_values`, …).
- **`ingestion_model`** — `IngestionModel`: named **`resources`** (actor sequences: *descend*, *transform*, *vertex*, *vertex_router*, *edge*, …) and a registry of reusable **`transforms`**.
- **`bindings`** — Connectors (`FileConnector`, `TableConnector`, `SparqlConnector`, `APIConnector`) plus **`resource_connector`** wiring. Optional **`connector_connection`** maps connectors to **`conn_proxy`** labels so YAML stays secret-free; a runtime **`ConnectionProvider`** supplies credentials. See [API connector and pagination](docs/concepts/connectors/api_connector.md).

Each block is optional: a manifest may carry only a schema, only bindings, or all three.

### Version control for models

```bash
# Record the change between two manifests as a content-addressed commit
graflo commit --from-manifest v1.yaml --to-manifest v2.yaml -m "key people by email"

# History, oldest first, with parent edges
graflo log --graph

# Replay every head from the root manifest and check each recorded tree hash
graflo verify --base v1.yaml

# Reconstruct the manifest as of a commit
graflo checkout <commit-id> --base v1.yaml --output-path at-that-point.yaml

# Three-way against the common ancestor; conflicts are reported per slot.
# Omit --take to see them rather than resolve them.
graflo merge <left-id> <right-id> --base v1.yaml

# Record a new commit undoing an earlier one
graflo revert <commit-id> --base v1.yaml
```

> `revert` is the least exercised of these verbs — it has no functional test in the
> suite and may need the history checked out from its base first. Prefer `checkout`
> when you want a known-good earlier state.


Composition and conformance run on manifests directly, with no history involved:

```bash
graflo compose left.yaml right.yaml --op equivalences.yaml -o composed.yaml
graflo check manifest.yaml --profile world-model --json
```

### Runtime path

1. **Source instance** — Batches from a `DataSourceType` adapter (`FileDataSource`, `SQLDataSource`, `SparqlEndpointDataSource`, `APIDataSource`, `KafkaDataSource`, …).
2. **Resource (actors)** — Maps records to graph elements against the logical schema.
3. **`GraphContainer`** — Intermediate, database-agnostic vertex/edge batches.
4. **DB-aware projection** — `Schema.resolve_db_aware()` plus `VertexConfigDBAware` / `EdgeConfigDBAware` for the active `DBType`.
5. **Graph DB** — `DBWriter` + `ConnectionManager` and the backend-specific `Connection`.

| Piece | Role | Code |
|-------|------|------|
| **Logical graph schema** | Manifest `schema`: vertex/edge definitions, identities, typed **properties**, DB profile. Constrains pipeline output and projection. | `Schema`, `VertexConfig`, `EdgeConfig` (under `core_schema`). |
| **Source instance** | Concrete input: file, SQL table, SPARQL endpoint, API payload, Kafka topic, in-memory rows. | `AbstractDataSource` + `DataSourceType`. |
| **Resource** | Ordered actors; looked up by name when sources are registered. | `Resource` in `IngestionModel`. |
| **Covariant graph** (`GraphContainer`) | Batches of vertices/edges before load. | `GraphContainer`. |
| **DB-aware projection** | Physical names, defaults, indexes for the target. | `Schema.resolve_db_aware()`, `VertexConfigDBAware`, `EdgeConfigDBAware`. |
| **Graph DB** | Target LPG; each `DBType` has its own connector, orchestrated the same way. | `ConnectionManager`, `DBWriter`, per-backend `Connection`. |
| **Change algebra** | Typed ops over a manifest, with inverses, content hashes and commits. | `graflo.architecture.evolution`, `graflo.cli.commit`. |

### Supported source types (`DataSourceType`)

| DataSourceType | Connector | DataSource | Schema inference |
|---|---|---|---|
| `FILE` — CSV / JSON / JSONL / Parquet | `FileConnector` | `FileDataSource` | manual |
| `SQL` — relational tables (docs focus on PostgreSQL; other engines via SQLAlchemy where supported) | `TableConnector` | `SQLDataSource` | automatic for PostgreSQL-style 3NF (PK/FK heuristics) |
| `SPARQL` — RDF files (`.ttl`, `.rdf`, `.n3`) | `SparqlConnector` | `RdfFileDataSource` | automatic (OWL/RDFS ontology) |
| `SPARQL` — SPARQL endpoints (Fuseki, …) | `SparqlConnector` | `SparqlEndpointDataSource` | automatic (OWL/RDFS ontology) |
| `API` — REST APIs | `APIConnector` | `APIDataSource` | manual |
| `KAFKA` — topics | — | `KafkaDataSource` | manual |
| `IN_MEMORY` — list / DataFrame | — | `InMemoryDataSource` | manual |

### Supported targets

The eight backends listed under **Project and load** are the supported **output** `DBType` values in `graflo.onto`. Each uses its own `Connection` implementation under the shared `ConnectionManager` / `DBWriter` / `GraphEngine` flow.

**Graph sources** (introspection and bulk export) are supported on **Neo4j**, **ArangoDB**, **PostgreSQL**, and the **GraFlo file backend**. Note that `GraphEngine.migrate_graph()` itself currently fails at the write step for every source/target pair; `export_graph()` and `infer_schema_from_graph()` are unaffected. See [Graph export and migration](docs/concepts/operations/graph_export_migration.md).

## More capabilities

- **Schema inference** — From 3NF relational layouts (PK/FK heuristics) on **any engine SQLAlchemy can reflect** — PostgreSQL reads its own catalogue, everything else arrives through reflection — or from OWL/RDFS (`owl:Class` → vertices, `owl:ObjectProperty` → edges, `owl:DatatypeProperty` → vertex fields).
- **GraFlo ontology (manifest RDF)** — Serialize any `GraphManifest` to RDF (Turtle, JSON-LD) using the published vocabulary at [`https://ontology.growgraph.dev/graflo`](https://ontology.growgraph.dev/graflo) (`owl:versionInfo` **1.0.0**). Covers schema, ingestion and bindings. Round-trip via `graflo.rdf` or the `manifest-to-rdf` / `rdf-to-manifest` CLI. This is the **meta-model** of GraFlo itself — distinct from importing a **domain** OWL ontology into an LPG schema (`RdfInferenceManager`).
- **SPARQL & RDF** — Endpoints and RDF files (`.ttl`, `.rdf`, `.n3`, …); optional OWL/RDFS **domain** schema inference (`rdflib`, `SPARQLWrapper` in the default install).
- **Graph export** — Introspect Neo4j, ArangoDB or PostgreSQL and export to a **chunked file backend** (`GraFloBackendConfig`), or ingest manifest resources straight to disk.
- **REST API env wiring** — Register `base_url` and `ApiAuth` credentials from environment variables per `conn_proxy` label (`register_all_api_configs_from_env`).
- **Batching & concurrency** — Configurable batch sizes, worker counts, and DB write concurrency on `IngestionParams` / `DBWriter`.
- **`GraphEngine`** — High-level orchestration for infer, define schema, and ingest (`define_and_ingest`, …); `Caster` stays available for lower-level control.

## Documentation
Full documentation is available at: [growgraph.github.io/graflo](https://growgraph.github.io/graflo)

## Installation

```bash
pip install graflo
```

Optional extras (see `pyproject.toml` → `[project.optional-dependencies]`):

- `dev` — pytest, ty, pre-commit
- `docs` — MkDocs stack for building the documentation site
- `plot` — `pygraphviz` for the `plot_manifest` CLI (install system Graphviz first)

```bash
pip install "graflo[dev]"
pip install "graflo[dev,docs,plot]"
```

## Usage Examples

### Simple ingest

```python
from suthing import FileHandle

from graflo import Bindings, GraphManifest
from graflo.connections.onto import ArangoConfig

manifest = GraphManifest.from_config(FileHandle.load("schema.yaml"))
manifest.finish_init()
schema = manifest.require_schema()
ingestion_model = manifest.require_ingestion_model()

# Option 1: Load config from docker/arango/.env (recommended)
conn_conf = ArangoConfig.from_docker_env()

# Option 2: Load from environment variables
# Set: ARANGO_URI, ARANGO_USERNAME, ARANGO_PASSWORD, ARANGO_DATABASE
conn_conf = ArangoConfig.from_env()

# Option 3: Load with custom prefix (for multiple configs)
# Set: USER_ARANGO_URI, USER_ARANGO_USERNAME, USER_ARANGO_PASSWORD, USER_ARANGO_DATABASE
user_conn_conf = ArangoConfig.from_env(prefix="USER")

# Option 4: Create config directly
# conn_conf = ArangoConfig(
#     uri="http://localhost:8535",
#     username="root",
#     password="123",
#     database="mygraph",  # For ArangoDB, 'database' maps to schema/graph
# )
# Note: If 'database' (or 'schema_name' for TigerGraph) is not set,
# Caster will automatically use Schema.metadata.name as fallback

from graflo.architecture.contract.bindings import FileConnector
import pathlib

# Create Bindings with file connectors
bindings = Bindings()
work_connector = FileConnector(regex="\Sjson$", sub_path=pathlib.Path("./data"))
bindings.add_connector(
    work_connector,
)
bindings.bind_resource("work", work_connector)

# Or initialize via connectors + resource_connector
# bindings = Bindings(
#     connectors=[
#         FileConnector(
#             name="work_files",
#             regex="^work\\.json$",
#             sub_path=pathlib.Path("./data"),
#         )
#     ],
#     resource_connector=[{"resource": "work", "connector": "work_files"}],
#     # Optional: for SQL/SPARQL connectors, name a proxy; register secrets via ConnectionProvider.
#     # connector_connection=[{"connector": "work_files", "conn_proxy": "files_readonly"}],
# )

from graflo.hq.caster import IngestionParams
from graflo.hq import GraphEngine

# Option 1: Use GraphEngine for schema definition and ingestion (recommended)
engine = GraphEngine()
ingestion_params = IngestionParams(
    clear_data=False,
    # max_items=1000,  # Optional: limit number of items to process
    # batch_size=10000,  # Optional: customize batch size
    # resources=["users"],  # Optional: ingest only listed resources
    # connectors=["users_files"],  # Optional: ingest only listed connectors (name or hash)
)

ingest_manifest = manifest.model_copy(update={"bindings": bindings})
ingest_manifest.finish_init()

engine.define_and_ingest(
    manifest=ingest_manifest,
    target_db_config=conn_conf,  # Target database config
    ingestion_params=ingestion_params,
    recreate_schema=False,  # Set to True to drop and redefine schema (script halts if schema exists)
)

# Option 2: Use Caster directly (schema must be defined separately)
# from graflo.hq import GraphEngine
# engine = GraphEngine()
# engine.define_schema(manifest=manifest, target_db_config=conn_conf, recreate_schema=False)
#
# caster = Caster(schema=schema, ingestion_model=ingestion_model)
# caster.ingest(
#     target_db_config=conn_conf,
#     bindings=bindings,
#     ingestion_params=ingestion_params,
# )
```

### PostgreSQL Schema Inference

```python
from graflo.hq import GraphEngine
from graflo.connections.onto import PostgresConfig, ArangoConfig
from graflo import Caster
from graflo.onto import DBType

# Connect to PostgreSQL
postgres_config = PostgresConfig.from_docker_env()  # or PostgresConfig.from_env()

# Create GraphEngine and infer schema from PostgreSQL 3NF database
# Connection is automatically managed inside infer_schema()
engine = GraphEngine(target_db_flavor=DBType.ARANGO)
manifest = engine.infer_manifest(
    postgres_config,
    schema_name="public",  # PostgreSQL schema name
)
schema = manifest.require_schema()
ingestion_model = manifest.require_ingestion_model()

# Define schema in target database (optional, can also use define_and_ingest)
target_config = ArangoConfig.from_docker_env()
engine.define_schema(
    manifest=manifest,
    target_db_config=target_config,
    recreate_schema=False,
)

# Use the inferred schema with Caster for ingestion
caster = Caster(schema=schema, ingestion_model=ingestion_model)
# ... continue with ingestion
```

### Graph export and migration

```python
from pathlib import Path

from graflo import GraphEngine, DBType
from graflo.db import Neo4jConfig, ArangoConfig, PostgresConfig
from graflo.db.graflo_backend.config import GraFloBackendConfig

engine = GraphEngine(target_db_flavor=DBType.ARANGO)

neo4j = Neo4jConfig.from_env()
arango = ArangoConfig.from_env()
postgres = PostgresConfig.from_env()
backend = GraFloBackendConfig(output_dir=Path("artifacts/neo4j-backend"))

# Neo4j → chunked file backend
engine.migrate_graph(neo4j, backend, recreate_schema=True)

# File backend → Arango migration
engine.migrate_graph(backend, arango, recreate_schema=True)

# File backend → Postgres (relational vertex + junction edge tables)
pg_engine = GraphEngine(target_db_flavor=DBType.POSTGRES)
pg_engine.migrate_graph(backend, postgres, recreate_schema=True)
```

See [Graph export and migration](docs/concepts/operations/graph_export_migration.md) and [Example 13](docs/examples/example-13.md).

### Manifest ↔ RDF (GraFlo ontology)

```bash
# Serialize manifest YAML to Turtle (embeds gf: vocabulary when --include-ontology is default)
uv run manifest-to-rdf manifest.yaml \
  --base-uri https://growgraph.dev/manifests/mygraph/v1 \
  --format turtle \
  --output mygraph.ttl

# Restore YAML from RDF
uv run rdf-to-manifest mygraph.ttl \
  --manifest-uri https://growgraph.dev/manifests/mygraph/v1 \
  --output manifest.restored.yaml
```

```python
from graflo import GraphManifest
from graflo.rdf import ManifestRdfDeserializer, ManifestRdfSerializer

manifest = GraphManifest.from_yaml("manifest.yaml")
base = "https://growgraph.dev/manifests/mygraph/v1"

ttl = ManifestRdfSerializer().to_turtle(manifest, base)
restored = ManifestRdfDeserializer().from_turtle(ttl, base.rstrip("/"))
```

Ontology source: `graflo/rdf/ontology/graflo.ttl`. See [GraFlo ontology](https://growgraph.github.io/graflo/concepts/schema/ontology/).

### RDF / SPARQL Ingestion (domain ontology → LPG)

```python
from pathlib import Path
from graflo.hq import GraphEngine
from graflo.connections.onto import ArangoConfig
from graflo.architecture.manifest import GraphManifest

engine = GraphEngine()

# Infer schema from an OWL/RDFS ontology file
ontology = Path("ontology.ttl")
schema, ingestion_model = engine.infer_schema_from_rdf(source=ontology)

# Create source bindings (reads a local .ttl file per rdf:Class)
bindings = engine.create_bindings_from_rdf(source=ontology)

# Or point at a SPARQL endpoint instead:
# from graflo.connections.onto import SparqlEndpointConfig
# sparql_cfg = SparqlEndpointConfig(uri="http://localhost:3030", dataset="mydata")
# bindings = engine.create_bindings_from_rdf(
#     source=ontology,
#     endpoint_url=sparql_cfg.query_endpoint,
# )

target = ArangoConfig.from_docker_env()
engine.define_and_ingest(
    manifest=GraphManifest(
        graph_schema=schema,
        ingestion_model=ingestion_model,
        bindings=bindings,
    ),
    target_db_config=target,
)
```

## Development

To install requirements

```shell
git clone git@github.com:growgraph/graflo.git && cd graflo
uv sync --extra dev
```

### Tests

#### Test databases

**Quick Start:** To start all test databases at once, use the convenience scripts from the [docker folder](./docker):

```shell
cd docker
./start-all.sh    # Start all services
./stop-all.sh      # Stop all services
./cleanup-all.sh   # Remove containers and volumes
```

**Individual Services:** To start individual databases, navigate to each database folder and run:

Spin up Arango from [arango docker folder](./docker/arango) by

```shell
docker-compose --env-file .env up arango
```

Neo4j from [neo4j docker folder](./docker/neo4j) by

```shell
docker-compose --env-file .env up neo4j
```

TigerGraph from [tigergraph docker folder](./docker/tigergraph) by

```shell
docker-compose --env-file .env up tigergraph
```

FalkorDB from [falkordb docker folder](./docker/falkordb) by

```shell
docker-compose --env-file .env up falkordb
```

Memgraph from [memgraph docker folder](./docker/memgraph) by

```shell
docker-compose --env-file .env up memgraph
```

NebulaGraph from [nebula docker folder](./docker/nebula) by

```shell
docker-compose --env-file .env up
```

and Apache Fuseki from [fuseki docker folder](./docker/fuseki) by

```shell
docker-compose --env-file .env up fuseki
```

To run unit tests

```shell
uv run pytest test
```

> **Note**: Tests require external database containers (ArangoDB, Neo4j, TigerGraph, FalkorDB, Memgraph, NebulaGraph, Fuseki) to be running. CI builds intentionally skip test execution. Tests must be run locally with the required database images started (see [Test databases](#test-databases) section above). NebulaGraph tests are gated behind `pytest --run-nebula`.

## Requirements

- Python 3.11+ (3.11, 3.12 and 3.13 are supported; CI runs 3.11)
- python-arango
- nebula3-python>=3.8.3 (NebulaGraph v3.x support)
- nebula5-python>=5.2.1 (NebulaGraph v5.x support)
- sqlalchemy>=2.0.0 (for PostgreSQL and SQL data sources)
- rdflib>=7.0.0 + SPARQLWrapper>=2.0.0 (included in the default install)

## License

Open source under the [Apache License 2.0](LICENSE). Copyright and trademark notices are in
[NOTICE](NOTICE): the licence grants no rights in the **GraFlo** and **GrowGraph** marks.
Releases before the relicensing shipped under the Business Source License 1.1 and keep those terms;
see the [changelog](CHANGELOG.md).

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.