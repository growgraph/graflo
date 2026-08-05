# Importing graflo: layers and light imports

graflo is organized in layers, and every package façade is **lazy** (PEP 562):
`import graflo` costs almost nothing, and importing any subpackage pulls in only
that subpackage's own dependencies. In particular, importing the declarative
contract (`GraphManifest`, `Schema`, bindings) never loads a database driver.

## Layering (low to high)

```
L0  graflo.onto            enums and core vocabulary (DBType, identity selectors, …)
    graflo.util            pure helpers (no graflo imports besides onto)
    graflo.architecture.base   ConfigBaseModel (pydantic + YAML)

L1  graflo.filter          FilterExpression, SelectSpec, JoinClause
    graflo.architecture.graph_types   containers, contexts, identifiers, EdgeDerivation
    graflo.architecture.onto_sql      leaf pydantic models (SQL introspection)
    graflo.architecture.util          helpers over graph_types only

L2  graflo.architecture.schema        Schema, VertexConfig, EdgeConfig,
                                      DatabaseProfile, db-aware projections,
                                      identity_uuid helpers
    graflo.architecture.query         read contract (QueryCaps, NodeQuery,
                                      NeighborQuery, TraverseQuery, AggregateQuery,
                                      and QueryResult)
    graflo.architecture.onto_sample   leaf sample models (ResourceSample, etc.)

L3  graflo.connections     connection *configs*: DBConfig + per-backend configs
                           (onto), source configs (sources), DBType mapping
    graflo.architecture.contract      GraphManifest, bindings, ingestion
                                      (resources, transforms, steps)

L4  graflo.architecture.pipeline      actors and executor (runtime)
    graflo.architecture.backend       chunked file backend
    graflo.architecture.evolution     schema migration ops
    graflo.data_source                file, sql, rdf, api, kafka, memory
    graflo.connections.provider       ConnectionProvider (resolves contract connectors)

L5  graflo.db              live connections — one subpackage per backend
    graflo.object_storage  S3/MinIO client

L6  graflo.hq              orchestration: GraphEngine, Caster, DBWriter, …
    graflo.migrate, graflo.rdf, graflo.plot, graflo.cli
```

Imports only ever point downward at import time. Runtime factories that cross
layers upward (e.g. `IngestionModel.finish_init` building `ResourceRuntime`,
connectors constructing data sources) use deferred, function-level imports.
This is enforced by `test/architecture/test_layering.py`.

## Recommended imports

For applications, the top-level façade is fine:

```python
from graflo import GraphEngine, GraphManifest, IngestionParams, Schema
```

For libraries and services that care about import cost or precise typing,
import from the concrete module (lazy façades type as `Any` under static
checkers):

```python
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.schema import Schema, VertexConfig
from graflo.connections.onto import PostgresConfig, DBConfig
from graflo.connections.provider import ConnectionProvider
from graflo.db.manager import ConnectionManager
from graflo.hq.graph_engine import GraphEngine
```

## Where things moved (2026-07 module composition refactor)

| Old path | New path |
|----------|----------|
| `graflo.db.connection.onto` | `graflo.connections.onto` |
| `graflo.db.connection.config_mapping` | `graflo.connections.mapping` |
| `graflo.connection_models` | `graflo.connections.sources` |
| `graflo.hq.connection_provider` | `graflo.connections.provider` |
| `graflo.db.graflo_backend.config` | `graflo.connections.graflo_backend` |
| `graflo.architecture.pipeline.runtime.actor.config.*` | `graflo.architecture.contract.ingestion.steps.*` |
| `graflo.architecture.contract.runtime.resource` | `graflo.architecture.pipeline.runtime.resource` |
| `graflo.architecture.contract.runtime.edge_derivation` | `graflo.architecture.graph_types.edge_derivation` |
| `graflo.architecture.edge_derivation` | `graflo.architecture.graph_types.edge_derivation` |
| `graflo.architecture.database_features` | `graflo.architecture.schema.database_features` |
| `graflo.db.identity_uuid` | `graflo.architecture.schema.identity_uuid` |
| `graflo.hq.fuzzy_matcher` | `graflo.util.fuzzy_matcher` |
| `graflo.util.chunker` | `graflo.data_source.chunker` |
| `graflo.util.merge` | `graflo.architecture.graph_types.merge` |
| `JoinClause` (in `contract.bindings`) | `graflo.filter.select` (re-exported from `graflo.filter`) |
| `PRIMARY_IDENTITY_SELECTOR`, `SECONDARY_IDENTITY_SUGAR` (in `schema.vertex`) | `graflo.onto` |

No compatibility shims were kept — imports of the old paths raise
`ModuleNotFoundError` / `ImportError`.
