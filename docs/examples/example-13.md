# Example 13: GraFlo file backend (export, migration, ingest)

This example shows how **`GraFloBackendConfig`** acts as a legitimate **source** and **target** in the existing pipeline — no live database required for dry-runs, and no monolithic YAML dumps.

A file backend directory looks like:

```
artifacts/neo4j-backend/
├── INDEX.json
├── schema.yaml
├── vertices/
│   └── person.000.jsonl.gz
└── edges/
    └── person__knows__person.000.jsonl.gz
```

## Prerequisites

- Python 3.11+
- Optional **graph source**: Neo4j or ArangoDB (for `export-backend`)
- Optional **targets**: another LPG or PostgreSQL (for replay commands)
- Connection configs via environment variables or `docker/<backend>/.env`

## Supported directions

| Task | API | Source | Target |
|---|---|---|---|
| Export graph to disk | `migrate_graph()` | Neo4j, ArangoDB, file backend | `GraFloBackendConfig` |
| Replay from disk | `migrate_graph()` | `GraFloBackendConfig` | Any LPG / PostgreSQL |
| Ingest resources to disk | `ingest()` / `define_and_ingest()` | CSV manifest resources | `GraFloBackendConfig` |
| Schema only | `infer_schema_from_graph()` | Neo4j, ArangoDB, file backend | — (`Schema`) |

## Step 1 — Export Neo4j to a file backend

`migrate_graph()` introspects the source, streams data into chunked gzip JSONL files, and writes `schema.yaml` plus `INDEX.json`.

```python
from pathlib import Path

from graflo import DBType, GraphEngine
from graflo.db import Neo4jConfig
from graflo.db.graflo_backend.config import GraFloBackendConfig

neo4j = Neo4jConfig.from_docker_env()
backend = GraFloBackendConfig(output_dir=Path("artifacts/neo4j-backend"))

engine = GraphEngine(target_db_flavor=DBType.ARANGO)
engine.migrate_graph(
    neo4j,
    backend,
    recreate_schema=True,
    sample_limit=100,
    data_limit=None,  # cap rows per type while testing, e.g. 1000
)
```

## Step 2 — Replay file backend → ArangoDB

The same `migrate_graph()` signature works when the **source** is a file backend:

```python
from pathlib import Path

from graflo import DBType, GraphEngine
from graflo.db import ArangoConfig
from graflo.db.graflo_backend.config import GraFloBackendConfig

backend = GraFloBackendConfig(output_dir=Path("artifacts/neo4j-backend"))
arango = ArangoConfig.from_docker_env()

engine = GraphEngine(target_db_flavor=DBType.ARANGO)
engine.migrate_graph(
    backend,
    arango,
    recreate_schema=True,
)
```

## Step 3 — Ingest CSV resources into a file backend

Use `ingest()` when the **source** is a manifest with file/SQL/API resources and the **target** is on disk. This is the same ingestion path as loading into ArangoDB or Neo4j — only the target config changes.

```python
from pathlib import Path

from suthing import FileHandle

from graflo import DBType, GraphEngine, GraphManifest
from graflo.db.graflo_backend.config import GraFloBackendConfig
from graflo.hq.caster import IngestionParams

manifest = GraphManifest.from_config(
    FileHandle.load("examples/13-graph-export-migration/manifest.yaml")
)
manifest.finish_init()

# Manifest file connectors use sub_path: data relative to the example directory.
import os
os.chdir("examples/13-graph-export-migration")

backend = GraFloBackendConfig(output_dir=Path("artifacts/csv-backend"))
engine = GraphEngine(target_db_flavor=DBType.GRAFLO_BACKEND)

engine.define_and_ingest(
    manifest=manifest,
    target_db_config=backend,
    ingestion_params=IngestionParams(clear_data=True),
    recreate_schema=True,
)
```

After ingestion, inspect the backend:

```python
from graflo.architecture.backend import GraFloBackendReader

reader = GraFloBackendReader(Path("artifacts/csv-backend"))
index = reader.read_index()
print(index.vertices)  # record counts and chunk paths per vertex type
```

## Step 4 — Pre-sanitize schema for a future target

When exporting to disk for a known downstream database, set `target_flavor_hint` so the stored `schema.yaml` is already sanitized:

```python
backend = GraFloBackendConfig(
    output_dir=Path("artifacts/for-arango"),
    target_flavor_hint=DBType.ARANGO,
)
engine.migrate_graph(neo4j, backend, recreate_schema=True)
```

## Step 5 — Migrate file backend → PostgreSQL

```python
from pathlib import Path

from graflo import DBType, GraphEngine
from graflo.db import PostgresConfig
from graflo.db.graflo_backend.config import GraFloBackendConfig

backend = GraFloBackendConfig(output_dir=Path("artifacts/neo4j-backend"))
postgres = PostgresConfig.from_docker_env()

engine = GraphEngine(target_db_flavor=DBType.POSTGRES)
engine.migrate_graph(backend, postgres, recreate_schema=True)
```

## Runnable scripts

Run from the example directory (file connectors use `sub_path: data` relative to it):

```bash
cd examples/13-graph-export-migration

# CSV manifest → file backend (no live DB required)
uv run python ingest.py --output-dir artifacts/csv-backend
uv run python inspect_backend.py --backend-dir artifacts/csv-backend

# Neo4j → file backend
uv run python export.py --output-dir artifacts/neo4j-backend

# File backend → ArangoDB
uv run python migrate.py arango --from-backend artifacts/csv-backend --recreate-schema

# File backend → PostgreSQL
uv run python migrate.py postgres --from-backend artifacts/neo4j-backend --recreate-schema
```

## Related docs

- [Graph export and migration](../concepts/graph_export_migration.md) — API reference and capability notes
- [Example 1](example-1.md) — manifest-based CSV ingestion into a live graph DB
