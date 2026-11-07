# How do I load a large graph into an empty database?

Run the ingestion in bulk mode. GraFlo casts the whole input first, folds every
vertex and edge to one row by identity, then writes each row once, with no
lookups in the target. The result is the graph an ordinary (upsert) ingestion
of the same input produces.

Bulk mode needs the `bulk` extra:

```bash
uv add 'graflo[bulk]'
```

## Run it

From the command line, add `--bulk` and a staging directory:

```bash
uv run ingest --db-config-path db.yaml --schema-path manifest.yaml \
    --bulk --staging-dir /data/staging/run-1
```

From Python, set the mode on the ingestion parameters:

```python
from graflo import GraphEngine, IngestionParams

engine = GraphEngine()
engine.ingest(
    manifest,
    conn_conf,
    IngestionParams(mode="bulk", staging_dir="/data/staging/run-1"),
)
```

A bulk run defines the schema itself, so do not call `define_schema` first.
The target must hold no schema yet; `--fresh-start` (or
`recreate_schema=True`) drops an existing one first.

## What happens

1. **Stage.** Each cast batch is written to the staging directory, after the
   same steps an upsert applies: reserved fields stamped, derived fields set,
   identities minted, documents with no identity dropped.
2. **Fold.** Rows that share an identity become one: later rows win per field,
   a field a later row lacks keeps its value, and `merge: union` lists keep
   every member once. An edge whose endpoint is not among the folded vertices
   is rejected.
3. **Write.** The backend writes the folded rows in chunks, vertices first.
4. **Verify.** The run fails unless the target holds exactly as many elements
   of each type as the fold produced.

The staging directory then holds `counts.json` (rows staged, folded and
rejected per type) and `rejects/` (each rejected row with its reason).

## What if the run stops?

Run the same command again with the same staging directory. A run stopped
while staging stages again; a later one keeps its fold, skips the chunks it
wrote, and writes again the one chunk whose commit it did not record.

## What can bulk mode not load?

Everything that reads the target while ingesting is refused when the run
starts, since an empty target has nothing to find:

- `find` and `lookup_only` vertex steps;
- `extra_weights`;
- edges whose endpoints match by a secondary identity;
- blank vertices.

## Which databases support it?

A backend loads in bulk when its connection declares `supports_bulk_load`:

| Backend | How it writes |
|---|---|
| Neo4j | Creates nodes with no lookups over parallel sessions, then builds the identity indexes. It then creates edges in rounds, where no two writers in a round touch the same node. |
| FalkorDB | `GRAPH.BULK` into a new graph, one writer; an existing empty graph is dropped first. If a failed batch drops the graph, running again writes everything again. |
| PostgreSQL | `COPY` into tables without keys, then primary keys, unique edge keys and validated foreign keys. |
| TigerGraph | A loading job fed over REST++, one file per vertex type and per edge; a rejected line fails the load. |
| File backend | Appends. |

## Tuning

An optional `bulk` section on the database config sets how the load runs:

```yaml
bulk:
  chunk_size: 50000    # rows per chunk: the unit written and resumed
  parallelism: 4       # chunks written at once
  memory_limit: 8GB    # memory the fold uses before spilling to the staging directory
```
