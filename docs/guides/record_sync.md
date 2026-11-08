# How do I keep a loaded graph current when source records change?

Run the ingestion in sync mode with the records that changed, or with all of
them. A record's re-ingest replaces what it wrote before: its vertex's
properties and the edges it owns. A record whose content did not change is
skipped. The result is the graph a bulk load of the new records into an empty
target would hold.

Sync mode needs the `bulk` extra and runs on Neo4j and FalkorDB.

## Declare what each record owns

A resource names, in a `sync` block, the vertex type its records replace and
the edges they own:

```yaml
resources:
  - name: works
    sync:
      owns: Work
      edges:
        - {source: Work, target: Topic, relation: about}
        - {source: Author, target: Work, relation: wrote}
        - {source: Author, target: Place, relation: at, owner_field: work}
```

An edge belongs to the record of the owned vertex at its source, else at its
target. An edge touching neither, such as `at` above, names the edge identity
field that holds its owner's identity value as `owner_field`; the owned type
then has a single identity field.

When the ingestion model initialises, it refuses a type owned twice, an owned
edge that another resource also writes, and an owned edge whose relation a step
reads from the data.

Then lift the `sync` aspect, which adds a reserved `sync_hash` to every owned
type:

```bash
graflo lift manifest.yaml --aspect sync -o lifted.yaml
```

## Run it

```bash
uv run ingest --db-config-path db.yaml --schema-path lifted.yaml \
    --sync --staging-dir /data/staging/sync-1
```

From Python, set `IngestionParams(mode="sync", staging_dir=...)`.

The run stages and folds its input as a [bulk load](bulk_load.md) does, so
records of one owner in several batches fold together. It then:

1. creates every vertex the target lacks, by key;
2. for each owner whose stored `sync_hash` differs from the new one, replaces
   its properties, deletes the edges it owns and creates its edges, then stores
   the new hash.

On Neo4j a chunk of owners is one transaction. On FalkorDB it is one
`MULTI`/`EXEC`, with the hashes stored only after it succeeded. An owner whose
write failed keeps no current hash, so the next sync rewrites it. Running the
same command again with the same staging directory resumes a stopped run.

A bulk load writes the hashes too, so the first sync after it skips every
unchanged record. An upsert sets them to null, so the next sync rewrites the
records it touched.

## What can a sync run not do?

It refuses, before reading anything:

- what bulk mode refuses (steps that read the target while ingesting);
- an edge that a resource in the run writes and no `sync` block owns;
- an owned type without the `sync_hash` field.

A vertex type the run does not own is written key-only: created if missing,
never changed. A run that would set its properties is refused before it writes.

Records deleted at the source are not removed: find their ids and call
`delete_vertices`, which removes their edges too.
