# Document cast errors

Real data has bad records: a date that does not parse, a field of the wrong
type, a value a function cannot handle. When one record fails while it is
[cast](../glossary.md#casting), GraFlo skips it, keeps loading the rest, and
records what went wrong. This page shows how to choose between skipping and
stopping, how to keep every failure in a file you can read afterwards, and how
to stop a run that fails too often, from Python and from the command line.

## What happens to a failing record

A record, or document, is one item of a batch: a CSV row, a JSON object, the
properties of one RDF subject, one API result, one Kafka message. Two kinds of
failure are recorded:

- **Document failure**: casting the record raised an error, so none of its
  vertices and edges is written. By default the record is skipped and the
  rest of the batch continues.
- **Transform failure**: one transform step raised, but the resource tolerates
  it (`tolerate_transform_errors`, on by default). The step's output fields
  are set to `null`, and the record is cast and written without them.

After a batch with failures, GraFlo logs one warning that names the resource,
the number of failures and the first error.

## Skip or stop

`IngestionParams.on_doc_error` chooses what a document failure does:

- `skip` (default): the record is skipped, and the batch continues.
- `fail`: the first failing record of the batch, in batch order, raises its
  error, and the ingest stops.

Transform failures that the resource tolerates do not stop an ingest, though
they count toward the budget described below. To make a failing transform
fail its record instead, set `tolerate_transform_errors: false` on the
resource:

```yaml
ingestion_model:
  resources:
    - name: readings
      tolerate_transform_errors: false
      pipeline:
        - transform:
            call: { use: parse_reading }
        - vertex: reading
```

## Keeping the failures in a file

Set `doc_error_sink_path` to append every failure, of both kinds, to a
gzip-compressed JSON Lines file. The usual suffix is `.jsonl.gz`.

```python
from pathlib import Path

from graflo.hq import IngestionParams

params = IngestionParams(
    on_doc_error="skip",
    doc_error_sink_path=Path("artifacts/cast_failures.jsonl.gz"),
    max_doc_errors=10_000,
)
engine.ingest(manifest=manifest, target_db_config=conn_conf, ingestion_params=params)
```

The file is appended to, never overwritten, so one file can collect the
failures of several runs. Each append adds a gzip member, which `zcat` and
`gzip -dc` read as one stream:

```bash
zcat artifacts/cast_failures.jsonl.gz | head
```

Each line is one failure, with these fields:

| Field | Holds |
|---|---|
| `resource_name` | the resource that cast the record |
| `doc_index` | the position of the record within its batch |
| `failure_kind` | `document` or `transform` |
| `exception_type`, `message` | the error |
| `traceback` | the formatted traceback, cut to 16,384 characters |
| `doc_preview` | a JSON copy of the record, to find it again |
| `location_path` | for a transform failure, where in the record the step ran |
| `transform_label` | for a transform failure, the named transform or `module.foo` |
| `nulled_fields` | for a transform failure, the output fields set to `null` |

`doc_index` counts within one batch, so use `doc_preview` to find the record
in the source. Two settings keep the preview small: `doc_error_preview_keys`
keeps only the listed fields of the record, and `doc_error_preview_max_bytes`
(default 4096) cuts the preview to that many bytes and marks it as truncated.

Without `doc_error_sink_path`, each failure is logged at `ERROR` instead, with
the same fields attached to the log record under `doc_cast_failure`. Use a
file when you want to read or replay the failures later.

## Stopping a run that fails too often

`max_doc_errors` sets a budget for the run. When the number of failures, of
both kinds, exceeds it, GraFlo writes the batch's failures to the file and
raises `DocErrorBudgetExceeded`, which carries the total, the limit and the
path of the file. Use it to stop early on a source that is mostly bad instead
of loading a fraction of it. The default, `None`, sets no budget.

```python
from graflo.hq import DocErrorBudgetExceeded

try:
    engine.ingest(
        manifest=manifest, target_db_config=conn_conf, ingestion_params=params
    )
except DocErrorBudgetExceeded as err:
    print(
        f"{err.total_failures} failures, limit {err.limit}: see {err.doc_error_sink_path}"
    )
```

## From the command line

`graflo ingest` takes the error policy and the file:

```bash
graflo ingest \
  --db-config-path config/db.yaml \
  --schema-path manifest.yaml \
  --source-path data \
  --on-doc-error skip \
  --doc-error-sink artifacts/cast_failures.jsonl.gz
```

`--on-doc-error` takes `skip` (default) or `fail`, and `--doc-error-sink` sets
`doc_error_sink_path`. The command sets no failure budget; use Python for
`max_doc_errors` and the preview settings.

## What to read next

- [Transforms](transforms.md): the steps whose failures are recorded as
  transform failures.
- [Parallelism](parallelism.md): the other settings of an ingest run.
- [`IngestionParams` reference](../../reference/hq/ingestion_parameters.md):
  every field, with its default.
