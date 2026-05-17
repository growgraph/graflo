# Document cast errors and doc error sink

When a **resource** maps a **source document** (one item from a batch: a JSON object, CSV row as dict, grouped RDF subject, API element, etc.) into graph data, a single document can fail while others in the same batch succeed. Ingestion behavior is controlled by **`IngestionParams`** on **`Caster`** (and the same parameters flow through **`GraphEngine`** and the **`ingest`** CLI).

## `on_doc_error`

- **`skip`** (default): the bad document is skipped; the batch continues. Failures are recorded (see below) and a summary is logged at WARNING for the batch.
- **`fail`**: any document exception fails the whole batch (same as a hard error during casting).

## Persisting failures: `doc_error_sink_path`

Set **`IngestionParams.doc_error_sink_path`** to a filesystem path (convention: **`*.jsonl.gz`**). The caster appends **gzip-compressed JSONL**: each line is one JSON object matching **`DocCastFailure`** (resource name, **`doc_index`** within the batch, exception type, message, traceback, optional document preview). Writes are serialized with an internal async lock so concurrent batches do not corrupt the file.

Records use **`failure_kind`**: **`document`** (default) when the whole source document failed, or **`transform`** when a single transform step failed but the document was still ingested (see below). Transform rows also include **`location_path`**, **`transform_label`**, and **`nulled_fields`**.

Each append may add a new gzip member to the file (normal for log-style gzip). Tools such as **`zcat`**, **`gzip -dc`**, or **`pigz -dc`** stream all concatenated members, for example:

```bash
zcat errors.jsonl.gz | head
```

## When no file sink is configured

If **`doc_error_sink_path`** is **`None`**, skipped failures are emitted as structured **`logger.error`** entries (with JSON-serializable metadata in the log **`extra`** under **`doc_cast_failure`**). Use a file sink when you need durable, replayable records for debugging or reprocessing.

## Optional caps

- **`max_doc_errors`**: if the **total** number of persisted failure records across the run (document **and** transform) exceeds this limit, ingestion raises **`DocErrorBudgetExceeded`** (after writing the failures that pushed over the limit). Use this to stop a bad source early.

## Per-transform tolerance: `tolerate_transform_errors`

On each **`Resource`**, **`tolerate_transform_errors`** defaults to **`True`**. When enabled, a failing transform step sets its declared output fields to **`None`**, records a **`failure_kind=transform`** row in the doc error sink, and the rest of the pipeline (vertices, edges, later transforms) still runs for that document. Set **`tolerate_transform_errors: false`** on a resource to restore fail-fast behavior for transform exceptions (the whole document is lost unless **`on_doc_error=skip`** at the caster).

Transform failures are persisted through the same **`doc_error_sink_path`** and count toward **`max_doc_errors`** as full document failures. With **`on_doc_error=fail`**, tolerated transform errors do not fail the batch; only unhandled document-level exceptions do.

- **`doc_error_preview_max_bytes`** and **`doc_error_preview_keys`**: bound the size and shape of the **`doc_preview`** field on **`DocCastFailure`** so logs and files stay readable and bounded.

## CLI

The **`ingest`** command accepts:

```bash
uv run ingest \
  --db-config-path config/db.yaml \
  --schema-path config/manifest.yaml \
  --source-path data/ \
  --on-doc-error skip \
  --doc-error-sink ./artifacts/doc_cast_failures.jsonl.gz
```

## Programmatic use

```python
from pathlib import Path

from graflo.hq.caster import IngestionParams

ingestion_params = IngestionParams(
    on_doc_error="skip",
    doc_error_sink_path=Path("artifacts/doc_cast_failures.jsonl.gz"),
    max_doc_errors=10_000,
)
```

## Extensibility

Additional sink types can implement the **`DocErrorSink`** protocol (**`async write_failures(failures)`**) and be wired from your own orchestration code; the built-in path is **`JsonlGzDocErrorSink`** behind **`doc_error_sink_path`**.
