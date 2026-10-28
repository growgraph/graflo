# Push connector

A push [connector](../glossary.md#connector) has no address. It declares that a [resource](../glossary.md#resource) takes records that the caller hands over at run time, instead of records that GraFlo fetches. Use it when the records already exist in your program — the output of another step, a message a service received — and you want them to go through the resource's pipeline like any other source. Declaring it in the manifest keeps what the resource accepts versioned and reviewable.

## A first example

A maintenance system's notes have been analysed by another step, which produced one record per finding:

```json
{"finding_id": "F-1", "work_order": "WO-1001", "quote": "bearing noise on SN-4471"}
```

Declare a push connector and bind it to the `findings` resource:

```yaml
bindings:
  connectors:
    - name: findings
      kind: push
  resource_connector:
    - resource: findings
      connector: findings
```

`kind: push` is required: it is how GraFlo tells a push connector from the other connector kinds. The name is the connector's identity, so one manifest can hold several push connectors with different names.

Hand the records over when you build the data sources, keyed by the connector's name:

```python
from graflo.hq.ingestion_parameters import IngestionParams
from graflo.hq.registry_builder import RegistryBuilder

registry = RegistryBuilder(schema, ingestion_model).build(
    bindings,
    IngestionParams(),
    pushed={
        "findings": [{"finding_id": "F-1", "work_order": "WO-1001", "quote": "..."}]
    },
)
engine.ingest(manifest, target_db_config, data_source_registry=registry)
```

The records go through the `findings` resource's pipeline and are written like records from a file or an API.

## Envelopes and annotations

- `records_path` opens an envelope: with `records_path: data.items`, each pushed item is read as `{"data": {"items": [...]}}` and the records are the list inside.
- `row_annotations` adds constant fields to every record; a field the record already has wins.

## When nothing is pushed

A push connector with no records under its name is a source that cannot be built. The build skips it with a warning in the log; with `strict=True` it raises a `ValueError` naming the connector.

## What to read next

- [API connector](api_connector.md): fetch records from a REST endpoint.
