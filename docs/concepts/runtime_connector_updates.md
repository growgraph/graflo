# Runtime connector updates

Each `FileConnector`, `TableConnector`, and `SparqlConnector` gets a deterministic **`hash`** from its defining fields (excluding `name` and `resource_name`). `Bindings` indexes connectors and resource wiring by that hash, and `connector_connection` maps each connector to a `conn_proxy` by resolved hash.

If you change defining fields (for example narrowing a `time_filter` window (`start` / `interval` / `end`), adding `filters`, or adjusting file `regex`), the hash changes. You must **replace** the old connector in `Bindings` and **re-wire** internal maps; appending a second connector or using `add_connector` alone can leave stale hash entries.

## Time filters (`ColumnTimeFilter`)

`FileConnector` and `TableConnector` share an optional nested **`time_filter`** (`ColumnTimeFilter`): a column name plus bounds. SQL is built with **`FilterExpression`** (same mechanism as **`filters`**).

| Field | Meaning |
| ----- | ------- |
| **`column`** | Identifier of the date/time column in generated SQL. |
| **`start`** | Optional lower bound (ISO date `YYYY-MM-DD` or ISO datetime). Combined with **`start_inclusive`** (default `true` → `>=`; `false` → `>`). |
| **`end`** | Optional upper bound. Combined with **`end_inclusive`** (default `false` → `<`; `true` → `<=`). |
| **`interval`** | Optional [pandas `Timedelta`](https://pandas.pydata.org/docs/reference/api/pandas.Timedelta.html) string (e.g. `"7D"`, `"2h"`). Requires **`start`**; defines half-open **`[start, start + interval)`** with `>=` and `<`. Mutually exclusive with **`end`**. |
| **`not_equals`** | Single value for `!=`; mutually exclusive with **`start`**, **`end`**, and **`interval`**. |

**Column-only hint:** `time_filter: { column: "created_at" }` (no bounds) records the default datetime column for ingestion (`IngestionParams.datetime_after` / `datetime_before`) without adding a `WHERE` clause from the connector itself.

Durations must parse as a fixed **`pandas.Timedelta`** (wall-clock offset). Calendar-style strings that are not valid timedeltas (for example ambiguous month rolls) are unsupported; use explicit **`start`** / **`end`** instead.

At runtime, the read-only **`date_field`** property on connectors resolves to **`time_filter.column`** when present (for code and docs that read “which column is the event time?”). Manifests and patches must use the nested **`time_filter`** object; older flat `date_*` keys are not accepted.

## Manifest vs patches

The **GraphManifest** (and its `bindings` block) holds the normal contract only: **`connectors`**, **`resource_connector`**, optional **`connector_connection`**, optional **`staging_proxy`**. It does **not** include a `connector_updates` key—patches are **outside** the canonical manifest.

You apply patches **after** the manifest is loaded (or merged from disk), from:

- a separate YAML/JSON file (your own schema: list of dicts),
- environment or CLI-derived parameters,
- or plain Python.

Then call **`Bindings.apply_connector_update`** or **`replace_connector`** before **`GraphEngine`**, **`RegistryBuilder.build`**, or any code that assumes bindings are final.

!!! note "Registry and `DataSourceRegistry`"

    Build the registry **after** patches are applied. Otherwise SQL/file/SPARQL sources may use stale hashes or queries.

## Patch shape (`ConnectorUpdate`)

`ConnectorUpdate` is the typed carrier for one patch:

- **Required:** `connector` — connector **`name`** or **`hash`** (same resolution as `resource_connector.connector` and `connector_connection.connector`).
- **Any other keys:** merged onto that connector’s current data; same **field names** as `TableConnector` / `FileConnector` / `SparqlConnector`, but **only for fields you change** (patch-only). Do not repeat `table_name`, `rdf_class`, etc. unless you are actually changing them.

Extra keys use Pydantic `extra="allow"`, so new connector fields do not require extending `ConnectorUpdate`.

### External YAML (example only)

Your application can define any file layout; below is a minimal list you might load with `yaml.safe_load` and apply in a loop. This file is **not** part of `GraphManifest`.

```yaml
# connector_patches.yaml (separate from manifest)
# Canonical: patch the nested time_filter (merged in full on the connector).
- connector: events_table
  time_filter:
    column: created_at
    start: "2021-06-01"
    interval: "30D"
```

### Baseline manifest `bindings` (no patches)

```yaml
bindings:
  connectors:
    - name: events_table
      table_name: events
      time_filter:
        column: created_at
        start: "2020-01-01"
        interval: "365D"
  resource_connector:
    - resource: events
      connector: events_table
```

## Apply after load

```python
from pathlib import Path

import yaml

from graflo.architecture.contract.bindings import Bindings, ConnectorUpdate

bindings = Bindings.model_validate(manifest_dict["bindings"])
for row in yaml.safe_load(Path("connector_patches.yaml").read_text()):
    bindings.apply_connector_update(ConnectorUpdate.model_validate(row))
# Then attach bindings to your manifest object / pass to GraphEngine / build registry.
```

Or build `ConnectorUpdate` instances directly without a side file:

```python
from graflo.architecture.contract.bindings import Bindings, ConnectorUpdate

bindings.apply_connector_update(
    ConnectorUpdate.model_validate(
        {
            "connector": "events_table",
            "time_filter": {
                "column": "created_at",
                "start": "2021-06-01",
                "interval": "30D",
            },
        }
    )
)
```

## API details

### `Bindings.apply_connector_update`

Resolves `update.connector`, merges `old.model_dump(mode="python")` with the patch, runs `old.__class__.model_validate(merged)`, then swaps the instance and reindexes. Validators (including **`hash`**) must run; a plain `model_copy(update=patch)` would **not** re-run `@model_validator` and would leave a stale `hash`.

### `Bindings.replace_connector`

Lower-level: `replace_connector(old, new)` where `old` is an existing connector instance or a **name/hash string**, and `new` is the fully built replacement (`TableConnector` | `FileConnector` | `SparqlConnector`). If `new.name` is unset and the old connector had a name, the name is copied onto `new`. Resource→connector hash lists and `conn_proxy` mappings move from the old hash to `new.hash`, then name/hash indexes are rebuilt.

Use this when you already built `new` yourself; use `apply_connector_update` for dict-shaped patches.

When patching **`time_filter`**, the merged payload replaces the entire nested object (provide a full `time_filter` dict in YAML/JSON, not single-key deltas inside it).

## Behaviour summary

| Concern | Behaviour |
| ------- | --------- |
| Resource bindings (`resource_connector` / `resource_name` on connector) | Hash entries in internal resource maps are rewritten to the new hash. |
| `connector_connection` / `conn_proxy` | Mapping follows the connector from old hash to new hash. |
| Connector `name` | Preserved on replace when the new instance has no `name`. |
| Empty patch | `apply_connector_update` no-ops if there are no extra keys besides `connector`. |
| Invalid patch fields | Validation error from the concrete connector model when merging. |

## Related concepts

- [Table connector views and `SelectSpec`](table_connector_views.md) — advanced `TableConnector` SQL shape.
- [Explicit `connector_connection` proxy wiring](../examples/example-9.md) — manifest example for `conn_proxy`.
- Bindings overview in [Concepts overview](index.md).

Implementation entry points:

- `graflo.architecture.contract.bindings.ColumnTimeFilter`
- `graflo.architecture.contract.bindings.ConnectorUpdate`
- `graflo.architecture.contract.bindings.Bindings.apply_connector_update`
- `graflo.architecture.contract.bindings.Bindings.replace_connector`
