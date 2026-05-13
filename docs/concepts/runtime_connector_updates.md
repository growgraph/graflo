# Runtime connector updates

Each `FileConnector`, `TableConnector`, and `SparqlConnector` gets a deterministic **`hash`** from its defining fields (excluding `name` and `resource_name`). `Bindings` indexes connectors and resource wiring by that hash, and `connector_connection` maps each connector to a `conn_proxy` by resolved hash.

If you change defining fields (for example narrowing `date_range_start` / `date_range_days`, adding `filters`, or adjusting file `regex`), the hash changes. You must **replace** the old connector in `Bindings` and **re-wire** internal maps; appending a second connector or using `add_connector` alone can leave stale hash entries.

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
- connector: events_table
  date_range_start: "2021-06-01"
  date_range_days: 30
```

### Baseline manifest `bindings` (no patches)

```yaml
bindings:
  connectors:
    - name: events_table
      table_name: events
      date_field: created_at
      date_range_start: "2020-01-01"
      date_range_days: 365
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
            "date_range_start": "2021-06-01",
            "date_range_days": 30,
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

- `graflo.architecture.contract.bindings.ConnectorUpdate`
- `graflo.architecture.contract.bindings.Bindings.apply_connector_update`
- `graflo.architecture.contract.bindings.Bindings.replace_connector`
