# Runtime connector updates

Some runs need a [connector](../glossary.md#connector) that differs a little from the one in the [manifest](../glossary.md#manifest): an incremental load that reads only the last week of work orders, a trial run against a copy of a table, or a sensor feed read from a later start date. You can change the connector in memory after the manifest loads, without editing the manifest file. This page shows how to write such a patch, how GraFlo merges it into the connector, and in which order to patch, connect and ingest.

## Patch a connector

The [bindings](../glossary.md#bindings) of this manifest read one week of work orders from a table:

```yaml
bindings:
  connectors:
    - name: work_orders
      table_name: work_orders
      time_filter:
        column: opened_at
        start: "2026-01-01"
        interval: 7D
  resource_connector:
    - {resource: work_orders, connector: work_orders}
  connector_connection:
    - {connector: work_orders, conn_proxy: plant_pg}
```

The next run reads the following week. It loads the manifest and patches the connector before anything else:

```python
from suthing import FileHandle

from graflo import GraphManifest
from graflo.architecture.contract.bindings import ConnectorUpdate

manifest = GraphManifest.from_config(FileHandle.load("manifest.yaml"))
bindings = manifest.require_bindings()
bindings.apply_connector_update(
    ConnectorUpdate(
        connector="work_orders",
        time_filter={"column": "opened_at", "start": "2026-01-08", "interval": "7D"},
    )
)
print(bindings.get_connectors_for_resource("work_orders")[0].build_query())
```

```text
SELECT * FROM "public"."work_orders" WHERE "opened_at" >= '2026-01-08' AND "opened_at" < '2026-01-15'
```

`connector` names the connector to change. Every other key is a field of that connector with its new value. After the patch, the manifest object holds the new connector, and you pass it to `GraphEngine.ingest` as usual. The file on disk does not change.

## What a patch can change

A patch can set any field of the connector's kind. The fields most often patched:

| To read | Patch |
| --- | --- |
| another time window of a table | `time_filter` |
| another table or schema | `table_name`, `schema_name` |
| a subset of the rows | `filters` |
| other files | `regex`, `sub_path` |
| an API with other query parameters or another path | `params`, `path` |
| other Kafka topics | `topics`, `group_id` |

The fields of each kind are described in [Table filters and views](table_views.md), [API connector](api_connector.md) and [Kafka connector](kafka_connector.md).

## How a patch is merged

- **Each key replaces the whole field.** A nested object, list or mapping is not merged key by key. A `time_filter` patch that gives only `start` fails, because the new `time_filter` has no `column`. A `params` patch that gives only `since` removes every other query parameter. A `filters` patch replaces the list; it does not add to it.
- **Fields you leave out keep their values**, including the connector's `name` and the resources it feeds.
- **The result is validated as a new connector of the same kind.** A misspelled field or an invalid value raises a `ValidationError`, and the bindings keep the old connector.
- **A patch with no key besides `connector` changes nothing.**
- **`connector` takes a name or a hash.** A reference that matches no connector raises a `ValueError`.

## Patch before you connect

GraFlo identifies each connector by a hash of its fields: all of them except `name` and `resource_name`. The bindings pair resources and [connection proxies](../glossary.md#connection-proxy) with connectors by that hash, so a patch that changes a field changes the hash. `apply_connector_update` moves those pairings from the old connector to the new one. Anything else that recorded the old hash does not follow, which gives two rules:

- **Patch before you build the connection provider.** `InMemoryConnectionProvider` records the hash of each connector when you bind configs to it, with `bind_single_config_for_bindings`, `bind_from_bindings` or the helpers that read configs from environment variables. A provider built before the patch has no config for the patched connector. For a table, API or Kafka connector, GraFlo then skips the connector with a warning, and its resource reads nothing from it.
- **Refer to connectors by name.** A hash written in `IngestionParams.connectors` or in a later patch names the old connector and stops resolving once the connector is patched.

## Patches from a file

Patches are not part of the manifest. The bindings have no key for them, and an unknown key such as `connector_updates` makes the manifest fail to load. To keep patches in a file, choose your own format. A YAML list of mappings with a `connector` key matches `ConnectorUpdate` directly:

```yaml
- connector: work_orders
  time_filter: {column: opened_at, start: "2026-01-08", interval: 7D}
  filters:
    - {field: plant, cmp_operator: "==", value: north}
```

```python
for row in FileHandle.load("patches.yaml"):
    bindings.apply_connector_update(ConnectorUpdate.model_validate(row))
```

The patches apply in order, so a later patch sees the result of an earlier one.

## Replace a connector

When you have a complete connector rather than a few changed fields, `replace_connector` swaps it in:

```python
from graflo.architecture.contract.bindings import TableConnector

bindings.replace_connector(
    "work_orders", TableConnector(table_name="work_orders_archive")
)
```

The first argument is the old connector, by name, by hash or as the connector object. The new connector takes over the old one's resources and connection proxy, and its name when it has none of its own. The same two rules apply: build the connection provider afterwards, and refer to the connector by name.

## Time windows with `time_filter`

`time_filter` limits a table connector to a window on one date or time column. An incremental run patches it to move the window forward.

| Key | Meaning |
| --- | --- |
| `column` | The date or time column. Required. |
| `start` | The lower bound: an ISO date (`2026-01-08`) or date and time (`2026-01-08T06:00:00`). Included unless `start_inclusive: false`. |
| `end` | The upper bound, in the same format. Excluded unless `end_inclusive: true`. |
| `interval` | The length of the window, from `start`, instead of `end`: `7D`, `8h`, `90min`, `1W`. |
| `not_equals` | A value the column must differ from. It cannot be combined with `start`, `end` or `interval`. |

The rules:

- `interval` needs `start` and cannot be combined with `end`. The window is then `start` included to `start + interval` excluded, whatever `start_inclusive` says.
- An interval is a fixed length of time, written as a [pandas `Timedelta`](https://pandas.pydata.org/docs/reference/api/pandas.Timedelta.html) string. Months and years have no fixed length and are rejected; for a calendar month, give `start` and `end`.
- Quote dates in YAML (`start: "2026-01-08"`). An unquoted date is read as a date object, not a string, and the manifest fails to load.
- The conditions are added to the query's `WHERE` clause with `AND`, next to the connector's `filters`.
- A `time_filter` with only `column` adds no condition of its own. It names the column that a run's `datetime_after` and `datetime_before` apply to; a connector that names no column takes it from `datetime_column`. The run's range is added next to any window the connector declares.
- A file connector refuses a `time_filter`: a file is read whole, so the window would not be applied.

## What to read next

- [Table filters and views](table_views.md): the `filters` and `view` fields a patch can set on a table connector.
- [Credentials outside the manifest (11)](../../examples/connection-proxy/index.md): how a connection provider supplies the settings behind a connection proxy.
- [API connector](api_connector.md): the fields of an API connector, such as `params` and `pagination`.
