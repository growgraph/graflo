# How do I configure several API sources from environment variables?

A plant runs two systems with REST APIs: the maintenance system serves work
orders, the sensor feed serves readings. Each API has its own address and its
own token, and both differ between test and production. You want one manifest
for both APIs, with no address or token in it, and you want to set them the way
your deployment already sets secrets: as environment variables.

The manifest names each API by a label. GraFlo turns the label into a prefix
and reads the variables with that prefix: the label `maintenance_api` reads
`MAINTENANCE_API_BASE_URL` and `MAINTENANCE_API_TOKEN`.

## What you need

- GraFlo installed (`pip install graflo`). No database is needed, and no API is
  contacted: the script only resolves the settings and prints them.

## The data

[`manifest_apis.yaml`](manifest_apis.yaml) holds only the `bindings` block of a
manifest: two API connectors and a label for each.

## Steps

### 1. Name each API by a label

```yaml
bindings:
    connectors:
    -   name: work_orders_api
        path: /api/work-orders
        resource_name: work_orders
    -   name: readings_api
        path: /api/readings
        resource_name: readings
    connector_connection:
    -   connector: work_orders_api
        conn_proxy: maintenance_api
    -   connector: readings_api
        conn_proxy: sensor_feed
```

A connector gives the path of an endpoint; `connector_connection` gives the
connector a label, `conn_proxy`. The address of the API is not in the manifest.

### 2. Set the variables for each label

The prefix is the label in upper case, with hyphens turned into underscores,
followed by `_`. For `maintenance_api` and `sensor_feed`, with example values:

```bash
export MAINTENANCE_API_BASE_URL=https://maintenance.example.com
export MAINTENANCE_API_TOKEN=example-maintenance-token
export SENSOR_FEED_BASE_URL=https://sensors.example.com
export SENSOR_FEED_TOKEN=example-sensor-token
```

| Variable after the prefix | Required | Meaning |
|---|---|---|
| `BASE_URL` | yes | Address of the API; the connector's `path` is added to it |
| `AUTH_TYPE` | no | `bearer` (default), `basic`, `digest` or `api_key` |
| `TOKEN` | for `bearer` and `api_key` | The token or key |
| `USERNAME`, `PASSWORD` | for `basic` and `digest` | The credentials |
| `HEADER_NAME` | no | Header that carries the token (default `Authorization`) |
| `PREFIX` | no | Word before a bearer token (default `Bearer`) |

### 3. Load the settings of every label

[`api_env_wiring.py`](api_env_wiring.py) reads the manifest and registers the
settings of each label found in it:

```python
provider = InMemoryConnectionProvider()
provider.register_all_api_configs_from_env(bindings=bindings)
```

To ingest, pass the provider to `engine.define_and_ingest(...,
connection_provider=provider)`, as [example 11](../11-connection-proxy/README.md)
does for a database.

### 4. Run it

```bash
cd examples/12-api-env-config
uv run python api_env_wiring.py
```

## What you should see

Each connector with the address it resolved to, its label and its kind of
authentication. The token is not printed.

```text
work_orders_api: https://maintenance.example.com/api/work-orders (label maintenance_api, auth bearer)
readings_api: https://sensors.example.com/api/readings (label sensor_feed, auth bearer)
```

## What goes wrong

**A variable is missing.** Without `SENSOR_FEED_BASE_URL`, loading stops with:

```text
ValueError: Environment variable SENSOR_FEED_BASE_URL is required for RestApiConnConfig
```

## Also possible

- When your variables use other names, map a label to its prefix:
  `provider.register_all_api_configs_from_env(bindings=bindings,
  env_prefix_map={"sensor_feed": "SENSORS_"})` reads `SENSORS_BASE_URL` and
  `SENSORS_TOKEN`.
- `provider.register_api_config_from_env(conn_proxy="maintenance_api")` loads
  one label; `provider.bind_from_bindings(bindings=bindings)` then connects the
  connectors to their labels.

## What to read next

- [Bulk load into TigerGraph](../13-tigergraph-bulk-s3/README.md).
- [API connector and pagination](../../docs/concepts/connectors/api_connector.md):
  paging through results, headers and authentication.
