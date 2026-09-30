# API env wiring

You read records from REST APIs, and each API has an address and credentials
that belong neither in the manifest nor in your code. This guide keeps them in
environment variables: the manifest names each API by a label, and one call
loads the address and credentials of every label. Afterwards you can run the
same manifest against a test API and a production API by changing
environment variables only.

## What you need

- A manifest whose `bindings` read from REST APIs; see
  [API connector](../concepts/connectors/api_connector.md) for the connector
  itself.
- A shell or a process manager where you can set environment variables.

## Steps

### 1. Name each API by a label

In `bindings`, give every API [connector](../concepts/glossary.md#connector)
only a path, and map the connector to a label with `connector_connection`.
The label is a `conn_proxy`, a name for a connection whose details are
supplied at run time (see [connection proxy](../concepts/glossary.md#connection-proxy)):

```yaml
bindings:
  connectors:
    - name: work_orders_api
      path: /api/work-orders
      resource_name: work_orders
    - name: readings_api
      path: /api/readings
      resource_name: readings
  connector_connection:
    - connector: work_orders_api
      conn_proxy: maintenance_api
    - connector: readings_api
      conn_proxy: sensor_feed
```

Here work orders come from the maintenance system's API and readings from the
sensor feed's API. Neither address appears in the manifest.

### 2. Set the variables

Each label becomes a variable prefix: upper case, `-` replaced by `_`, and a
trailing `_`. `maintenance_api` reads `MAINTENANCE_API_*`, and a label
`sensor-feed` would read `SENSOR_FEED_*`.

| Variable | Required | Meaning |
|---|---|---|
| `{PREFIX}BASE_URL` | yes | Base URL; the connector's `path` is appended to it |
| `{PREFIX}AUTH_TYPE` | no, default `bearer` | `bearer`, `basic`, `digest` or `api_key` |
| `{PREFIX}TOKEN` | for `bearer` and `api_key` | The token or key |
| `{PREFIX}USERNAME`, `{PREFIX}PASSWORD` | for `basic` and `digest` | The credentials |
| `{PREFIX}HEADER_NAME` | no, default `Authorization` | The header that carries the token or key |
| `{PREFIX}PREFIX` | no, default `Bearer` | Text put before a `bearer` token in the header |

```bash
export MAINTENANCE_API_BASE_URL=https://maintenance.example.com
export MAINTENANCE_API_TOKEN=...
export SENSOR_FEED_BASE_URL=https://sensors.example.com
export SENSOR_FEED_AUTH_TYPE=api_key
export SENSOR_FEED_HEADER_NAME=X-API-Key
export SENSOR_FEED_TOKEN=...
```

### 3. Load the variables and ingest

```python
from suthing import FileHandle

from graflo import GraphEngine, GraphManifest
from graflo.connections import ArangoConfig, InMemoryConnectionProvider

manifest = GraphManifest.from_config(FileHandle.load("manifest.yaml"))
manifest.finish_init()

provider = InMemoryConnectionProvider()
provider.register_all_api_configs_from_env(bindings=manifest.require_bindings())

conn_conf = ArangoConfig.from_env()
engine = GraphEngine(target_db_flavor=conn_conf.connection_type)
engine.define_and_ingest(
    manifest=manifest,
    target_db_config=conn_conf,
    connection_provider=provider,
    recreate_schema=True,
)
```

`register_all_api_configs_from_env` finds every label used by an API
connector, reads that label's variables, and ties each connector to its
label. The provider then hands the address and credentials to the ingest.

### 4. Use other variable names, if you must

When the variables already exist under other names, map a label to its
prefix:

```python
provider.register_all_api_configs_from_env(
    bindings=manifest.require_bindings(),
    env_prefix_map={"maintenance_api": "WORK_ORDERS_"},
)
```

`maintenance_api` then reads `WORK_ORDERS_BASE_URL` and the rest; other labels
keep the default rule.

## What you should see

To check the wiring without contacting any API, print what each connector
resolved to:

```python
bindings = manifest.require_bindings()
for connector in bindings.connectors:
    api = provider.get_generalized_conn_config(connector).config
    print(connector.name, api.base_url + connector.path)
```

```text
work_orders_api https://maintenance.example.com/api/work-orders
readings_api https://sensors.example.com/api/readings
```

Problems show up as a `ValueError` from `register_all_api_configs_from_env`:
a missing `BASE_URL` names the variable, an unknown `AUTH_TYPE` lists the
valid ones, and bindings with no API connector tied to a label are refused.

## What to read next

- [API connector](../concepts/connectors/api_connector.md): paths, parameters, pagination and authentication in detail.
- [API sources from environment variables (12)](../examples/api-env-config/index.md): this guide as a runnable script.
- [Database connections](database_connections.md): the same idea for the database you write to.
