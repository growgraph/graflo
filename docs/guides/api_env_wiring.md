# API env wiring

Register REST API `base_url` and credentials from environment variables instead of hard-coding `RestApiConnConfig` in Python.

## Prerequisites

- Python 3.11+
- A manifest with `APIConnector` entries and `connector_connection` proxy labels
- Environment variables set per `conn_proxy` label

## Step 1 — Declare proxy labels in the manifest

Keep secrets out of YAML. Map each connector to a `conn_proxy` label:

```yaml
bindings:
  connectors:
    - name: users_api
      path: /api/users
    - name: orders_api
      path: /api/orders
  connector_connection:
    - connector: users_api
      conn_proxy: user_service
    - connector: orders_api
      conn_proxy: order_service
```

## Step 2 — Set environment variables

Each `conn_proxy` maps to an uppercase env prefix (`user_service` → `USER_SERVICE_`):

| Variable | Required | Meaning |
| -------- | -------- | ------- |
| `{PREFIX}BASE_URL` | yes | API base URL |
| `{PREFIX}AUTH_TYPE` | no (default `bearer`) | `bearer`, `basic`, `digest`, or `api_key` |
| `{PREFIX}TOKEN` | when using bearer/api_key | Token or API key value |
| `{PREFIX}USERNAME` / `{PREFIX}PASSWORD` | when using basic/digest | Credentials |

Example:

```bash
export USER_SERVICE_BASE_URL=https://users.example.com
export USER_SERVICE_AUTH_TYPE=bearer
export USER_SERVICE_TOKEN=secret
```

## Step 3 — Register configs at runtime

```python
from graflo.hq.connection_provider import InMemoryConnectionProvider
from graflo.hq.ingestion_parameters import IngestionParams

provider = InMemoryConnectionProvider()
provider.register_all_api_configs_from_env(bindings=bindings)

engine.define_and_ingest(
    manifest=manifest,
    target_db_config=conn_conf,
    connection_provider=provider,
    ingestion_params=IngestionParams(),
)
```

Override a single prefix when env naming differs:

```python
provider.register_all_api_configs_from_env(
    bindings=bindings,
    env_prefix_map={"user_service": "USERS_API_"},
)
```

## Full runnable example

See [Example 14](../examples/example-14.md) and `examples/14-api-env-wiring/`.

## Related documentation

- [API connector and pagination](../concepts/connectors/api_connector.md) — pagination, auth types, manual registration
- [Runtime connector updates](../concepts/connectors/runtime_updates.md) — patch connectors without editing the manifest
