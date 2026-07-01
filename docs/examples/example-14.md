# Example 14: API env wiring

This example registers REST API runtime credentials from environment variables
instead of building `RestApiConnConfig` objects in code.

## Manifest: proxy labels only

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

## Environment variables

Each `conn_proxy` maps to an uppercase env prefix (`user_service` → `USER_SERVICE_`):

| Variable | Required | Meaning |
| -------- | -------- | ------- |
| `{PREFIX}BASE_URL` | yes | API base URL |
| `{PREFIX}AUTH_TYPE` | no (default `bearer`) | `bearer`, `basic`, `digest`, or `api_key` |
| `{PREFIX}TOKEN` | when using bearer/api_key | Token or API key value |
| `{PREFIX}USERNAME` / `{PREFIX}PASSWORD` | when using basic/digest | Credentials |
| `{PREFIX}HEADER_NAME` | no | Header for bearer/api_key (default `Authorization`) |
| `{PREFIX}PREFIX` | no | Bearer prefix (default `Bearer`) |

Example:

```bash
export USER_SERVICE_BASE_URL=https://users.example.com
export USER_SERVICE_AUTH_TYPE=bearer
export USER_SERVICE_TOKEN=secret

export ORDER_SERVICE_BASE_URL=https://orders.example.com
export ORDER_SERVICE_AUTH_TYPE=bearer
export ORDER_SERVICE_TOKEN=secret
```

## Runtime: one-call wiring

```python
from graflo.hq.connection_provider import InMemoryConnectionProvider

provider = InMemoryConnectionProvider()
provider.register_all_api_configs_from_env(bindings=bindings)

engine.define_and_ingest(
    manifest=manifest,
    target_db_config=conn_conf,
    connection_provider=provider,
    ingestion_params=IngestionParams(),
)
```

Override a single proxy prefix when your env naming differs:

```python
provider.register_all_api_configs_from_env(
    bindings=bindings,
    env_prefix_map={"user_service": "USERS_API_"},
)
```

Register one proxy explicitly:

```python
provider.register_api_config_from_env(conn_proxy="user_service")
```

## Full script

- `examples/14-api-env-wiring/api_env_wiring.py`
- `examples/14-api-env-wiring/README.md`

See [API connector and pagination](../concepts/connectors/api_connector.md) for auth types, pagination, and manual registration.
