# Example 14: API connection wiring from environment variables

This example shows how to register REST API runtime credentials from environment
variables instead of constructing `RestApiConnConfig` manually.

Key points:

- Manifest `connector_connection` rows stay secret-free (`conn_proxy` labels only).
- Each `conn_proxy` maps to an env prefix: `user_service` → `USER_SERVICE_BASE_URL`, `USER_SERVICE_AUTH_TYPE`, …
- `register_all_api_configs_from_env(bindings)` discovers API proxies and binds all connectors in one call.
- Multi-service manifests can use different prefixes per proxy via `env_prefix_map`.

## Required environment variables

For the default two-proxy bindings in `api_env_wiring.py`:

```bash
export USER_SERVICE_BASE_URL=https://users.example.com
export USER_SERVICE_AUTH_TYPE=bearer
export USER_SERVICE_TOKEN=your-user-token

export ORDER_SERVICE_BASE_URL=https://orders.example.com
export ORDER_SERVICE_AUTH_TYPE=bearer
export ORDER_SERVICE_TOKEN=your-order-token
```

## Run

```bash
uv run python examples/14-api-env-wiring/api_env_wiring.py
```

See also [Example 14](../../docs/examples/example-14.md) and [API connector and pagination](../../docs/concepts/api_connector.md).
