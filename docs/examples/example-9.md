# Example 9: Explicit `connector_connection` Proxy Wiring

This example shows the full proxy chain end-to-end:

`Resource -> Connector -> ConnectionProxy -> RuntimeConnectionConfig`

The manifest stays credential-free: `bindings.connector_connection` only contains proxy labels (`conn_proxy`). The script then registers the real connection config at runtime.

## Manifest: what `connector_connection` looks like

Inside `bindings` you explicitly map each connector to a proxy label. The `connector` field must be a **connector `name`** or **canonical hash**, not an ingestion resource name (a resource may be bound to several connectors).

```yaml
bindings:
  connector_connection:
    - connector: users
      conn_proxy: postgres_source
    - connector: products
      conn_proxy: postgres_source
    - connector: purchases
      conn_proxy: postgres_source
    - connector: follows
      conn_proxy: postgres_source
```

In the companion script, each `TableConnector` sets `name` to match those references (here they match the table/resource names only for readability).

## Runtime: how the proxy label becomes a real DB config

The script wires runtime config and binds the manifest connectors to the chosen proxy:

```python
from graflo.hq.connection_provider import (
    InMemoryConnectionProvider,
    PostgresGeneralizedConnConfig,
)

provider = InMemoryConnectionProvider()

provider.register_generalized_config(
    conn_proxy="postgres_source",
    config=PostgresGeneralizedConnConfig(config=postgres_conf),
)

provider.bind_from_bindings(bindings=bindings)
```

For the common single-DB / single-proxy case, you can also use:

```python
provider.bind_single_config_for_bindings(
    bindings=bindings,
    conn_proxy="postgres_source",
    config=PostgresGeneralizedConnConfig(config=postgres_conf),
)
```

## Full script

See:

- `examples/9-connector-connection-proxy/explicit_proxy_binding.py`
- `examples/9-connector-connection-proxy/README.md`

