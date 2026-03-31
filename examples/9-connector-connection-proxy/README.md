# Example 9: Explicit connector_connection proxy wiring

This example demonstrates the non-secret runtime indirection:

`Resource -> Connector -> ConnectionProxy -> RuntimeConnectionConfig`

Key points:
- The manifest stores only `conn_proxy` labels inside `bindings.connector_connection`.
- Each `connector` row references a connector by **`name` or `hash`** (not by ingestion resource name).
- The runtime script registers the real `PostgresConfig` under that proxy label
  via `InMemoryConnectionProvider`.
- `provider.bind_from_bindings(bindings=...)` connects manifest connectors
  to the proxy label so ingestion can resolve `conn_proxy -> config`.

