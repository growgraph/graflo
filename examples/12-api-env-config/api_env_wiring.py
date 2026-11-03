"""How do I configure several API sources from environment variables?

Reads ``manifest_apis.yaml``, whose two API connectors name their API only by a
label, loads the address and credentials of each label from environment
variables and prints what each connector resolved to. Nothing is contacted. Set
the variables listed in the README, then run it from this directory:

    uv run python api_env_wiring.py
"""

from suthing import FileHandle

from graflo import GraphManifest
from graflo.connections import (
    InMemoryConnectionProvider,
    RestApiConnConfig,
    connector_config,
)

manifest = GraphManifest.from_config(FileHandle.load("manifest_apis.yaml"))
manifest.finish_init()
bindings = manifest.require_bindings()

# One config per label in the bindings, read from {LABEL}_BASE_URL, {LABEL}_TOKEN, ...
provider = InMemoryConnectionProvider.from_env(bindings)

for connector in bindings.connectors:
    api = connector_config(connector, bindings, provider, RestApiConnConfig)
    auth_type = api.auth.auth_type if api.auth is not None else "none"
    print(
        f"{connector.name}: {api.base_url}{connector.path} "
        f"(label {bindings.get_conn_proxy_for_connector(connector)}, auth {auth_type})"
    )
