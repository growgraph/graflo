"""
Example 14: API connection wiring from environment variables.

Demonstrates InMemoryConnectionProvider.register_all_api_configs_from_env for
multi-proxy API manifests. Set env vars before running (see README.md).
"""

from __future__ import annotations

import logging

from graflo.architecture.contract.bindings import APIConnector, Bindings
from graflo.hq.connection_provider import (
    ApiGeneralizedConnConfig,
    InMemoryConnectionProvider,
)

logger = logging.getLogger(__name__)


def make_multi_api_bindings() -> Bindings:
    """Bindings with two API connectors and distinct conn_proxy labels."""
    users_connector = APIConnector(name="users_api", path="/api/users")
    orders_connector = APIConnector(name="orders_api", path="/api/orders")
    return Bindings(
        connectors=[users_connector, orders_connector],
        connector_connection=[
            {"connector": "users_api", "conn_proxy": "user_service"},
            {"connector": "orders_api", "conn_proxy": "order_service"},
        ],
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])

    bindings = make_multi_api_bindings()
    provider = InMemoryConnectionProvider()
    provider.register_all_api_configs_from_env(bindings=bindings)

    for connector in bindings.connectors:
        generalized = provider.get_generalized_conn_config(connector)
        if not isinstance(generalized, ApiGeneralizedConnConfig):
            raise TypeError(f"Expected ApiGeneralizedConnConfig for {connector.name}")
        runtime = generalized.config
        auth_type = runtime.auth.auth_type if runtime.auth is not None else None
        logger.info(
            "connector=%s base_url=%s auth_type=%s",
            connector.name,
            runtime.base_url,
            auth_type,
        )


if __name__ == "__main__":
    main()
