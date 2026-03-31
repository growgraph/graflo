"""
Example 9: Explicit connector_connection proxy wiring

This example focuses on the runtime indirection:

Resource -> Connector -> ConnectionProxy -> RuntimeConnectionConfig

Unlike `examples/5-ingest-postgres`, we explicitly author:
- `Bindings.connector_connection` in the manifest (proxy labels, no secrets)
- the runtime `InMemoryConnectionProvider` wiring that registers proxy configs
  and binds manifest connectors to those proxies.
"""

from __future__ import annotations

import logging
from pathlib import Path

from graflo.architecture.contract.bindings import Bindings, TableConnector
from graflo.hq import GraphEngine, IngestionParams
from graflo.hq.connection_provider import (
    InMemoryConnectionProvider,
    PostgresGeneralizedConnConfig,
)
from graflo.db.postgres.util import load_schema_from_sql_file
from graflo.db import PostgresConfig, TigergraphConfig

logger = logging.getLogger(__name__)


def _load_mock_postgres_schema(*, postgres_conf: PostgresConfig) -> None:
    schema_file = (
        Path(__file__).resolve().parents[1]
        / "5-ingest-postgres"
        / "data"
        / "mock_schema.sql"
    )
    if not schema_file.exists():
        logger.warning("Mock schema file not found: %s", schema_file)
        return

    load_schema_from_sql_file(
        config=postgres_conf,
        schema_file=schema_file,
        continue_on_error=True,  # e.g. DROP TABLE IF EXISTS ...
    )


def make_explicit_postgres_bindings(conn_proxy: str) -> Bindings:
    """Create manifest bindings with explicit connector_connection proxy labels."""
    # Each connector has an explicit `name` so `connector_connection.connector`
    # can reference it. Ingestion resource names still come from `resource_name`
    # (or `resource_connector`); those names are not valid connector refs.
    connectors = [
        TableConnector(
            name="users",
            table_name="users",
            schema_name="public",
            resource_name="users",
        ),
        TableConnector(
            name="products",
            table_name="products",
            schema_name="public",
            resource_name="products",
        ),
        TableConnector(
            name="purchases",
            table_name="purchases",
            schema_name="public",
            resource_name="purchases",
        ),
        TableConnector(
            name="follows",
            table_name="follows",
            schema_name="public",
            resource_name="follows",
        ),
    ]

    connector_connection = [
        {"connector": "users", "conn_proxy": conn_proxy},
        {"connector": "products", "conn_proxy": conn_proxy},
        {"connector": "purchases", "conn_proxy": conn_proxy},
        {"connector": "follows", "conn_proxy": conn_proxy},
    ]

    return Bindings(
        connectors=connectors,
        # We don't need `resource_connector` because each connector defines
        # `resource_name`, which Bindings uses to map resources -> connectors.
        resource_connector=[],
        connector_connection=connector_connection,
    )


def main() -> None:
    logging.basicConfig(level=logging.WARNING, handlers=[logging.StreamHandler()])
    logging.getLogger("graflo").setLevel(logging.INFO)

    # Step 1: Connect to PostgreSQL (source database)
    postgres_conf = PostgresConfig.from_docker_env()

    # Optional: initialize a mock schema (for quick local experimentation)
    _load_mock_postgres_schema(postgres_conf=postgres_conf)

    # Step 2: Connect to target graph database
    conn_conf = TigergraphConfig.from_docker_env()
    db_type = conn_conf.connection_type

    # Step 3: Infer schema (optional for this example; bindings are explicit below)
    engine = GraphEngine(target_db_flavor=db_type)
    manifest = engine.infer_manifest(
        postgres_conf, schema_name="public", fuzzy_threshold=0.8
    )
    schema = manifest.require_schema()
    schema.metadata.name = "accounting"
    ingestion_model = manifest.require_ingestion_model()
    _ = ingestion_model  # kept to emphasize the manifest contains ingestion resources

    # Step 4: Author bindings with explicit connector_connection proxy labels
    conn_proxy = "postgres_source"
    bindings = make_explicit_postgres_bindings(conn_proxy=conn_proxy)

    manifest = manifest.model_copy(update={"bindings": bindings})
    manifest.finish_init()

    # Step 5: Wire runtime proxy config + bind manifest connectors to the proxy
    # Manifest contains proxy labels only; register the real connection config here.
    provider = InMemoryConnectionProvider()
    provider.register_generalized_config(
        conn_proxy=conn_proxy,
        config=PostgresGeneralizedConnConfig(config=postgres_conf),
    )
    provider.bind_from_bindings(bindings=bindings)

    # Step 6: Define schema and ingest (runtime uses `connection_provider=provider`)
    engine.define_and_ingest(
        manifest=manifest,
        target_db_config=conn_conf,
        ingestion_params=IngestionParams(clear_data=True),
        recreate_schema=True,
        connection_provider=provider,
    )

    print("\n" + "=" * 80)
    print("Explicit proxy binding ingestion complete!")
    print("=" * 80)
    print(f"Schema: {schema.metadata.name}")
    print(f"Bindings.connector_connection: {manifest.bindings.connector_connection}")


if __name__ == "__main__":
    main()
