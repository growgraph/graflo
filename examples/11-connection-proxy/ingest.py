"""How do I keep database credentials out of the manifest?

Reads ``manifest_shop.yaml``, whose connectors name the shop database only by the
label ``shop_db``. Supplies the PostgreSQL connection settings for that label at
run time, loads the sample shop of example 09 into PostgreSQL and writes the
graph to ArangoDB. Run it from this directory:

    uv run python ingest.py
"""

from pathlib import Path

from suthing import FileHandle

from graflo import GraphManifest
from graflo.connections import (
    ArangoConfig,
    InMemoryConnectionProvider,
    PostgresConfig,
    PostgresGeneralizedConnConfig,
)
from graflo.db.postgres.util import load_schema_from_sql_file
from graflo.hq import GraphEngine, IngestionParams

SHOP_SQL = (
    Path(__file__).resolve().parents[1] / "09-infer-from-postgres" / "data" / "shop.sql"
)

manifest = GraphManifest.from_config(FileHandle.load("manifest_shop.yaml"))
manifest.finish_init()

# The credentials: read from the settings of the PostgreSQL container, not from
# the manifest.
postgres_conf = PostgresConfig.from_docker_env()
load_schema_from_sql_file(
    config=postgres_conf, schema_file=SHOP_SQL, continue_on_error=False
)

# Give the label "shop_db" its connection settings.
provider = InMemoryConnectionProvider()
provider.bind_single_config_for_bindings(
    bindings=manifest.require_bindings(),
    conn_proxy="shop_db",
    config=PostgresGeneralizedConnConfig(config=postgres_conf),
)

conn_conf = ArangoConfig.from_docker_env()
engine = GraphEngine(target_db_flavor=conn_conf.connection_type)
engine.define_and_ingest(
    manifest=manifest,
    target_db_config=conn_conf,
    ingestion_params=IngestionParams(clear_data=True),
    recreate_schema=True,
    connection_provider=provider,
)
