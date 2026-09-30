"""How do I get a graph from a PostgreSQL database without writing a schema?

Loads the sample shop (``data/shop.sql``) into PostgreSQL, infers a manifest from
the tables and their keys, saves the inferred schema to
``generated-manifest.yaml`` and writes the graph to ArangoDB. Run it from this
directory:

    uv run python ingest.py
"""

from pathlib import Path

from suthing import FileHandle

from graflo.connections import ArangoConfig, PostgresConfig
from graflo.db.postgres.util import load_schema_from_sql_file
from graflo.hq import GraphEngine, IngestionParams

EXAMPLE_DIR = Path(__file__).resolve().parent

# 1. Connect to PostgreSQL and load the sample shop.
postgres_conf = PostgresConfig.from_docker_env()
load_schema_from_sql_file(
    config=postgres_conf,
    schema_file=EXAMPLE_DIR / "data" / "shop.sql",
    continue_on_error=False,
)

# 2. Infer a manifest from the tables in the PostgreSQL schema "public".
conn_conf = ArangoConfig.from_docker_env()
engine = GraphEngine(target_db_flavor=conn_conf.connection_type)
manifest = engine.infer_manifest(postgres_conf, schema_name="public")

# The inferred graph is named after the PostgreSQL schema. The name also becomes
# the ArangoDB database when the connection settings name none.
schema = manifest.require_schema()
schema.metadata.name = "shop"

# 3. Save what was inferred, to read or edit.
FileHandle.dump(
    schema.model_dump(exclude_defaults=True), EXAMPLE_DIR / "generated-manifest.yaml"
)

# 4. Write the graph. The engine keeps the PostgreSQL settings it inferred from,
# so the inferred bindings can read the tables.
engine.define_and_ingest(
    manifest=manifest,
    target_db_config=conn_conf,
    ingestion_params=IngestionParams(clear_data=True),
    recreate_schema=True,
)
