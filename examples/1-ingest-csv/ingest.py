from suthing import FileHandle
from graflo import GraphManifest
from graflo.db import ArangoConfig
from graflo.hq import GraphEngine
from graflo.hq.caster import IngestionParams

manifest = GraphManifest.from_config(FileHandle.load("manifest.yaml"))
manifest.finish_init()
schema = manifest.require_schema()
ingestion_model = manifest.require_ingestion_model()

# Load config from docker/arango/.env (recommended)
# This automatically reads ARANGO_URI, ARANGO_USERNAME, ARANGO_PASSWORD, etc.
conn_conf = ArangoConfig.from_docker_env()

# Alternative: Create config directly or use environment variables
# Set ARANGO_URI, ARANGO_USERNAME, ARANGO_PASSWORD, ARANGO_DATABASE env vars
# conn_conf = ArangoConfig()  # Reads from environment variables
# Or specify directly:
# conn_conf = ArangoConfig(
#     uri="http://localhost:8535",
#     username="root",
#     password="123",
#     database="_system",
# )

# Determine DB type from connection config
db_type = conn_conf.connection_type

# Create GraphEngine and define schema + ingest in one operation
engine = GraphEngine(target_db_flavor=db_type)
ingestion_params = IngestionParams(
    clear_data=True,
    # Optional: append per-document cast failures as gzip JSONL (see docs/concepts/ingestion_doc_errors.md).
    # from pathlib import Path
    # doc_error_sink_path=Path("row_failures.jsonl.gz"),
)
engine.define_and_ingest(
    manifest=manifest,
    target_db_config=conn_conf,
    ingestion_params=ingestion_params,
    recreate_schema=True,
)
