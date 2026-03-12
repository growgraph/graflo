import pathlib
from suthing import FileHandle
from graflo import Bindings, IngestionModel, Schema
from graflo.util.onto import FilePattern
from graflo.db import ArangoConfig
from graflo.hq import GraphEngine
from graflo.hq.caster import IngestionParams

schema_raw = FileHandle.load("schema.yaml")
schema = Schema.from_config(schema_raw)
ingestion_model = IngestionModel.from_config(schema_raw)

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

# Create Bindings with file patterns
bindings = Bindings()
bindings.add_file_pattern(
    "work",
    FilePattern(regex="\Sjson$", sub_path=pathlib.Path("."), resource_name="work"),
)

# Or use resource_mapping for simpler initialization
# bindings = Bindings(
#     _resource_mapping={
#         "work": "./work.json",
#     }
# )

# Create GraphEngine and define schema + ingest in one operation
engine = GraphEngine(target_db_flavor=db_type)
ingestion_params = IngestionParams(clear_data=True)
engine.define_and_ingest(
    schema=schema,
    target_db_config=conn_conf,
    ingestion_model=ingestion_model,
    bindings=bindings,
    ingestion_params=ingestion_params,
    recreate_schema=True,
)
