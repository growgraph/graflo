from suthing import FileHandle
from graflo import Bindings, GraphManifest
from graflo.db import Neo4jConfig
from graflo.hq import GraphEngine
from graflo.hq.caster import IngestionParams

import logging


# Configure logging: INFO level for graflo module, WARNING for others
logging.basicConfig(level=logging.WARNING, handlers=[logging.StreamHandler()])
# Set graflo module to INFO level
logging.getLogger("graflo").setLevel(logging.DEBUG)

manifest = GraphManifest.from_config(FileHandle.load("manifest.yaml"))
manifest.finish_init()
schema = manifest.require_schema()
ingestion_model = manifest.require_ingestion_model()

# Load config from docker/neo4j/.env (recommended)
# This automatically reads NEO4J_BOLT_PORT, NEO4J_AUTH, etc.
conn_conf = Neo4jConfig.from_docker_env()

# from graflo.db import TigergraphConfig
# conn_conf = TigergraphConfig.from_docker_env()

# Alternative: Create config directly or use environment variables
# Set NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD, NEO4J_BOLT_PORT env vars
# conn_conf = Neo4jConfig()  # Reads from environment variables
# Or specify directly:
# conn_conf = Neo4jConfig(
#     uri="bolt://localhost:7688",
#     username="neo4j",
#     password="test!passfortesting",
#     bolt_port=7688
# )

# Determine DB type from connection config
db_type = conn_conf.connection_type

# Load connectors from YAML file (same approach as Schema)
bindings = Bindings.from_dict(FileHandle.load("patterns.yaml"))

# Alternative: Create connectors programmatically
# from graflo.util.onto import FileConnector
# import pathlib
# bindings = Bindings()
# bindings.add_file_connector(
#     "relations",
#     FileConnector(regex="^relations.*\.csv$", sub_path=pathlib.Path("."), resource_name="relations")
# )

# Create GraphEngine and define schema + ingest in one operation
engine = GraphEngine(target_db_flavor=db_type)
ingestion_params = IngestionParams(clear_data=True)
manifest = manifest.model_copy(update={"bindings": bindings})
manifest.finish_init()
engine.define_and_ingest(
    manifest=manifest,
    target_db_config=conn_conf,
    ingestion_params=ingestion_params,
    recreate_schema=True,
)
