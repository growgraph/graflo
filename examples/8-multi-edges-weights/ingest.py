from suthing import FileHandle
from graflo import Bindings, GraphManifest
from graflo.db import Neo4jConfig
from graflo.hq import GraphEngine
from graflo.hq.caster import IngestionParams

import logging
from graflo.architecture.contract.bindings import FileConnector
import pathlib


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


# Determine DB type from connection config
db_type = conn_conf.connection_type

# Alternative: Create connectors programmatically
bindings = Bindings()
bindings.add_file_connector(
    "ticker_data",
    FileConnector(
        regex=r"^data.*\.csv$",
        sub_path=pathlib.Path("."),
        resource_name="ticker_data",
    ),
)

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
