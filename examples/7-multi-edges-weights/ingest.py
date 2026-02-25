from suthing import FileHandle
from graflo import Patterns, Schema
from graflo.db import Neo4jConfig
from graflo.hq import GraphEngine
from graflo.hq.caster import IngestionParams

import logging
from graflo.util.onto import FilePattern
import pathlib


# Configure logging: INFO level for graflo module, WARNING for others
logging.basicConfig(level=logging.WARNING, handlers=[logging.StreamHandler()])
# Set graflo module to INFO level
logging.getLogger("graflo").setLevel(logging.DEBUG)

schema = Schema.from_dict(FileHandle.load("schema.yaml"))

# Load config from docker/neo4j/.env (recommended)
# This automatically reads NEO4J_BOLT_PORT, NEO4J_AUTH, etc.
conn_conf = Neo4jConfig.from_docker_env()


# Determine DB type from connection config
db_type = conn_conf.connection_type

# Alternative: Create patterns programmatically
patterns = Patterns()
patterns.add_file_pattern(
    "ticker_data",
    FilePattern(
        regex="^data.*\.csv$", sub_path=pathlib.Path("."), resource_name="relations"
    ),
)

# Create GraphEngine and define schema + ingest in one operation
engine = GraphEngine(target_db_flavor=db_type)
ingestion_params = IngestionParams(clear_data=True)
engine.define_and_ingest(
    schema=schema,
    target_db_config=conn_conf,
    patterns=patterns,
    ingestion_params=ingestion_params,
    recreate_schema=True,
)
