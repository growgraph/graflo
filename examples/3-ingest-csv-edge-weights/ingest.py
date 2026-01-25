from suthing import FileHandle
from graflo import Caster, Patterns, Schema
from graflo.db.connection.onto import Neo4jConfig
from graflo.hq.caster import IngestionParams
import logging


# Configure logging: INFO level for graflo module, WARNING for others
logging.basicConfig(level=logging.WARNING, handlers=[logging.StreamHandler()])
# Set graflo module to INFO level
logging.getLogger("graflo").setLevel(logging.DEBUG)

schema = Schema.from_dict(FileHandle.load("schema.yaml"))

# Load config from docker/neo4j/.env (recommended)
# This automatically reads NEO4J_BOLT_PORT, NEO4J_AUTH, etc.
conn_conf = Neo4jConfig.from_docker_env()

# from graflo.db.connection.onto import TigergraphConfig
#
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

# Load patterns from YAML file (same pattern as Schema)
patterns = Patterns.from_dict(FileHandle.load("patterns.yaml"))

# Alternative: Create patterns programmatically
# from graflo.util.onto import FilePattern
# import pathlib
# patterns = Patterns()
# patterns.add_file_pattern(
#     "relations",
#     FilePattern(regex="^relations.*\.csv$", sub_path=pathlib.Path("."), resource_name="relations")
# )

caster = Caster(schema)


ingestion_params = IngestionParams(clean_start=True)
caster.ingest(
    output_config=conn_conf, patterns=patterns, ingestion_params=ingestion_params
)
