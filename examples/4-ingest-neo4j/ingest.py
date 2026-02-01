import pathlib
from suthing import FileHandle
from graflo import Patterns, Schema
from graflo.util.onto import FilePattern
from graflo.db.connection.onto import Neo4jConfig
from graflo.hq import GraphEngine
from graflo.hq.caster import IngestionParams

schema = Schema.from_dict(FileHandle.load("schema.yaml"))

# Load config from docker/neo4j/.env (recommended)
# This automatically reads NEO4J_BOLT_PORT, NEO4J_AUTH, etc.
conn_conf = Neo4jConfig.from_docker_env()

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

# Create Patterns with file patterns
patterns = Patterns()
patterns.add_file_pattern(
    "package",
    FilePattern(
        regex=r"^package\.meta.*\.json(?:\.gz)?$",
        sub_path=pathlib.Path("./data"),
        resource_name="package",
    ),
)
patterns.add_file_pattern(
    "bug",
    FilePattern(
        regex=r"^bugs.*\.json(?:\.gz)?$",
        sub_path=pathlib.Path("./data"),
        resource_name="bugs",
    ),
)

# Or use resource_mapping for simpler initialization
# patterns = Patterns(
#     _resource_mapping={
#         "package": "./data/package.meta.json",
#         "bugs": "./data/bugs.head.json",
#     }
# )

# Create GraphEngine and define schema + ingest in one operation
engine = GraphEngine(target_db_flavor=db_type)
ingestion_params = IngestionParams(
    # max_items=5,
)
engine.define_and_ingest(
    schema=schema,
    target_db_config=conn_conf,  # Target database config
    patterns=patterns,  # Source data patterns
    ingestion_params=ingestion_params,
    recreate_schema=True,  # Wipe existing schema before defining and ingesting
)
