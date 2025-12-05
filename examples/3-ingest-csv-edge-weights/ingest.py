from suthing import FileHandle
from graflo import Caster, Patterns, Schema
from graflo.backend.connection.onto import Neo4jConfig

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
#     bolt_port=7688,
# )

patterns = Patterns.from_dict(
    {
        "patterns": {
            "people": {"regex": "^relations.*\.csv$"},
        }
    }
)

caster = Caster(schema)

caster.ingest_files(path=".", conn_conf=conn_conf, patterns=patterns, clean_start=True)
