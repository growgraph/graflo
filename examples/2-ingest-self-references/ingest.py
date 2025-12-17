import pathlib
from suthing import FileHandle
from graflo import Caster, Patterns, Schema
from graflo.util.onto import FilePattern
from graflo.db.connection.onto import ArangoConfig
from graflo.caster import IngestionParams

schema = Schema.from_dict(FileHandle.load("schema.yaml"))

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

# Create Patterns with file patterns
patterns = Patterns()
patterns.add_file_pattern(
    "work",
    FilePattern(regex="\Sjson$", sub_path=pathlib.Path("."), resource_name="work"),
)

# Or use resource_mapping for simpler initialization
# patterns = Patterns(
#     _resource_mapping={
#         "work": "./work.json",
#     }
# )

caster = Caster(schema)


ingestion_params = IngestionParams(clean_start=True)
caster.ingest(
    output_config=conn_conf, patterns=patterns, ingestion_params=ingestion_params
)
