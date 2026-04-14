"""Example 12: Vertex roles and multi-link edges.

Each CSV row encodes a person along with their parent and child IDs.
Three 'vertex: person' steps with distinct roles (self / parent / child) extract
the three role-distinct vertices from the same flat row. A single 'edge' step
with a 'links' list emits both relationship types — is_child_of and is_parent_of
— without needing to repeat the vertex extraction.

Run from this directory:
    uv run python ingest.py

Requires a graph database (ArangoDB, Neo4j, TigerGraph, or FalkorDB) running.
Load config from docker/<db>/.env, e.g. ArangoConfig.from_docker_env().
"""

from suthing import FileHandle

from graflo import GraphManifest
from graflo.db import ArangoConfig
from graflo.hq import GraphEngine
from graflo.hq.caster import IngestionParams

manifest = GraphManifest.from_config(FileHandle.load("manifest.yaml"))
manifest.finish_init()
schema = manifest.require_schema()
ingestion_model = manifest.require_ingestion_model()

# Load config from docker/arango/.env (or neo4j, tigergraph, falkordb)
conn_conf = ArangoConfig.from_docker_env()

# Alternative: specify directly or read from env vars
# conn_conf = ArangoConfig(
#     uri="http://localhost:8529",
#     username="root",
#     password="yourpassword",
#     database="_system",
# )

db_type = conn_conf.connection_type

engine = GraphEngine(target_db_flavor=db_type)
engine.define_and_ingest(
    manifest=manifest,
    target_db_config=conn_conf,
    ingestion_params=IngestionParams(clear_data=True),
    recreate_schema=True,
)

print("Ingestion complete!")
print(f"Schema: {schema.metadata.name}")
print(f"Vertices: {[v.name for v in schema.core_schema.vertex_config.vertices]}")
print(
    f"Edges: {[(e.source, e.target, e.relation) for e in schema.core_schema.edge_config.edges]}"
)
