"""Example 7: Objects and Relations with vertex_router and edge_router.

Ingests polymorphic entities (Person, Vehicle, Institution) from objects.csv
and dynamic relations (EMPLOYED_BY, OWNS, FUNDS, etc.) from relations.csv
into a graph database.

Run from this directory:
    uv run python ingest.py

Requires a graph database (ArangoDB, Neo4j, TigerGraph, or FalkorDB) running.
Load config from docker/<db>/.env, e.g. Neo4jConfig.from_docker_env().
"""

import pathlib

from suthing import FileHandle

from graflo import Bindings, GraphManifest
from graflo.db import ArangoConfig
from graflo.hq import GraphEngine
from graflo.hq.caster import IngestionParams
from graflo.architecture.contract.bindings import FileConnector

manifest = GraphManifest.from_config(FileHandle.load("manifest.yaml"))
manifest.finish_init()
schema = manifest.require_schema()
ingestion_model = manifest.require_ingestion_model()

# Load config from docker/arango/.env (or neo4j, tigergraph, falkordb)
conn_conf = ArangoConfig.from_docker_env()
db_type = conn_conf.connection_type

bindings = Bindings()
bindings.add_file_connector(
    "objects",
    FileConnector(
        regex=r"^objects\.csv$",
        sub_path=pathlib.Path("."),
        resource_name="objects",
    ),
)
bindings.add_file_connector(
    "relations",
    FileConnector(
        regex=r"^relations\.csv$",
        sub_path=pathlib.Path("."),
        resource_name="relations",
    ),
)

engine = GraphEngine(target_db_flavor=db_type)
manifest = manifest.model_copy(update={"bindings": bindings})
manifest.finish_init()
engine.define_and_ingest(
    manifest=manifest,
    target_db_config=conn_conf,
    ingestion_params=IngestionParams(clear_data=True),
    recreate_schema=True,
)

print("Ingestion complete!")
print(f"Schema: {schema.metadata.name}")
print(f"Vertices: {[v.name for v in schema.core_schema.vertex_config.vertices]}")
