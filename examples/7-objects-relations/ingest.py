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

from graflo import Bindings, IngestionModel, Schema
from graflo.db import ArangoConfig
from graflo.hq import GraphEngine
from graflo.hq.caster import IngestionParams
from graflo.util.onto import FilePattern

schema_raw = FileHandle.load("schema.yaml")
schema = Schema.from_config(schema_raw)
ingestion_model = IngestionModel.from_config(schema_raw)

# Load config from docker/arango/.env (or neo4j, tigergraph, falkordb)
conn_conf = ArangoConfig.from_docker_env()
db_type = conn_conf.connection_type

bindings = Bindings()
bindings.add_file_pattern(
    "objects",
    FilePattern(
        regex=r"^objects\.csv$",
        sub_path=pathlib.Path("."),
        resource_name="objects",
    ),
)
bindings.add_file_pattern(
    "relations",
    FilePattern(
        regex=r"^relations\.csv$",
        sub_path=pathlib.Path("."),
        resource_name="relations",
    ),
)

engine = GraphEngine(target_db_flavor=db_type)
engine.define_and_ingest(
    schema=schema,
    target_db_config=conn_conf,
    ingestion_model=ingestion_model,
    bindings=bindings,
    ingestion_params=IngestionParams(clear_data=True),
    recreate_schema=True,
)

print("Ingestion complete!")
print(f"Schema: {schema.metadata.name}")
print(f"Vertices: {[v.name for v in schema.graph.vertex_config.vertices]}")
