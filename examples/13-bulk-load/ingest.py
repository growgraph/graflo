"""How do I load a large graph into an empty database?

Reads ``manifest.yaml`` and loads ``data/works.csv`` into Neo4j in bulk: the
input is cast and staged in ``artifacts/staging``, folded to one row per vertex
and edge, then written once. Run it from this directory:

    uv run python ingest.py

Run it again to resume a stopped load; delete ``artifacts/staging`` to start over.
"""

import json
from pathlib import Path

from suthing import FileHandle

from graflo import GraphManifest
from graflo.connections import Neo4jConfig
from graflo.hq import GraphEngine
from graflo.hq.caster import IngestionParams

manifest = GraphManifest.from_config(FileHandle.load("manifest.yaml"))
manifest.finish_init()

# Connection settings of the Neo4j container started from docker/neo4j.
conn_conf = Neo4jConfig.from_docker_env()
staging = Path("artifacts/staging")

engine = GraphEngine(target_db_flavor=conn_conf.connection_type)
engine.define_and_ingest(
    manifest=manifest,
    target_db_config=conn_conf,
    ingestion_params=IngestionParams(mode="bulk", staging_dir=staging),
    # The target must be empty: drop whatever an earlier run left there.
    recreate_schema=True,
)

for run in json.loads((staging / "counts.json").read_text())["runs"]:
    kind = run["type"]
    name = kind["name"] if kind["kind"] == "vertex" else " -> ".join(kind["edge_id"])
    print(f"{name}: {run['staged']} staged, {run['folded']} folded")
