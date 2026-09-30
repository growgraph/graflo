"""How do I link records that refer to records of the same kind?

Reads ``manifest.yaml``, creates the schema in ArangoDB and loads the scholarly
works in ``data/works.json``, with an edge from each work to every work it
cites. Run it from this directory:

    uv run python ingest.py
"""

from suthing import FileHandle

from graflo import GraphManifest
from graflo.connections import ArangoConfig
from graflo.hq import GraphEngine
from graflo.hq.caster import IngestionParams

manifest = GraphManifest.from_config(FileHandle.load("manifest.yaml"))
manifest.finish_init()

# Connection settings of the ArangoDB container started from docker/arango.
conn_conf = ArangoConfig.from_docker_env()

engine = GraphEngine(target_db_flavor=conn_conf.connection_type)
engine.define_and_ingest(
    manifest=manifest,
    target_db_config=conn_conf,
    ingestion_params=IngestionParams(clear_data=True),
    recreate_schema=True,
)
