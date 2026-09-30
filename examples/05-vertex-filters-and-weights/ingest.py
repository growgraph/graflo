"""How do I turn price columns into measurements and skip invalid values?

Reads ``manifest.yaml``, creates the schema in ArangoDB and loads the daily
stock prices in ``data/prices.csv``. Each price and volume becomes a measurement
linked to its ticker; values that are not positive are dropped. Run it from
this directory:

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
