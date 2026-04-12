"""Example: two ``causes`` edges per CSV row between ``incident`` vertices.

Wide columns ``id``, ``causes``, ``parent_incident`` are normalized into a
``cause_edges`` list via ``incident_row_to_cause_edge_list``, then each pair is
ingested like example 3 (two vertex projections + one edge).
"""

from suthing import FileHandle

from graflo import GraphManifest
from graflo.db import TigergraphConfig
from graflo.hq import GraphEngine
from graflo.hq.caster import IngestionParams

manifest = GraphManifest.from_config(FileHandle.load("manifest.yaml"))
manifest.finish_init()

conn_conf = TigergraphConfig.from_docker_env()
conn_conf.max_job_size = 5000

db_type = conn_conf.connection_type
engine = GraphEngine(target_db_flavor=db_type)
ingestion_params = IngestionParams(clear_data=True)
engine.define_and_ingest(
    manifest=manifest,
    target_db_config=conn_conf,
    ingestion_params=ingestion_params,
    recreate_schema=True,
)
