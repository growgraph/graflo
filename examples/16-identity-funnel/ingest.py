"""Records arrive with different identifiers filled in. How do I still get one vertex per thing?

Reads ``manifest.yaml``, loads the CRM and billing files into the file backend
in ``artifacts/csv-backend``, and prints how many ``party`` records were
written and how many distinct ids they carry. Run it from this directory:

    uv run python ingest.py
"""

from pathlib import Path

from suthing import FileHandle

from graflo import GraphManifest
from graflo.architecture.backend import GraFloBackendReader
from graflo.connections import GraFloBackendConfig
from graflo.hq import GraphEngine
from graflo.hq.caster import IngestionParams

manifest = GraphManifest.from_config(FileHandle.load("manifest.yaml"))
manifest.finish_init()

backend = GraFloBackendConfig(output_dir=Path("artifacts/csv-backend"))
engine = GraphEngine(target_db_flavor=backend.connection_type)
engine.define_and_ingest(
    manifest=manifest,
    target_db_config=backend,
    ingestion_params=IngestionParams(clear_data=True),
    recreate_schema=True,
)

# The file backend appends records; records with the same id are one vertex
# in a database, so count the ids as well.
reader = GraFloBackendReader(backend.output_dir)
parties = [doc for batch in reader.iter_vertex_batches("party") for doc in batch]
distinct = {doc["id"] for doc in parties}
print(f"party: {len(parties)} records, {len(distinct)} distinct ids")
