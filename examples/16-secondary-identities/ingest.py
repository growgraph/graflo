"""How do I link to a record when the source only knows its alternative identifier?

Reads ``manifest.yaml`` and loads instruments, issuers, and then a file that
links them by ISIN and LEI, into the file backend in ``artifacts/csv-backend``.
Run it from this directory:

    uv run python ingest.py
    uv run python inspect_graph.py
"""

from pathlib import Path

from suthing import FileHandle

from graflo import GraphManifest
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
print(f"Wrote {backend.output_dir}")
