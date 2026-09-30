"""How do I try GraFlo without a database, and export a graph to files?

Reads ``manifest.yaml`` (the manifest of example 01) and writes the graph built
from the two CSV files in ``data/`` to the directory ``artifacts/csv-backend``
instead of a database. Run it from this directory:

    uv run python ingest.py
"""

from pathlib import Path

from suthing import FileHandle

from graflo import GraphManifest
from graflo.connections import GraFloBackendConfig
from graflo.hq import GraphEngine
from graflo.hq.caster import IngestionParams

manifest = GraphManifest.from_config(FileHandle.load("manifest.yaml"))
manifest.finish_init()

# The target is a directory: the only change from example 01's ingest.py.
backend = GraFloBackendConfig(output_dir=Path("artifacts/csv-backend"))

engine = GraphEngine(target_db_flavor=backend.connection_type)
engine.define_and_ingest(
    manifest=manifest,
    target_db_config=backend,
    ingestion_params=IngestionParams(clear_data=True),
    recreate_schema=True,
)
print(f"Wrote {backend.output_dir}")
