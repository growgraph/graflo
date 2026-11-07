"""Ingest the products and suppliers with the identities that infer.py proposed.

Reads ``artifacts/manifest-inferred.yaml``, writes the graph to the file
backend in ``artifacts/csv-backend`` and prints, per vertex type, how many
records were written and how many distinct identities they have. Run it from
this directory, after ``infer.py``:

    uv run python ingest.py
"""

from pathlib import Path

from suthing import FileHandle

from graflo import GraphManifest
from graflo.architecture.backend import GraFloBackendReader
from graflo.connections import GraFloBackendConfig
from graflo.hq import GraphEngine
from graflo.hq.caster import IngestionParams

manifest = GraphManifest.from_config(
    FileHandle.load("artifacts/manifest-inferred.yaml")
)
manifest.finish_init()

backend = GraFloBackendConfig(output_dir=Path("artifacts/csv-backend"))
engine = GraphEngine(target_db_flavor=backend.connection_type)
engine.define_and_ingest(
    manifest=manifest,
    target_db_config=backend,
    ingestion_params=IngestionParams(clear_data=True),
    recreate_schema=True,
)

reader = GraFloBackendReader(backend.output_dir)
vertex_config = manifest.require_schema().core_schema.vertex_config
for vertex in vertex_config.vertices:
    records = [
        doc for batch in reader.iter_vertex_batches(vertex.name) for doc in batch
    ]
    distinct = {tuple(doc[field] for field in vertex.identity) for doc in records}
    print(
        f"{vertex.name:<9} {len(records)} records, "
        f"{len(distinct)} distinct identities ({', '.join(vertex.identity)})"
    )
