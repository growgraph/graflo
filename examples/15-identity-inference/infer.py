"""My data has no obvious key. How do I find out what identifies a record?

Reads ``manifest.yaml``, whose vertices declare no identity, and the CSV files
in ``data/``. Proposes an identity for each vertex type from the rows, prints
the proposal and writes the manifest with the proposed identities to
``artifacts/manifest-inferred.yaml``. Run it from this directory:

    uv run python infer.py
"""

import csv
from pathlib import Path

import yaml
from suthing import FileHandle

from graflo import GraphManifest
from graflo.architecture.schema.vertex import VertexConfig
from graflo.db.identity_inference import apply_identity_inference_to_vertices

#: The file whose rows are the sample for each vertex type.
SAMPLE_FILES = {"product": "data/products.csv", "supplier": "data/suppliers.csv"}
OUTPUT = Path("artifacts/manifest-inferred.yaml")


def read_rows(path: str) -> list[dict[str, str]]:
    """Read one CSV file as a list of rows."""
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


manifest = GraphManifest.from_config(FileHandle.load("manifest.yaml"))
manifest.finish_init()
schema = manifest.require_schema()
vertex_config = schema.core_schema.vertex_config

samples = {name: read_rows(path) for name, path in SAMPLE_FILES.items()}
vertices, results = apply_identity_inference_to_vertices(
    list(vertex_config.vertices), samples
)

for name, result in results.items():
    print(
        f"{name:<9} {result.strategy:<10} identity={result.identity} "
        f"confidence={result.confidence}"
    )

# Every vertex now has an identity; from here on, a missing one is an error.
inferred_config = VertexConfig(
    vertices=vertices,
    force_types=vertex_config.force_types,
    identity_from_all_properties=False,
)
core_schema = schema.core_schema.model_copy(update={"vertex_config": inferred_config})
inferred = manifest.model_copy(
    update={"graph_schema": schema.model_copy(update={"core_schema": core_schema})}
)

OUTPUT.parent.mkdir(parents=True, exist_ok=True)
with OUTPUT.open("w", encoding="utf-8") as f:
    yaml.safe_dump(inferred.to_minimal_canonical_dict(), f, indent=4, sort_keys=False)
print(f"Wrote {OUTPUT}")
