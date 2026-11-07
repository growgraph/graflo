"""Show the edges each source writes, before and after the repair.

Casts the three CSV files in ``data/`` through ``manifest.yaml`` and through
``artifacts/manifest_repaired.yaml``, prints every edge written, and counts
the stored ``employs`` edges. No database is involved.

    cd examples/19-edge-inverses
    uv run python inspect_edges.py
"""

from __future__ import annotations

import asyncio
import csv
from pathlib import Path

from suthing import FileHandle

from graflo import GraphManifest
from graflo.hq.document_caster import DocumentCaster
from graflo.hq.ingestion_parameters import IngestionParams

EXAMPLE_DIR = Path(__file__).resolve().parent
RESOURCES = ["hr", "registry", "alumni"]
MANIFESTS = ["manifest.yaml", "artifacts/manifest_repaired.yaml"]
#: The identity field of each vertex type, used to print an edge's endpoints.
KEYS = {"person": "person_id", "institution": "institution_id"}


def read_rows(resource: str) -> list[dict[str, str]]:
    """Read the CSV file of one resource."""
    with open(EXAMPLE_DIR / "data" / f"{resource}.csv", newline="") as f:
        return list(csv.DictReader(f))


def main() -> None:
    """Print the edges of every source, through both manifests."""
    for path in MANIFESTS:
        manifest = GraphManifest.from_config(FileHandle.load(EXAMPLE_DIR / path))
        manifest.finish_init()
        caster = DocumentCaster(manifest.require_ingestion_model())

        print(path)
        employs = 0
        for resource in RESOURCES:
            result = asyncio.run(
                caster.cast_batch(
                    read_rows(resource), resource, params=IngestionParams()
                )
            )
            for (source, target, relation), rows in result.graph.edges.items():
                for start, end, properties in rows:
                    since = (
                        f"  since {properties['since']}"
                        if "since" in properties
                        else ""
                    )
                    print(
                        f"  {resource:<9} {start[KEYS[source]]} -[{relation}]-> "
                        f"{end[KEYS[target]]}{since}"
                    )
                    employs += relation == "employs"
        print(f"  stored employs edges: {employs}")


if __name__ == "__main__":
    main()
