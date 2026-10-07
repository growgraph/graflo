"""Show where each register row and each device lands in the combined manifest.

Reads the combined manifest, runs the two CSV files through it and prints, for
every record, the vertex type it becomes, the matching key derived for it, and
the vertex id. No database is involved.

    cd examples/21-router-union-alignment
    uv run python inspect_fusion.py
"""

from __future__ import annotations

import asyncio
import csv
from pathlib import Path
from typing import Any

from suthing import FileHandle

from graflo import GraphManifest
from graflo.hq.document_caster import DocumentCaster
from graflo.hq.ingestion_parameters import IngestionParams

EXAMPLE_DIR = Path(__file__).resolve().parent
#: Resource name, its file, and the property holding the record's own key per
#: vertex type. The combined manifest names a machine's own key by its origin; a
#: production line keeps its own `asset_id`.
SOURCES = [
    (
        "register",
        "register.csv",
        {"Machine": "maintenance__asset_id", "ProductionLine": "asset_id"},
    ),
    ("devices", "devices.csv", {"Machine": "sensors__device_id"}),
]
ID_WIDTH = 12


def read_rows(name: str) -> list[dict[str, str]]:
    """Read one CSV file of the example as a list of rows."""
    with open(EXAMPLE_DIR / "data" / name, newline="") as f:
        return list(csv.DictReader(f))


def cast(caster: DocumentCaster, resource: str, filename: str) -> Any:
    """Run one file through one resource and return the resulting graph."""
    rows = read_rows(filename)
    result = asyncio.run(caster.cast_batch(rows, resource, params=IngestionParams()))
    return result.graph


def main() -> None:
    """Print every vertex produced, then the machine count."""
    manifest = GraphManifest.from_config(
        FileHandle.load(EXAMPLE_DIR / "artifacts" / "manifest_union.yaml")
    )
    manifest.finish_init()
    caster = DocumentCaster(manifest.require_ingestion_model())

    machine_ids: list[str] = []
    print(
        f"{'resource':<10}{'own key':<9}{'vertex type':<16}{'matched on':<16}vertex id"
    )
    for resource, filename, keys in SOURCES:
        for vertex_type, docs in cast(caster, resource, filename).vertices.items():
            key = keys[vertex_type]
            for doc in docs:
                # The derived key a machine is matched on. A production line has
                # none: it keeps its own identity, asset_id.
                matched_on = doc.get("match_key") or doc.get("local_key") or "-"
                # A machine is keyed by a digest; a production line by its asset_id.
                vertex_id = doc.get("id", doc[key])[:ID_WIDTH]
                print(
                    f"{resource:<10}{doc[key]:<9}{vertex_type:<16}{matched_on:<16}"
                    f"{vertex_id}"
                )
                if vertex_type == "Machine":
                    machine_ids.append(vertex_id)

    print(f"{len(machine_ids)} machine records -> {len(set(machine_ids))} vertices")


if __name__ == "__main__":
    main()
