"""Show which records of the two systems become the same machine.

Reads the combined manifest, runs the three CSV files through it and prints
the vertex each record lands on. No database is involved.

    cd examples/20-manifest-union
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
#: Resource name, its file, and the column holding the record's own key.
MACHINE_SOURCES = [
    ("assets", "assets.csv", "asset_id"),
    ("devices", "devices.csv", "device_id"),
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
    """Print the machines, the count, and where each work order lands."""
    manifest = GraphManifest.from_config(
        FileHandle.load(EXAMPLE_DIR / "artifacts" / "manifest_union.yaml")
    )
    manifest.finish_init()
    caster = DocumentCaster(manifest.require_ingestion_model())

    machines: list[dict[str, Any]] = []
    print(f"{'resource':<10}{'own key':<9}{'serial':<9}vertex id")
    for resource, filename, key in MACHINE_SOURCES:
        for doc in cast(caster, resource, filename).vertices.get("Machine", []):
            machines.append(doc)
            serial = doc.get("serial_number") or "-"
            vertex_id = doc["id"][:ID_WIDTH]
            print(f"{resource:<10}{doc[key]:<9}{serial:<9}{vertex_id}")

    ids = {doc["id"] for doc in machines}
    print(f"{len(machines)} records -> {len(ids)} vertices")

    # A work order carries only an asset_id. The combined manifest matches it
    # against the asset_id that each machine keeps as a lookup key.
    by_asset_id = {doc["asset_id"]: doc for doc in machines if doc.get("asset_id")}
    edges = cast(caster, "work_orders", "work_orders.csv").edges
    for rows in edges.values():
        for work_order, target, _ in rows:
            machine = by_asset_id[target["asset_id"]]
            print(
                f"{work_order['work_order_id']} -> {machine['name']} "
                f"(vertex {machine['id'][:ID_WIDTH]})"
            )


if __name__ == "__main__":
    main()
