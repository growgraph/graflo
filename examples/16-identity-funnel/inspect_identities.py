"""Show which identifier keys each customer record, and the vertex id it gets.

Runs every row of ``data/crm.csv`` and ``data/billing.csv`` through
``manifest.yaml`` in memory, as ingestion does, and prints the funnel branch
that fired and the id the record received. No database or file is written.
Run it from this directory:

    uv run python inspect_identities.py
"""

import asyncio
import csv

from suthing import FileHandle

from graflo import GraphManifest
from graflo.hq.caster import IngestionParams
from graflo.hq.document_caster import DocumentCaster

ID_WIDTH = 12

manifest = GraphManifest.from_config(FileHandle.load("manifest.yaml"))
manifest.finish_init()
vertices = manifest.require_schema().core_schema.vertex_config.vertices
funnel = next(v for v in vertices if v.name == "party").identity_funnel
assert funnel is not None
caster = DocumentCaster(manifest.require_ingestion_model())


def winning_branch(row: dict[str, str]) -> str:
    """The first branch whose required fields are all filled in, or "-"."""
    for branch in funnel.branches:
        if all(row.get(field) for field in branch.required_fields):
            return branch.id
    return "-"


def cast_row(resource: str, row: dict[str, str]) -> dict | None:
    """Cast one row as ingestion does; None when the record is dropped."""
    result = asyncio.run(caster.cast_batch([row], resource, params=IngestionParams()))
    records = result.graph.vertices.get("party", [])
    return records[0] if records else None


rows = records = 0
ids: set[str] = set()
print(f"{'source':<9}{'name':<17}{'branch':<8}vertex id")
for resource in ("crm", "billing"):
    with open(f"data/{resource}.csv", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows += 1
            record = cast_row(resource, row)
            if record is None:
                vertex_id = "none, dropped"
            else:
                records += 1
                ids.add(record["id"])
                vertex_id = record["id"][:ID_WIDTH]
            print(f"{resource:<9}{row['name']:<17}{winning_branch(row):<8}{vertex_id}")

print(f"{rows} rows -> {records} records -> {len(ids)} distinct ids")
