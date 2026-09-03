"""
Cast both sources through the union manifest and show which records fuse.

    cd examples/19-union-canonical-equivalence
    uv run python inspect_fusion.py

Expected: the ``abc_``-gated row of source A and the row of source B digest to
the same synthetic ``id`` (one vertex after upsert); the non-``abc_`` row keeps a
side-local identity even though it carries the same raw shared value.
"""

from __future__ import annotations

import asyncio
import csv
from pathlib import Path

import click
from build_union import build_union

from graflo.hq.document_caster import DocumentCaster
from graflo.hq.ingestion_parameters import IngestionParams

EXAMPLE_DIR = Path(__file__).resolve().parent


def _rows(name: str) -> list[dict]:
    with open(EXAMPLE_DIR / "data" / name, newline="") as f:
        return list(csv.DictReader(f))


@click.command()
def main() -> None:
    union = build_union()
    caster = DocumentCaster(union.require_ingestion_model())

    emitted: list[tuple[str, dict]] = []
    for resource, filename in (
        ("r_a", "a.csv"),
        ("r_shop", "shop.csv"),
        ("r_b", "b.csv"),
        ("r_branch", "branch.csv"),
    ):
        result = asyncio.run(
            caster.cast_batch(_rows(filename), resource, params=IngestionParams())
        )
        emitted.extend(
            (resource, doc) for doc in result.graph.vertices.get("Company", [])
        )

    click.echo(f"{'resource':<10}{'local_key':<14}{'gate':<8}{'match_key':<12}id")
    for resource, doc in emitted:
        local_key = doc.get("local_key") or "-"
        gate = doc.get("secondary_key", "-")
        match_key = doc.get("match_key") or "-"
        click.echo(f"{resource:<10}{local_key:<14}{gate:<8}{match_key:<12}{doc['id']}")

    ids = [doc["id"] for _, doc in emitted]
    fused = len(ids) - len(set(ids))
    click.echo(f"\n{len(ids)} records → {len(set(ids))} vertices ({fused} fused)")


if __name__ == "__main__":
    main()
