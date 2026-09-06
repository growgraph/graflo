"""
Cast the routed view and the plain B-side sources through the union, and show
which records fuse.

    cd examples/21-router-union-alignment
    uv run python inspect_fusion.py

Expected: the ``ABC-``-marked ``firm`` row and B's ``org`` row digest to one
synthetic ``id``; the ``shop`` row and B's ``branch`` row likewise; the ``firm``
row whose ``firm_ref`` is ``Alpha`` — the same business name, without the marker
— derives no ``match_key`` and keeps a side-local identity. It is not dropped:
falling through is how a record stays ingested outside the cluster. The
``person`` row still flows through the SAME router and is emitted as its own
class, carrying none of the canonical attributes.
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

    # The view is one nested document: the router lives under `descend: records`.
    payloads = {
        "r_view": [{"records": _rows("view.csv")}],
        "r_b": _rows("b.csv"),
        "r_branch": _rows("branch.csv"),
    }

    emitted: list[tuple[str, dict]] = []
    others: list[tuple[str, str, dict]] = []
    for resource, docs in payloads.items():
        result = asyncio.run(
            caster.cast_batch(docs, resource, params=IngestionParams())
        )
        emitted.extend(
            (resource, doc) for doc in result.graph.vertices.get("Company", [])
        )
        for name, docs_out in result.graph.vertices.items():
            if name != "Company":
                others.extend((resource, name, doc) for doc in docs_out)

    click.echo(
        f"{'resource':<10}{'local_key':<14}{'secondary_key':<15}{'match_key':<12}id"
    )
    for resource, doc in emitted:
        click.echo(
            f"{resource:<10}{doc.get('local_key') or '-':<14}"
            f"{doc.get('secondary_key', '-'):<15}{doc.get('match_key') or '-':<12}"
            f"{doc['id']}"
        )

    ids = [doc["id"] for _, doc in emitted]
    click.echo(
        f"\n{len(ids)} records → {len(set(ids))} vertices "
        f"({len(ids) - len(set(ids))} fused)"
    )

    click.echo("\nSame router, classes outside the cluster:")
    for resource, name, doc in others:
        canonical = {k: doc.get(k) for k in ("match_key", "local_key") if k in doc}
        click.echo(f"  {resource}: {name} {doc} canonical={canonical or 'none'}")


if __name__ == "__main__":
    main()
