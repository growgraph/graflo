"""
Cast both sources through the union manifest and show which records fuse.

    cd examples/19-union-canonical-equivalence
    uv run python inspect_fusion.py

Expected: the ``abc_``-gated row of source A and the row of source B digest to
the same synthetic ``id`` (one vertex after upsert); the non-``abc_`` row keeps a
side-local identity even though it carries the same raw shared value.

Then the agreements: ``r_agreements`` only references a ``Firm`` by
``firm_id``, which is no longer the primary key. Its ``signedBy`` edge selects
the demoted ``by_company_id`` secondary identity instead, so each agreement
still lands on the company it names.
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

    # The reference-only resource: resolve each edge the way the writer does,
    # by the secondary identity its step now selects.
    (agreements,) = (
        r for r in union.require_ingestion_model().resources if r.name == "r_agreements"
    )
    (selector,) = {
        step["target_match"] for step in agreements.pipeline if "target_match" in step
    }
    by_company_id = {
        doc["company_id"]: doc["id"] for _, doc in emitted if doc.get("company_id")
    }
    result = asyncio.run(
        caster.cast_batch(
            _rows("agreements.csv"), "r_agreements", params=IngestionParams()
        )
    )
    click.echo(f"\nr_agreements signedBy → Company, matched on {selector}:")
    for (source, target, _), rows in result.graph.edges.items():
        for contract, company, _weight in rows:
            resolved = by_company_id.get(company["company_id"], "unresolved")
            click.echo(
                f"  {source} {contract['agreement_id']} → {target} "
                f"company_id={company['company_id']} → id {resolved}"
            )


if __name__ == "__main__":
    main()
