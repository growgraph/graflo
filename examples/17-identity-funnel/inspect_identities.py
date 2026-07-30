"""
Show which funnel branch keyed each row, without touching the backend.

    cd examples/17-identity-funnel
    uv run python inspect_identities.py
"""

from __future__ import annotations

import csv

import click
from _common import EXAMPLE_DIR, MANIFEST_PATH
from suthing import FileHandle

from graflo import GraphManifest
from graflo.architecture.schema.identity_digest import compute_funnel_identity


def _winning_branch(doc: dict, funnel) -> str | None:
    """Which branch fires for *doc* — the funnel returns only the digest."""
    for branch in funnel.branches:
        values = [doc.get(field) for field in branch.required_fields]
        if all(value not in (None, "") for value in values):
            return branch.id
    return None


@click.command()
def main() -> None:
    """Print the branch and synthetic id each source row resolves to."""
    manifest = GraphManifest.from_config(FileHandle.load(MANIFEST_PATH))
    manifest.finish_init()
    vertex_config = manifest.graph_schema.core_schema.vertex_config
    funnel = vertex_config._get_vertex_by_name("party").identity_funnel

    click.echo(f"{'source':<9} {'branch':<7} {'id':<18} evidence")
    for source in ("crm", "billing"):
        path = EXAMPLE_DIR / "data" / f"{source}.csv"
        with path.open(encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                branch = _winning_branch(row, funnel)
                identity = compute_funnel_identity(row, funnel)
                evidence = ", ".join(
                    f"{k}={v}" for k, v in row.items() if v not in (None, "")
                )
                click.echo(
                    f"{source:<9} {branch or '-':<7} "
                    f"{(identity or '-')[:16]:<18} {evidence}"
                )

    click.echo(
        "\nNote: Alan Turing carries an email in both sources, so both rows take "
        "the 'email' branch and upsert onto the same vertex. The other two people "
        "key by email in the CRM and by phone+country in billing — different "
        "branches, hence different vertices. Resolving that is cross-resource "
        "identity discovery, not the funnel's job."
    )


if __name__ == "__main__":
    main()
