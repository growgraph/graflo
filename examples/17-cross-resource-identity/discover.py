"""Two systems describe the same customers. How do I find the columns that match them?

Reads the two generated CSV exports in ``data/``, compares their columns by
name and by values, and prints the identity GraFlo proposes for a ``party``
vertex that both describe, with the evidence behind it. With ``--apply`` it
also prints a ``party`` vertex patched with the proposal. Nothing is written.
Run it from this directory:

    uv run python discover.py
    uv run python discover.py --apply
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

import click
import yaml

from graflo.architecture.onto_sample import ResourceSample, SourceSample
from graflo.architecture.schema.vertex import Vertex
from graflo.db.cross_resource_identity import (
    apply_proposal_to_vertex,
    infer_from_source_sample,
)

DATA_DIR = Path(__file__).resolve().parent / "data"


def read_rows(name: str) -> list[dict]:
    """Read one CSV file of the example as a list of rows."""
    with (DATA_DIR / name).open(encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


@click.command()
@click.option(
    "--apply",
    "apply_",
    is_flag=True,
    help="Patch a party vertex with the proposal and print the resulting YAML.",
)
def main(apply_: bool) -> None:
    """Sample two CSV resources and propose one identity policy for both."""
    source = SourceSample(
        source_name="customer-stack",
        description="CRM and billing exports describing the same customers",
        samples=[
            ResourceSample(resource_name="crm", docs=read_rows("crm_customers.csv")),
            ResourceSample(
                resource_name="billing", docs=read_rows("billing_accounts.csv")
            ),
        ],
    )

    proposal = infer_from_source_sample(source, vertex_name="party")

    click.echo(f"strategy    : {proposal.strategy}")
    click.echo(f"identity    : {proposal.identity}")
    click.echo(f"confidence  : {proposal.confidence:.2f}")
    if proposal.warning:
        click.echo(f"warning     : {proposal.warning}")

    click.echo("\nColumn alignments (how the resources were matched up):")
    click.echo(
        f"  {'left':<28} {'right':<28} {'name':>6} {'values':>7} {'declared':>9}"
    )
    for alignment in proposal.alignments:
        left = f"{alignment.left_resource}.{alignment.left_field}"
        right = f"{alignment.right_resource}.{alignment.right_field}"
        click.echo(
            f"  {left:<28} {right:<28} "
            f"{alignment.name_score:>6.2f} {alignment.value_jaccard:>7.2f} "
            f"{alignment.declared!s:>9}"
        )

    click.echo("\nPer-resource field maps (source -> canonical):")
    for resource, mapping in sorted(proposal.resource_field_maps.items()):
        click.echo(f"  {resource}: {mapping}")

    click.echo("\nSuggested pipeline steps:")
    for step in proposal.suggested_transforms:
        click.echo(f"  {json.dumps(step)}")

    click.echo("\nEvidence:")
    for key, value in sorted(proposal.evidence.items()):
        click.echo(f"  {key}: {value}")

    if apply_:
        vertex = Vertex(name="party", properties=["full_name", "invoice_total"])
        patched = apply_proposal_to_vertex(vertex, proposal)
        click.echo("\nPatched vertex:\n")
        click.echo(yaml.safe_dump(patched.to_minimal_canonical_dict(), sort_keys=False))

    click.echo(
        "\nThis is a proposal, not a decision. Nothing was written; review the "
        "alignments and evidence before accepting it into a manifest."
    )


if __name__ == "__main__":
    main()
