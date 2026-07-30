"""
Propose a shared vertex identity for two customer sources that name their
columns differently.

No live graph database and no LLM — this is deterministic inference over sampled
documents:

    cd examples/18-cross-resource-identity
    uv run python discover.py
    uv run python discover.py --apply          # patch a vertex and print the YAML
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

import click

from graflo.architecture.onto_sample import ResourceSample, SourceSample
from graflo.architecture.schema.vertex import Vertex
from graflo.db.cross_resource_identity import (
    CrossResourceIdentityConfig,
    apply_proposal_to_vertex,
    infer_from_source_sample,
)

DATA_DIR = Path(__file__).resolve().parent / "data"


def _load(name: str) -> list[dict]:
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
            ResourceSample(resource_name="crm", docs=_load("crm_customers.csv")),
            ResourceSample(resource_name="billing", docs=_load("billing_accounts.csv")),
        ],
    )

    proposal = infer_from_source_sample(
        source,
        vertex_name="party",
        # The fixtures carry 150 rows per resource; the shipped default of 100
        # is a production floor, not a demo one.
        config=CrossResourceIdentityConfig(min_sample_size=50),
    )

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
        click.echo(patched.to_yaml_str())

    click.echo(
        "\nThis is a proposal, not a decision. Nothing was written; review the "
        "alignments and evidence before accepting it into a manifest."
    )


if __name__ == "__main__":
    main()
