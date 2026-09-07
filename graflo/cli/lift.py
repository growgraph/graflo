"""``graflo lift`` -- convert a manifest into a twin-ready schema, via Operations.

The verb is a thin shell over
:func:`~graflo.architecture.evolution.state_core.plan_lift`. It reads a manifest
and a spec, plans the ops, and -- unless asked only to show them -- applies them
and writes the result.

``--emit-ops`` is the point of doing this through Operations at all: the plan is
a document you can read, diff and check in before anything is applied.

The written manifest states its declarations explicitly (see
``graflo.cli.io.DECLARED_KEYS``). Without that, ``directed: true`` would be
dropped as a default and the very declaration the lift just made would be
invisible to ``graflo check``.
"""

from __future__ import annotations

from pathlib import Path

import click

from graflo.architecture.evolution.apply import apply_evolution
from graflo.architecture.evolution.codec import ops_to_yaml_str
from graflo.architecture.evolution.state_core import LiftError, LiftSpec, plan_lift
from graflo.architecture.profile import check_manifest_config
from graflo.cli.io import (
    DECLARED_KEYS,
    dump_manifest,
    load_manifest,
    load_mapping,
    manifest_to_dict,
)

#: The lift refused the manifest -- a missing declaration, a name collision.
#: Distinct from 2 (bad invocation, unreadable file).
EXIT_REFUSED = 1


class _LiftSetupError(click.ClickException):
    """The lift could not be attempted at all. Exits 2, never 1."""

    exit_code = 2


@click.command("lift")
@click.argument(
    "manifest", type=click.Path(exists=True, dir_okay=False, path_type=Path)
)
@click.option(
    "--spec",
    "spec_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help=(
        "LiftSpec document: what the types mean. Required -- a lift can see "
        "structure but not meaning, so grounding, units and which properties "
        "change over time have to be declared."
    ),
)
@click.option(
    "-o",
    "--output",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Where to write the lifted manifest. Omitted prints a summary only.",
)
@click.option(
    "--emit-ops",
    "ops_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Write the planned ops as YAML. The reviewable artifact.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Plan and report, but write nothing.",
)
@click.option(
    "--check/--no-check",
    "do_check",
    default=True,
    show_default=True,
    help="Check the lifted manifest against a conformance profile.",
)
@click.option(
    "--profile",
    "profile_name",
    default="world-model",
    show_default=True,
    help="Profile to check the result against.",
)
def lift(
    manifest: Path,
    spec_path: Path,
    output: Path | None,
    ops_path: Path | None,
    dry_run: bool,
    do_check: bool,
    profile_name: str,
) -> None:
    """Lift MANIFEST into a twin-ready schema.

    Adds temporal validity and provenance: mutable facts move onto their own
    `<Type>State` with a validity interval, measurements gain units, and
    `Evidence` / `Agent` make lineage expressible.

    This changes the *contract*, not the data flow. Nothing populates the new
    types: a manifest with an ingestion_model still needs its pipelines wired
    to them, which this verb deliberately does not attempt.
    """
    authored = load_mapping(manifest)
    try:
        source = load_manifest(manifest)
    except (ValueError, TypeError) as exc:
        raise _LiftSetupError(
            f"{manifest}: not a valid manifest -- {type(exc).__name__}: {exc}"
        ) from exc

    try:
        spec = LiftSpec.model_validate(load_mapping(spec_path))
    except ValueError as exc:
        raise _LiftSetupError(f"{spec_path}: invalid lift spec -- {exc}") from exc

    try:
        ops = plan_lift(source, spec, authored=authored)
    except LiftError as exc:
        # Every LiftError names the declaration that is missing; a traceback
        # would bury the one sentence the user needs.
        click.echo(f"lift refused: {exc}", err=True)
        raise SystemExit(EXIT_REFUSED) from None

    click.echo(f"planned {len(ops)} operation(s):")
    for op in ops:
        click.echo(f"  {op.op}")

    if ops_path is not None and not dry_run:
        ops_path.parent.mkdir(parents=True, exist_ok=True)
        ops_path.write_text(ops_to_yaml_str(ops), encoding="utf-8")
        click.echo(f"written: {ops_path}")

    try:
        lifted = apply_evolution(source, ops)
    except ValueError as exc:
        click.echo(f"lift refused on apply: {type(exc).__name__}: {exc}", err=True)
        raise SystemExit(EXIT_REFUSED) from None

    payload = manifest_to_dict(lifted, declare=DECLARED_KEYS)
    if do_check:
        report = check_manifest_config(
            payload, profile=profile_name, subject=str(output or manifest)
        )
        for line in report.to_lines():
            click.echo(line)

    if dry_run:
        click.echo("dry run: nothing written")
        return
    dump_manifest(lifted, output, declare=DECLARED_KEYS)


__all__ = ["lift"]
