"""``graflo inverses`` -- see, realize, repair, switch and withdraw declared edge inverses.

A thin shell over the audit
(:func:`~graflo.architecture.profile.inverses.audit_inverses`) and the planners
(:mod:`graflo.architecture.evolution.inverse_plan`). Every verb that changes
anything follows ``graflo lift``: it plans primitive ops, shows them, and --
unless asked only to show them -- applies them and writes the result.
``--emit-ops`` writes the plan as YAML, which is the reviewable artifact: a plan
is a list of ordinary evolution ops, so it can be read, diffed, checked in and
replayed without this command.

A manifest that does not load can still be audited and repaired. That is the
case the command exists for: a manifest assembled from several sources is
exactly where a declaration and the edges it names stop agreeing.
"""

from __future__ import annotations

import json
from pathlib import Path

import click

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution.apply import apply_evolution
from graflo.architecture.evolution.codec import ops_to_yaml_str
from graflo.architecture.evolution.inverse_plan import (
    InversePlan,
    plan_realize_inverses,
    plan_repair_inverses,
    plan_switch_realization,
    plan_withdraw_realization,
)
from graflo.architecture.profile.inverses import audit_inverses, manifest_for_audit
from graflo.cli.io import DECLARED_KEYS, dump_manifest, load_mapping

#: Conflicts remain -- findings only the author can settle.
#: Distinct from 2 (bad invocation, unreadable or malformed file).
EXIT_CONFLICTS = 1


class _SetupError(click.ClickException):
    """The command could not be attempted at all. Exits 2, never 1."""

    exit_code = 2


def _load(path: Path) -> GraphManifest:
    """The manifest at *path*, even if its blocks no longer agree with each other."""
    try:
        return manifest_for_audit(load_mapping(path))
    except (ValueError, TypeError) as exc:
        raise _SetupError(
            f"{path}: not a valid manifest -- {type(exc).__name__}: {exc}"
        ) from exc


_MANIFEST = click.argument(
    "manifest", type=click.Path(exists=True, dir_okay=False, path_type=Path)
)
_RELATIONS = click.option(
    "-r",
    "--relation",
    "relations",
    multiple=True,
    help="Restrict to the pair this relation belongs to (either side). Repeatable.",
)


def _writes(command):
    """The options every verb that writes a manifest shares, in ``lift``'s spelling."""
    for option in (
        click.option(
            "--dry-run", is_flag=True, help="Plan and report, but write nothing."
        ),
        click.option(
            "--emit-ops",
            "ops_path",
            type=click.Path(dir_okay=False, path_type=Path),
            default=None,
            help="Write the planned ops as YAML. The reviewable artifact.",
        ),
        click.option(
            "-o",
            "--output",
            type=click.Path(dir_okay=False, path_type=Path),
            default=None,
            help="Where to write the changed manifest. Omitted prints the plan only.",
        ),
    ):
        command = option(command)
    return command


def _carry_out(
    source: GraphManifest,
    plan: InversePlan,
    *,
    output: Path | None,
    ops_path: Path | None,
    dry_run: bool,
) -> None:
    """Show *plan*, then write its ops and the manifest it produces unless told not to."""
    for line in plan.to_lines():
        click.echo(line)

    if plan.ops and ops_path is not None and not dry_run:
        ops_path.parent.mkdir(parents=True, exist_ok=True)
        ops_path.write_text(ops_to_yaml_str(plan.ops), encoding="utf-8")
        click.echo(f"written: {ops_path}")

    if dry_run:
        click.echo("dry run: nothing written")
    elif plan.ops and output is not None:
        try:
            changed = apply_evolution(source, plan.ops)
        except ValueError as exc:
            click.echo(f"refused on apply: {type(exc).__name__}: {exc}", err=True)
            raise SystemExit(EXIT_CONFLICTS) from None
        dump_manifest(changed, output, declare=DECLARED_KEYS)

    if plan.after.conflicts():
        raise SystemExit(EXIT_CONFLICTS)


@click.group("inverses")
def inverses() -> None:
    """Declared edge inverses: how they are realized, and what to do about it.

    Declaring a pair in `edge_config.inverses` stores nothing, and is usually
    enough: the inverse name reads the forward edge backwards, which is free on
    most backends. Storing the inverse is optional, one way per pair. *native*:
    the database maintains the reverse type (TigerGraph). *materialized*: the
    inverse is a declared edge, written from the same rows by edge steps that
    set `emit_inverse`.
    """


@inverses.command("audit")
@_MANIFEST
@click.option("--json", "as_json", is_flag=True, help="Print the report as JSON.")
@click.option(
    "--exit-zero",
    is_flag=True,
    help="Exit 0 even when conflicts are found (the report is still printed).",
)
def audit(manifest: Path, as_json: bool, exit_zero: bool) -> None:
    """Report how MANIFEST realizes each declared inverse, and what is wrong.

    Findings are *repairable* (one place omits what another states; `repair`
    propagates it), *conflict* (two places disagree; only you can settle it) or
    *note*. Exits 1 when conflicts are found.
    """
    report = audit_inverses(_load(manifest))
    if as_json:
        click.echo(json.dumps(report.model_dump(mode="json"), indent=2))
    else:
        for line in report.to_lines():
            click.echo(line)
    if report.conflicts() and not exit_zero:
        raise SystemExit(EXIT_CONFLICTS)


@inverses.command("realize")
@_MANIFEST
@click.option(
    "--strategy",
    type=click.Choice(["auto", "native", "materialized"]),
    default="auto",
    show_default=True,
    help=(
        "native: the database maintains each eligible pair. materialized: store "
        "the inverse as declared edges fed by the same rows. auto: decide from "
        "what a reverse read costs on the target backend -- usually nothing, in "
        "which case nothing is stored and the pair stays declared."
    ),
)
@_RELATIONS
@_writes
def realize(
    manifest: Path,
    strategy: str,
    relations: tuple[str, ...],
    output: Path | None,
    ops_path: Path | None,
    dry_run: bool,
) -> None:
    """Realize declared pairs of MANIFEST; every pair left alone comes with a reason."""
    source = _load(manifest)
    plan = plan_realize_inverses(
        source,
        strategy=strategy,  # ty: ignore[invalid-argument-type]
        relations=list(relations) or None,
    )
    _carry_out(source, plan, output=output, ops_path=ops_path, dry_run=dry_run)


@inverses.command("repair")
@_MANIFEST
@_writes
def repair(
    manifest: Path, output: Path | None, ops_path: Path | None, dry_run: bool
) -> None:
    """Propagate what MANIFEST under-reports; contradictions are listed, never touched.

    Typical after merging sources: the inverse edge exists but one resource
    that writes the forward relation feeds nothing into it; one mirror declares
    a property the other lacks; a relation is undirected everywhere but not
    declared symmetric. Each repair is kept only if it removes its finding and
    introduces no other.
    """
    source = _load(manifest)
    plan = plan_repair_inverses(source)
    _carry_out(source, plan, output=output, ops_path=ops_path, dry_run=dry_run)


@inverses.command("switch")
@_MANIFEST
@click.option(
    "--to",
    "to",
    type=click.Choice(["native", "materialized"]),
    required=True,
    help="The realization to move the pairs to.",
)
@click.option(
    "-r",
    "--relation",
    "relations",
    multiple=True,
    required=True,
    help=(
        "The relation that stays stored; its inverse is what changes "
        "realization. Repeatable."
    ),
)
@_writes
def switch(
    manifest: Path,
    to: str,
    relations: tuple[str, ...],
    output: Path | None,
    ops_path: Path | None,
    dry_run: bool,
) -> None:
    """Move pairs of MANIFEST from one realization to another: withdraw, then add.

    A pair is realized one way. Eligibility for the new realization is checked
    before anything is planned, so a pair is never left withdrawn and unrealized.
    """
    source = _load(manifest)
    plan = plan_switch_realization(
        source,
        list(relations),
        to=to,  # ty: ignore[invalid-argument-type]
    )
    _carry_out(source, plan, output=output, ops_path=ops_path, dry_run=dry_run)


@inverses.command("withdraw")
@_MANIFEST
@click.option(
    "-r",
    "--relation",
    "relations",
    multiple=True,
    required=True,
    help="The relation that stays stored; its inverse stops being stored. Repeatable.",
)
@_writes
def withdraw(
    manifest: Path,
    relations: tuple[str, ...],
    output: Path | None,
    ops_path: Path | None,
    dry_run: bool,
) -> None:
    """Stop storing the inverse of pairs of MANIFEST; the declaration stays.

    Undoes a realization: a native inverse is withdrawn from the profile, and
    the declared edges of a materialized inverse are removed together with the
    `emit_inverse` flags that fed them. The inverse name keeps resolving on
    reads wherever the backend can follow an edge from its target.
    """
    source = _load(manifest)
    plan = plan_withdraw_realization(source, list(relations))
    _carry_out(source, plan, output=output, ops_path=ops_path, dry_run=dry_run)


__all__ = ["inverses"]
