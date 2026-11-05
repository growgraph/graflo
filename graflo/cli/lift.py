"""``graflo lift`` -- lift a manifest with one or more aspects, via Operations.

The verb is a thin shell over
:func:`~graflo.architecture.evolution.aspects.plan_lift`. It reads a manifest
and, for the aspects that need one, a spec; plans the ops; and -- unless asked
only to show them -- applies them and writes the result.

``--emit-ops`` is the point of doing this through Operations at all: the plan is
a document you can read, diff and check in before anything is applied.

``-m LABEL`` records the lift in a commit store as a ``lift`` commit carrying
its recipe (the aspects, their versions and the spec), and stamps the written
manifest with it. The input manifest must already be a tree in that store.

The written manifest states its declarations explicitly (see
``graflo.cli.io.DECLARED_KEYS``). Without that, ``directed: true`` would be
dropped as a default and the very declaration the lift just made would be
invisible to ``graflo check``.
"""

from __future__ import annotations

from pathlib import Path

import click

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution.apply import apply_evolution
from graflo.architecture.evolution.aspects import (
    ASPECTS,
    DEFAULT_ASPECTS,
    LiftError,
    LiftSpec,
    plan_lift,
    resolve_aspects,
)
from graflo.architecture.evolution.codec import ops_to_yaml_str
from graflo.architecture.evolution.commit import Commit
from graflo.architecture.evolution.hashing import manifest_hash
from graflo.architecture.evolution.ops import ManifestOp
from graflo.architecture.profile import check_manifest_config
from graflo.cli._store import append_entry, store_option
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
    "--aspect",
    "aspects",
    multiple=True,
    type=click.Choice(sorted(ASPECTS)),
    default=DEFAULT_ASPECTS,
    show_default=True,
    help=(
        "Which aspects to lift; repeatable. `provenance` adds SourceRecord and "
        "Agent with the PROV-O edges and needs no spec; `state` adds <Type>State "
        "and <Type>Observation and needs --spec."
    ),
)
@click.option(
    "--spec",
    "spec_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help=(
        "LiftSpec document: what the types mean. Required by the `state` aspect "
        "-- a lift can see structure but not meaning, so grounding, units and "
        "which properties change over time have to be declared."
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
@store_option
@click.option(
    "-m",
    "--label",
    "record_label",
    default=None,
    help=(
        "Record the lift in the store as a `lift` commit under this label. The "
        "manifest must already be in the history; without this the verb writes "
        "only the manifest."
    ),
)
def lift(
    manifest: Path,
    spec_path: Path | None,
    output: Path | None,
    ops_path: Path | None,
    dry_run: bool,
    do_check: bool,
    profile_name: str,
    store: Path,
    record_label: str | None,
    aspects: tuple[str, ...] = DEFAULT_ASPECTS,
) -> None:
    """Lift MANIFEST with the named aspects.

    `provenance` makes lineage expressible: `SourceRecord` and `Agent`, with
    `wasDerivedFrom` and `wasAttributedTo`. `state` makes change and
    measurement expressible: mutable facts move onto their own `<Type>State`
    with a validity interval, and measured types gain a `<Type>Observation`
    with a unit per row. Both are lifted by default.

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
        chosen = resolve_aspects(aspects)
    except LiftError as exc:
        raise _LiftSetupError(str(exc)) from exc
    needing_spec = [aspect.name for aspect in chosen if aspect.needs_spec]
    if spec_path is None and needing_spec:
        raise _LiftSetupError(
            f"--spec is required by the {', '.join(needing_spec)} aspect"
        )

    spec = LiftSpec()
    if spec_path is not None:
        try:
            spec = LiftSpec.model_validate(load_mapping(spec_path))
        except ValueError as exc:
            raise _LiftSetupError(f"{spec_path}: invalid lift spec -- {exc}") from exc

    try:
        ops = plan_lift(source, spec, aspects=aspects, authored=authored)
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

    entry: Commit | None = None
    if record_label is not None:
        # A lift of a manifest that already carries the aspects may still plan
        # ops (declarations it restates); what decides is whether it changed.
        if manifest_hash(lifted) != manifest_hash(source):
            # Recorded before the manifest is written, so the file on disk
            # carries its own lineage.
            entry = _record(
                source, lifted, ops, spec, [a.name for a in chosen], store, record_label
            )
        else:
            click.echo(
                f"nothing to record: {manifest} is already lifted with "
                f"{', '.join(a.name for a in chosen)}"
            )
    dump_manifest(lifted, output, declare=DECLARED_KEYS)
    if entry is not None:
        click.echo(f"commit: {entry.id}")
        append_entry(store, entry)


def _record(
    source: GraphManifest,
    lifted: GraphManifest,
    ops: list[ManifestOp],
    spec: LiftSpec,
    aspects: list[str],
    store: Path,
    label: str,
) -> Commit:
    """Build the lift commit, stamping *lifted* with its own lineage.

    The input is resolved to a commit by content address, as ``graflo merge``
    resolves its sides: a manifest that is in no history cannot be named as a
    parent.
    """
    from datetime import UTC, datetime

    from graflo.architecture.contract.provenance import stamp_provenance
    from graflo.architecture.evolution.aspects.recipe import build_lift_recipe
    from graflo.architecture.evolution.canonicalize import CANON_VERSION
    from graflo.architecture.evolution.commit import CommitError
    from graflo.architecture.evolution.history import FileCommitStore
    from graflo.architecture.evolution.lift_commit import build_lift_commit
    from graflo.architecture.evolution.merge_commit import find_commit_by_tree

    parent = find_commit_by_tree(FileCommitStore(store).load(), source)
    if parent is None:
        raise _LiftSetupError(
            f"the manifest is not in {store}, so it cannot be named as a parent. "
            "Record that lineage first, or drop -m to write the lifted manifest "
            "without recording it."
        )

    recipe = build_lift_recipe(source, spec, aspects=aspects)
    try:
        entry = build_lift_commit(
            source,
            ops,
            parent=parent.id,
            recipe=recipe,
            label=label,
            created_at=datetime.now(UTC).isoformat(),
        )
    except CommitError as exc:
        raise _LiftSetupError(str(exc)) from exc

    # Stamped after the commit is built, so the artifact can name it; the
    # stamp is outside the content hash, so it cannot move the recorded tree.
    stamp_provenance(
        lifted,
        content_hash=manifest_hash(lifted),
        canon=CANON_VERSION,
        commit=entry.id,
        parents=[parent.id],
        recipe=recipe.content_hash(),
    )
    return entry


__all__ = ["lift"]
