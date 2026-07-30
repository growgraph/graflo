"""Manifest revision-chain CLI.

Distinct from ``migrate_schema``: that plans and executes changes against a
*database*. This records and replays changes to the *manifest* — the contract
plane. Applying a revision chain to a live database is not supported yet.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

import click
from suthing import FileHandle

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution.autogenerate import (
    RenameHints,
    diff_manifests_verified,
)
from graflo.architecture.evolution.codec import ops_to_yaml_str
from graflo.architecture.evolution.hashing import manifest_hash
from graflo.architecture.evolution.revision import (
    FileRevisionStore,
    RevisionError,
    apply_revisions,
    build_revision,
    downgrade_to,
)

logger = logging.getLogger(__name__)

DEFAULT_STORE = Path(".graflo/revisions")


def _load(path: str | Path) -> GraphManifest:
    manifest = GraphManifest.from_config(FileHandle.load(path))
    manifest.finish_init()
    return manifest


def _hints(path: Path | None) -> RenameHints:
    if path is None:
        return RenameHints()
    return RenameHints.model_validate(FileHandle.load(path))


@click.group()
def revision() -> None:
    """Record and replay manifest change sets."""


@revision.command("new")
@click.option(
    "--from-manifest",
    "from_path",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Base manifest (the state before the change).",
)
@click.option(
    "--to-manifest",
    "to_path",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Target manifest (the state after the change).",
)
@click.option("--label", default=None, help="Short human-readable name.")
@click.option(
    "--hints",
    "hints_path",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="YAML RenameHints; a drop plus an add is otherwise not a rename.",
)
@click.option(
    "--store",
    type=click.Path(path_type=Path),
    default=DEFAULT_STORE,
    show_default=True,
)
@click.option("--dry", is_flag=True, default=False, help="Print without storing.")
def new_cmd(
    from_path: Path,
    to_path: Path,
    label: str | None,
    hints_path: Path | None,
    store: Path,
    dry: bool,
) -> None:
    """Derive a change set between two manifests and record it."""
    base, target = _load(from_path), _load(to_path)
    ops, warnings = diff_manifests_verified(base, target, hints=_hints(hints_path))

    for warning in warnings:
        click.echo(f"warning: {warning}", err=True)
    if not ops:
        click.echo("No operations derived; nothing to record.")
        return

    chain = FileRevisionStore(store).load()
    head = chain.head()
    try:
        entry = build_revision(
            base,
            ops,
            down_revision=head.revision if head else None,
            label=label,
            created_at=datetime.now(UTC).isoformat(),
        )
    except RevisionError as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(f"revision  : {entry.revision}")
    click.echo(f"parent    : {entry.down_revision or '-'}")
    click.echo(f"label     : {entry.label or '-'}")
    click.echo(f"reversible: {entry.reversible}")
    click.echo(f"before    : {entry.manifest_hash_before[:12]}")
    click.echo(f"after     : {entry.manifest_hash_after[:12]}")
    click.echo("\noperations:")
    click.echo(ops_to_yaml_str(list(entry.ops)))

    if warnings:
        click.echo(
            "Refusing to store: the derived change set does not fully reproduce "
            "the target manifest (see warnings above).",
            err=True,
        )
        raise click.ClickException("incomplete change set")
    if dry:
        click.echo("(dry run — not stored)")
        return

    path = FileRevisionStore(store).append(entry)
    click.echo(f"stored: {path}")


@revision.command("apply")
@click.option(
    "--base",
    "base_path",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Manifest the chain starts from.",
)
@click.option("--upto", default=None, help="Stop after this revision id.")
@click.option(
    "--store", type=click.Path(path_type=Path), default=DEFAULT_STORE, show_default=True
)
@click.option(
    "--output-path",
    type=click.Path(path_type=Path),
    default=None,
    help="Write the replayed manifest here.",
)
def apply_cmd(
    base_path: Path, upto: str | None, store: Path, output_path: Path | None
) -> None:
    """Replay the stored chain onto a base manifest, verifying every hash."""
    base = _load(base_path)
    chain = FileRevisionStore(store).load()
    if not chain.revisions:
        click.echo("No revisions stored.")
        return

    try:
        result = apply_revisions(base, chain, upto=upto)
    except RevisionError as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(f"replayed {len(chain.path_to(upto))} revision(s)")
    click.echo(f"manifest hash: {manifest_hash(result)[:12]}")
    if output_path is not None:
        FileHandle.dump(result.to_dict(skip_defaults=True), output_path)
        click.echo(f"written: {output_path}")


@revision.command("history")
@click.option(
    "--store", type=click.Path(path_type=Path), default=DEFAULT_STORE, show_default=True
)
def history_cmd(store: Path) -> None:
    """List the stored chain, oldest first."""
    chain = FileRevisionStore(store).load()
    if not chain.revisions:
        click.echo("No revisions stored.")
        return

    click.echo(f"{'revision':<14} {'parent':<14} {'rev?':<5} {'ops':<4} label")
    for entry in chain.revisions:
        click.echo(
            f"{entry.revision:<14} {entry.down_revision or '-':<14} "
            f"{entry.reversible!s:<5} {len(entry.ops):<4} {entry.label or '-'}"
        )


@revision.command("verify")
@click.option(
    "--base",
    "base_path",
    type=click.Path(exists=True, path_type=Path),
    required=True,
)
@click.option(
    "--against",
    "against_path",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Manifest the fully replayed chain should equal.",
)
@click.option(
    "--store", type=click.Path(path_type=Path), default=DEFAULT_STORE, show_default=True
)
def verify_cmd(base_path: Path, against_path: Path | None, store: Path) -> None:
    """Check that the chain replays cleanly, and optionally matches a manifest."""
    base = _load(base_path)
    chain = FileRevisionStore(store).load()

    try:
        result = apply_revisions(base, chain)
    except RevisionError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"chain replays cleanly ({len(chain.revisions)} revision(s))")

    if against_path is not None:
        expected = manifest_hash(_load(against_path))
        actual = manifest_hash(result)
        if actual != expected:
            raise click.ClickException(
                f"replayed manifest hashes {actual[:12]}, expected {expected[:12]}"
            )
        click.echo(f"matches {against_path}")


@revision.command("downgrade")
@click.option(
    "--base",
    "base_path",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Base manifest — replaying from it is exact and always preferred.",
)
@click.option(
    "--current",
    "current_path",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Current manifest, inverted from when no base is available.",
)
@click.option(
    "--upto", default=None, help="Revision to return to; omit for the base state."
)
@click.option(
    "--store", type=click.Path(path_type=Path), default=DEFAULT_STORE, show_default=True
)
@click.option("--output-path", type=click.Path(path_type=Path), default=None)
def downgrade_cmd(
    base_path: Path | None,
    current_path: Path | None,
    upto: str | None,
    store: Path,
    output_path: Path | None,
) -> None:
    """Reconstruct the manifest as of an earlier revision."""
    if base_path is None and current_path is None:
        raise click.ClickException("give --base (preferred) or --current")

    chain = FileRevisionStore(store).load()
    try:
        result = downgrade_to(
            chain,
            upto,
            base=_load(base_path) if base_path else None,
            current=_load(current_path) if current_path else None,
        )
    except RevisionError as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(f"manifest hash: {manifest_hash(result)[:12]}")
    if output_path is not None:
        FileHandle.dump(result.to_dict(skip_defaults=True), output_path)
        click.echo(f"written: {output_path}")


if __name__ == "__main__":
    revision()
