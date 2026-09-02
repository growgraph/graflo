"""Git-shaped verbs over a manifest's commit history.

Distinct from ``migrate-schema``, which plans and executes changes against a
*database*. These record and replay changes to the *manifest* -- the contract
plane. Nothing here touches a backend.

The verbs are deliberately the ones a git user already knows: ``commit``,
``log``, ``verify``, ``checkout``, ``merge``, ``revert``. Where the semantics
differ from git they differ visibly -- ``checkout`` replays from a base manifest
rather than restoring a snapshot, because a manifest history stores change sets,
not trees.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

import click
from pydantic import ValidationError
from suthing import FileHandle

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.contract.provenance import stamp_provenance
from graflo.architecture.evolution.autogenerate import (
    RenameHints,
    diff_manifests_verified,
)
from graflo.architecture.evolution.canonicalize import CANON_VERSION
from graflo.architecture.evolution.codec import ops_to_yaml_str
from graflo.architecture.evolution.commit import (
    CommitError,
    build_commit,
    build_merge_commit,
    build_revert_commit,
)
from graflo.architecture.evolution.hashing import manifest_hash
from graflo.architecture.evolution.history import (
    FileCommitStore,
    History,
    checkout,
    verify_history,
)
from graflo.architecture.evolution.merge3 import (
    build_recipe,
    describe_slot,
    find_merge_base,
    merge_three_way,
    take_left,
    take_right,
)

logger = logging.getLogger(__name__)

DEFAULT_STORE = Path(".graflo/commits")

_store_option = click.option(
    "--store",
    type=click.Path(path_type=Path),
    default=DEFAULT_STORE,
    show_default=True,
    help="Directory holding the commit history.",
)


def _load(path: str | Path) -> GraphManifest:
    manifest = GraphManifest.from_config(FileHandle.load(path))
    manifest.finish_init()
    return manifest


def _hints(path: Path | None) -> RenameHints:
    if path is None:
        return RenameHints()
    return RenameHints.model_validate(FileHandle.load(path))


def _append(store: Path, entry) -> Path:
    """Store *entry*, turning a DAG-validation failure into a usable message.

    ``History`` validates on construction, so a commit that does not line up
    surfaces as a raw pydantic ``ValidationError``. That is the correct refusal
    reaching the user in the wrong shape -- it names a pydantic model and a
    tree hash, and says nothing about what to do next.
    """
    try:
        return FileCommitStore(store).append(entry)
    except ValidationError as exc:
        first = exc.errors()[0]["msg"].removeprefix("Value error, ")
        raise click.ClickException(
            f"{first}\n\nThe change was derived from a manifest that is not this "
            "commit's parent state. Check out the parent first, or record the "
            "commit onto the branch it actually extends with --onto."
        ) from exc


def _write(manifest: GraphManifest, path: Path | None) -> None:
    if path is None:
        return
    FileHandle.dump(manifest.to_dict(skip_defaults=True), path)
    click.echo(f"written: {path}")


# ── commit ──────────────────────────────────────────────────────────────────


@click.command("commit")
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
@click.option("-m", "--label", default=None, help="Short human-readable name.")
@click.option(
    "--onto",
    default=None,
    help="Parent commit id. Defaults to the single head; required when forked.",
)
@click.option(
    "--hints",
    "hints_path",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="YAML RenameHints; a drop plus an add is otherwise not a rename.",
)
@_store_option
@click.option("--dry", is_flag=True, default=False, help="Print without storing.")
def commit_cmd(
    from_path: Path,
    to_path: Path,
    label: str | None,
    onto: str | None,
    hints_path: Path | None,
    store: Path,
    dry: bool,
) -> None:
    """Record the change between two manifests as a commit."""
    base, target = _load(from_path), _load(to_path)
    ops, warnings = diff_manifests_verified(base, target, hints=_hints(hints_path))

    for warning in warnings:
        click.echo(f"warning: {warning}", err=True)
    if not ops:
        click.echo("No operations derived; nothing to record.")
        return

    history = FileCommitStore(store).load()
    parents = _resolve_parents(history, onto)

    try:
        entry = build_commit(
            base,
            ops,
            parents=parents,
            label=label,
            created_at=datetime.now(UTC).isoformat(),
        )
    except CommitError as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(f"commit    : {entry.id}")
    click.echo(f"parents   : {', '.join(entry.parents) or '-'}")
    click.echo(f"label     : {entry.label or '-'}")
    click.echo(f"reversible: {entry.reversible}")
    click.echo(f"tree      : {entry.tree_before[:12]} -> {entry.tree[:12]}")
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
        click.echo("(dry run -- not stored)")
        return

    click.echo(f"stored: {_append(store, entry)}")


def _resolve_parents(history: History, onto: str | None) -> list[str]:
    """The parents a new commit should carry."""
    if onto is not None:
        return [history.require(onto).id]
    heads = history.heads()
    if not heads:
        return []
    if len(heads) > 1:
        raise click.ClickException(
            "history has forked into "
            f"{len(heads)} heads ({', '.join(h.short() for h in heads)}); "
            "name the parent with --onto"
        )
    return [heads[0].id]


# ── log ─────────────────────────────────────────────────────────────────────


@click.command("log")
@_store_option
@click.option("--graph", is_flag=True, default=False, help="Show parent edges.")
def log_cmd(store: Path, graph: bool) -> None:
    """List the commit history, oldest first."""
    history = FileCommitStore(store).load()
    if not history.commits:
        click.echo("No commits stored.")
        return

    heads = {head.id for head in history.heads()}
    click.echo(f"{'commit':<10} {'kind':<8} {'rev?':<5} {'ops':<4} label")
    for entry in history.topological():
        marker = " (head)" if entry.id in heads else ""
        click.echo(
            f"{entry.short():<10} {entry.kind:<8} {entry.reversible!s:<5} "
            f"{len(entry.ops):<4} {entry.label or '-'}{marker}"
        )
        if graph and entry.parents:
            click.echo(
                f"           └─ parents: {', '.join(p[:8] for p in entry.parents)}"
            )

    if len(heads) > 1:
        click.echo(
            f"\nhistory has {len(heads)} heads -- it has forked. "
            "Use `graflo merge` to reconcile them."
        )


# ── verify ──────────────────────────────────────────────────────────────────


@click.command("verify")
@click.option(
    "--base",
    "base_path",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Manifest the root commit starts from.",
)
@click.option(
    "--against",
    "against_path",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Manifest the fully replayed history should equal.",
)
@_store_option
def verify_cmd(base_path: Path, against_path: Path | None, store: Path) -> None:
    """Replay every head, checking each recorded tree hash."""
    base = _load(base_path)
    history = FileCommitStore(store).load()

    problems = verify_history(base, history)
    if problems:
        for problem in problems:
            click.echo(f"error: {problem}", err=True)
        raise click.ClickException(f"{len(problems)} head(s) failed to replay")
    click.echo(
        f"history replays cleanly ({len(history.commits)} commit(s), "
        f"{len(history.heads())} head(s))"
    )

    if against_path is not None:
        expected = manifest_hash(_load(against_path))
        actual = manifest_hash(checkout(base, history))
        if actual != expected:
            raise click.ClickException(
                f"replayed manifest hashes {actual[:12]}, expected {expected[:12]}"
            )
        click.echo(f"matches {against_path}")


# ── checkout ────────────────────────────────────────────────────────────────


@click.command("checkout")
@click.argument("commit_id", required=False)
@click.option(
    "--base",
    "base_path",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Manifest the root commit starts from; replay is exact from here.",
)
@_store_option
@click.option("--output-path", type=click.Path(path_type=Path), default=None)
def checkout_cmd(
    commit_id: str | None, base_path: Path, store: Path, output_path: Path | None
) -> None:
    """Reconstruct the manifest as of a commit (default: the head)."""
    base = _load(base_path)
    history = FileCommitStore(store).load()
    try:
        result = checkout(base, history, commit_id)
    except CommitError as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(f"manifest hash: {manifest_hash(result)[:12]}")
    _write(result, output_path)


# ── merge ───────────────────────────────────────────────────────────────────


@click.command("merge")
@click.argument("left")
@click.argument("right")
@click.option(
    "--base",
    "base_path",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Manifest the root commit starts from.",
)
@_store_option
@click.option(
    "--take",
    type=click.Choice(["left", "right"]),
    default=None,
    help="Resolve every conflict by taking one side. Omit to see them first.",
)
@click.option("-m", "--label", default=None, help="Short human-readable name.")
@click.option("--output-path", type=click.Path(path_type=Path), default=None)
@click.option("--dry", is_flag=True, default=False, help="Report without storing.")
def merge_cmd(
    left: str,
    right: str,
    base_path: Path,
    store: Path,
    take: str | None,
    label: str | None,
    output_path: Path | None,
    dry: bool,
) -> None:
    """Merge two commits, reconciling against their common ancestor."""
    base_manifest = _load(base_path)
    history = FileCommitStore(store).load()

    left_commit = history.require(left)
    right_commit = history.require(right)

    merge_base_id = find_merge_base(history, left_commit.id, right_commit.id)
    if merge_base_id is None:
        raise click.ClickException(
            f"{left_commit.short()} and {right_commit.short()} share no ancestor. "
            "Unrelated lineages are joined by compose (declared equivalence), "
            "not by merge."
        )

    ancestor = checkout(base_manifest, history, merge_base_id)
    left_state = checkout(base_manifest, history, left_commit.id)
    right_state = checkout(base_manifest, history, right_commit.id)

    click.echo(f"merge base: {merge_base_id[:8]}")
    merged, result = merge_three_way(ancestor, left_state, right_state)

    if result.conflicts and take is None:
        click.echo(f"\n{len(result.conflicts)} conflict(s):")
        for conflict in result.conflicts:
            click.echo(f"  {describe_slot(conflict.slot_key)}  -- {conflict.reason}")
            click.echo(f"    left : {[op.op for op in conflict.left_ops]}")
            click.echo(f"    right: {[op.op for op in conflict.right_ops]}")
        raise click.ClickException(
            "unresolved conflicts; re-run with --take left/right, or resolve "
            "them through the server API"
        )

    resolutions = []
    if result.conflicts:
        chooser = take_left if take == "left" else take_right
        resolutions = [chooser(conflict) for conflict in result.conflicts]
        merged, result = merge_three_way(
            ancestor, left_state, right_state, resolutions=resolutions
        )

    if merged is None:
        raise click.ClickException("merge did not resolve; nothing to record")

    for warning in result.warnings:
        click.echo(f"warning: {warning}", err=True)
    click.echo(f"merged hash: {manifest_hash(merged)[:12]}")

    recipe = build_recipe(ancestor, left_state, right_state, resolutions=resolutions)
    stamp_provenance(
        merged,
        content_hash=manifest_hash(merged),
        canon=CANON_VERSION,
        parents=[left_commit.id, right_commit.id],
        merge_recipe=recipe.content_hash(),
    )
    _write(merged, output_path)

    if dry:
        click.echo("(dry run -- not stored)")
        return

    from graflo.architecture.evolution.commit import MergeRecipeRef

    try:
        entry = build_merge_commit(
            left_state,
            merged,
            parents=[left_commit.id, right_commit.id],
            label=label or f"merge {right_commit.short()} into {left_commit.short()}",
            created_at=datetime.now(UTC).isoformat(),
            merge_recipe=MergeRecipeRef(
                hash=recipe.content_hash(), kind=recipe.kind, payload=recipe.to_dict()
            ),
        )
    except CommitError as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(f"commit: {entry.id}")
    click.echo(f"stored: {_append(store, entry)}")


# ── revert ──────────────────────────────────────────────────────────────────


@click.command("revert")
@click.argument("commit_id")
@click.option(
    "--base",
    "base_path",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Manifest the root commit starts from.",
)
@_store_option
@click.option("-m", "--label", default=None)
@click.option("--output-path", type=click.Path(path_type=Path), default=None)
@click.option("--dry", is_flag=True, default=False)
def revert_cmd(
    commit_id: str,
    base_path: Path,
    store: Path,
    label: str | None,
    output_path: Path | None,
    dry: bool,
) -> None:
    """Record a new commit undoing an earlier one.

    History is append-only: undoing a change moves forward, it never edits what
    was recorded.
    """
    base_manifest = _load(base_path)
    history = FileCommitStore(store).load()
    target = history.require(commit_id)

    heads = history.heads()
    if len(heads) != 1:
        raise click.ClickException(
            f"expected a single head, found {len(heads)}; reconcile them first"
        )
    head = heads[0]
    current = checkout(base_manifest, history, head.id)

    try:
        entry = build_revert_commit(
            current,
            target,
            parents=[head.id],
            label=label,
            created_at=datetime.now(UTC).isoformat(),
        )
    except CommitError as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(f"commit: {entry.id}")
    click.echo(f"tree  : {entry.tree_before[:12]} -> {entry.tree[:12]}")
    click.echo("\noperations:")
    click.echo(ops_to_yaml_str(list(entry.ops)))

    if output_path is not None:
        from graflo.architecture.evolution.apply import apply_evolution

        _write(
            apply_evolution(
                current, list(entry.ops), bump_version=False, finish_init=False
            ),
            output_path,
        )
    if dry:
        click.echo("(dry run -- not stored)")
        return
    click.echo(f"stored: {_append(store, entry)}")


# ── stamp ───────────────────────────────────────────────────────────────────


@click.command("stamp")
@click.argument("manifest_path", type=click.Path(exists=True, path_type=Path))
@click.option("--commit", "commit_id", default=None, help="Commit that produced it.")
@_store_option
@click.option("--output-path", type=click.Path(path_type=Path), default=None)
def stamp_cmd(
    manifest_path: Path, commit_id: str | None, store: Path, output_path: Path | None
) -> None:
    """Write the content address and lineage into a manifest's metadata.

    Stamping is explicit rather than automatic because a manifest that stamps
    itself on every apply would disagree with itself about its own lineage.
    """
    manifest = _load(manifest_path)
    history = FileCommitStore(store).load()

    parents: list[str] = []
    if commit_id is not None:
        commit = history.require(commit_id)
        parents = list(commit.parents)
        commit_id = commit.id

    provenance = stamp_provenance(
        manifest,
        content_hash=manifest_hash(manifest),
        canon=CANON_VERSION,
        commit=commit_id,
        parents=parents,
    )
    click.echo(f"content_hash: {provenance.content_hash}")
    click.echo(f"canon       : {provenance.canon}")
    click.echo(f"commit      : {provenance.commit or '-'}")
    click.echo(f"parents     : {', '.join(provenance.parents) or '-'}")
    _write(manifest, output_path or manifest_path)


def commit_group() -> dict[str, click.Command]:
    """The verbs this module contributes to the ``graflo`` group."""
    return {
        "commit": commit_cmd,
        "log": log_cmd,
        "verify": verify_cmd,
        "checkout": checkout_cmd,
        "merge": merge_cmd,
        "revert": revert_cmd,
        "stamp": stamp_cmd,
    }
