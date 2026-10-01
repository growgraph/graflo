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
from itertools import combinations
from pathlib import Path
from typing import Any

import click
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
    Commit,
    CommitError,
    build_commit,
    build_multi_parent_commit,
    build_revert_commit,
    compute_commit_id,
)
from graflo.architecture.evolution.hashing import manifest_hash
from graflo.architecture.evolution.history import (
    FileCommitStore,
    History,
    checkout,
    checkout_parent,
    rehash_trees,
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
from graflo.cli.io import dump_manifest, load_manifest

logger = logging.getLogger(__name__)

from graflo.cli._store import append_entry, store_option

_store_option = store_option


def _load(path: str | Path) -> GraphManifest:
    return load_manifest(path)


def _hints(path: Path | None) -> RenameHints:
    if path is None:
        return RenameHints()
    return RenameHints.model_validate(FileHandle.load(path))


_append = append_entry


def _write(manifest: GraphManifest, path: Path | None) -> None:
    dump_manifest(manifest, path)


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
@click.option(
    "--root",
    "as_root",
    is_flag=True,
    default=False,
    help=(
        "Start a new lineage instead of extending the head. A store can hold "
        "several unrelated lineages, which is what merge later joins."
    ),
)
@click.option("--dry", is_flag=True, default=False, help="Print without storing.")
def commit_cmd(
    from_path: Path,
    to_path: Path,
    label: str | None,
    onto: str | None,
    hints_path: Path | None,
    store: Path,
    as_root: bool,
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
    parents = _resolve_parents(history, onto, as_root=as_root)

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
    click.echo(f"tree      : {(entry.tree_before or '-')[:12]} -> {entry.tree[:12]}")
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


def _resolve_parents(
    history: History, onto: str | None, *, as_root: bool = False
) -> list[str]:
    """The parents a new commit should carry."""
    if as_root:
        if onto is not None:
            raise click.ClickException("--root and --onto contradict each other")
        return []
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
        click.echo("\n" + _heads_hint(history, sorted(heads)))


def _heads_hint(history: Any, heads: list[str]) -> str:
    """Name the verb that joins *heads*: ``merge3`` needs a common ancestor."""
    pairs = list(combinations(heads, 2))
    forked = sum(find_merge_base(history, a, b) is not None for a, b in pairs)
    count = f"history has {len(heads)} heads"
    if forked == len(pairs):
        return f"{count} -- it has forked. Use `graflo merge3` to reconcile them."
    if forked == 0:
        return (
            f"{count} from unrelated lineages. Use `graflo merge` to join them "
            "by declared equivalence."
        )
    return (
        f"{count}: reconcile the ones that share an ancestor with `graflo merge3`, "
        "and join unrelated lineages with `graflo merge`."
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
        provenance = base.metadata.provenance if base.metadata else None
        if provenance is not None and provenance.canon != CANON_VERSION:
            click.echo(
                f"hint: the base was hashed under {provenance.canon} and this "
                f"release hashes under {CANON_VERSION}; `graflo rehash --base` "
                "recomputes the recorded trees",
                err=True,
            )
        else:
            click.echo(
                "hint: trees recorded by an earlier release are recomputed by "
                "`graflo rehash --base`",
                err=True,
            )
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


@click.command("merge3")
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
@click.option(
    "--plot",
    "plot_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help=(
        "Draw the slot tree here -- where the two branches met, and what each "
        "did. Written even when conflicts stop the merge."
    ),
)
@click.option(
    "--plot-history",
    "history_plot_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Draw the commit DAG here, with the merge base marked.",
)
def merge3_cmd(
    left: str,
    right: str,
    base_path: Path,
    store: Path,
    take: str | None,
    label: str | None,
    output_path: Path | None,
    dry: bool,
    plot_path: Path | None,
    history_plot_path: Path | None,
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
            "Unrelated lineages are joined by `graflo merge` (declared "
            "equivalence), not by a three-way merge3."
        )

    ancestor = checkout(base_manifest, history, merge_base_id)
    left_state = checkout(base_manifest, history, left_commit.id)
    right_state = checkout(base_manifest, history, right_commit.id)

    click.echo(f"merge base: {merge_base_id[:8]}")
    if history_plot_path is not None:
        _plot_history(
            history,
            history_plot_path,
            heads=[left_commit.id, right_commit.id],
            merge_base=merge_base_id,
        )

    merged, result = merge_three_way(ancestor, left_state, right_state)

    # Drawn before the refusal below, since an unresolved merge is exactly the
    # case the picture is for.
    if plot_path is not None:
        _plot_slots(result, plot_path)

    if result.conflicts and take is None:
        click.echo(f"\n{len(result.conflicts)} conflict(s):")
        for conflict in result.conflicts:
            click.echo(f"  {describe_slot(conflict.slot_key)}  -- {conflict.reason}")
            click.echo(f"    left : {[op.op for op in conflict.left_ops]}")
            click.echo(f"    right: {[op.op for op in conflict.right_ops]}")
        raise click.ClickException(
            "unresolved conflicts; re-run with --take left/right, or resolve "
            "them per slot in Python with merge_three_way(..., resolutions=...)"
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
        entry = build_multi_parent_commit(
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


def _plot_slots(result: Any, path: Path) -> None:
    """Draw the slot tree of a merge result, or say why it could not."""
    from graflo.architecture.evolution.preview import build_merge3_preview

    try:
        from graflo.plot.merge3 import plot_merge3_preview
    except ImportError as exc:  # pragma: no cover - depends on the environment
        raise click.ClickException(f"--plot: {exc}") from exc
    try:
        written = plot_merge3_preview(build_merge3_preview(result), path)
    except (RuntimeError, ValueError) as exc:
        raise click.ClickException(f"--plot: {exc}") from exc
    click.echo(f"plot: {written}")


def _plot_history(
    history: Any, path: Path, *, heads: list[str], merge_base: str | None
) -> None:
    """Draw the commit DAG, or say why it could not."""
    try:
        from graflo.plot.merge3 import plot_history as draw_history
    except ImportError as exc:  # pragma: no cover - depends on the environment
        raise click.ClickException(f"--plot-history: {exc}") from exc
    try:
        written = draw_history(history, path, heads=heads, merge_base=merge_base)
    except (RuntimeError, ValueError) as exc:
        raise click.ClickException(f"--plot-history: {exc}") from exc
    click.echo(f"history plot: {written}")


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
            before=checkout_parent(base_manifest, history, target.id),
            parents=[head.id],
            label=label,
            created_at=datetime.now(UTC).isoformat(),
        )
    except CommitError as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(f"commit: {entry.id}")
    click.echo(f"tree  : {(entry.tree_before or '-')[:12]} -> {entry.tree[:12]}")
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


# ── rehash ──────────────────────────────────────────────────────────────────


def _rehash_bases(history: History, specs: tuple[str, ...]) -> dict[str, GraphManifest]:
    """Resolve ``--base`` values to ``{root id: manifest}``.

    ``ROOT=PATH`` names the root a base belongs to (a unique id prefix is
    enough); a bare ``PATH`` is accepted when the history has a single root.
    """
    roots = [commit.id for commit in history.roots()]
    bases: dict[str, GraphManifest] = {}
    for spec in specs:
        prefix, separator, path = spec.partition("=")
        matches = [root for root in roots if root.startswith(prefix)]
        if separator and prefix and len(matches) == 1:
            bases[matches[0]] = _load(path)
            continue
        if len(roots) != 1:
            raise click.UsageError(
                f"the history has {len(roots)} roots; name the root each base "
                "belongs to as ROOT=PATH"
            )
        bases[roots[0]] = _load(spec)
    return bases


@click.command("rehash")
@_store_option
@click.option(
    "--base",
    "base_specs",
    multiple=True,
    help=(
        "Manifest a root starts from, as PATH or ROOT=PATH. Given, trees are "
        "recomputed under the current canonical form as well."
    ),
)
@click.option("--dry", is_flag=True, default=False, help="Report without rewriting.")
def rehash_cmd(store: Path, base_specs: tuple[str, ...], dry: bool) -> None:
    """Recompute commit ids, and with ``--base`` trees, under this release.

    A commit id is derived from its ops as serialized, so a release that renames
    an op field changes the id of every commit carrying that op -- and, through
    the parent chain, of every commit after it. A ``CANON_VERSION`` bump moves
    every tree instead; recomputing trees needs the base each root starts from,
    and keeps every id. Trees are rehashed first, then ids, and the store is
    rewritten once.
    """
    history = FileCommitStore(store).load()
    moved_trees = 0
    if base_specs:
        rehashed, skipped = rehash_trees(history, _rehash_bases(history, base_specs))
        for before, after in zip(history.topological(), rehashed.topological()):
            if before.tree != after.tree:
                moved_trees += 1
                click.echo(
                    f"tree {before.short()}: {before.tree[:12]} -> {after.tree[:12]}"
                )
        if skipped:
            click.echo(
                f"{len(skipped)} commit(s) kept their trees: no base for their root"
            )
        history = rehashed

    mapping: dict[str, str] = {}
    rebuilt: list[Commit] = []
    for commit in history.topological():
        parents = [mapping.get(parent, parent) for parent in commit.parents]
        new_id = (
            compute_commit_id(list(commit.ops), parents) if commit.ops else commit.id
        )
        mapping[commit.id] = new_id
        rebuilt.append(commit.model_copy(update={"id": new_id, "parents": parents}))

    changed = {old: new for old, new in mapping.items() if old != new}
    for old, new in changed.items():
        click.echo(f"{old} -> {new}")
    if not changed and not moved_trees:
        click.echo(f"every commit is current ({len(history.commits)} commit(s))")
        return
    summary = f"{len(changed)} commit id(s) and {moved_trees} tree(s)"
    if dry:
        click.echo(f"{summary} would change (dry run)")
        return
    FileCommitStore(store).save(History(commits=rebuilt))
    click.echo(f"rewrote {summary} in {store}")


def commit_group() -> dict[str, click.Command]:
    """The verbs this module contributes to the ``graflo`` group."""
    return {
        "commit": commit_cmd,
        "log": log_cmd,
        "verify": verify_cmd,
        "checkout": checkout_cmd,
        "merge3": merge3_cmd,
        "revert": revert_cmd,
        "stamp": stamp_cmd,
        "rehash": rehash_cmd,
    }
