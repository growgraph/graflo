"""Merge the two branches recorded by ``build_history.py`` against their base.

Finds the commit both branches start from, runs a three-way merge, prints the
conflict, resolves it by taking one side, and records a merge commit with both
parents in ``artifacts/commits/``. Run it from this directory:

    uv run python merge_branches.py                 # take the SSN branch
    uv run python merge_branches.py --take right    # take the email branch
    uv run python merge_branches.py --advance-left  # change a branch, merge again
    uv run python merge_branches.py --plot-dir figs # draw the slots and the history

``--advance-left`` adds a property to the SSN branch and merges again with the
decision stored in the merge commit, so the conflict is not asked a second
time. It stores nothing.
"""

from __future__ import annotations

import os
from pathlib import Path

import click
from build_history import STAMP, build_history

from graflo import GraphManifest
from graflo.architecture.evolution import (
    FileCommitStore,
    History,
    MergeRecipe,
    MergeRecipeRef,
    apply_evolution,
    build_multi_parent_commit,
    build_recipe,
    checkout,
    describe_slot,
    find_merge_base,
    manifest_hash,
    merge_three_way,
    re_merge,
    take_left,
    take_right,
)
from graflo.architecture.evolution.ops import AddVertexPropertiesOp
from graflo.architecture.schema.vertex import Vertex

EXAMPLE_DIR = Path(__file__).resolve().parent
STORE_ROOT = EXAMPLE_DIR / "artifacts" / "commits"


def _person(manifest: GraphManifest) -> Vertex:
    """The ``person`` vertex of *manifest*."""
    schema = manifest.graph_schema
    assert schema is not None
    return schema.core_schema.vertex_config.vertices[0]


def _print_merged(merged: GraphManifest, how: str) -> None:
    """Print the key, the properties and the hash of the merged manifest."""
    person = _person(merged)
    click.echo(f"\nmerged ({how}):")
    click.echo(f"  identity  : {person.identity}")
    click.echo(f"  properties: {person.property_names}")
    click.echo(f"  hash      : {manifest_hash(merged)[:12]}")


def _stored_recipe(store: Path) -> MergeRecipe:
    """The decision recorded in the merge commit of the stored history."""
    for commit in FileCommitStore(store).load().commits:
        if commit.merge_recipe is not None:
            return MergeRecipe.model_validate(commit.merge_recipe.payload)
    raise click.ClickException(
        "no merge commit is stored yet; run merge_branches.py without flags first"
    )


@click.command()
@click.option(
    "--plot-dir",
    type=click.Path(path_type=Path),
    default=None,
    help="Draw the slot tree and the commit history into this directory.",
)
@click.option(
    "--take",
    type=click.Choice(["left", "right"]),
    default="left",
    show_default=True,
    help="Which side wins the conflict.",
)
@click.option(
    "--advance-left",
    is_flag=True,
    help="Change the left branch, then merge again with the stored decision.",
)
@click.option(
    "--store",
    type=click.Path(path_type=Path),
    default=STORE_ROOT,
    show_default=True,
)
def main(plot_dir: Path | None, take: str, advance_left: bool, store: Path) -> None:
    """Merge the two heads, resolving the conflict on the identity."""
    base, history = build_history()

    heads = history.heads()
    if len(heads) != 2:
        raise click.ClickException(
            f"expected a forked history with two heads, found {len(heads)}"
        )
    # Sorted by label, so that the SSN branch is always the left side.
    left_commit, right_commit = sorted(heads, key=lambda commit: commit.label or "")

    click.echo(f"left  : {left_commit.short()}  {left_commit.label}")
    click.echo(f"right : {right_commit.short()}  {right_commit.label}")

    base_id = find_merge_base(history, left_commit.id, right_commit.id)
    if base_id is None:
        raise click.ClickException(
            "the two heads share no ancestor; combine unrelated manifests with "
            "a union (`graflo merge`) instead"
        )
    click.echo(f"base  : {base_id[:8]}\n")

    ancestor = checkout(base, history, base_id)
    left_state = checkout(base, history, left_commit.id)
    right_state = checkout(base, history, right_commit.id)

    if advance_left:
        left_state = apply_evolution(
            left_state,
            [AddVertexPropertiesOp(additions={"person": ["nickname"]})],
            bump_version=False,
            finish_init=False,
        )
        click.echo("left branch advanced: person gains 'nickname'\n")

    # The merge without a decision: it stops at the conflict.
    merged, result = merge_three_way(ancestor, left_state, right_state)

    if plot_dir is not None:
        # Drawn from the unresolved result, where the conflict is still open.
        # Drawing writes nothing to the store.
        _plot(
            result,
            history,
            plot_dir,
            heads=[left_commit.id, right_commit.id],
            merge_base=base_id,
        )
        return
    if merged is not None:
        raise click.ClickException("expected a conflict; the example is stale")

    click.echo(f"{len(result.conflicts)} conflict(s):")
    for conflict in result.conflicts:
        click.echo(f"  slot   : {describe_slot(conflict.slot_key)}")
        click.echo(f"  reason : {conflict.reason}")
        click.echo(f"  left   : {[op.op for op in conflict.left_ops]}")
        click.echo(f"  right  : {[op.op for op in conflict.right_ops]}")
        click.echo(f"  base   : identity {conflict.base_excerpt.get('identity')}")

    if advance_left:
        merged, result = re_merge(
            _stored_recipe(store), ancestor, left_state, right_state
        )
        click.echo("\nreplayed the stored decision:")
        for warning in result.warnings:
            click.echo(f"  note: {warning}")
        if merged is None:
            raise click.ClickException("the merge did not resolve")
        _print_merged(merged, "stored decision")
        click.echo("\n(not stored: --advance-left merges a changed branch)")
        return

    # The decision, kept in a recipe so that a later merge can replay it.
    chooser = take_left if take == "left" else take_right
    resolutions = [chooser(conflict) for conflict in result.conflicts]
    recipe = build_recipe(ancestor, left_state, right_state, resolutions=resolutions)
    merged, result = merge_three_way(
        ancestor, left_state, right_state, resolutions=resolutions
    )
    if merged is None:
        raise click.ClickException("the merge did not resolve")
    _print_merged(merged, f"took {take}")

    # The merge commit: both heads as parents, the recipe attached.
    commit = build_multi_parent_commit(
        left_state,
        merged,
        parents=[left_commit.id, right_commit.id],
        label=f"merge {right_commit.short()} into {left_commit.short()}",
        created_at=STAMP,
        merge_recipe=MergeRecipeRef(
            hash=recipe.content_hash(), kind=recipe.kind, payload=recipe.to_dict()
        ),
    )
    click.echo(f"\nmerge commit: {commit.short()}")
    click.echo(f"  parents   : {', '.join(p[:8] for p in commit.parents)}")
    click.echo(f"  recipe    : {recipe.content_hash()[:12]}")

    merged_history = History(commits=[*history.commits, commit])
    FileCommitStore(store).save(merged_history)
    click.echo(f"\nheads after merging: {len(merged_history.heads())}")
    click.echo(f"stored: {os.path.relpath(store)}")


def _plot(
    result,
    history: History,
    plot_dir: Path,
    *,
    heads: list[str],
    merge_base: str,
) -> None:
    """Draw where the branches met, and the history they met on."""
    from graflo.architecture.evolution.preview import build_merge3_preview
    from graflo.plot.merge3 import plot_history, plot_merge3_preview

    preview = build_merge3_preview(result)
    slots = plot_merge3_preview(preview, plot_dir / "merge-slots.svg")
    lineage = plot_history(
        history, plot_dir / "merge-history.svg", heads=heads, merge_base=merge_base
    )
    click.echo(f"slot tree ({preview.conflicts} contested) -> {os.path.relpath(slots)}")
    click.echo(f"history -> {os.path.relpath(lineage)}\n")


if __name__ == "__main__":
    main()
