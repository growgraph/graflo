"""Reconcile the fork: find the ancestor, see the conflict, decide, record.

    cd examples/20-version-control
    uv run python merge_branches.py                 # the conflict, and its resolution
    uv run python merge_branches.py --take right    # decide the other way
    uv run python merge_branches.py --advance-left  # the recorded decision, replayed

No database. Everything here is the contract plane.

The point of the last flag is the one that is hard to see from an API listing:
a merge whose resolutions were *recorded* can be run again after one side moves
on, and only genuinely new conflicts come back. That is what makes an overlay
maintainable instead of a fork someone has to re-litigate every release.

``--plot-dir`` draws two pictures of the same run: the **slot tree**, which
says where in the model the branches met — slots contain one another, so a
contested vertex sits above the field edits inside it — and the **commit DAG**
with the merge base marked.
"""

from __future__ import annotations

from pathlib import Path

import click
from build_history import STAMP, build_history

from graflo import GraphManifest
from graflo.architecture.contract.provenance import stamp_provenance
from graflo.architecture.evolution import (
    CANON_VERSION,
    FileCommitStore,
    History,
    MergeRecipeRef,
    apply_evolution,
    build_merge_commit,
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

EXAMPLE_DIR = Path(__file__).resolve().parent
STORE_ROOT = EXAMPLE_DIR / "artifacts" / "commits"


def _identity_of(manifest: GraphManifest) -> list[str]:
    schema = manifest.graph_schema
    assert schema is not None
    return schema.core_schema.vertex_config.vertices[0].identity


def _properties_of(manifest: GraphManifest) -> list[str]:
    schema = manifest.graph_schema
    assert schema is not None
    return [
        getattr(field, "name", field)
        for field in schema.core_schema.vertex_config.vertices[0].properties
    ]


@click.command()
@click.option(
    "--plot-dir",
    type=click.Path(path_type=Path),
    default=None,
    help="Draw the slot tree and the commit DAG into this directory.",
)
@click.option(
    "--take",
    type=click.Choice(["left", "right"]),
    default="left",
    show_default=True,
    help="Which side wins the contested slot.",
)
@click.option(
    "--advance-left",
    is_flag=True,
    help="Move the left branch on, then replay the recorded decision.",
)
@click.option(
    "--store",
    type=click.Path(path_type=Path),
    default=STORE_ROOT,
    show_default=True,
)
def main(plot_dir: Path | None, take: str, advance_left: bool, store: Path) -> None:
    """Merge the two heads, resolving the identity conflict."""
    base, history = build_history()

    heads = history.heads()
    if len(heads) != 2:
        raise click.ClickException(
            f"expected a forked history with two heads, found {len(heads)}"
        )
    # Deterministic sides regardless of how the store happened to order them.
    left_commit, right_commit = sorted(heads, key=lambda commit: commit.label or "")

    click.echo(f"left  : {left_commit.short()}  {left_commit.label}")
    click.echo(f"right : {right_commit.short()}  {right_commit.label}")

    base_id = find_merge_base(history, left_commit.id, right_commit.id)
    if base_id is None:
        raise click.ClickException(
            "the two heads share no ancestor; unrelated lineages are joined by "
            "compose, not by merge"
        )
    click.echo(f"base  : {base_id[:8]}\n")

    ancestor = checkout(base, history, base_id)
    left_state = checkout(base, history, left_commit.id)
    right_state = checkout(base, history, right_commit.id)

    if advance_left:
        # The left branch moves on with a change nobody contests.
        left_state = apply_evolution(
            left_state,
            [AddVertexPropertiesOp(additions={"person": ["nickname"]})],
            bump_version=False,
            finish_init=False,
        )
        click.echo("left branch advanced: person gains 'nickname'\n")

    # ── the conflict ────────────────────────────────────────────────────────
    merged, result = merge_three_way(ancestor, left_state, right_state)

    if plot_dir is not None:
        # Drawn from the *unresolved* result: an open conflict is what the
        # slot tree is for, and it is gone once a decision is recorded. Then
        # stop -- drawing is a read, and writing the store as a side effect of
        # asking for a figure would rewrite the committed artifacts.
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

    # ── the decision ────────────────────────────────────────────────────────
    chooser = take_left if take == "left" else take_right
    resolutions = [chooser(conflict) for conflict in result.conflicts]
    recipe = build_recipe(ancestor, left_state, right_state, resolutions=resolutions)

    if advance_left:
        # The whole point: replay the decision rather than ask again.
        merged, result = re_merge(recipe, ancestor, left_state, right_state)
        click.echo("\nreplayed the recorded resolution:")
        for warning in result.warnings:
            click.echo(f"  note: {warning}")
    else:
        merged, result = merge_three_way(
            ancestor, left_state, right_state, resolutions=resolutions
        )

    if merged is None:
        raise click.ClickException("the merge did not resolve")

    click.echo(f"\nmerged (took {take}):")
    click.echo(f"  identity  : {_identity_of(merged)}")
    click.echo(f"  properties: {_properties_of(merged)}")
    click.echo(f"  hash      : {manifest_hash(merged)[:12]}")

    # ── the record ──────────────────────────────────────────────────────────
    commit = build_merge_commit(
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

    # The artifact is self-describing outside any registry.
    stamp_provenance(
        merged,
        content_hash=manifest_hash(merged),
        canon=CANON_VERSION,
        commit=commit.id,
        parents=list(commit.parents),
        merge_recipe=recipe.content_hash(),
    )

    if advance_left:
        click.echo("\n(not stored: --advance-left is a what-if over a moved branch)")
        return

    reconciled = History(commits=[*history.commits, commit])
    FileCommitStore(store).save(reconciled)
    click.echo(f"\nheads after merging: {len(reconciled.heads())}")
    click.echo(f"stored: {store}")


def _plot(
    result,
    history: History,
    plot_dir: Path,
    *,
    heads: list[str],
    merge_base: str,
) -> None:
    """Draw where the branches met, and the lineage they met on."""
    from graflo.architecture.evolution.preview import build_merge_preview
    from graflo.plot.merge import plot_history, plot_merge_preview

    preview = build_merge_preview(result)
    slots = plot_merge_preview(preview, plot_dir / "merge-slots.svg")
    lineage = plot_history(
        history, plot_dir / "merge-history.svg", heads=heads, merge_base=merge_base
    )
    click.echo(f"slot tree ({preview.conflicts} contested) → {slots}")
    click.echo(f"lineage → {lineage}\n")


if __name__ == "__main__":
    main()
