"""Two people changed the same manifest. How do I merge their changes and keep the history?

Records the history of ``manifest.yaml`` as commits: one change both people
start from, then one branch that keys people by SSN and one that keys them by
email. Writes one YAML file per commit to ``artifacts/commits/`` and prints
the log. ``merge_branches.py`` merges the two branches. Run it from this
directory:

    uv run python build_history.py            # write artifacts/commits/
    uv run python build_history.py --show     # print the stored log only
"""

from __future__ import annotations

import os
from pathlib import Path

import click
from suthing import FileHandle

from graflo import GraphManifest
from graflo.architecture.evolution import (
    FileCommitStore,
    History,
    apply_evolution,
    build_commit,
)
from graflo.architecture.evolution.ops import (
    AddVertexPropertiesOp,
    IdentityReplacement,
    NaturalIdentityTarget,
    ReplaceIdentityOp,
)

EXAMPLE_DIR = Path(__file__).resolve().parent
MANIFEST_PATH = EXAMPLE_DIR / "manifest.yaml"
STORE_ROOT = EXAMPLE_DIR / "artifacts" / "commits"

#: A commit id is computed from its operations and its parents. The timestamp
#: is fixed so that the stored commit files are the same on every run.
STAMP = "2026-01-01T00:00:00+00:00"


def load_base() -> GraphManifest:
    """The manifest every commit in this history descends from."""
    manifest = GraphManifest.from_config(FileHandle.load(MANIFEST_PATH))
    manifest.finish_init()
    return manifest


def _rekey(field: str) -> ReplaceIdentityOp:
    """Make *field* the identity of ``person``, keeping the old key as a property."""
    return ReplaceIdentityOp(
        replacements={
            "person": IdentityReplacement(
                to=NaturalIdentityTarget(identity=[field]), retire="keep"
            )
        }
    )


def build_history() -> tuple[GraphManifest, History]:
    """The base manifest and the forked history over it.

    Writes nothing: ``merge_branches.py`` imports this function, so both
    scripts work on the same commits.
    """
    base = load_base()

    # The change both branches start from: their common ancestor.
    shared = build_commit(
        base,
        [AddVertexPropertiesOp(additions={"person": ["created_at"]})],
        label="track when a person was first seen",
        created_at=STAMP,
    )
    after_shared = apply_evolution(
        base, list(shared.ops), bump_version=False, finish_init=False
    )

    # Each branch changes the key and also adds a property the other branch
    # does not touch. If the key were the only change, taking one side would
    # reproduce that side exactly, and a merge commit would have nothing to
    # record.
    by_ssn = build_commit(
        after_shared,
        [
            _rekey("ssn"),
            AddVertexPropertiesOp(additions={"person": ["ssn_verified"]}),
        ],
        parents=[shared.id],
        label="key people by SSN",
        created_at=STAMP,
    )
    by_email = build_commit(
        after_shared,
        [
            _rekey("email"),
            AddVertexPropertiesOp(additions={"person": ["email_verified"]}),
        ],
        parents=[shared.id],
        label="key people by email",
        created_at=STAMP,
    )
    return base, History(commits=[shared, by_ssn, by_email])


def print_log(history: History) -> None:
    """Print the history, oldest first, marking the heads."""
    heads = {head.id for head in history.heads()}
    click.echo(f"{'commit':<10} {'kind':<8} {'ops':<4} label")
    for commit in history.topological():
        marker = "  (head)" if commit.id in heads else ""
        click.echo(
            f"{commit.short():<10} {commit.kind:<8} {len(commit.ops):<4} "
            f"{commit.label}{marker}"
        )
    if len(heads) > 1:
        click.echo(
            f"\n{len(heads)} heads: the history has forked. "
            "Run merge_branches.py to merge them."
        )


@click.command()
@click.option(
    "--store",
    type=click.Path(path_type=Path),
    default=STORE_ROOT,
    show_default=True,
    help="Where the commits are written.",
)
@click.option("--show", is_flag=True, help="Print the stored log without rewriting it.")
def main(store: Path, show: bool) -> None:
    """Record the history, or print the one already recorded."""
    if show:
        print_log(FileCommitStore(store).load())
        return

    _base, history = build_history()
    FileCommitStore(store).save(history)
    print_log(history)
    click.echo(f"\nstored: {os.path.relpath(store)}")


if __name__ == "__main__":
    main()
