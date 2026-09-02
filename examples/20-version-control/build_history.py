"""Record a commit history over one manifest, then fork it.

    cd examples/20-version-control
    uv run python build_history.py            # → artifacts/commits/
    uv run python build_history.py --show     # print the log without rewriting

Nothing here touches a database. A commit history is a fact about the
*contract*, not about any deployment of it.

The shape is the one a schema registry cannot express. Two people start from
the same version. One decides the SSN is the real key; the other decides the
email is. Both are recorded, both replay, and the history says plainly that it
has two heads — where a linear revision chain would have had to drop one of
them, and a version-number scheme would have silently made the later write win.
"""

from __future__ import annotations

from datetime import UTC, datetime
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

#: Fixed so the example is reproducible: commit ids are content-derived, but
#: `created_at` is supplied by the caller and would otherwise change every run.
STAMP = "2026-01-01T00:00:00+00:00"


def load_base() -> GraphManifest:
    """The manifest every commit in this history descends from."""
    manifest = GraphManifest.from_config(FileHandle.load(MANIFEST_PATH))
    manifest.finish_init()
    return manifest


def _rekey(field: str) -> ReplaceIdentityOp:
    """Make *field* the primary identity, keeping the old key as a property."""
    return ReplaceIdentityOp(
        vertices={
            "person": IdentityReplacement(
                to=NaturalIdentityTarget(identity=[field]), retire="keep"
            )
        }
    )


def build_history() -> tuple[GraphManifest, History]:
    """The base manifest and a forked history over it.

    Side-effect free: `merge_branches.py` imports this rather than re-deriving
    the same commits, so the two scripts cannot drift.
    """
    base = load_base()

    # A shared edit both branches inherit. Without it the two branches would
    # have no common ancestor and there would be nothing to merge *against* --
    # which is a compose, not a merge.
    shared = build_commit(
        base,
        [AddVertexPropertiesOp(additions={"person": ["created_at"]})],
        label="track when a person was first seen",
        created_at=STAMP,
    )
    after_shared = apply_evolution(
        base, list(shared.ops), bump_version=False, finish_init=False
    )

    # The fork. Both are honest readings of the same business, recorded rather
    # than reconciled at write time.
    #
    # Each branch also makes a change the other does not contest. That is not
    # decoration: it is what makes the merge a merge. Were the identity the
    # only difference, taking one side would reproduce that side exactly, and
    # `build_merge_commit` would rightly refuse to record a commit that moves
    # nothing.
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
    """The history as `graflo log` renders it."""
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
            f"\n{len(heads)} heads — the history has forked. "
            "Run merge_branches.py to reconcile them."
        )


@click.command()
@click.option(
    "--store",
    type=click.Path(path_type=Path),
    default=STORE_ROOT,
    show_default=True,
    help="Where the commits are written.",
)
@click.option("--show", is_flag=True, help="Print the log without rewriting the store.")
def main(store: Path, show: bool) -> None:
    """Record the history, or print the one already recorded."""
    if show:
        print_log(FileCommitStore(store).load())
        return

    _base, history = build_history()
    FileCommitStore(store).save(history)
    print_log(history)
    click.echo(f"\nstored: {store}")
    click.echo(f"recorded at: {datetime.now(UTC).isoformat(timespec='seconds')}")


if __name__ == "__main__":
    main()
