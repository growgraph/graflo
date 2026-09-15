"""Commit-store plumbing shared by the ``commit`` and ``merge`` verbs.

Both verbs write to the same history, so the store path option and the append
refusal live here rather than in either command module -- a command importing
another command is the arrangement that makes the second one impossible to read
in isolation.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import click
from pydantic import ValidationError

from graflo.architecture.evolution.history import FileCommitStore

DEFAULT_STORE = Path(".graflo/commits")

store_option = click.option(
    "--store",
    type=click.Path(path_type=Path),
    default=DEFAULT_STORE,
    show_default=True,
    help="Directory holding the commit history.",
)


def append_entry(store: Path, entry: Any) -> Path:
    """Store *entry*, turning a DAG-validation failure into a usable message.

    ``History`` validates on construction, so an entry that does not line up
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
            "entry's parent state. Check out the parent first, or record it "
            "onto the branch it actually extends with --onto."
        ) from exc


__all__ = ["DEFAULT_STORE", "append_entry", "store_option"]
