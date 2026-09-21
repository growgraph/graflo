"""Persisted content hashes must not drift.

Commit ids, root ids, merge-recipe addresses and connector ids are written to
disk and compared across runs, so the canonical JSON they hash (sorted keys,
compact separators, ASCII-escaped) and the digest must stay byte-identical.
These values were computed before the hashing moved to ``suthing.stable_hash``;
a failure here means stored ids would no longer match.
"""

from __future__ import annotations

from graflo.architecture.contract.bindings.connectors import FileConnector
from graflo.architecture.evolution.commit import (
    compute_commit_id,
    compute_root_commit_id,
)
from graflo.architecture.evolution.hashing import stable_hash
from graflo.architecture.evolution.merge3 import MergeRecipe
from graflo.architecture.evolution.ops import AddVertexPropertiesOp


def test_canonical_stable_hash_is_unchanged():
    assert (
        stable_hash({"b": [1, "é"], "a": None})
        == "8beaee9397badc3e31e198eb78acf82f2f272009deecfdf242a6502795760760"
    )


def test_commit_id_is_unchanged():
    op = AddVertexPropertiesOp(additions={"a": ["y"]})
    assert compute_commit_id([op], ["p1"]) == "1e4b4950be6e"


def test_root_commit_id_is_unchanged():
    assert compute_root_commit_id("abc", "scope-1") == "21e26bfaaade"
    assert compute_root_commit_id("abc") == "b4a4cf23ae96"


def test_connector_hash_is_unchanged():
    connector = FileConnector(regex=r"^a.*\.csv$")
    assert (
        connector.hash
        == "01ed3090d6b09691b11b9c35c20baf55c748d0135909b8a53d28f4f5b9a8d9c2"
    )


def test_merge_recipe_content_hash_is_unchanged():
    recipe = MergeRecipe(left="l", right="r", base="b")
    assert (
        recipe.content_hash()
        == "1410dea992378213097878d0ce1f98da03c519b550baa2c538fab25d67b7f0d7"
    )
