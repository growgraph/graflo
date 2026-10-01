"""The git-shaped CLI verbs, end to end through Click.

These drive the real command group against real manifest files, so they cover
the wiring the unit tests deliberately do not: option parsing, the store on
disk, and the messages a user actually reads when something is refused.
"""

from __future__ import annotations

import copy
import pathlib

import pytest
import yaml
from click.testing import CliRunner, Result

from graflo.cli.main import graflo

BASE_MANIFEST: dict = {
    "schema": {
        "metadata": {"name": "shop", "version": "1.0.0"},
        "graph": {
            "vertex_config": {
                "vertices": [
                    {
                        "name": "person",
                        "properties": [{"name": "id"}],
                        "identity": ["id"],
                    }
                ]
            },
            "edge_config": {"edges": []},
        },
    }
}


def _write(path: pathlib.Path, payload: dict) -> pathlib.Path:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _with_property(name: str) -> dict:
    payload = copy.deepcopy(BASE_MANIFEST)
    payload["schema"]["graph"]["vertex_config"]["vertices"][0]["properties"].append(
        {"name": name}
    )
    return payload


@pytest.fixture
def workspace(tmp_path: pathlib.Path) -> dict[str, pathlib.Path]:
    return {
        "v1": _write(tmp_path / "v1.yaml", BASE_MANIFEST),
        "age": _write(tmp_path / "age.yaml", _with_property("age")),
        "email": _write(tmp_path / "email.yaml", _with_property("email")),
        "store": tmp_path / "commits",
        "out": tmp_path / "out.yaml",
    }


def _property_names(path: pathlib.Path) -> list[str]:
    """Property names of the first vertex in a written manifest.

    Read through the model rather than by dict key: the dump uses
    `core_schema`, since `graph` is a *validation* alias only.
    """
    from graflo.architecture.contract.manifest import GraphManifest

    manifest = GraphManifest.from_dict(yaml.safe_load(path.read_text()))
    schema = manifest.graph_schema
    assert schema is not None
    vertex = schema.core_schema.vertex_config.vertices[0]
    return [str(getattr(f, "name", f)) for f in vertex.properties]


def _run(*args: object) -> Result:
    return CliRunner().invoke(graflo, [str(a) for a in args])


def _record(workspace, target: str, label: str, *extra: str) -> Result:
    return _run(
        "commit",
        "--from-manifest",
        workspace["v1"],
        "--to-manifest",
        workspace[target],
        "-m",
        label,
        "--store",
        workspace["store"],
        *extra,
    )


# ── the group ───────────────────────────────────────────────────────────────


def test_the_umbrella_group_exposes_the_version_control_verbs() -> None:
    result = _run("--help")
    assert result.exit_code == 0
    for verb in (
        "commit",
        "log",
        "verify",
        "checkout",
        "merge3",
        "revert",
        "stamp",
        "rehash",
    ):
        assert verb in result.output


def test_the_group_still_carries_the_pre_existing_scripts() -> None:
    """`graflo ingest` and the standalone `ingest` script are the same code."""
    result = _run("--help")
    assert "ingest" in result.output
    assert "migrate-schema" in result.output


# ── recording ───────────────────────────────────────────────────────────────


def test_recording_a_change_stores_a_commit(workspace) -> None:
    result = _record(workspace, "age", "add age")
    assert result.exit_code == 0, result.output
    assert "stored:" in result.output
    assert len(list(workspace["store"].glob("*.yaml"))) == 1


def test_a_dry_run_stores_nothing(workspace) -> None:
    result = _record(workspace, "age", "add age", "--dry")
    assert result.exit_code == 0, result.output
    assert "dry run" in result.output
    assert not workspace["store"].exists()


def test_recording_an_unchanged_manifest_records_nothing(workspace) -> None:
    result = _run(
        "commit",
        "--from-manifest",
        workspace["v1"],
        "--to-manifest",
        workspace["v1"],
        "--store",
        workspace["store"],
    )
    assert result.exit_code == 0
    assert "nothing to record" in result.output.lower()


# ── log ─────────────────────────────────────────────────────────────────────


def test_the_log_is_empty_before_anything_is_recorded(workspace) -> None:
    result = _run("log", "--store", workspace["store"])
    assert result.exit_code == 0
    assert "No commits stored." in result.output


def test_the_log_marks_the_head(workspace) -> None:
    _record(workspace, "age", "add age")
    result = _run("log", "--store", workspace["store"])
    assert result.exit_code == 0
    assert "add age" in result.output
    assert "(head)" in result.output


# ── forks ───────────────────────────────────────────────────────────────────


def test_a_second_commit_over_the_same_base_needs_an_explicit_parent(
    workspace,
) -> None:
    """The head advanced, so a second commit from v1 would not line up."""
    _record(workspace, "age", "add age")
    result = _record(workspace, "email", "add email")
    # Refused, because its tree_before is v1 while the head produces the age tree.
    assert result.exit_code != 0
    assert "expects to start from tree" in result.output


def test_the_log_reports_a_forked_history(workspace) -> None:
    from graflo.architecture.contract.manifest import GraphManifest
    from graflo.architecture.evolution.commit import build_commit
    from graflo.architecture.evolution.history import FileCommitStore, History
    from graflo.architecture.evolution.ops import AddVertexPropertiesOp

    base = GraphManifest.from_dict(BASE_MANIFEST)
    left = build_commit(
        base, [AddVertexPropertiesOp(additions={"person": ["age"]})], label="add age"
    )
    right = build_commit(
        base,
        [AddVertexPropertiesOp(additions={"person": ["email"]})],
        label="add email",
    )
    FileCommitStore(workspace["store"]).save(History(commits=[left, right]))

    result = _run("log", "--store", workspace["store"])
    assert result.exit_code == 0
    assert "2 heads" in result.output
    # Two roots share no ancestor commit, so `merge3` would refuse them.
    assert "`graflo merge`" in result.output
    assert "merge3" not in result.output
    assert "forked" not in result.output


def test_the_log_names_merge3_for_heads_that_forked(workspace) -> None:
    from graflo.architecture.contract.manifest import GraphManifest
    from graflo.architecture.evolution.apply import apply_evolution
    from graflo.architecture.evolution.commit import build_commit
    from graflo.architecture.evolution.history import FileCommitStore, History
    from graflo.architecture.evolution.ops import AddVertexPropertiesOp

    base = GraphManifest.from_dict(BASE_MANIFEST)
    root_op = AddVertexPropertiesOp(additions={"person": ["name"]})
    root = build_commit(base, [root_op], label="add name")
    after_root = apply_evolution(base, [root_op], bump_version=False)
    left = build_commit(
        after_root,
        [AddVertexPropertiesOp(additions={"person": ["age"]})],
        parents=[root.id],
        label="add age",
    )
    right = build_commit(
        after_root,
        [AddVertexPropertiesOp(additions={"person": ["email"]})],
        parents=[root.id],
        label="add email",
    )
    FileCommitStore(workspace["store"]).save(History(commits=[root, left, right]))

    result = _run("log", "--store", workspace["store"])

    assert result.exit_code == 0
    assert "forked" in result.output
    assert "`graflo merge3`" in result.output


# ── revert ──────────────────────────────────────────────────────────────────


def _head_id(workspace) -> str:
    from graflo.architecture.evolution.history import FileCommitStore

    (head,) = FileCommitStore(workspace["store"]).load().heads()
    return head.id


def _revert(workspace, commit_id: str) -> Result:
    return _run(
        "revert",
        commit_id,
        "--base",
        workspace["v1"],
        "--store",
        workspace["store"],
        "--output-path",
        workspace["out"],
    )


def test_reverting_an_added_property_removes_it(workspace) -> None:
    assert _record(workspace, "age", "add age").exit_code == 0

    result = _revert(workspace, _head_id(workspace))

    assert result.exit_code == 0, result.output
    assert _property_names(workspace["out"]) == ["id"]


def test_reverting_a_re_key_restores_the_old_key(workspace, tmp_path) -> None:
    from graflo.architecture.contract.manifest import GraphManifest

    with_email = _with_property("email")
    rekeyed = copy.deepcopy(with_email)
    rekeyed["schema"]["graph"]["vertex_config"]["vertices"][0]["identity"] = ["email"]
    workspace["v1"] = _write(tmp_path / "v1-email.yaml", with_email)
    workspace["rekeyed"] = _write(tmp_path / "rekeyed.yaml", rekeyed)
    assert _record(workspace, "rekeyed", "key by email").exit_code == 0

    result = _revert(workspace, _head_id(workspace))

    assert result.exit_code == 0, result.output
    reverted = GraphManifest.from_dict(yaml.safe_load(workspace["out"].read_text()))
    vertex = reverted.require_schema().core_schema.vertex_config.vertices[0]
    assert vertex.identity == ["id"]


def test_reverting_an_older_commit_keeps_the_newer_one(workspace, tmp_path) -> None:
    both = _with_property("age")
    both["schema"]["graph"]["vertex_config"]["vertices"][0]["properties"].append(
        {"name": "email"}
    )
    workspace["both"] = _write(tmp_path / "both.yaml", both)
    assert _record(workspace, "age", "add age").exit_code == 0
    first = _head_id(workspace)
    second = _run(
        "commit",
        "--from-manifest",
        workspace["age"],
        "--to-manifest",
        workspace["both"],
        "-m",
        "add email",
        "--store",
        workspace["store"],
    )
    assert second.exit_code == 0, second.output

    result = _revert(workspace, first)

    assert result.exit_code == 0, result.output
    assert _property_names(workspace["out"]) == ["id", "email"]


# ── verify and checkout ─────────────────────────────────────────────────────


def test_verify_replays_the_history_against_its_base(workspace) -> None:
    _record(workspace, "age", "add age")
    result = _run("verify", "--base", workspace["v1"], "--store", workspace["store"])
    assert result.exit_code == 0, result.output
    assert "replays cleanly" in result.output


def test_rehash_restores_content_derived_ids(workspace) -> None:
    """Ids move with the op serialization; rehash recomputes them in order.

    Simulated here by tampering a stored id and its child's parent reference,
    the way a history written under an older serialization looks after an
    upgrade: the trees still replay, only the names on disk are stale.
    """
    _record(workspace, "age", "add age")
    store = workspace["store"]
    chained = _run(
        "commit",
        "--from-manifest",
        workspace["age"],
        "--to-manifest",
        workspace["email"],
        "-m",
        "swap age for email",
        "--store",
        store,
    )
    assert chained.exit_code == 0, chained.output
    first, second = sorted(store.glob("*.yaml"))
    stale = "deadbeefcafe"
    real = yaml.safe_load(first.read_text())["id"]
    for path in (first, second):
        path.write_text(path.read_text().replace(real, stale), encoding="utf-8")

    dry = _run("rehash", "--store", store, "--dry")
    assert dry.exit_code == 0, dry.output
    assert f"{stale} -> {real}" in dry.output and "dry run" in dry.output
    assert stale in yaml.safe_load(first.read_text())["id"]

    result = _run("rehash", "--store", store)
    assert result.exit_code == 0, result.output
    assert "rewrote 1 commit id" in result.output
    ids = [yaml.safe_load(p.read_text())["id"] for p in sorted(store.glob("*.yaml"))]
    assert ids[0] == real
    verify = _run("verify", "--base", workspace["v1"], "--store", store)
    assert verify.exit_code == 0, verify.output

    again = _run("rehash", "--store", store)
    assert "every commit is current" in again.output


def test_rehash_with_a_base_recomputes_trees_and_keeps_ids(workspace) -> None:
    """A canon bump moves every tree; rehash replays from the base to restore them.

    Simulated by overwriting the stored trees, the way a history hashed under
    an older canon looks after an upgrade: the ids are still right, the trees
    no longer verify.
    """
    _record(workspace, "age", "add age")
    store = workspace["store"]
    (path,) = store.glob("*.yaml")
    stored = yaml.safe_load(path.read_text())
    for key in ("tree", "tree_before"):
        path.write_text(
            path.read_text().replace(stored[key], f"stale{key}".ljust(64, "0")),
            encoding="utf-8",
        )

    broken = _run("verify", "--base", workspace["v1"], "--store", store)
    assert broken.exit_code != 0
    assert "graflo rehash --base" in broken.output

    result = _run("rehash", "--store", store, "--base", str(workspace["v1"]))
    assert result.exit_code == 0, result.output
    assert "1 tree(s)" in result.output
    rewritten = yaml.safe_load(next(store.glob("*.yaml")).read_text())
    assert rewritten["id"] == stored["id"]
    assert (rewritten["tree"], rewritten["tree_before"]) == (
        stored["tree"],
        stored["tree_before"],
    )
    verify = _run("verify", "--base", workspace["v1"], "--store", store)
    assert verify.exit_code == 0, verify.output


def test_verify_can_assert_the_result_matches_a_manifest(workspace) -> None:
    _record(workspace, "age", "add age")
    result = _run(
        "verify",
        "--base",
        workspace["v1"],
        "--against",
        workspace["age"],
        "--store",
        workspace["store"],
    )
    assert result.exit_code == 0, result.output
    assert "matches" in result.output


def test_verify_fails_against_the_wrong_manifest(workspace) -> None:
    _record(workspace, "age", "add age")
    result = _run(
        "verify",
        "--base",
        workspace["v1"],
        "--against",
        workspace["email"],
        "--store",
        workspace["store"],
    )
    assert result.exit_code != 0
    assert "expected" in result.output


def test_checkout_writes_the_reconstructed_manifest(workspace) -> None:
    _record(workspace, "age", "add age")
    result = _run(
        "checkout",
        "--base",
        workspace["v1"],
        "--store",
        workspace["store"],
        "--output-path",
        workspace["out"],
    )
    assert result.exit_code == 0, result.output
    assert "age" in _property_names(workspace["out"])


# ── merge ───────────────────────────────────────────────────────────────────


def test_merging_two_branches_reconciles_them(workspace) -> None:
    from graflo.architecture.contract.manifest import GraphManifest
    from graflo.architecture.evolution.apply import apply_evolution
    from graflo.architecture.evolution.commit import build_commit
    from graflo.architecture.evolution.history import FileCommitStore, History
    from graflo.architecture.evolution.ops import AddVertexPropertiesOp

    base = GraphManifest.from_dict(BASE_MANIFEST)
    root = build_commit(
        base, [AddVertexPropertiesOp(additions={"person": ["shared"]})], label="root"
    )
    after_root = apply_evolution(
        base, list(root.ops), bump_version=False, finish_init=False
    )
    left = build_commit(
        after_root,
        [AddVertexPropertiesOp(additions={"person": ["age"]})],
        parents=[root.id],
        label="add age",
    )
    right = build_commit(
        after_root,
        [AddVertexPropertiesOp(additions={"person": ["email"]})],
        parents=[root.id],
        label="add email",
    )
    FileCommitStore(workspace["store"]).save(History(commits=[root, left, right]))

    result = _run(
        "merge3",
        left.id,
        right.id,
        "--base",
        workspace["v1"],
        "--store",
        workspace["store"],
        "--output-path",
        workspace["out"],
    )
    assert result.exit_code == 0, result.output
    assert "merge base:" in result.output

    assert {"age", "email", "shared"} <= set(_property_names(workspace["out"]))
    # The merge is stamped with its lineage, and a merge commit was recorded.
    merged = yaml.safe_load(workspace["out"].read_text())
    assert merged["metadata"]["provenance"]["parents"] == [left.id, right.id]
    assert FileCommitStore(workspace["store"]).load().heads()[0].is_multi_parent


def test_merging_unrelated_lineages_points_at_union(workspace) -> None:
    """The signal that the operation wanted is a union, not a three-way merge."""
    from graflo.architecture.contract.manifest import GraphManifest
    from graflo.architecture.evolution.commit import build_commit
    from graflo.architecture.evolution.history import FileCommitStore, History
    from graflo.architecture.evolution.ops import AddVertexPropertiesOp

    base = GraphManifest.from_dict(BASE_MANIFEST)
    one = build_commit(
        base, [AddVertexPropertiesOp(additions={"person": ["age"]})], label="one"
    )
    two = build_commit(
        base, [AddVertexPropertiesOp(additions={"person": ["email"]})], label="two"
    )
    FileCommitStore(workspace["store"]).save(History(commits=[one, two]))

    result = _run(
        "merge3",
        one.id,
        two.id,
        "--base",
        workspace["v1"],
        "--store",
        workspace["store"],
    )
    assert result.exit_code != 0
    assert "share no ancestor" in result.output
    assert "`graflo merge`" in result.output


def _forked_history(workspace):
    """Two branches over one root, saved to the store. Returns both commits."""
    from graflo.architecture.contract.manifest import GraphManifest
    from graflo.architecture.evolution.apply import apply_evolution
    from graflo.architecture.evolution.commit import build_commit
    from graflo.architecture.evolution.history import FileCommitStore, History
    from graflo.architecture.evolution.ops import AddVertexPropertiesOp

    base = GraphManifest.from_dict(BASE_MANIFEST)
    root = build_commit(
        base, [AddVertexPropertiesOp(additions={"person": ["shared"]})], label="root"
    )
    after_root = apply_evolution(
        base, list(root.ops), bump_version=False, finish_init=False
    )
    left = build_commit(
        after_root,
        [AddVertexPropertiesOp(additions={"person": ["age"]})],
        parents=[root.id],
        label="add age",
    )
    right = build_commit(
        after_root,
        [AddVertexPropertiesOp(additions={"person": ["email"]})],
        parents=[root.id],
        label="add email",
    )
    FileCommitStore(workspace["store"]).save(History(commits=[root, left, right]))
    return left, right


def test_merge3_draws_the_slot_tree_and_the_lineage(workspace, tmp_path) -> None:
    """``dot`` output, so the figures assert without Graphviz installed."""
    left, right = _forked_history(workspace)
    slots = tmp_path / "figs" / "slots.dot"
    history = tmp_path / "figs" / "history.dot"

    result = _run(
        "merge3",
        left.id,
        right.id,
        "--base",
        workspace["v1"],
        "--store",
        workspace["store"],
        "--output-path",
        workspace["out"],
        "--plot",
        str(slots),
        "--plot-history",
        str(history),
    )

    assert result.exit_code == 0, result.output
    # The directory did not exist: creating it is the CLI's job, not Graphviz's.
    assert slots.is_file() and history.is_file()
    assert "digraph" in slots.read_text()
    assert "digraph" in history.read_text()


def test_a_refused_merge3_still_draws_its_slot_tree(workspace, tmp_path) -> None:
    """An unresolved conflict is exactly the case the picture is for.

    Drawn before the refusal, not instead of it -- the same rule the merge
    preview follows, and the run that would otherwise leave nothing behind.
    """
    from graflo.architecture.contract.manifest import GraphManifest
    from graflo.architecture.evolution.apply import apply_evolution
    from graflo.architecture.evolution.commit import build_commit
    from graflo.architecture.evolution.history import FileCommitStore, History
    from graflo.architecture.evolution.ops import AddVertexPropertiesOp

    base = GraphManifest.from_dict(BASE_MANIFEST)
    root = build_commit(
        base, [AddVertexPropertiesOp(additions={"person": ["shared"]})], label="root"
    )
    after_root = apply_evolution(
        base, list(root.ops), bump_version=False, finish_init=False
    )
    # Both branches retype the same property: one slot, two answers.
    left = build_commit(
        after_root,
        [
            AddVertexPropertiesOp(
                additions={"person": [{"name": "cost", "type": "INT"}]}
            )
        ],
        parents=[root.id],
        label="cost as int",
    )
    right = build_commit(
        after_root,
        [
            AddVertexPropertiesOp(
                additions={"person": [{"name": "cost", "type": "STRING"}]}
            )
        ],
        parents=[root.id],
        label="cost as string",
    )
    FileCommitStore(workspace["store"]).save(History(commits=[root, left, right]))

    slots = tmp_path / "conflict.dot"
    result = _run(
        "merge3",
        left.id,
        right.id,
        "--base",
        workspace["v1"],
        "--store",
        workspace["store"],
        "--plot",
        str(slots),
    )

    assert result.exit_code != 0, "an unresolved conflict is still a refusal"
    assert slots.is_file(), "written even though the merge refused"


# ── stamp ───────────────────────────────────────────────────────────────────


def test_stamping_writes_the_content_address(workspace) -> None:
    result = _run(
        "stamp",
        workspace["age"],
        "--store",
        workspace["store"],
        "--output-path",
        workspace["out"],
    )
    assert result.exit_code == 0, result.output
    stamped = yaml.safe_load(workspace["out"].read_text())
    provenance = stamped["metadata"]["provenance"]
    assert len(provenance["content_hash"]) == 64
    assert provenance["canon"].startswith("graflo/canon@")


def test_the_stamped_hash_excludes_the_stamp_itself(workspace) -> None:
    """Otherwise stamping would change the very thing it records."""
    from graflo.architecture.contract.manifest import GraphManifest
    from graflo.architecture.evolution.hashing import manifest_hash

    _run(
        "stamp",
        workspace["age"],
        "--store",
        workspace["store"],
        "--output-path",
        workspace["out"],
    )
    stamped = yaml.safe_load(workspace["out"].read_text())
    recorded = stamped["metadata"]["provenance"]["content_hash"]
    assert manifest_hash(GraphManifest.from_dict(stamped)) == recorded
