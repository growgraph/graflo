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
    for verb in ("commit", "log", "verify", "checkout", "merge", "revert", "stamp"):
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
    assert "graflo merge" in result.output


# ── verify and checkout ─────────────────────────────────────────────────────


def test_verify_replays_the_history_against_its_base(workspace) -> None:
    _record(workspace, "age", "add age")
    result = _run("verify", "--base", workspace["v1"], "--store", workspace["store"])
    assert result.exit_code == 0, result.output
    assert "replays cleanly" in result.output


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
        "merge",
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
    assert FileCommitStore(workspace["store"]).load().heads()[0].is_merge


def test_merging_unrelated_lineages_points_at_compose(workspace) -> None:
    """The signal that the operation wanted is compose, not merge."""
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
        "merge",
        one.id,
        two.id,
        "--base",
        workspace["v1"],
        "--store",
        workspace["store"],
    )
    assert result.exit_code != 0
    assert "share no ancestor" in result.output
    assert "compose" in result.output


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
