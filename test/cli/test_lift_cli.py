"""``graflo lift`` end to end through Click.

The verb's whole claim is checkable in one line: a manifest that fails the
profile, plus a spec, becomes one that passes. These tests hold that line, plus
the wiring around it -- the op stream as a reviewable artifact, the dry run, and
the exit codes, which have to tell "the lift refused your manifest" from "I
could not run".
"""

from __future__ import annotations

import pathlib

import yaml
from click.testing import CliRunner

from graflo.architecture.contract.provenance import read_provenance
from graflo.architecture.evolution.commit import build_root_commit
from graflo.architecture.evolution.hashing import manifest_hash
from graflo.architecture.evolution.history import FileCommitStore
from graflo.architecture.evolution.lift_commit import lift_recipe_of
from graflo.cli.io import load_manifest
from graflo.cli.main import graflo

EXAMPLE = pathlib.Path(__file__).resolve().parents[2] / "examples" / "22-lift-aspects"
MANIFEST_IN = EXAMPLE / "manifest_in.yaml"
SPEC = EXAMPLE / "lift.yaml"


def _run(*args: str):
    return CliRunner().invoke(graflo, ["lift", *args])


def test_the_shipped_example_lifts_to_a_clean_pass() -> None:
    result = _run(str(MANIFEST_IN), "--spec", str(SPEC), "--dry-run")
    assert result.exit_code == 0, result.output
    assert "overall: PASS" in result.output


def test_the_written_manifest_passes_check_on_its_own(
    tmp_path: pathlib.Path,
) -> None:
    """The declarations have to survive the write, or the pass was an artefact
    of holding the model in memory."""
    out = tmp_path / "lifted.yaml"
    assert _run(str(MANIFEST_IN), "--spec", str(SPEC), "-o", str(out)).exit_code == 0

    checked = CliRunner().invoke(
        graflo, ["check", "--profile", "world-model", str(out)]
    )
    assert checked.exit_code == 0, checked.output


def test_the_op_stream_is_written_and_reads_back(tmp_path: pathlib.Path) -> None:
    ops_path = tmp_path / "ops.yaml"
    result = _run(
        str(MANIFEST_IN),
        "--spec",
        str(SPEC),
        "--emit-ops",
        str(ops_path),
        "-o",
        str(tmp_path / "lifted.yaml"),
    )
    assert result.exit_code == 0, result.output
    ops = yaml.safe_load(ops_path.read_text(encoding="utf-8"))
    kinds = [op["op"] for op in ops]
    assert "set_vertex_semantics" in kinds
    assert kinds[-1] == "remove_vertex_properties"


def test_dry_run_writes_nothing(tmp_path: pathlib.Path) -> None:
    out = tmp_path / "lifted.yaml"
    ops_path = tmp_path / "ops.yaml"
    result = _run(
        str(MANIFEST_IN),
        "--spec",
        str(SPEC),
        "-o",
        str(out),
        "--emit-ops",
        str(ops_path),
        "--dry-run",
    )
    assert result.exit_code == 0, result.output
    assert not out.exists()
    assert not ops_path.exists()
    assert "dry run" in result.output


def test_a_refused_lift_exits_one_naming_the_missing_declaration(
    tmp_path: pathlib.Path,
) -> None:
    spec = tmp_path / "spec.yaml"
    spec.write_text(
        yaml.safe_dump({"stateful": {"Machine": ["machine_id"]}}),
        encoding="utf-8",
    )
    result = _run(str(MANIFEST_IN), "--spec", str(spec), "--dry-run")
    assert result.exit_code == 1


def test_an_invalid_spec_exits_two(tmp_path: pathlib.Path) -> None:
    """Bad invocation, not a statement about the manifest."""
    spec = tmp_path / "spec.yaml"
    spec.write_text(yaml.safe_dump({"measured": {"nodot": "Cel"}}), encoding="utf-8")
    result = _run(str(MANIFEST_IN), "--spec", str(spec), "--dry-run")
    assert result.exit_code == 2


def test_no_check_suppresses_the_report(tmp_path: pathlib.Path) -> None:
    result = _run(str(MANIFEST_IN), "--spec", str(SPEC), "--dry-run", "--no-check")
    assert result.exit_code == 0, result.output
    assert "overall:" not in result.output
    assert "planned" in result.output


def test_the_shipped_example_lifts_to_its_shipped_artifacts(
    tmp_path: pathlib.Path,
) -> None:
    """The checked-in ops and manifest are what the command produces today."""
    out, ops_path = tmp_path / "lifted.yaml", tmp_path / "ops.yaml"
    result = _run(
        str(MANIFEST_IN),
        "--spec",
        str(SPEC),
        "--emit-ops",
        str(ops_path),
        "-o",
        str(out),
    )
    assert result.exit_code == 0, result.output

    shipped = EXAMPLE / "artifacts"
    assert yaml.safe_load(ops_path.read_text()) == yaml.safe_load(
        (shipped / "ops.yaml").read_text()
    )
    assert yaml.safe_load(out.read_text()) == yaml.safe_load(
        (shipped / "manifest_lifted.yaml").read_text()
    )


def _stored(manifest: pathlib.Path, store: pathlib.Path) -> str:
    """A store whose one root names *manifest*; returns the root's id."""
    root = build_root_commit(load_manifest(manifest), label="base")
    FileCommitStore(store).append(root)
    return root.id


class TestRecordingTheLift:
    """``-m`` records the lift in the store as a ``lift`` commit.

    Without it a lifted manifest cannot say which aspects, at which version
    and under which spec, produced it.
    """

    def test_without_the_flag_no_store_is_written(self, tmp_path: pathlib.Path) -> None:
        store = tmp_path / "commits"
        result = _run(
            str(MANIFEST_IN),
            "--spec",
            str(SPEC),
            "-o",
            str(tmp_path / "lifted.yaml"),
            "--store",
            str(store),
        )

        assert result.exit_code == 0, result.output
        assert not store.exists()

    def test_the_label_records_a_lift_commit_and_stamps_the_output(
        self, tmp_path: pathlib.Path
    ) -> None:
        store, out = tmp_path / "commits", tmp_path / "lifted.yaml"
        root = _stored(MANIFEST_IN, store)

        result = _run(
            str(MANIFEST_IN),
            "--spec",
            str(SPEC),
            "-o",
            str(out),
            "--store",
            str(store),
            "-m",
            "lift aspects",
        )

        assert result.exit_code == 0, result.output
        head = FileCommitStore(store).load().heads()[0]
        assert head.kind == "lift" and head.parents == [root]
        assert head.label == "lift aspects"
        assert lift_recipe_of(head).aspects == ["provenance", "state"]
        provenance = read_provenance(load_manifest(out))
        assert provenance is not None and head.recipe is not None
        assert provenance.commit == head.id
        assert provenance.parents == [root]
        assert provenance.recipe == head.recipe.hash
        assert manifest_hash(load_manifest(out)) == head.tree

    def test_a_manifest_absent_from_the_store_is_refused(
        self, tmp_path: pathlib.Path
    ) -> None:
        """Exit 2: the lift is fine, the store cannot name a parent for it."""
        out = tmp_path / "lifted.yaml"
        result = _run(
            str(MANIFEST_IN),
            "--spec",
            str(SPEC),
            "-o",
            str(out),
            "--store",
            str(tmp_path / "empty"),
            "-m",
            "lift",
        )

        assert result.exit_code == 2
        assert "cannot be named as a parent" in result.output
        assert not out.exists()

    def test_lifting_again_records_nothing_and_exits_zero(
        self, tmp_path: pathlib.Path
    ) -> None:
        store, once = tmp_path / "commits", tmp_path / "once.yaml"
        _stored(MANIFEST_IN, store)
        args = ["--spec", str(SPEC), "--store", str(store), "-m", "lift"]
        assert _run(str(MANIFEST_IN), *args, "-o", str(once)).exit_code == 0

        again = _run(str(once), *args, "-o", str(tmp_path / "twice.yaml"))

        assert again.exit_code == 0, again.output
        assert "nothing to record" in again.output
        assert len(FileCommitStore(store).load().commits) == 2

    def test_dry_run_records_nothing(self, tmp_path: pathlib.Path) -> None:
        store = tmp_path / "commits"
        _stored(MANIFEST_IN, store)

        result = _run(
            str(MANIFEST_IN),
            "--spec",
            str(SPEC),
            "--store",
            str(store),
            "-m",
            "x",
            "--dry-run",
        )

        assert result.exit_code == 0, result.output
        assert len(FileCommitStore(store).load().commits) == 1
