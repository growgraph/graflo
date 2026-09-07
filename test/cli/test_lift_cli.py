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

from graflo.cli.main import graflo

EXAMPLE = pathlib.Path(__file__).resolve().parents[2] / "examples" / "22-state-core"
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
        yaml.safe_dump({"stateful": {"ConfigurationItem": ["ci_id"]}}),
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
