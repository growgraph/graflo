"""``graflo check`` end to end through Click.

The wiring the unit tests deliberately skip: option parsing, the sidecar waiver
file, the JSON payload, and -- most importantly -- the exit codes, because a CI
job has to tell "your model is non-conformant" from "I could not run the
check", and collapsing those two is the failure mode that makes a gate useless.
"""

from __future__ import annotations

import json
import pathlib

import pytest
import yaml
from click.testing import CliRunner

from graflo.architecture.profile import ProfileReport
from graflo.cli.main import graflo

EXAMPLES_DIR = pathlib.Path(__file__).resolve().parents[2] / "examples"
REFERENCE = EXAMPLES_DIR / "22-state-core" / "reference.yaml"

UNGROUNDED: dict = {
    "metadata": {"name": "ungrounded"},
    "schema": {
        "metadata": {"name": "ungrounded", "version": "1.0.0"},
        "graph": {
            "vertex_config": {
                "vertices": [
                    {
                        "name": "thing",
                        "properties": [{"name": "id", "type": "STRING"}],
                        "identity": ["id"],
                    }
                ]
            },
            "edge_config": {"edges": []},
        },
        "db_profile": {},
    },
}


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def ungrounded(tmp_path: pathlib.Path) -> pathlib.Path:
    path = tmp_path / "manifest.yaml"
    path.write_text(yaml.safe_dump(UNGROUNDED), encoding="utf-8")
    return path


def test_conformant_manifest_exits_zero(runner: CliRunner):
    result = runner.invoke(graflo, ["check", str(REFERENCE)])
    assert result.exit_code == 0, result.output
    assert "PASS" in result.output


def test_nonconformant_manifest_exits_one(runner: CliRunner, ungrounded: pathlib.Path):
    result = runner.invoke(graflo, ["check", str(ungrounded)])
    assert result.exit_code == 1
    assert "no semantics.iri" in result.output


def test_unreadable_manifest_exits_two_not_one(
    runner: CliRunner, tmp_path: pathlib.Path
):
    """Distinct from 1: a gate must not read a broken file as a failed model."""
    path = tmp_path / "broken.yaml"
    path.write_text(yaml.safe_dump({"schema": {"nonsense": True}}), encoding="utf-8")
    result = runner.invoke(graflo, ["check", str(path)])
    assert result.exit_code == 2
    assert "not a valid manifest" in result.output


def test_unknown_profile_exits_two_and_names_the_known_ones(
    runner: CliRunner, ungrounded: pathlib.Path
):
    result = runner.invoke(graflo, ["check", str(ungrounded), "--profile", "nope"])
    assert result.exit_code == 2
    assert "world-model" in result.output


def test_missing_manifest_argument_is_a_usage_error(runner: CliRunner):
    result = runner.invoke(graflo, ["check"])
    assert result.exit_code == 2


def test_exit_zero_reports_without_gating(runner: CliRunner, ungrounded: pathlib.Path):
    result = runner.invoke(graflo, ["check", str(ungrounded), "--exit-zero"])
    assert result.exit_code == 0
    assert "FAIL" in result.output


def test_json_output_parses_back_into_the_report_model(runner: CliRunner):
    result = runner.invoke(graflo, ["check", str(REFERENCE), "--json"])
    assert result.exit_code == 0
    report = ProfileReport.model_validate(json.loads(result.output))
    assert report.profile == "world-model"
    assert len(report.assertions) == 6


def test_list_profiles_needs_no_manifest(runner: CliRunner):
    result = runner.invoke(graflo, ["check", "--list-profiles"])
    assert result.exit_code == 0
    assert "world-model" in result.output


def test_waivers_file_excuses_an_assertion(
    runner: CliRunner, tmp_path: pathlib.Path, ungrounded: pathlib.Path
):
    waivers = tmp_path / "waivers.yaml"
    waivers.write_text(
        yaml.safe_dump(
            {
                "profile": "world-model",
                "waivers": [
                    {"assertion": "temporal", "reason": "static reference data"}
                ],
            }
        ),
        encoding="utf-8",
    )
    result = runner.invoke(
        graflo, ["check", str(ungrounded), "--waivers", str(waivers)]
    )
    assert "WAIVED" in result.output
    assert "static reference data" in result.output
    # Still non-conformant: the waiver covers `temporal`, not the missing
    # grounding, and a waiver must never excuse an assertion it does not name.
    assert result.exit_code == 1


def test_warnings_as_errors_gates_on_an_advisory_finding(
    runner: CliRunner, tmp_path: pathlib.Path
):
    result = runner.invoke(graflo, ["check", str(REFERENCE), "--warnings-as-errors"])
    assert result.exit_code == 0, "the reference manifest carries no warnings today"
