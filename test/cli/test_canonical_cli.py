"""``graflo canonical-check`` end to end through Click.

The wiring the unit tests skip: option parsing, which manifest a scope resolves
to, the JSON payload, the written trim -- and the exit codes, because "this map
does not fit" and "I could not run the check" are different answers and a gate
that collapses them is useless.
"""

from __future__ import annotations

import json
import pathlib

import pytest
import yaml
from click.testing import CliRunner

from graflo.cli.main import graflo

EXAMPLE = (
    pathlib.Path(__file__).resolve().parents[2]
    / "examples"
    / "19-union-canonical-equivalence"
)
LEFT = EXAMPLE / "manifest_a.yaml"
RIGHT = EXAMPLE / "manifest_b.yaml"


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def fitting_map(tmp_path: pathlib.Path) -> pathlib.Path:
    """Every entry names something the left manifest declares."""
    path = tmp_path / "fits.yaml"
    path.write_text(yaml.safe_dump({"vertices": {"Firm": "Company"}}), encoding="utf-8")
    return path


@pytest.fixture
def dangling_map(tmp_path: pathlib.Path) -> pathlib.Path:
    path = tmp_path / "dangling.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "vertices": {"Firm": "Company", "Ghost": "Spectre"},
                "relations": {"has_parent": "parent_of"},
            }
        ),
        encoding="utf-8",
    )
    return path


def test_a_map_that_fits_exits_zero(runner, fitting_map):
    result = runner.invoke(
        graflo, ["canonical-check", str(fitting_map), "--left", str(LEFT)]
    )
    assert result.exit_code == 0, result.output
    assert "ok:" in result.output


def test_a_dangling_map_exits_one_and_names_every_entry(runner, dangling_map):
    result = runner.invoke(
        graflo, ["canonical-check", str(dangling_map), "--left", str(LEFT)]
    )
    assert result.exit_code == 1, result.output
    assert "'Ghost'" in result.output
    assert "'has_parent'" in result.output


def test_exit_zero_reports_without_failing(runner, dangling_map):
    result = runner.invoke(
        graflo,
        ["canonical-check", str(dangling_map), "--left", str(LEFT), "--exit-zero"],
    )
    assert result.exit_code == 0, result.output
    assert "'Ghost'" in result.output


def test_json_lists_the_entries_as_data(runner, dangling_map):
    result = runner.invoke(
        graflo,
        [
            "canonical-check",
            str(dangling_map),
            "--left",
            str(LEFT),
            "--json",
            "--exit-zero",
        ],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["count"] == 2
    assert {entry["source"] for entry in payload["dangling"]} == {"Ghost", "has_parent"}


def test_both_manifests_go_through_the_preview(runner, dangling_map):
    """Two sides is the merge report, one finding per declaration."""
    result = runner.invoke(
        graflo,
        [
            "canonical-check",
            str(dangling_map),
            "--left",
            str(LEFT),
            "--right",
            str(RIGHT),
        ],
    )
    assert result.exit_code == 1, result.output
    assert result.output.count("dangling") >= 2


def test_trim_writes_the_narrowed_map(runner, dangling_map, tmp_path):
    out = tmp_path / "trimmed.yaml"
    result = runner.invoke(
        graflo,
        [
            "canonical-check",
            str(dangling_map),
            "--left",
            str(LEFT),
            "--trim",
            str(out),
            "--exit-zero",
        ],
    )
    assert result.exit_code == 0, result.output
    written = yaml.safe_load(out.read_text(encoding="utf-8"))
    assert written["vertices"] == {"Firm": "Company"}
    assert "relations" not in written or written["relations"] == {}

    # The narrowed map is what the check now passes on.
    again = runner.invoke(graflo, ["canonical-check", str(out), "--left", str(LEFT)])
    assert again.exit_code == 0, again.output


def test_trim_needs_a_single_manifest_to_narrow_against(runner, dangling_map, tmp_path):
    result = runner.invoke(
        graflo,
        [
            "canonical-check",
            str(dangling_map),
            "--left",
            str(LEFT),
            "--right",
            str(RIGHT),
            "--scope",
            "both",
            "--trim",
            str(tmp_path / "out.yaml"),
        ],
    )
    assert result.exit_code == 2, result.output
    assert "--scope left" in result.output


def test_no_manifest_is_a_usage_error(runner, fitting_map):
    result = runner.invoke(graflo, ["canonical-check", str(fitting_map)])
    assert result.exit_code == 2, result.output
    assert "--left" in result.output


def test_a_map_that_is_not_one_exits_two(runner, tmp_path):
    """Unreadable input is 2, never 1: it says nothing about the map."""
    path = tmp_path / "bad.yaml"
    path.write_text(
        yaml.safe_dump({"vertices": {"X": ["not", "a", "name"]}}), encoding="utf-8"
    )
    result = runner.invoke(graflo, ["canonical-check", str(path), "--left", str(LEFT)])
    assert result.exit_code == 2, result.output
