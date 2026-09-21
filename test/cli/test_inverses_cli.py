"""``graflo inverses`` end to end through Click.

The verbs are shells over the audit and the planners, so what is held here is
the wiring: that a plan is shown, that ``--emit-ops`` writes ops which replay
without the command, that ``--dry-run`` writes nothing, that a manifest which
does not load is still handled, and that the exit code tells "conflicts remain"
from "I could not run".
"""

from __future__ import annotations

import json
import pathlib
from typing import Any

import yaml
from click.testing import CliRunner

from graflo.architecture.contract import GraphManifest
from graflo.architecture.evolution import apply_evolution
from graflo.architecture.evolution.codec import ops_from_yaml
from graflo.architecture.profile.inverses import audit_inverses
from graflo.cli.main import graflo

FORWARD = {"source": "person", "target": "institution", "relation": "employed_by"}
INVERSE = {"source": "institution", "target": "person", "relation": "employs"}
STEP = {"edge": {"from": "person", "to": "institution", "relation": "employed_by"}}
VERTICES = [{"vertex": "person"}, {"vertex": "institution"}]


def _write(
    tmp_path: pathlib.Path,
    *,
    edges: list[dict[str, Any]],
    flavor: str = "neo4j",
    symmetric: list[str] | None = None,
    infer_edges: bool = True,
) -> pathlib.Path:
    payload = {
        "schema": {
            "metadata": {"name": "cli", "version": "1.0.0"},
            "graph": {
                "vertex_config": {
                    "vertices": [
                        {"name": "person", "identity": ["pid"], "properties": ["pid"]},
                        {
                            "name": "institution",
                            "identity": ["iid"],
                            "properties": ["iid"],
                        },
                    ]
                },
                "edge_config": {
                    "edges": edges,
                    "inverses": [{"relation": "employed_by", "inverse": "employs"}],
                    "symmetric": symmetric or [],
                },
            },
            "db_profile": {"db_flavor": flavor},
        },
        "ingestion_model": {
            "resources": [
                {
                    "name": "rows",
                    "pipeline": [*VERTICES, STEP],
                    "infer_edges": infer_edges,
                }
            ]
        },
    }
    path = tmp_path / "manifest.yaml"
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")
    return path


def _run(*args: str):
    return CliRunner().invoke(graflo, ["inverses", *args])


def _load(path: pathlib.Path) -> GraphManifest:
    manifest = GraphManifest.from_config(yaml.safe_load(path.read_text()))
    manifest.finish_init()
    return manifest


def test_audit_says_how_each_pair_is_realized(tmp_path: pathlib.Path) -> None:
    result = _run("audit", str(_write(tmp_path, edges=[FORWARD])))
    assert result.exit_code == 0, result.output
    assert "employed_by <-> employs: declared" in result.output


def test_audit_as_json_is_the_report_model(tmp_path: pathlib.Path) -> None:
    result = _run("audit", "--json", str(_write(tmp_path, edges=[FORWARD])))
    assert result.exit_code == 0, result.output
    report = json.loads(result.output)
    assert report["pairs"][0]["state"] == "declared"
    assert report["pairs"][0]["native_candidate"] == "employed_by"


def test_audit_exits_1_on_conflicts_unless_told_not_to(
    tmp_path: pathlib.Path,
) -> None:
    same_side = {**FORWARD, "relation": "employs"}
    path = _write(tmp_path, edges=[FORWARD, same_side])
    assert _run("audit", str(path)).exit_code == 1
    assert _run("audit", "--exit-zero", str(path)).exit_code == 0


def test_a_file_that_is_not_a_manifest_exits_2(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "broken.yaml"
    path.write_text("schema: {graph: {edge_config: {edges: 7}}}\n", encoding="utf-8")
    result = _run("audit", str(path))
    assert result.exit_code == 2
    assert "not a valid manifest" in result.output


def test_realize_auto_explains_why_nothing_needs_storing(
    tmp_path: pathlib.Path,
) -> None:
    result = _run("realize", str(_write(tmp_path, edges=[FORWARD])))
    assert result.exit_code == 0, result.output
    assert "0 op(s)" in result.output
    assert "[declaration_suffices]" in result.output


def test_realize_writes_the_manifest_and_ops_that_replay_without_the_command(
    tmp_path: pathlib.Path,
) -> None:
    source = _write(tmp_path, edges=[FORWARD])
    out, ops_path = tmp_path / "out.yaml", tmp_path / "ops.yaml"
    result = _run(
        "realize",
        str(source),
        "--strategy",
        "materialized",
        "-o",
        str(out),
        "--emit-ops",
        str(ops_path),
    )
    assert result.exit_code == 0, result.output

    written = audit_inverses(_load(out))
    assert written.pairs[0].state == "materialized"
    assert written.pairs[0].feeding == {"rows": "emit_inverse"}

    replayed = apply_evolution(
        _load(source), ops_from_yaml(ops_path.read_text()), bump_version=False
    )
    assert audit_inverses(replayed).pairs[0].state == "materialized"


def test_dry_run_writes_nothing(tmp_path: pathlib.Path) -> None:
    out, ops_path = tmp_path / "out.yaml", tmp_path / "ops.yaml"
    result = _run(
        "realize",
        str(_write(tmp_path, edges=[FORWARD])),
        "--strategy",
        "materialized",
        "-o",
        str(out),
        "--emit-ops",
        str(ops_path),
        "--dry-run",
    )
    assert result.exit_code == 0, result.output
    assert "add_inverse_edges" in result.output
    assert "dry run: nothing written" in result.output
    assert not out.exists() and not ops_path.exists()


def test_repair_feeds_an_inverse_nobody_was_feeding(tmp_path: pathlib.Path) -> None:
    source = _write(tmp_path, edges=[FORWARD, INVERSE], infer_edges=False)
    assert "unfed_inverse" in _run("audit", str(source)).output

    out = tmp_path / "out.yaml"
    result = _run("repair", str(source), "-o", str(out))
    assert result.exit_code == 0, result.output
    assert "set_inverse_emission: rows:2" in result.output
    assert audit_inverses(_load(out)).findings == []


def test_repair_handles_a_manifest_that_does_not_load(
    tmp_path: pathlib.Path,
) -> None:
    knows = {"source": "person", "target": "person", "relation": "knows"}
    source = _write(tmp_path, edges=[FORWARD, knows], symmetric=["knows"])

    out = tmp_path / "out.yaml"
    result = _run("repair", str(source), "-o", str(out))
    assert result.exit_code == 0, result.output
    assert "set_edge_directed" in result.output
    _load(out)


def test_switch_moves_a_pair_and_checks_eligibility_first(
    tmp_path: pathlib.Path,
) -> None:
    source = _write(tmp_path, edges=[FORWARD, INVERSE])
    refused = _run("switch", str(source), "--to", "native", "-r", "employed_by")
    assert refused.exit_code == 0, refused.output
    assert "0 op(s)" in refused.output
    assert "[not_tigergraph]" in refused.output

    assert _run("switch", str(source), "--to", "virtual", "-r", "x").exit_code == 2


def test_withdraw_stops_storing_the_inverse_and_keeps_the_declaration(
    tmp_path: pathlib.Path,
) -> None:
    source = _write(tmp_path, edges=[FORWARD, INVERSE])
    out = tmp_path / "out.yaml"
    result = _run("withdraw", str(source), "-r", "employed_by", "-o", str(out))
    assert result.exit_code == 0, result.output
    assert "remove_edges" in result.output

    (pair,) = audit_inverses(_load(out)).pairs
    assert (pair.state, pair.stored_sides) == ("declared", ["employed_by"])


def test_check_runs_the_same_audit_as_a_profile(tmp_path: pathlib.Path) -> None:
    source = _write(tmp_path, edges=[FORWARD, INVERSE], infer_edges=False)
    result = CliRunner().invoke(graflo, ["check", str(source), "--profile", "inverses"])
    assert "inverses-complete" in result.output
    assert "WARN" in result.output


EXAMPLE = pathlib.Path(__file__).resolve().parents[2] / "examples" / "23-edge-inverses"


def test_the_shipped_example_audits_as_its_readme_says() -> None:
    result = _run("audit", str(EXAMPLE / "manifest.yaml"))
    assert result.exit_code == 0, result.output
    assert "alumnus_of <-> has_alumnus: declared" in result.output
    assert "fed in registry: none" in result.output
    for kind in ("property_drift", "undirected_not_symmetric", "unfed_inverse"):
        assert f"[{kind}]" in result.output


def test_the_shipped_example_repairs_to_its_shipped_artifacts(
    tmp_path: pathlib.Path,
) -> None:
    """The checked-in ops and manifest are what the command produces today."""
    out, ops_path = tmp_path / "out.yaml", tmp_path / "ops.yaml"
    result = _run(
        "repair",
        str(EXAMPLE / "manifest.yaml"),
        "-o",
        str(out),
        "--emit-ops",
        str(ops_path),
    )
    assert result.exit_code == 0, result.output

    shipped = EXAMPLE / "artifacts"
    assert yaml.safe_load(ops_path.read_text()) == yaml.safe_load(
        (shipped / "repair.ops.yaml").read_text()
    )
    assert yaml.safe_load(out.read_text()) == yaml.safe_load(
        (shipped / "manifest_repaired.yaml").read_text()
    )
    assert audit_inverses(_load(shipped / "manifest_repaired.yaml")).findings == []
