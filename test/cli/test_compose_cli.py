"""``graflo compose`` end to end through Click.

The verb is ``examples/19-union-canonical-equivalence/build_union.py``
generalised, so the example's own fixtures are the fixtures here: if the CLI
does not reproduce that script's recipe -- the op and its canonical maps
applied together, in one step -- it is not the same operation and the
example's README points somewhere wrong.

Exit codes carry the load: 1 means compose looked at the manifests and
refused, 2 means the command could not be run. A CI job that cannot tell those
apart cannot gate on either.
"""

from __future__ import annotations

import json
import pathlib

import yaml
from click.testing import CliRunner

from graflo.cli.main import graflo

EXAMPLES_DIR = pathlib.Path(__file__).resolve().parents[2] / "examples"
EX19 = EXAMPLES_DIR / "19-union-canonical-equivalence"
MANIFEST_A = EX19 / "manifest_a.yaml"
MANIFEST_B = EX19 / "manifest_b.yaml"
CANONICAL_MAP = EX19 / "canonical_map.yaml"

#: The n-ary boundary cluster from ``build_union.py``, in canonical names.
BOUNDARY_OP: dict = {
    "op": "compose_manifests",
    "allow_merges": True,
    "vertices": [
        {"left": ["Company", "Shop"], "right": ["Org", "Branch"], "into": "Company"}
    ],
}


#: The same cluster with its identity settled, so it composes. Without it the
#: four members disagree on their natural key -- which the preview reports and
#: compose refuses.
KEYED_OP: dict = {
    **BOUNDARY_OP,
    "vertices": [{**BOUNDARY_OP["vertices"][0], "identity": ["company_id"]}],
}


def _write(tmp_path: pathlib.Path, name: str, payload: dict) -> pathlib.Path:
    path = tmp_path / name
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")
    return path


def test_no_op_composes_a_disjoint_union(tmp_path: pathlib.Path) -> None:
    out = tmp_path / "union.yaml"
    result = CliRunner().invoke(
        graflo,
        ["compose", str(MANIFEST_A), str(MANIFEST_B), "-o", str(out)],
    )
    assert result.exit_code == 0, result.output
    composed = yaml.safe_load(out.read_text(encoding="utf-8"))
    vertices = composed["schema"]["core_schema"]["vertex_config"]["vertices"]
    assert {v["name"] for v in vertices} == {"Firm", "Shop", "Org", "Branch"}


def test_a_canonical_map_lets_the_op_name_canonical_classes(
    tmp_path: pathlib.Path,
) -> None:
    """The op names ``Company``; ``manifest_a`` declares ``Firm``.

    The map establishes ``Company`` as ``Firm``'s canonical name, so the
    equivalence binds to ``Firm`` and the composed class is ``Company``.
    Without the map the same op has no left member to bind to.
    """
    op = dict(BOUNDARY_OP)
    # The four members disagree on their natural key; naming one resolves it,
    # which keeps this test about the rename rather than about identity.
    op["vertices"] = [{**BOUNDARY_OP["vertices"][0], "identity": ["company_id"]}]
    op_path = _write(tmp_path, "op.yaml", op)
    out = tmp_path / "union.yaml"
    result = CliRunner().invoke(
        graflo,
        [
            "compose",
            str(MANIFEST_A),
            str(MANIFEST_B),
            "--op",
            str(op_path),
            "--canonical-map",
            f"left={CANONICAL_MAP}",
            "-o",
            str(out),
        ],
    )
    assert result.exit_code == 0, result.output
    composed = yaml.safe_load(out.read_text(encoding="utf-8"))
    names = {
        v["name"]
        for v in composed["schema"]["core_schema"]["vertex_config"]["vertices"]
    }
    assert names == {"Company"}


def test_the_example_op_document_composes_from_the_shell(
    tmp_path: pathlib.Path,
) -> None:
    """``boundary_op.yaml`` names members in ``manifest_a``'s own vocabulary and
    no ``into``; the map names the composed class."""
    out = tmp_path / "union.yaml"
    result = CliRunner().invoke(
        graflo,
        [
            "compose",
            str(MANIFEST_A),
            str(MANIFEST_B),
            "--op",
            str(EX19 / "boundary_op.yaml"),
            "--canonical-map",
            f"left={CANONICAL_MAP}",
            "-o",
            str(out),
        ],
    )
    assert result.exit_code == 0, result.output
    composed = yaml.safe_load(out.read_text(encoding="utf-8"))
    names = {
        v["name"]
        for v in composed["schema"]["core_schema"]["vertex_config"]["vertices"]
    }
    assert names == {"Company"}


def test_without_a_map_an_unnamed_cluster_is_refused(tmp_path: pathlib.Path) -> None:
    result = CliRunner().invoke(
        graflo,
        [
            "compose",
            str(MANIFEST_A),
            str(MANIFEST_B),
            "--op",
            str(EX19 / "boundary_op.yaml"),
            "--dry-run",
        ],
    )
    assert result.exit_code == 1
    assert "unnamed" in result.output


def test_without_the_canonical_map_the_same_op_is_refused_on_membership(
    tmp_path: pathlib.Path,
) -> None:
    op_path = _write(tmp_path, "op.yaml", BOUNDARY_OP)
    result = CliRunner().invoke(
        graflo,
        [
            "compose",
            str(MANIFEST_A),
            str(MANIFEST_B),
            "--op",
            str(op_path),
            "--dry-run",
        ],
    )
    assert result.exit_code == 1
    assert "not in left manifest" in result.output


def test_dry_run_writes_nothing(tmp_path: pathlib.Path) -> None:
    out = tmp_path / "union.yaml"
    result = CliRunner().invoke(
        graflo,
        [
            "compose",
            str(MANIFEST_A),
            str(MANIFEST_B),
            "-o",
            str(out),
            "--dry-run",
        ],
    )
    assert result.exit_code == 0, result.output
    assert not out.exists()
    assert "dry run" in result.output


def test_bump_version_none_leaves_the_left_schema_version(
    tmp_path: pathlib.Path,
) -> None:
    out = tmp_path / "union.yaml"
    result = CliRunner().invoke(
        graflo,
        [
            "compose",
            str(MANIFEST_A),
            str(MANIFEST_B),
            "-o",
            str(out),
            "--bump-version",
            "none",
        ],
    )
    assert result.exit_code == 0, result.output
    left = yaml.safe_load(MANIFEST_A.read_text(encoding="utf-8"))
    composed = yaml.safe_load(out.read_text(encoding="utf-8"))
    assert (
        composed["schema"]["metadata"]["version"]
        == left["schema"]["metadata"]["version"]
    )


def test_name_collision_under_error_policy_exits_one(tmp_path: pathlib.Path) -> None:
    """Compose refused the manifests -- exit 1, and say what to declare."""
    result = CliRunner().invoke(
        graflo,
        ["compose", str(MANIFEST_A), str(MANIFEST_A), "--dry-run"],
    )
    assert result.exit_code == 1
    assert "name_conflict='prefix_right'" in result.output


def test_prefix_right_resolves_that_collision(tmp_path: pathlib.Path) -> None:
    out = tmp_path / "union.yaml"
    result = CliRunner().invoke(
        graflo,
        [
            "compose",
            str(MANIFEST_A),
            str(MANIFEST_A),
            "-o",
            str(out),
            "--name-conflict",
            "prefix_right",
        ],
    )
    assert result.exit_code == 0, result.output
    composed = yaml.safe_load(out.read_text(encoding="utf-8"))
    names = {
        v["name"]
        for v in composed["schema"]["core_schema"]["vertex_config"]["vertices"]
    }
    assert names == {"Firm", "Shop", "r_Firm", "r_Shop"}


def test_a_malformed_canonical_map_option_is_a_usage_error(
    tmp_path: pathlib.Path,
) -> None:
    result = CliRunner().invoke(
        graflo,
        [
            "compose",
            str(MANIFEST_A),
            str(MANIFEST_B),
            "--canonical-map",
            str(CANONICAL_MAP),
        ],
    )
    assert result.exit_code == 2
    assert "SIDE=PATH" in result.output


def test_an_unknown_side_token_is_a_usage_error(tmp_path: pathlib.Path) -> None:
    result = CliRunner().invoke(
        graflo,
        [
            "compose",
            str(MANIFEST_A),
            str(MANIFEST_B),
            "--canonical-map",
            f"middle={CANONICAL_MAP}",
        ],
    )
    assert result.exit_code == 2


def test_an_unreadable_manifest_exits_two(tmp_path: pathlib.Path) -> None:
    broken = tmp_path / "broken.yaml"
    broken.write_text("schema: {metadata: {}}\n", encoding="utf-8")
    result = CliRunner().invoke(
        graflo, ["compose", str(broken), str(MANIFEST_B), "--dry-run"]
    )
    assert result.exit_code == 2


def test_check_profile_prints_a_report(tmp_path: pathlib.Path) -> None:
    result = CliRunner().invoke(
        graflo,
        [
            "compose",
            str(MANIFEST_A),
            str(MANIFEST_B),
            "--dry-run",
            "--check-profile",
            "world-model",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "profile world-model" in result.output


def test_a_schemaless_overlay_composes(tmp_path: pathlib.Path) -> None:
    """The shape the relaxed guard exists for: source wiring, no types."""
    overlay = _write(
        tmp_path,
        "overlay.yaml",
        {
            "ingestion_model": {
                "resources": [{"name": "r_feed", "apply": []}],
                "transforms": [],
            }
        },
    )
    out = tmp_path / "union.yaml"
    result = CliRunner().invoke(
        graflo,
        ["compose", str(MANIFEST_A), str(overlay), "-o", str(out)],
    )
    assert result.exit_code == 0, result.output
    composed = yaml.safe_load(out.read_text(encoding="utf-8"))
    assert composed["schema"]["core_schema"]["vertex_config"]["vertices"]
    assert {r["name"] for r in composed["ingestion_model"]["resources"]} >= {"r_feed"}


# ── the preview artifacts ───────────────────────────────────────────────────


def test_a_refused_run_still_writes_its_plot_and_its_preview(tmp_path):
    """The case the artifacts exist for.

    A refusal is exactly when an author wants the picture, and it is the run
    that would otherwise leave nothing behind -- so both are written before
    the non-zero exit, not instead of it.
    """
    op_path = _write(tmp_path, "op.yaml", BOUNDARY_OP)
    plot = tmp_path / "figs" / "preview.dot"
    payload = tmp_path / "preview.json"

    result = CliRunner().invoke(
        graflo,
        [
            "compose",
            str(MANIFEST_A),
            str(MANIFEST_B),
            "--op",
            str(op_path),
            "--plot",
            str(plot),
            "--preview-json",
            str(payload),
        ],
    )

    assert result.exit_code == 1, "a refusal is still a refusal"
    assert plot.is_file(), "written even though compose refused"
    assert payload.is_file()

    document = json.loads(payload.read_text())
    assert document["outcome"]["status"] == "refused"
    assert any(f["severity"] == "refusal" for f in document["findings"])
    assert document["nodes"], "the declaration graph is in there too"


def test_a_composing_run_writes_a_preview_with_no_findings(tmp_path):
    op_path = _write(tmp_path, "op.yaml", KEYED_OP)
    payload = tmp_path / "preview.json"

    result = CliRunner().invoke(
        graflo,
        [
            "compose",
            str(MANIFEST_A),
            str(MANIFEST_B),
            "--op",
            str(op_path),
            "--canonical-map",
            f"left={CANONICAL_MAP}",
            "--preview-json",
            str(payload),
            "--dry-run",
        ],
    )

    assert result.exit_code == 0
    document = json.loads(payload.read_text())
    assert document["outcome"]["status"] == "composed"
    assert not [f for f in document["findings"] if f["severity"] != "note"]


def test_the_findings_table_leads_with_the_refusal_and_names_its_nodes():
    """What `--dry-run` and every plotted run print.

    Asserted on the formatter rather than on the captured stream: this
    suite runs with live logging, which reroutes the stream CliRunner
    hands back, and the ordering is the part worth pinning anyway.
    """
    from graflo.architecture.evolution.preview import ComposeFinding, ComposePreview
    from graflo.cli.compose import _findings_table

    preview = ComposePreview(
        findings=[
            ComposeFinding(
                kind="satisfied",
                severity="note",
                message="already applied",
                source="structure",
            ),
            ComposeFinding(
                kind="identity_disagreement",
                severity="possible",
                message="members disagree",
                source="structure",
                nodes=["composed:Company"],
            ),
            ComposeFinding(
                kind="cluster_overlap",
                severity="refusal",
                message="claimed twice",
                source="compose",
                nodes=["right:Org"],
            ),
        ]
    )

    lines = _findings_table(preview)

    assert lines[0] == "findings: 2 blocking, 3 total"
    assert "refusal" in lines[1] and "cluster_overlap" in lines[1]
    assert "right:Org" in lines[1]
    assert "possible" in lines[2]
    assert "note" in lines[3], "a note sorts last; it blocks nothing"


def test_no_findings_says_so():
    from graflo.architecture.evolution.preview import ComposePreview
    from graflo.cli.compose import _findings_table

    assert _findings_table(ComposePreview()) == ["findings: none"]


def test_a_plot_format_this_cannot_write_is_a_bad_invocation(tmp_path):
    """Exit 2, not 1: a statement about the command, not about the manifests."""
    op_path = _write(tmp_path, "op.yaml", BOUNDARY_OP)

    result = CliRunner().invoke(
        graflo,
        [
            "compose",
            str(MANIFEST_A),
            str(MANIFEST_B),
            "--op",
            str(op_path),
            "--plot",
            str(tmp_path / "preview.jpeg"),
        ],
    )

    assert result.exit_code == 2
    assert "unsupported format" in result.output
