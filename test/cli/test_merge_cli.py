"""``graflo merge`` end to end through Click.

The verb is ``examples/19-union-canonical-equivalence/build_union.py``
generalised, so the example's own fixtures are the fixtures here: if the CLI
does not reproduce that script's recipe -- the op and its canonical maps
applied together, in one step -- it is not the same operation and the
example's README points somewhere wrong.

Exit codes carry the load: 1 means merge looked at the manifests and
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
    "op": "merge_manifests",
    "allow_merges": True,
    "vertices": [
        {"left": ["Company", "Shop"], "right": ["Org", "Branch"], "into": "Company"}
    ],
}


#: The same cluster with its identity settled, so it merges. Without it the
#: four members disagree on their natural key -- which the preview reports and
#: merge refuses.
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
        ["merge", str(MANIFEST_A), str(MANIFEST_B), "-o", str(out)],
    )
    assert result.exit_code == 0, result.output
    merged = yaml.safe_load(out.read_text(encoding="utf-8"))
    vertices = merged["schema"]["core_schema"]["vertex_config"]["vertices"]
    assert {v["name"] for v in vertices} == {"Firm", "Shop", "Org", "Branch"}


def test_a_canonical_map_lets_the_op_name_canonical_classes(
    tmp_path: pathlib.Path,
) -> None:
    """The op names ``Company``; ``manifest_a`` declares ``Firm``.

    The map establishes ``Company`` as ``Firm``'s canonical name, so the
    equivalence binds to ``Firm`` and the merged class is ``Company``.
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
            "merge",
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
    merged = yaml.safe_load(out.read_text(encoding="utf-8"))
    names = {
        v["name"] for v in merged["schema"]["core_schema"]["vertex_config"]["vertices"]
    }
    assert names == {"Company"}


def test_the_example_op_document_composes_from_the_shell(
    tmp_path: pathlib.Path,
) -> None:
    """``boundary_op.yaml`` names members in ``manifest_a``'s own vocabulary and
    no ``into``; the map names the merged class."""
    out = tmp_path / "union.yaml"
    result = CliRunner().invoke(
        graflo,
        [
            "merge",
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
    merged = yaml.safe_load(out.read_text(encoding="utf-8"))
    names = {
        v["name"] for v in merged["schema"]["core_schema"]["vertex_config"]["vertices"]
    }
    assert names == {"Company"}


def test_without_a_map_an_unnamed_cluster_is_refused(tmp_path: pathlib.Path) -> None:
    result = CliRunner().invoke(
        graflo,
        [
            "merge",
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
            "merge",
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
            "merge",
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
            "merge",
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
    merged = yaml.safe_load(out.read_text(encoding="utf-8"))
    assert (
        merged["schema"]["metadata"]["version"] == left["schema"]["metadata"]["version"]
    )


def test_name_collision_under_error_policy_exits_one(tmp_path: pathlib.Path) -> None:
    """Merge refused the manifests -- exit 1, and say what to declare."""
    result = CliRunner().invoke(
        graflo,
        ["merge", str(MANIFEST_A), str(MANIFEST_A), "--dry-run"],
    )
    assert result.exit_code == 1
    assert "name_conflict='prefix_right'" in result.output


def test_prefix_right_resolves_that_collision(tmp_path: pathlib.Path) -> None:
    out = tmp_path / "union.yaml"
    result = CliRunner().invoke(
        graflo,
        [
            "merge",
            str(MANIFEST_A),
            str(MANIFEST_A),
            "-o",
            str(out),
            "--name-conflict",
            "prefix_right",
        ],
    )
    assert result.exit_code == 0, result.output
    merged = yaml.safe_load(out.read_text(encoding="utf-8"))
    names = {
        v["name"] for v in merged["schema"]["core_schema"]["vertex_config"]["vertices"]
    }
    assert names == {"Firm", "Shop", "r_Firm", "r_Shop"}


def test_a_malformed_canonical_map_option_is_a_usage_error(
    tmp_path: pathlib.Path,
) -> None:
    result = CliRunner().invoke(
        graflo,
        [
            "merge",
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
            "merge",
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
        graflo, ["merge", str(broken), str(MANIFEST_B), "--dry-run"]
    )
    assert result.exit_code == 2


def test_check_profile_prints_a_report(tmp_path: pathlib.Path) -> None:
    result = CliRunner().invoke(
        graflo,
        [
            "merge",
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
        ["merge", str(MANIFEST_A), str(overlay), "-o", str(out)],
    )
    assert result.exit_code == 0, result.output
    merged = yaml.safe_load(out.read_text(encoding="utf-8"))
    assert merged["schema"]["core_schema"]["vertex_config"]["vertices"]
    assert {r["name"] for r in merged["ingestion_model"]["resources"]} >= {"r_feed"}


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
            "merge",
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
    assert plot.is_file(), "written even though merge refused"
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
            "merge",
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
    assert document["outcome"]["status"] == "merged"
    assert not [f for f in document["findings"] if f["severity"] != "note"]


def test_the_findings_table_leads_with_the_refusal_and_names_its_nodes():
    """What `--dry-run` and every plotted run print.

    Asserted on the formatter because building the three severities by hand is
    the only way to pin their *order*; the test below covers the same table
    coming out of the CLI for real.
    """
    from graflo.architecture.evolution.preview import MergeFinding, MergePreview
    from graflo.cli.merge import _findings_table

    preview = MergePreview(
        findings=[
            MergeFinding(
                kind="satisfied",
                severity="note",
                message="already applied",
                source="structure",
            ),
            MergeFinding(
                kind="identity_disagreement",
                severity="possible",
                message="members disagree",
                source="structure",
                nodes=["merged:Company"],
            ),
            MergeFinding(
                kind="cluster_overlap",
                severity="refusal",
                message="claimed twice",
                source="merge",
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


def test_a_dry_run_prints_the_findings_table_on_stderr(tmp_path):
    """The table an author actually sees, through the command.

    On stderr, and only on stderr: ``_write_preview`` echoes with
    ``err=preview.refused``, so a refused run -- the only kind with a refusal to
    lead with -- writes there, and ``click>=8.2`` keeps the two streams apart.
    A ``result.stdout`` assertion here reads as an empty stream and looks like a
    capture problem, which is not what it is.
    """
    op_path = _write(tmp_path, "op.yaml", BOUNDARY_OP)

    result = CliRunner().invoke(
        graflo,
        [
            "merge",
            str(MANIFEST_A),
            str(MANIFEST_B),
            "--op",
            str(op_path),
            "--dry-run",
        ],
    )

    assert result.exit_code == 1, "a refusal is still a refusal"
    assert result.stdout == "", "nothing about a refusal belongs on stdout"
    lines = result.stderr.splitlines()
    assert lines[0].startswith("findings: "), result.stderr
    assert "refusal" in lines[1], "the refusal leads"
    assert "merge refused" in result.stderr


def test_no_findings_says_so():
    from graflo.architecture.evolution.preview import MergePreview
    from graflo.cli.merge import _findings_table

    assert _findings_table(MergePreview()) == ["findings: none"]


def test_a_plot_format_this_cannot_write_is_a_bad_invocation(tmp_path):
    """Exit 2, not 1: a statement about the command, not about the manifests."""
    op_path = _write(tmp_path, "op.yaml", BOUNDARY_OP)

    result = CliRunner().invoke(
        graflo,
        [
            "merge",
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


class TestRecordingTheCompose:
    """``-m`` records the merge in the store as a two-parent commit.

    Without it a merged manifest is a lineage dead-end: nothing says which
    two manifests produced it, or under which equivalences. The commit itself
    is exercised in ``test_compose_commit.py``; these two pin the verb's
    contract around it.
    """

    def test_without_the_flag_no_store_is_written(self, tmp_path: pathlib.Path) -> None:
        """The default stays "two files in, one file out"."""
        out = tmp_path / "union.yaml"
        store = tmp_path / "cs"
        result = CliRunner().invoke(
            graflo,
            [
                "merge",
                str(MANIFEST_A),
                str(MANIFEST_B),
                "--op",
                str(_write(tmp_path, "op.yaml", KEYED_OP)),
                "--canonical-map",
                f"left={CANONICAL_MAP}",
                "-o",
                str(out),
                "--store",
                str(store),
            ],
        )

        assert result.exit_code == 0, result.output
        assert out.exists()
        assert not store.exists()

    def test_a_side_absent_from_the_store_is_refused_by_name(
        self, tmp_path: pathlib.Path
    ) -> None:
        """Half a lineage is worse than none; say which side is missing."""
        result = CliRunner().invoke(
            graflo,
            [
                "merge",
                str(MANIFEST_A),
                str(MANIFEST_B),
                "--op",
                str(_write(tmp_path, "op.yaml", KEYED_OP)),
                "--canonical-map",
                f"left={CANONICAL_MAP}",
                "-o",
                str(tmp_path / "union.yaml"),
                "--store",
                str(tmp_path / "empty-store"),
                "-m",
                "join",
            ],
        )

        # Exit 2, not 1: the manifests merge fine, the store cannot name a
        # parent for them. The message is checked where it is emitted.
        assert result.exit_code == 2
