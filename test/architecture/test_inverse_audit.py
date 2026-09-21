"""``audit_inverses``: how a manifest realizes its declared inverses, across all three planes.

The schema says whether a pair is only declared, or realized natively or as
declared edges. Only the
pipelines say whether a materialized inverse is actually fed, and that is where
a manifest assembled from several sources goes quietly wrong: one resource
writes both readings of a fact, another only the forward one.
"""

from __future__ import annotations

import copy
from typing import Any

import pytest

from graflo.architecture.contract import GraphManifest
from graflo.architecture.profile import check_manifest, list_profiles
from graflo.architecture.profile.inverses import audit_inverses, manifest_for_audit

FORWARD = {"source": "person", "target": "institution", "relation": "employed_by"}
INVERSE = {"source": "institution", "target": "person", "relation": "employs"}
PAIR = {"relation": "employed_by", "inverse": "employs"}
VERTICES = [{"vertex": "person"}, {"vertex": "institution"}]
FORWARD_STEP = {
    "edge": {"from": "person", "to": "institution", "relation": "employed_by"}
}
INVERSE_STEP = {"edge": {"from": "institution", "to": "person", "relation": "employs"}}


def _payload(
    resources: dict[str, dict[str, Any]] | None = None,
    *,
    edges: list[dict[str, Any]] | None = None,
    inverses: list[dict[str, str]] | None = None,
    symmetric: list[str] | None = None,
    db_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    schema: dict[str, Any] = {
        "metadata": {"name": "audit", "version": "1.0.0"},
        "graph": {
            "vertex_config": {
                "vertices": [
                    {"name": "person", "identity": ["pid"], "properties": ["pid"]},
                    {"name": "institution", "identity": ["iid"], "properties": ["iid"]},
                ]
            },
            "edge_config": {
                "edges": copy.deepcopy(edges if edges is not None else [FORWARD]),
                "inverses": inverses if inverses is not None else [PAIR],
                "symmetric": symmetric or [],
            },
        },
    }
    if db_profile is not None:
        schema["db_profile"] = db_profile
    payload: dict[str, Any] = {"schema": schema}
    if resources is not None:
        payload["ingestion_model"] = {
            "resources": [{"name": name, **body} for name, body in resources.items()]
        }
    return payload


def _manifest(*args: Any, **kwargs: Any) -> GraphManifest:
    manifest = GraphManifest.from_dict(_payload(*args, **kwargs))
    manifest.finish_init()
    return manifest


def _kinds(manifest: GraphManifest) -> list[str]:
    return [finding.kind for finding in audit_inverses(manifest).findings]


class TestRealizationAcrossPlanes:
    def test_a_pair_that_is_only_declared_is_clean(self) -> None:
        report = audit_inverses(_manifest({"rows": {"pipeline": [*VERTICES]}}))
        (pair,) = report.pairs
        assert pair.state == "declared"
        assert pair.feeding == {}
        assert report.findings == []

    def test_a_materialized_pair_fed_by_mirroring(self) -> None:
        step = {"edge": {**FORWARD_STEP["edge"], "emit_inverse": True}}
        report = audit_inverses(
            _manifest(
                {"rows": {"pipeline": [*VERTICES, step]}}, edges=[FORWARD, INVERSE]
            )
        )
        (pair,) = report.pairs
        assert pair.state == "materialized"
        assert pair.feeding == {"rows": "emit_inverse"}
        assert report.findings == []

    def test_a_materialized_pair_fed_by_a_step_of_its_own(self) -> None:
        """A source that reports both readings needs no mirroring."""
        report = audit_inverses(
            _manifest(
                {"rows": {"pipeline": [*VERTICES, FORWARD_STEP, INVERSE_STEP]}},
                edges=[FORWARD, INVERSE],
            )
        )
        assert report.pairs[0].feeding == {"rows": "step"}
        assert report.findings == []

    def test_a_materialized_pair_fed_by_inference(self) -> None:
        report = audit_inverses(
            _manifest(
                {"rows": {"pipeline": [*VERTICES, FORWARD_STEP]}},
                edges=[FORWARD, INVERSE],
            )
        )
        assert report.pairs[0].feeding == {"rows": "inference"}
        assert report.findings == []

    def test_native_eligibility_names_the_side_that_has_edges(self) -> None:
        report = audit_inverses(_manifest(db_profile={"db_flavor": "tigergraph"}))
        (pair,) = report.pairs
        assert pair.native_candidate == "employed_by"
        assert pair.native_eligibility == []

    def test_native_eligibility_says_why_not(self) -> None:
        report = audit_inverses(_manifest(db_profile={"db_flavor": "neo4j"}))
        (pair,) = report.pairs
        assert pair.native_eligibility is not None
        assert [v.code for v in pair.native_eligibility] == ["not_tigergraph"]

    def test_a_native_pair_has_no_eligibility_to_report(self) -> None:
        report = audit_inverses(
            _manifest(
                db_profile={
                    "db_flavor": "tigergraph",
                    "native_inverses": ["employed_by"],
                }
            )
        )
        (pair,) = report.pairs
        assert pair.state == "native"
        assert pair.native_eligibility is None


class TestUnderReporting:
    """One place states the fact and another omits it: repairable."""

    def test_a_resource_that_writes_forward_and_feeds_nothing(self) -> None:
        manifest = _manifest(
            {"rows": {"pipeline": [*VERTICES, FORWARD_STEP], "infer_edges": False}},
            edges=[FORWARD, INVERSE],
        )
        report = audit_inverses(manifest)

        (finding,) = report.repairable()
        assert finding.kind == "unfed_inverse"
        assert finding.relations == ["employed_by", "employs"]
        assert [str(step) for step in finding.steps] == ["rows:2"]
        assert finding.detail == {"certain": True}
        assert report.pairs[0].feeding == {"rows": "none"}

    def test_two_sources_merged_where_only_one_reported_both_readings(self) -> None:
        manifest = _manifest(
            {
                "hr": {
                    "pipeline": [*VERTICES, FORWARD_STEP, INVERSE_STEP],
                    "infer_edges": False,
                },
                "registry": {
                    "pipeline": [*VERTICES, FORWARD_STEP],
                    "infer_edges": False,
                },
            },
            edges=[FORWARD, INVERSE],
        )
        report = audit_inverses(manifest)

        assert report.pairs[0].feeding == {"hr": "step", "registry": "none"}
        (finding,) = report.repairable()
        assert [step.resource for step in finding.steps] == ["registry"]

    def test_a_relation_read_from_the_data_is_reported_as_uncertain(self) -> None:
        step = {
            "edge": {"from": "person", "to": "institution", "relation_field": "rel"}
        }
        manifest = _manifest(
            {"rows": {"pipeline": [*VERTICES, step], "infer_edges": False}},
            edges=[FORWARD, INVERSE],
        )
        (finding,) = audit_inverses(manifest).repairable()
        assert finding.kind == "unfed_inverse"
        assert finding.detail == {"certain": False}

    def test_a_step_inside_a_nested_pipeline_is_addressed_by_its_path(self) -> None:
        nested = {"key": "jobs", "pipeline": [{"vertex": "institution"}, FORWARD_STEP]}
        manifest = _manifest(
            {
                "rows": {
                    "pipeline": [{"vertex": "person"}, nested],
                    "infer_edges": False,
                }
            },
            edges=[FORWARD, INVERSE],
        )
        (finding,) = audit_inverses(manifest).repairable()
        assert [str(step) for step in finding.steps] == ["rows:1/1"]

    def test_a_flag_that_mirrors_nothing(self) -> None:
        step = {
            "edge": {
                "source_role": "s",
                "target_role": "t",
                "relation_field": "rel",
                "relation_map": {"X": "advises"},
                "relation_map_only": True,
                "emit_inverse": True,
            }
        }
        advises = {"source": "person", "target": "institution", "relation": "advises"}
        manifest = _manifest(
            {"rows": {"pipeline": [step]}}, edges=[FORWARD, INVERSE, advises]
        )
        assert "idle_emit_inverse" in _kinds(manifest)


class TestContradictions:
    """Two places disagree: only the author can say which is right."""

    def test_a_step_writing_an_inverse_that_labels_no_edge(self) -> None:
        manifest = _manifest(
            {"rows": {"pipeline": [*VERTICES, FORWARD_STEP, INVERSE_STEP]}},
        )
        (finding,) = audit_inverses(manifest).conflicts()
        assert finding.kind == "orphan_inverse_step"
        assert [str(step) for step in finding.steps] == ["rows:3"]

    def test_a_pair_touched_by_a_conflict_is_reported_conflicting(self) -> None:
        same_side = {**FORWARD, "relation": "employs"}
        report = audit_inverses(_manifest(edges=[FORWARD, same_side]))
        assert report.pairs[0].state == "conflicting"
        assert [f.kind for f in report.conflicts()] == ["same_side_pair"]


class TestNotes:
    def test_mirroring_next_to_a_step_of_its_own(self) -> None:
        step = {"edge": {**FORWARD_STEP["edge"], "emit_inverse": True}}
        manifest = _manifest(
            {"rows": {"pipeline": [*VERTICES, step, INVERSE_STEP]}},
            edges=[FORWARD, INVERSE],
        )
        (note,) = audit_inverses(manifest).notes()
        assert note.kind == "double_fed"


class TestTheReport:
    def test_a_manifest_that_does_not_load_can_still_be_audited(self) -> None:
        directed_knows = {"source": "person", "target": "person", "relation": "knows"}
        payload = _payload(edges=[FORWARD, directed_knows], symmetric=["knows"])
        with pytest.raises(ValueError, match="must be undirected"):
            GraphManifest.from_dict(payload).finish_init()

        report = audit_inverses(manifest_for_audit(payload))
        assert [f.kind for f in report.repairable()] == ["symmetric_on_directed"]

    def test_what_a_change_introduced(self) -> None:
        before = audit_inverses(
            _manifest(
                {"rows": {"pipeline": [*VERTICES, FORWARD_STEP]}},
                edges=[FORWARD, INVERSE],
            )
        )
        after = audit_inverses(
            _manifest(
                {"rows": {"pipeline": [*VERTICES, FORWARD_STEP], "infer_edges": False}},
                edges=[FORWARD, INVERSE],
            )
        )
        assert [f.kind for f in after.introduced_since(before)] == ["unfed_inverse"]
        assert before.introduced_since(after) == []

    def test_the_text_report_groups_findings_by_severity(self) -> None:
        manifest = _manifest(
            {"rows": {"pipeline": [*VERTICES, FORWARD_STEP], "infer_edges": False}},
            edges=[FORWARD, INVERSE],
        )
        lines = audit_inverses(manifest).to_lines()
        assert lines[0] == "employed_by <-> employs: materialized (2/2 mirrored)"
        assert "    fed in rows: none" in lines
        assert "repairable (1):" in lines
        assert "      at rows:2" in lines

    def test_a_manifest_without_a_schema_has_nothing_to_report(self) -> None:
        manifest = GraphManifest.from_dict(
            {"ingestion_model": {"resources": [{"name": "rows", "pipeline": []}]}}
        )
        assert audit_inverses(manifest).to_lines() == ["no declared inverses"]

    def test_a_manifest_that_loads_is_audited_as_loaded(self) -> None:
        payload = _payload({"rows": {"pipeline": [*VERTICES]}})
        assert audit_inverses(manifest_for_audit(payload)).pairs[0].state == "declared"


class TestTheProfile:
    def test_it_is_registered_beside_the_others(self) -> None:
        assert "inverses" in {name for name, _version in list_profiles()}

    def test_a_clean_manifest_passes(self) -> None:
        report = check_manifest(_manifest(), profile="inverses")
        assert report.status == "pass"

    def test_a_contradiction_fails_and_an_omission_warns(self) -> None:
        same_side = {**FORWARD, "relation": "employs"}
        failed = check_manifest(
            _manifest(edges=[FORWARD, same_side]), profile="inverses"
        )
        assert failed.status == "fail"
        assert [f.detail["kind"] for f in failed.errors()] == ["same_side_pair"]

        warned = check_manifest(
            _manifest(
                {"rows": {"pipeline": [*VERTICES, FORWARD_STEP], "infer_edges": False}},
                edges=[FORWARD, INVERSE],
            ),
            profile="inverses",
        )
        assert warned.status == "warn"
        assert warned.ok

    def test_a_manifest_with_no_inverses_is_not_applicable(self) -> None:
        report = check_manifest(_manifest(inverses=[]), profile="inverses")
        assert report.status == "not_applicable"
