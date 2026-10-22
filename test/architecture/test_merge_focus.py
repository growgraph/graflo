"""A merge preview narrowed to one equivalence, one merged attribute, or the rest.

Each focus is a projection of the one preview: whatever it keeps, it keeps
whole -- no edge left pointing at a node it dropped -- and no finding is left
out of every target.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from suthing import FileHandle

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution.ops import (
    MergeManifestsOp,
    PropertyEquivalence,
    RelationEquivalence,
    VertexEquivalence,
)
from graflo.architecture.evolution.preview import MergePreview, preview_merge
from graflo.architecture.evolution.preview_focus import REST, finding_positions
from test.architecture.test_merge_manifests import (
    _left_client_manifest,
    _right_customer_manifest,
)

EXAMPLE = Path(__file__).resolve().parents[2] / "examples" / "20-manifest-union"


def _manifest(path: Path) -> GraphManifest:
    return GraphManifest.from_config(FileHandle.load(path))


@pytest.fixture(scope="module")
def preview() -> MergePreview:
    """Example 20: ``Asset`` + ``Device`` into ``Machine``, a derived key then local keys."""
    op = MergeManifestsOp.model_validate(FileHandle.load(EXAMPLE / "merge.yaml"))
    return preview_merge(
        _manifest(EXAMPLE / "manifest_maintenance.yaml"),
        _manifest(EXAMPLE / "manifest_sensors.yaml"),
        op,
    )


@pytest.fixture(scope="module")
def refused() -> MergePreview:
    """Example 20 without an identity: the members disagree on their key."""
    op = MergeManifestsOp.model_validate(
        FileHandle.load(EXAMPLE / "merge_no_identity.yaml")
    )
    return preview_merge(
        _manifest(EXAMPLE / "manifest_maintenance.yaml"),
        _manifest(EXAMPLE / "manifest_sensors.yaml"),
        op,
    )


@pytest.fixture(scope="module")
def with_relation() -> MergePreview:
    """``Client`` ~ ``Customer`` into ``Person``, and ``places`` ~ ``billed``."""
    op = MergeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(
                left="Client",
                right="Customer",
                into="Person",
                properties=[
                    PropertyEquivalence(left="email", right="email_addr", into="email")
                ],
                identity=["email"],
                allow=["self_relations"],
            )
        ],
        relation_equivalences=[
            RelationEquivalence(left="places", right="billed", into="activity")
        ],
    )
    return preview_merge(_left_client_manifest(), _right_customer_manifest(), op)


def _closed(preview: MergePreview) -> None:
    ids = {n.id for n in preview.nodes}
    for edge in preview.edges:
        assert edge.source in ids and edge.target in ids, edge.id
    for node in preview.nodes:
        assert node.owner is None or node.owner in ids, node.id


# ── the cluster's own record ────────────────────────────────────────────────


def test_a_cluster_records_the_declarations_folded_into_it(preview):
    (cluster,) = preview.clusters
    assert cluster.declarations == [0]
    assert cluster.declared_left == ["Asset"]
    assert cluster.declared_right == ["Device"]


def test_a_cluster_records_what_its_declaration_allows(with_relation):
    person = next(c for c in with_relation.clusters if c.into == "Person")
    assert person.allow == ["self_relations"]


def test_a_local_key_branch_carries_its_separator(preview):
    (cluster,) = preview.clusters
    assert cluster.identity is not None
    seps = {b.id: b.sep for b in cluster.identity.branches}
    assert seps == {"match_key": None, "local_key": ":"}


def test_the_new_fields_survive_a_round_trip_through_json(preview):
    again = MergePreview.model_validate(json.loads(preview.model_dump_json()))
    assert again == preview


# ── targets ─────────────────────────────────────────────────────────────────


def test_the_default_targets_are_the_cluster_then_its_nontrivial_attributes(preview):
    assert preview.focus_targets() == [
        "vertex-cluster-0",
        "merged:Machine.serial_number",
        "merged:Machine.maintenance__asset_id",
        "merged:Machine.sensors__device_id",
        "merged:Machine.match_key",
        "merged:Machine.local_key",
        "merged:Machine.id",
    ]


def test_all_targets_add_the_attributes_a_fusion_alone_decides(preview):
    extra = set(preview.focus_targets("all")) - set(preview.focus_targets())
    assert extra == {"merged:Machine.name", "merged:Machine.model"}


def test_relation_clusters_come_after_the_vertex_ones(with_relation):
    targets = with_relation.focus_targets()
    relation = next(c.id for c in with_relation.clusters if c.kind == "relation")
    assert targets[0].startswith("vertex-cluster-")
    assert targets[-1] == relation


@pytest.mark.parametrize("scope", ["nontrivial", "all"])
def test_every_finding_lands_in_some_target(preview, refused, with_relation, scope):
    for whole in (preview, refused, with_relation):
        held = {
            i
            for target in whole.focus_targets(scope)
            for i in finding_positions(whole, target)
        }
        assert held == set(range(len(whole.findings)))


def test_an_unknown_target_names_the_valid_ones(preview):
    with pytest.raises(KeyError, match="vertex-cluster-0"):
        preview.focus("vertex-cluster-9")


# ── what a focus keeps ──────────────────────────────────────────────────────


def test_every_focus_is_closed(preview, refused, with_relation):
    for whole in (preview, refused, with_relation):
        for target in whole.focus_targets("all"):
            _closed(whole.focus(target))


def test_a_cluster_focus_holds_its_members_resources_and_findings(preview):
    focused = preview.focus("vertex-cluster-0")

    classes = {n.id for n in focused.nodes if n.owner is None}
    assert classes == {
        "left:Asset",
        "right:Device",
        "merged:Machine",
        "resource:assets",
        "resource:devices",
    }
    assert "left:WorkOrder" not in {n.id for n in focused.nodes}
    assert {e.kind for e in focused.edges} >= {
        "member",
        "demotion",
        "fusion",
        "derivation",
        "local_key",
    }
    assert [f.kind for f in focused.findings] == ["lookup_demotion"] * 2
    assert [c.id for c in focused.clusters] == ["vertex-cluster-0"]


def test_an_attribute_focus_holds_only_the_edges_into_it(preview):
    focused = preview.focus("merged:Machine.match_key")

    assert {e.kind for e in focused.edges} == {"derivation"}
    assert {e.target for e in focused.edges} == {"merged:Machine.match_key"}
    rows = {n.id for n in focused.nodes if n.owner is not None}
    assert rows == {
        "merged:Machine.match_key",
        "resource:assets.serial_number",
        "resource:devices.serial",
    }
    assert focused.clusters[0].identity is not None


def test_a_relation_focus_draws_no_vertex_rows(with_relation):
    relation = next(c for c in with_relation.clusters if c.kind == "relation")
    focused = with_relation.focus(relation.id)

    assert {n.id for n in focused.nodes} == {
        "left:places",
        "right:billed",
        "merged:activity",
    }
    assert {e.kind for e in focused.edges} == {"member"}


def test_a_refusal_lands_on_the_cluster_it_is_about(refused):
    focused = refused.focus("vertex-cluster-0")
    assert any(f.severity == "refusal" for f in focused.findings)
    assert REST not in refused.focus_targets()
