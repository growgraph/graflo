"""One equivalence drawn as four small graphs: members, attributes, identity, lookups.

Asserts against the networkx graph from ``build_equivalence_view``; the
writer tests write ``.dot`` files, which need no layout run.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from suthing import FileHandle

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution.ops import (
    CanonicalMap,
    MergeManifestsOp,
    PropertyEquivalence,
    RelationEquivalence,
    VertexEquivalence,
)
from graflo.architecture.evolution.preview import MergePreview, preview_merge
from graflo.architecture.evolution.preview_declared import declared_preview
from graflo.plot.equivalence import (
    VIEWS,
    build_equivalence_view,
    plot_equivalence,
    plot_equivalences,
    select_targets,
)
from test.architecture.test_merge_manifests import (
    _left_client_manifest,
    _right_customer_manifest,
)

ROOT = Path(__file__).resolve().parents[2] / "examples"
UNION = ROOT / "20-manifest-union"
ALIGNMENT = ROOT / "21-router-union-alignment"


def _preview(directory: Path, op: str) -> MergePreview:
    return preview_merge(
        GraphManifest.from_config(
            FileHandle.load(directory / "manifest_maintenance.yaml")
        ),
        GraphManifest.from_config(FileHandle.load(directory / "manifest_sensors.yaml")),
        MergeManifestsOp.model_validate(FileHandle.load(directory / op)),
    )


@pytest.fixture(scope="module")
def preview() -> MergePreview:
    """``Asset`` + ``Device`` into ``Machine``: a derived branch, then local keys."""
    return _preview(UNION, "merge.yaml")


@pytest.fixture(scope="module")
def vocabulary() -> MergePreview:
    """A vocabulary joins ``ProductionLine`` to the declared ``Machine`` ~ ``Device``."""
    return _preview(ALIGNMENT, "merge_vocabulary.yaml")


@pytest.fixture(scope="module")
def boundary() -> MergePreview:
    """Four members, each keyed on its own key; Firm's key is renamed by a vocabulary."""
    fixtures = Path(__file__).resolve().parents[1] / "data" / "merge_union"
    left = GraphManifest.from_config(FileHandle.load(fixtures / "manifest_a.yaml"))
    right = GraphManifest.from_config(FileHandle.load(fixtures / "manifest_b.yaml"))
    left.finish_init()
    right.finish_init()
    vocabulary = CanonicalMap.model_validate(
        FileHandle.load(fixtures / "canonical_map.yaml")
    )
    return preview_merge(
        left,
        right,
        MergeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(
                    left=["Firm", "Shop"],
                    right=["Org", "Branch"],
                    identity=["company_id", "shop_id", "org_id", "branch_id"],
                )
            ],
            canonical_maps={"left": vocabulary},
            origins={"left": "source_a", "right": "source_b"},
        ),
    )


@pytest.fixture(scope="module")
def with_relation() -> MergePreview:
    """``Client`` ~ ``Customer`` into ``Person``, keyed on email; ``places`` ~ ``billed``."""
    return preview_merge(
        _left_client_manifest(),
        _right_customer_manifest(),
        MergeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(
                    left="Client",
                    right="Customer",
                    into="Person",
                    properties=[
                        PropertyEquivalence(
                            left="email", right="email_addr", into="email"
                        )
                    ],
                    identity=["email"],
                )
            ],
            relation_equivalences=[
                RelationEquivalence(left="places", right="billed", into="activity")
            ],
        ),
    )


def _view(preview: MergePreview, view, target="vertex-cluster-0"):
    graph = build_equivalence_view(preview, target, view)
    assert graph is not None, view
    return graph


def _label(graph, preview_id: str) -> str:
    return next(
        d["label"]
        for _, d in graph.nodes(data=True)
        if d.get("preview_id") == preview_id
    )


def _edges(graph, kind: str | None = None) -> list[tuple[str, str, dict]]:
    return [
        (graph.nodes[u]["preview_id"], graph.nodes[v]["preview_id"], d)
        for u, v, d in graph.edges(data=True)
        if d["kind"] != "order" and (kind is None or d["kind"] == kind)
    ]


def _between(graph, kind: str | None = None) -> set[tuple[str, str]]:
    """Edges as ``(tail, head)`` preview ids, read in the drawn direction."""
    out = set()
    for tail, head, data in _edges(graph, kind):
        out.add((head, tail) if data.get("dir") == "back" else (tail, head))
    return out


# ── a figure is clean ───────────────────────────────────────────────────────


@pytest.mark.parametrize("view", VIEWS)
def test_a_view_carries_no_title_legend_or_note(preview, view):
    graph = _view(preview, view)

    assert "label" not in graph.graph["graph"]
    kinds = {d.get("kind") for _, d in graph.nodes(data=True)}
    assert not kinds & {"legend", "key", "footer"}
    assert all("[" not in str(d.get("label", "")) for _, d in graph.nodes(data=True))


def test_each_view_uses_few_edge_styles(preview):
    for view in VIEWS:
        styles = {d["kind"] for _, _, d in _edges(_view(preview, view))}
        assert len(styles) <= 3, (view, styles)


# ── members ─────────────────────────────────────────────────────────────────


def test_members_are_left_merged_right(preview):
    graph = _view(preview, "members")
    assert _between(graph) == {
        ("left:Asset", "merged:Machine"),
        ("right:Device", "merged:Machine"),
    }


def test_a_member_a_vocabulary_joins_has_a_dashed_edge(vocabulary):
    graph = _view(vocabulary, "members")
    style = {tail: d["kind"] for tail, _, d in _edges(graph)}
    assert style["left:ProductionLine"] == "joined"
    assert style["left:Machine"] == "declared"


def test_a_relation_equivalence_has_only_a_members_view(with_relation):
    relation = next(c.id for c in with_relation.clusters if c.kind == "relation")
    graph = _view(with_relation, "members", relation)

    assert _between(graph) == {
        ("left:places", "merged:activity"),
        ("right:billed", "merged:activity"),
    }
    for view in ("attributes", "identity", "lookups"):
        assert build_equivalence_view(with_relation, relation, view) is None


# ── attributes ──────────────────────────────────────────────────────────────


def test_attributes_land_on_the_merged_type_by_kind(preview):
    graph = _view(preview, "attributes")

    assert _between(graph, "vocabulary") == {
        ("right:Device.serial", "merged:Machine.serial_number")
    }
    assert ("left:Asset.name", "merged:Machine.name") in _between(graph, "kept")


def test_attributes_sit_in_boxes_named_after_their_type(preview):
    graph = _view(preview, "attributes")
    boxes = {label for label, _side, _members in graph.graph["boxes"].values()}
    assert boxes == {"Asset", "Machine", "Device"}
    assert _label(graph, "left:Asset.serial_number") == "serial_number"


def test_a_declared_equivalence_that_keeps_the_name_is_drawn_as_declared(
    with_relation,
):
    graph = _view(with_relation, "attributes")
    assert ("left:Client.email", "merged:Person.email") in _between(graph, "declared")


def test_lookups_and_identity_rows_stay_out_of_the_attributes_view(preview):
    ids = {d["preview_id"] for _, d in _view(preview, "attributes").nodes(data=True)}
    assert "merged:Machine.maintenance__asset_id" not in ids
    assert "merged:Machine.match_key" not in ids


# ── identity ────────────────────────────────────────────────────────────────


def test_raw_fields_feed_branches_that_feed_the_digest(preview):
    graph = _view(preview, "identity")
    sources = {
        (tail.rsplit(".", 1)[-1], d["label"]) for tail, _, d in _edges(graph, "source")
    }
    assert sources == {
        ("serial_number", "normalized_key(serial_number)"),
        ("serial", "normalized_key(serial)"),
        ("asset_id", "tag maintenance"),
        ("device_id", "tag sensors"),
    }
    keys = {head for _, head, _ in _edges(graph, "key")}
    assert keys == {"merged:Machine.id"}


def test_branches_are_ordered_and_say_their_value_format(preview):
    graph = _view(preview, "identity")
    (rank,) = graph.graph["ranks"]
    assert [graph.nodes[n]["label"] for n in rank] == [
        "#1 match_key",
        "#2 local_key\\ntag:value",
    ]
    (order,) = [(u, v) for u, v, d in graph.edges(data=True) if d["kind"] == "order"]
    assert order == (rank[0], rank[1])


def test_a_member_keyed_source_names_its_member(vocabulary):
    labels = {d["label"] for _, _, d in _edges(_view(vocabulary, "identity"), "source")}

    assert "Machine: normalized_key(serial_number)" in labels
    assert "ProductionLine: tag maintenance:ProductionLine · auto" in labels
    assert "normalized_key(serial)" in labels, "an unkeyed source has no prefix"


def test_a_property_branch_reads_the_members_own_attribute(boundary):
    graph = _view(boundary, "identity")
    assert ("left:Firm.firm_id", "vertex-cluster-0#branch:1:company_id") in {
        (t, h) for t, h, _ in _edges(graph, "source")
    }


def test_a_natural_key_feeds_the_attribute_not_a_digest(with_relation):
    graph = _view(with_relation, "identity")
    assert {head for _, head, _ in _edges(graph, "key")} == {"merged:Person.email"}


def test_no_declared_identity_draws_no_identity_view():
    preview = _preview(UNION, "merge_no_identity.yaml")
    assert build_equivalence_view(preview, 0, "identity") is None


# ── lookups ─────────────────────────────────────────────────────────────────


def test_each_member_key_becomes_a_lookup_in_its_key_space(preview):
    graph = _view(preview, "lookups")
    lookups = {(t, h, d["label"]) for t, h, d in _edges(graph, "lookup")}
    assert lookups == {
        ("left:Asset.asset_id", "merged:Machine.maintenance__asset_id", "maintenance"),
        ("merged:Machine.sensors__device_id", "right:Device.device_id", "sensors"),
    }


def test_a_lookup_is_named_as_its_member_spells_it(boundary):
    graph = _view(boundary, "lookups")
    assert ("left:Firm.firm_id", "merged:Company.company_id") in _between(graph)


# ── the op alone ────────────────────────────────────────────────────────────


def test_a_declaration_alone_draws_members_and_identity():
    op = MergeManifestsOp.model_validate(FileHandle.load(UNION / "merge.yaml"))
    declared = declared_preview(op)

    assert _between(_view(declared, "members", 0)) == {
        ("left:Asset", "merged:Machine"),
        ("right:Device", "merged:Machine"),
    }
    assert len(_edges(_view(declared, "identity", 0), "source")) == 4
    assert build_equivalence_view(declared, 0, "lookups") is None


# ── files, index and summary ────────────────────────────────────────────────


def test_every_view_is_written_with_an_index_and_a_summary(preview, tmp_path):
    index = plot_equivalences(preview, tmp_path, output_format="dot")

    data = json.loads(index.read_text())
    (entry,) = data["equivalences"]
    assert entry["views"] == {view: f"vertex-Machine.{view}.dot" for view in VIEWS}
    assert entry["declarations"] == ["vertex_equivalences[0]"]
    assert [f["kind"] for f in entry["findings"]] == ["lookup_demotion"] * 2
    assert entry["lookups"][0]["key_space"] == "maintenance"
    for name in entry["views"].values():
        assert (tmp_path / name).read_text().startswith("digraph")

    summary = (tmp_path / "summary.md").read_text()
    assert "## Machine: Asset ~ Device" in summary
    assert "![identity](vertex-Machine.identity.dot)" in summary
    assert "**note** `lookup_demotion`" in summary


def test_a_view_with_nothing_to_show_is_not_written(tmp_path):
    preview = _preview(UNION, "merge_no_identity.yaml")
    written = plot_equivalence(preview, 0, tmp_path, output_format="dot")
    assert set(written) == {"members", "attributes"}


def test_views_can_be_chosen(preview, tmp_path):
    written = plot_equivalence(
        preview, 0, tmp_path, views=("identity",), output_format="dot"
    )
    assert list(written) == ["identity"]


def test_boxes_and_ranks_reach_the_dot_source(preview, tmp_path):
    written = plot_equivalence(
        preview, 0, tmp_path, views=("identity",), output_format="dot"
    )
    source = written["identity"].read_text()
    assert 'subgraph "cluster_resource_assets"' in source
    assert '"rank"="same"' in source
    assert '"boxes"' not in source and '"ranks"' not in source


def test_select_by_name_position_or_cluster_id(preview):
    assert select_targets(preview, []) == ["vertex-cluster-0"]
    assert select_targets(preview, ["Machine"]) == ["vertex-cluster-0"]
    assert select_targets(preview, ["0"]) == ["vertex-cluster-0"]
    assert select_targets(preview, ["vertex-*"]) == ["vertex-cluster-0"]
    assert select_targets(preview, ["Nothing"]) == []
