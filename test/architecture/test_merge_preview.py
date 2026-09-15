"""The merge preview: every conflict at once, and agreement with merge.

The invariant these are really about: **whatever merge refuses, the
structural pass has a finding of a matching kind for.** The preview re-walks
the declarations through the same checks merge uses, one unit at a time, so
if the two ever disagree one of them is wrong — and since merge is the one
that decides, a disagreement is a defect here.
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
    VertexEquivalence,
)
from graflo.architecture.evolution.preview import (
    MergeOutcome,
    MergePreview,
    expected_kinds,
    kind_for_check,
    preview_merge,
)

EXAMPLE = (
    Path(__file__).resolve().parents[2] / "examples" / "19-union-canonical-equivalence"
)


def _load(name: str) -> GraphManifest:
    manifest = GraphManifest.from_config(FileHandle.load(EXAMPLE / name))
    manifest.finish_init()
    return manifest


@pytest.fixture(scope="module")
def left() -> GraphManifest:
    """Source A: ``Firm`` and ``Shop``, each on its own natural key."""
    return _load("manifest_a.yaml")


@pytest.fixture(scope="module")
def right() -> GraphManifest:
    """Source B: ``Org`` and ``Branch``."""
    return _load("manifest_b.yaml")


@pytest.fixture(scope="module")
def canonical_map() -> CanonicalMap:
    """``Firm`` is ``Company``; ``firm_id`` is ``company_id``."""
    return CanonicalMap.model_validate(FileHandle.load(EXAMPLE / "canonical_map.yaml"))


def _boundary(canonical_map: CanonicalMap, **updates) -> MergeManifestsOp:
    """The n-ary boundary cluster: ``{Firm, Shop} ~ {Org, Branch}``."""
    payload: dict = {
        "vertex_equivalences": [
            VertexEquivalence(left=["Firm", "Shop"], right=["Org", "Branch"])
        ],
        "allow_merges": True,
        "canonical_maps": {"left": canonical_map},
        # A declared key, standing in for the example's identity alignment:
        # without one the members disagree, which is its own test below.
        **updates,
    }
    return MergeManifestsOp(**payload)


def _keyed(canonical_map: CanonicalMap, **updates) -> MergeManifestsOp:
    """The boundary cluster with its identity settled, so it merges."""
    return MergeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(
                left=["Firm", "Shop"],
                right=["Org", "Branch"],
                identity=["company_id"],
            )
        ],
        allow_merges=True,
        canonical_maps={"left": canonical_map},
        **updates,
    )


# ── the graph ───────────────────────────────────────────────────────────────


def test_the_declaration_graph_carries_both_sides_and_their_attributes(
    left, right, canonical_map
):
    preview = preview_merge(left, right, _keyed(canonical_map), attempt=False)

    classes = {n.id for n in preview.nodes if n.kind == "class"}
    assert classes == {"left:Firm", "left:Shop", "right:Org", "right:Branch"}
    assert {n.name for n in preview.attributes_of("left:Firm")} == {
        "firm_id",
        "secondary_key",
        "shared_raw",
    }
    firm_id = preview.node("left:Firm.firm_id")
    shared_raw = preview.node("left:Firm.shared_raw")
    assert firm_id is not None and shared_raw is not None
    assert firm_id.identity, "firm_id is Firm's natural key"
    assert not shared_raw.identity


def test_the_composed_class_carries_the_union_of_its_members_attributes(
    left, right, canonical_map
):
    preview = preview_merge(left, right, _keyed(canonical_map), attempt=False)

    assert {n.name for n in preview.attributes_of("merged:Company")} == {
        # firm_id arrives under its canonical name
        "company_id",
        "secondary_key",
        "shared_raw",
        "shop_id",
        "org_id",
        "branch_id",
    }


def test_one_cluster_with_a_member_edge_per_member(left, right, canonical_map):
    preview = preview_merge(left, right, _keyed(canonical_map), attempt=False)

    (cluster,) = preview.clusters
    assert cluster.into == "Company"
    assert cluster.left == ["Firm", "Shop"]
    assert cluster.right == ["Org", "Branch"]
    assert cluster.declared_identity

    members = {(e.source, e.target) for e in preview.edges if e.kind == "member"}
    assert members == {
        ("left:Firm", "merged:Company"),
        ("left:Shop", "merged:Company"),
        ("right:Org", "merged:Company"),
        ("right:Branch", "merged:Company"),
    }


def test_a_declared_attribute_rename_is_an_attribute_level_edge(
    left, right, canonical_map
):
    preview = preview_merge(left, right, _keyed(canonical_map), attempt=False)

    renames = [e for e in preview.edges if e.kind == "property_map"]
    assert [(e.source, e.target) for e in renames] == [
        ("left:Firm.firm_id", "merged:Company.company_id")
    ]


def test_every_edge_lands_on_a_node_the_preview_declares(left, right, canonical_map):
    """A picture cannot draw an edge to a name that is not in the graph."""
    for op in (
        _keyed(canonical_map),
        _boundary(canonical_map),
        MergeManifestsOp(
            vertex_equivalences=[VertexEquivalence(left="Frim", right="Org")]
        ),
    ):
        preview = preview_merge(left, right, op, attempt=False)
        ids = {n.id for n in preview.nodes}
        for edge in preview.edges:
            assert edge.source in ids, edge
            assert edge.target in ids, edge
        for node in preview.nodes:
            if node.owner is not None:
                assert node.owner in ids, node


# ── the happy path is silent ────────────────────────────────────────────────


def test_a_clean_compose_reports_nothing(left, right, canonical_map):
    preview = preview_merge(left, right, _keyed(canonical_map))

    assert preview.outcome.status == "merged"
    assert preview.outcome.vertices == 1
    assert not preview.findings
    assert not preview.blocking
    assert not preview.refused


def test_each_dangling_entry_is_its_own_finding(left, right):
    """The preview classifies one entry at a time; merge batches them.

    Merge refuses a whole side at once, which is what an author of a large
    map wants from a refusal. The preview must not inherit that: a finding is
    per declaration, and collapsing four entries into one would cost it the
    only number it exists to report.
    """
    op = MergeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(left="Firm", right="Org", into="Company")
        ],
        canonical_maps={
            "left": CanonicalMap(
                vertices={"Nope": "Whatever", "AlsoNope": "Something"},
                relations={"never": "ever"},
            )
        },
    )
    preview = preview_merge(left, right, op, attempt=False)

    dangling = [f for f in preview.findings if f.kind == "dangling"]
    assert len(dangling) == 3
    # A dangling source names nothing, so it is no node of the declaration
    # graph and the finding carries it in the message rather than in `nodes`.
    for name in ("Nope", "AlsoNope", "never"):
        assert sum(repr(name) in f.message for f in dangling) == 1


def test_not_attempting_leaves_the_outcome_open(left, right, canonical_map):
    preview = preview_merge(left, right, _keyed(canonical_map), attempt=False)

    assert preview.outcome.status == "not_attempted"
    assert preview.outcome.message is None
    assert expected_kinds(preview.outcome) == frozenset()


# ── one refusal at a time from merge, all of them from the preview ────────


def test_an_unnamed_cluster_is_found_and_refused(left, right):
    """No `into`, no map, no shared spelling: the cluster has no name."""
    op = MergeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(left=["Firm", "Shop"], right=["Org", "Branch"])
        ],
        allow_merges=True,
    )
    preview = preview_merge(left, right, op)

    assert preview.outcome.status == "refused"
    assert preview.outcome.check == "unnamed vertex cluster"
    (finding,) = [f for f in preview.findings if f.severity == "refusal"]
    assert finding.kind == "unnamed_cluster"
    assert set(finding.nodes) == {
        "left:Firm",
        "left:Shop",
        "right:Org",
        "right:Branch",
    }


def test_a_disagreeing_map_is_found_and_refused(left, right, canonical_map):
    """The map says ``Firm`` is ``Company``; the cluster names it ``Party``."""
    op = MergeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(
                left=["Firm", "Shop"], right=["Org", "Branch"], into="Party"
            )
        ],
        allow_merges=True,
        canonical_maps={"left": canonical_map},
    )
    preview = preview_merge(left, right, op)

    assert preview.outcome.error_type == "MergeCanonicalConflictError"
    assert "disagreement" in {f.kind for f in preview.findings}
    refusal = next(f for f in preview.findings if f.severity == "refusal")
    assert "left:Firm" in refusal.nodes


def test_a_forgotten_member_carries_the_cluster_that_would_settle_it(
    left, right, canonical_map
):
    """The map sends ``Shop`` to ``Company`` without declaring it a member."""
    extended = canonical_map.model_copy(
        update={
            "vertices": {**canonical_map.vertices, "Shop": "Company"},
            "allow_merges": True,
        }
    )
    op = MergeManifestsOp(
        vertex_equivalences=[VertexEquivalence(left="Firm", right=["Org", "Branch"])],
        allow_merges=True,
        canonical_maps={"left": extended},
    )
    preview = preview_merge(left, right, op)

    refusal = next(f for f in preview.findings if f.severity == "refusal")
    assert refusal.kind == "incomplete"
    assert refusal.completion is not None
    assert refusal.completion["kind"] == "extend_cluster"
    assert set(refusal.nodes) == {"left:Shop", "merged:Company"}


def test_overlapping_clusters_report_every_problem_not_only_the_first(
    left, right, canonical_map
):
    """The point of the preview: merge stops at one, this does not.

    Two declarations claim ``right:Org``. Merge refuses on that and never
    reaches the identity of either merged class; the preview reports all
    three, so one run tells the author everything to fix.
    """
    op = MergeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(left="Firm", right=["Org", "Branch"]),
            VertexEquivalence(left="Shop", right="Org", into="Party"),
        ],
        allow_merges=True,
        canonical_maps={"left": canonical_map},
    )
    preview = preview_merge(left, right, op)

    assert preview.outcome.error_type == "ClusterConflictError"
    kinds = [f.kind for f in preview.findings]
    assert kinds.count("cluster_overlap") == 1, "the refusal folds into its finding"
    assert kinds.count("identity_disagreement") == 2, (
        "merge never reached either; the preview did"
    )
    overlap = next(f for f in preview.findings if f.kind == "cluster_overlap")
    assert "right:Org" in overlap.nodes


def test_a_shared_name_under_error_is_refused_with_its_equivalences(
    left, right, canonical_map
):
    """Both sides canonicalize a class to ``Outlet`` and no cluster merges it."""
    left_map = canonical_map.model_copy(
        update={
            "vertices": {**canonical_map.vertices, "Shop": "Outlet"},
            "properties": {
                **canonical_map.properties,
                "Shop": {"shop_id": "outlet_id"},
            },
        }
    )
    right_map = CanonicalMap(
        vertices={"Branch": "Outlet"},
        properties={
            "Branch": {"branch_id": "outlet_id"},
            "Org": {"org_id": "company_id"},
        },
    )
    op = MergeManifestsOp(
        vertex_equivalences=[VertexEquivalence(left="Firm", right="Org")],
        canonical_maps={"left": left_map, "right": right_map},
    )
    preview = preview_merge(left, right, op)

    refusal = next(f for f in preview.findings if f.severity == "refusal")
    assert refusal.kind == "name_collision"
    assert refusal.completion is not None
    assert "merged:Outlet" in refusal.nodes


def test_union_right_composes_the_shared_name_through_a_synthesized_cluster(
    left, right, canonical_map
):
    left_map = canonical_map.model_copy(
        update={
            "vertices": {**canonical_map.vertices, "Shop": "Outlet"},
            "properties": {
                **canonical_map.properties,
                "Shop": {"shop_id": "outlet_id"},
            },
        }
    )
    right_map = CanonicalMap(
        vertices={"Branch": "Outlet"},
        properties={
            "Branch": {"branch_id": "outlet_id"},
            "Org": {"org_id": "company_id"},
        },
    )
    op = MergeManifestsOp(
        vertex_equivalences=[VertexEquivalence(left="Firm", right="Org")],
        canonical_maps={"left": left_map, "right": right_map},
        name_conflict="union_right",
    )
    preview = preview_merge(left, right, op)

    assert preview.outcome.status == "merged"
    assert not preview.blocking
    assert any(c.synthesized for c in preview.clusters), (
        "the shared name becomes a real cluster"
    )


def test_an_unknown_member_becomes_a_ghost_with_a_spelling_hint(left, right):
    op = MergeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(left="Frim", right="Org", into="Company")
        ]
    )
    preview = preview_merge(left, right, op, attempt=False)

    ghost = preview.node("left:Frim")
    assert ghost is not None
    assert ghost.kind == "ghost"
    assert not ghost.exists
    finding = next(f for f in preview.findings if f.kind == "unknown_member")
    assert "left:Frim" in finding.nodes


def test_an_unresolved_identity_is_found_before_compose_reaches_it(
    left, right, canonical_map
):
    preview = preview_merge(left, right, _boundary(canonical_map), attempt=False)

    finding = next(f for f in preview.findings if f.kind == "identity_disagreement")
    assert "merged:Company" in finding.nodes
    assert "left:Firm" in finding.nodes


def test_an_identity_alignment_settles_it(left, right, canonical_map):
    """A class whose identity an alignment supplies is not a disagreement."""
    from graflo.architecture.evolution.ops import (
        AlignmentAttribute,
        DerivationSpec,
        IdentityAlignment,
    )

    op = _boundary(
        canonical_map,
        identity_alignments=[
            IdentityAlignment(
                vertex="Company",
                attributes=[
                    AlignmentAttribute(
                        name="match_key",
                        sources={"r_a": DerivationSpec(input=["shared_raw"])},
                    )
                ],
            )
        ],
    )
    preview = preview_merge(left, right, op, attempt=False)

    assert "identity_disagreement" not in {f.kind for f in preview.findings}


def test_a_property_equivalence_flagged_as_identity_settles_it(
    left, right, canonical_map
):
    op = MergeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(
                left=["Firm", "Shop"],
                right=["Org", "Branch"],
                properties=[
                    PropertyEquivalence(
                        left={"Firm": "shared_raw", "Shop": "shared_raw"},
                        right={"Org": "shared_raw", "Branch": "shared_raw"},
                        into="match_key",
                        identity=True,
                    )
                ],
            )
        ],
        allow_merges=True,
        canonical_maps={"left": canonical_map},
    )
    preview = preview_merge(left, right, op, attempt=False)

    assert "identity_disagreement" not in {f.kind for f in preview.findings}


# ── the invariant ───────────────────────────────────────────────────────────


def _cases(canonical_map: CanonicalMap) -> dict[str, MergeManifestsOp]:
    extended = canonical_map.model_copy(
        update={
            "vertices": {**canonical_map.vertices, "Shop": "Company"},
            "allow_merges": True,
        }
    )
    return {
        "clean": _keyed(canonical_map),
        "unnamed": MergeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(left=["Firm", "Shop"], right=["Org", "Branch"])
            ],
            allow_merges=True,
        ),
        "disagreeing-map": MergeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(
                    left=["Firm", "Shop"], right=["Org", "Branch"], into="Party"
                )
            ],
            allow_merges=True,
            canonical_maps={"left": canonical_map},
        ),
        "overlap": MergeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(left="Firm", right=["Org", "Branch"]),
                VertexEquivalence(left="Shop", right="Org", into="Party"),
            ],
            allow_merges=True,
            canonical_maps={"left": canonical_map},
        ),
        "forgotten-member": MergeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(left="Firm", right=["Org", "Branch"])
            ],
            allow_merges=True,
            canonical_maps={"left": extended},
        ),
        "unknown-member": MergeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(left="Frim", right="Org", into="Company")
            ]
        ),
        "dangling": MergeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(left="Firm", right="Org", into="Company")
            ],
            canonical_maps={"left": CanonicalMap(vertices={"Nope": "Whatever"})},
        ),
        "unresolved-identity": _boundary(canonical_map),
    }


# ── what the schema union itself refuses ────────────────────────────────────
#
# These cannot be expressed against the example manifests: `manifest_a` and
# `manifest_b` are untyped, natural-identity and carry no edges, so none of the
# identity modes, types, units or edge kinds below has anywhere to live. They
# get purpose-built pairs instead, and the invariant covers them all the same.


def _schema_manifest(
    name: str, vertices: list[dict], edges: list[dict]
) -> GraphManifest:
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": name, "version": "1.0.0"},
                "graph": {
                    "vertex_config": {"vertices": vertices},
                    "edge_config": {"edges": edges},
                },
            }
        }
    )
    manifest.finish_init()
    return manifest


def _pair(
    left_vertex: dict,
    right_vertex: dict,
    *,
    left_edges: list[dict] | None = None,
    right_edges: list[dict] | None = None,
    equivalences: list[VertexEquivalence] | None = None,
    name_conflict: str = "error",
) -> tuple[GraphManifest, GraphManifest, MergeManifestsOp]:
    """One cluster over one vertex per side, and the op that merges them."""
    return (
        _schema_manifest("union-left", [left_vertex], left_edges or []),
        _schema_manifest("union-right", [right_vertex], right_edges or []),
        MergeManifestsOp(
            vertex_equivalences=equivalences
            or [
                VertexEquivalence(
                    left=left_vertex["name"], right=right_vertex["name"], into="Party"
                )
            ],
            allow_merges=True,
            name_conflict=name_conflict,
        ),
    )


def _natural(name: str, **overrides) -> dict:
    return {"name": name, "properties": ["p", "q"], "identity": ["p"], **overrides}


def _funnel(name: str, branch_id: str, field: str) -> dict:
    return {
        "name": name,
        "properties": ["p", "q"],
        "identity_funnel": {"branches": [{"id": branch_id, "fields": [field]}]},
    }


#: Two vertices per side that a pair of clusters renames onto one pair of
#: endpoints, which is what makes two edges collide by ``edge_id`` at all.
_EDGE_LEFT = [_natural("A"), {"name": "X", "properties": ["k"], "identity": ["k"]}]
_EDGE_RIGHT = [_natural("B"), {"name": "Y", "properties": ["k"], "identity": ["k"]}]
_EDGE_CLUSTERS = [
    VertexEquivalence(left="A", right="B", into="Party"),
    VertexEquivalence(left="X", right="Y", into="Place"),
]


def _edge_pair(
    left_edge: dict, right_edge: dict
) -> tuple[GraphManifest, GraphManifest, MergeManifestsOp]:
    """``union_right`` so the shared relation name is a cluster, not a collision.

    Under the default ``error`` the merge refuses on the relation name before
    the union ever folds the two edges, which tests the wrong rule.
    """
    return (
        _schema_manifest("union-left", _EDGE_LEFT, [left_edge]),
        _schema_manifest("union-right", _EDGE_RIGHT, [right_edge]),
        MergeManifestsOp(
            vertex_equivalences=_EDGE_CLUSTERS,
            allow_merges=True,
            name_conflict="union_right",
        ),
    )


def _union_cases() -> dict[str, tuple[GraphManifest, GraphManifest, MergeManifestsOp]]:
    """One case per rule ``merge_core`` and ``union_field_lists`` refuse on."""
    return {
        "blank-x-assigned": _pair(
            _natural("A", blank=True), _natural("B", assigned=True)
        ),
        "assigned-x-hash": _pair(
            _natural("A", assigned=True), _natural("B", hash_identity_properties=["q"])
        ),
        # The pair neither the kernel nor Vertex.set_identity used to refuse.
        "blank-x-hash": _pair(
            _natural("A", blank=True), _natural("B", hash_identity_properties=["q"])
        ),
        "funnel-x-hash": _pair(
            _funnel("A", "b1", "p"), _natural("B", hash_identity_properties=["q"])
        ),
        "funnel-x-blank": _pair(_funnel("A", "b1", "p"), _natural("B", blank=True)),
        "divergent-funnels": _pair(_funnel("A", "b1", "p"), _funnel("B", "b2", "q")),
        "blank-x-secondary": _pair(
            _natural("A", blank=True),
            _natural("B", secondary_identities=[{"name": "k", "fields": ["q"]}]),
        ),
        "secondary-name-clash": _pair(
            _natural("A", secondary_identities=[{"name": "k", "fields": ["q"]}]),
            _natural("B", secondary_identities=[{"name": "k", "fields": ["p", "q"]}]),
        ),
        "type-clash": _pair(
            {
                "name": "A",
                "properties": [{"name": "p", "type": "STRING"}],
                "identity": ["p"],
            },
            {
                "name": "B",
                "properties": [{"name": "p", "type": "INT"}],
                "identity": ["p"],
            },
        ),
        "unit-clash": _pair(
            {
                "name": "A",
                "properties": ["p", {"name": "s", "semantics": {"unit": "m/s"}}],
                "identity": ["p"],
            },
            {
                "name": "B",
                "properties": ["p", {"name": "s", "semantics": {"unit": "km/h"}}],
                "identity": ["p"],
            },
        ),
        "edge-kind-clash": _edge_pair(
            {"source": "A", "target": "X", "relation": "r"},
            {
                "source": "B",
                "target": "Y",
                "relation": "r",
                "type": "indirect",
                "by": "M",
            },
        ),
        "edge-property-type-clash": _edge_pair(
            {
                "source": "A",
                "target": "X",
                "relation": "r",
                "properties": [{"name": "w", "type": "INT"}],
            },
            {
                "source": "B",
                "target": "Y",
                "relation": "r",
                "properties": [{"name": "w", "type": "STRING"}],
            },
        ),
    }


#: Merges that must succeed *and* draw no objection. The preview reporting a
#: problem where merge has none is the other half of the invariant, and the
#: name-conflict policies are where the union passes can most easily
#: over-report: they rename the right side after the composite relabel, so a
#: pass that models only one of the two sees collisions that do not happen.
def _clean_cases() -> dict[str, tuple[GraphManifest, GraphManifest, MergeManifestsOp]]:
    agreeing_edge_left = {"source": "A", "target": "X", "relation": "r"}
    agreeing_edge_right = {"source": "B", "target": "Y", "relation": "r"}
    cases = {}
    for policy in ("prefix_right", "union_right"):
        cases[f"agreeing-edges-{policy}"] = (
            _schema_manifest("union-left", _EDGE_LEFT, [agreeing_edge_left]),
            _schema_manifest("union-right", _EDGE_RIGHT, [agreeing_edge_right]),
            MergeManifestsOp(
                vertex_equivalences=_EDGE_CLUSTERS,
                allow_merges=True,
                name_conflict=policy,
            ),
        )
    cases["shared-name-prefixed"] = (
        _schema_manifest("union-left", [_natural("Same")], []),
        _schema_manifest("union-right", [_natural("Same")], []),
        MergeManifestsOp(allow_merges=True, name_conflict="prefix_right"),
    )
    cases["untyped-side-gives-way"] = _pair(
        {
            "name": "A",
            "properties": [{"name": "p", "type": "STRING"}],
            "identity": ["p"],
        },
        {"name": "B", "properties": ["p"], "identity": ["p"]},
    )
    return cases


#: Refusals the structural pass is deliberately not asked to anticipate, keyed
#: by exception type, with the reason. An entry here is a documented exemption;
#: anything else that classifies to nothing **fails** the invariant below
#: rather than skipping it, which is how every union refusal stayed invisible
#: while the suite was green.
NOT_ANTICIPATED: dict[str, str] = {}


def _all_cases(
    left: GraphManifest, right: GraphManifest, canonical_map: CanonicalMap
) -> dict[str, tuple[GraphManifest, GraphManifest, MergeManifestsOp]]:
    """Every case as a ``(left, right, op)`` triple.

    The example-backed rows share one pair of manifests; the union rows each
    bring their own, because the example pair cannot express what they test.
    """
    cases: dict[str, tuple[GraphManifest, GraphManifest, MergeManifestsOp]] = {
        name: (left, right, op) for name, op in _cases(canonical_map).items()
    }
    cases.update(_union_cases())
    cases.update(_clean_cases())
    return cases


@pytest.mark.parametrize(
    "case", sorted([*_cases(CanonicalMap()), *_union_cases(), *_clean_cases()])
)
def test_whatever_compose_refuses_the_structural_pass_also_found(
    case, left, right, canonical_map
):
    """The one invariant holding the preview and merge together.

    Both call the same checks, so a refusal the structural pass missed means
    the preview stopped somewhere merge did not — and a preview that says
    "nothing wrong" about a pair merge refuses is worse than no preview.
    """
    manifests = _all_cases(left, right, canonical_map)[case]
    case_left, case_right, op = manifests

    attempted = preview_merge(case_left, case_right, op)
    # The structural pass on its own: what the preview reports when merge is
    # never run, which is how `--plot` on a refused run uses it.
    alone = preview_merge(case_left, case_right, op, attempt=False)
    structural = {f.kind for f in alone.findings if f.severity == "possible"}

    if attempted.outcome.status == "merged":
        assert not [f for f in attempted.findings if f.severity == "refusal"]
        assert not structural, f"{case} merges but the preview objected"
        return

    wanted = expected_kinds(attempted.outcome)
    assert wanted, (
        f"{case}: merge refused with an unclassifiable "
        f"{attempted.outcome.error_type} (check={attempted.outcome.check!r}). "
        "Give the refusal a typed error carrying `check`, or record it in "
        "NOT_ANTICIPATED with the reason it is exempt — an empty expectation "
        "used to skip this assertion, which is how a whole class of refusals "
        f"stayed invisible. Message: {attempted.outcome.message}"
    )
    assert wanted & structural, (
        f"{case}: merge refused with {sorted(wanted)}, "
        f"the structural pass saw {sorted(structural)}"
    )


@pytest.mark.parametrize("case", sorted(_cases(CanonicalMap())))
def test_a_preview_survives_a_round_trip_through_json(case, left, right, canonical_map):
    """It is an API payload as much as a picture, so it has to serialize."""
    preview = preview_merge(left, right, _cases(canonical_map)[case])

    payload = preview.to_dict()
    restored = MergePreview.model_validate(json.loads(json.dumps(payload)))

    assert restored.to_dict() == payload
    assert len(restored.nodes) == len(preview.nodes)
    assert restored.outcome.status == preview.outcome.status


# ── classification ──────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("check", "expected"),
    [
        ("vertex disagreement", "disagreement"),
        ("relation disagreement", "disagreement"),
        ("ambiguous vertex member", "ambiguity"),
        ("unnamed vertex cluster", "unnamed_cluster"),
        ("dangling entry", "dangling"),
        ("vertex joining a merged class", "incomplete"),
        ("vertex name collision", "name_collision"),
        # Longest match wins, or this would read as a plain collision.
        ("property rename collision", "property_collision"),
        ("property re-target", "property_retarget"),
        ("unknown property", "unknown_property"),
        ("cluster overlap", "cluster_overlap"),
        # What the schema union refuses.
        ("field type conflict", "type_conflict"),
        ("field units conflict", "unit_conflict"),
        ("vertex identity mode conflict", "identity_mode_conflict"),
        ("vertex identity funnel conflict", "identity_funnel_conflict"),
        ("vertex secondary identity conflict", "secondary_identity_conflict"),
        # Longest match again: this must not read as the bare `disagreement`.
        ("edge type disagreement", "edge_conflict"),
    ],
)
def test_a_refusals_check_phrase_names_its_finding_kind(check, expected):
    assert kind_for_check(check) == expected


def test_an_unclassifiable_refusal_asserts_nothing():
    outcome = MergeOutcome(
        status="refused", error_type="ValueError", message="something deeper"
    )
    assert expected_kinds(outcome) == frozenset()


def test_every_type_keyed_expectation_is_a_refusal_class():
    """``_KINDS_BY_TYPE`` is keyed by the exception classes themselves.

    A renamed or deleted refusal class is now a ``NameError`` when this module
    is imported, so the old failure -- a key matching nothing, and refusals
    quietly classifying to nothing -- cannot happen. What still needs asserting
    is the derived index: ``error_type`` arrives as a serialized string, and it
    is looked up by name.
    """
    from graflo.architecture.evolution.preview import (
        _KINDS_BY_TYPE,
        _KINDS_BY_TYPE_NAME,
    )

    for exc in _KINDS_BY_TYPE:
        assert isinstance(exc, type) and issubclass(exc, BaseException), (
            f"_KINDS_BY_TYPE is keyed on {exc!r}, which is not an exception class"
        )

    assert _KINDS_BY_TYPE_NAME == {
        exc.__name__: kinds for exc, kinds in _KINDS_BY_TYPE.items()
    }
    assert len(_KINDS_BY_TYPE_NAME) == len(_KINDS_BY_TYPE), (
        "two refusal classes share a name; the serialized error_type is ambiguous"
    )


def test_every_union_refusal_carries_a_check():
    """A refusal without one classifies to nothing, and the invariant skips it.

    The kernel's own guard: ``merge_vertex_models`` and ``merge_edge_pair``
    raise only typed refusals, and every one of them names its rule. The single
    exception is the empty-input guard, which is a programming error rather
    than an authoring one.
    """
    import inspect

    from graflo.architecture.evolution import merge_core

    source = inspect.getsource(merge_core)
    bare = [
        line.strip()
        for line in source.splitlines()
        if line.strip().startswith("raise ValueError(")
    ]
    assert len(bare) == 1, (
        f"expected only the empty-input guard to raise a bare ValueError, found {bare}"
    )
