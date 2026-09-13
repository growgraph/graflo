"""The compose preview: every conflict at once, and agreement with compose.

The invariant these are really about: **whatever compose refuses, the
structural pass has a finding of a matching kind for.** The preview re-walks
the declarations through the same checks compose uses, one unit at a time, so
if the two ever disagree one of them is wrong — and since compose is the one
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
    ComposeManifestsOp,
    PropertyEquivalence,
    VertexEquivalence,
)
from graflo.architecture.evolution.preview import (
    ComposeOutcome,
    ComposePreview,
    expected_kinds,
    kind_for_check,
    preview_compose,
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


def _boundary(canonical_map: CanonicalMap, **updates) -> ComposeManifestsOp:
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
    return ComposeManifestsOp(**payload)


def _keyed(canonical_map: CanonicalMap, **updates) -> ComposeManifestsOp:
    """The boundary cluster with its identity settled, so it composes."""
    return ComposeManifestsOp(
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
    preview = preview_compose(left, right, _keyed(canonical_map), attempt=False)

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
    preview = preview_compose(left, right, _keyed(canonical_map), attempt=False)

    assert {n.name for n in preview.attributes_of("composed:Company")} == {
        # firm_id arrives under its canonical name
        "company_id",
        "secondary_key",
        "shared_raw",
        "shop_id",
        "org_id",
        "branch_id",
    }


def test_one_cluster_with_a_member_edge_per_member(left, right, canonical_map):
    preview = preview_compose(left, right, _keyed(canonical_map), attempt=False)

    (cluster,) = preview.clusters
    assert cluster.into == "Company"
    assert cluster.left == ["Firm", "Shop"]
    assert cluster.right == ["Org", "Branch"]
    assert cluster.declared_identity

    members = {(e.source, e.target) for e in preview.edges if e.kind == "member"}
    assert members == {
        ("left:Firm", "composed:Company"),
        ("left:Shop", "composed:Company"),
        ("right:Org", "composed:Company"),
        ("right:Branch", "composed:Company"),
    }


def test_a_declared_attribute_rename_is_an_attribute_level_edge(
    left, right, canonical_map
):
    preview = preview_compose(left, right, _keyed(canonical_map), attempt=False)

    renames = [e for e in preview.edges if e.kind == "property_map"]
    assert [(e.source, e.target) for e in renames] == [
        ("left:Firm.firm_id", "composed:Company.company_id")
    ]


def test_every_edge_lands_on_a_node_the_preview_declares(left, right, canonical_map):
    """A picture cannot draw an edge to a name that is not in the graph."""
    for op in (
        _keyed(canonical_map),
        _boundary(canonical_map),
        ComposeManifestsOp(
            vertex_equivalences=[VertexEquivalence(left="Frim", right="Org")]
        ),
    ):
        preview = preview_compose(left, right, op, attempt=False)
        ids = {n.id for n in preview.nodes}
        for edge in preview.edges:
            assert edge.source in ids, edge
            assert edge.target in ids, edge
        for node in preview.nodes:
            if node.owner is not None:
                assert node.owner in ids, node


# ── the happy path is silent ────────────────────────────────────────────────


def test_a_clean_compose_reports_nothing(left, right, canonical_map):
    preview = preview_compose(left, right, _keyed(canonical_map))

    assert preview.outcome.status == "composed"
    assert preview.outcome.vertices == 1
    assert not preview.findings
    assert not preview.blocking
    assert not preview.refused


def test_not_attempting_leaves_the_outcome_open(left, right, canonical_map):
    preview = preview_compose(left, right, _keyed(canonical_map), attempt=False)

    assert preview.outcome.status == "not_attempted"
    assert preview.outcome.message is None
    assert expected_kinds(preview.outcome) == frozenset()


# ── one refusal at a time from compose, all of them from the preview ────────


def test_an_unnamed_cluster_is_found_and_refused(left, right):
    """No `into`, no map, no shared spelling: the cluster has no name."""
    op = ComposeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(left=["Firm", "Shop"], right=["Org", "Branch"])
        ],
        allow_merges=True,
    )
    preview = preview_compose(left, right, op)

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
    op = ComposeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(
                left=["Firm", "Shop"], right=["Org", "Branch"], into="Party"
            )
        ],
        allow_merges=True,
        canonical_maps={"left": canonical_map},
    )
    preview = preview_compose(left, right, op)

    assert preview.outcome.error_type == "ComposeCanonicalConflictError"
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
    op = ComposeManifestsOp(
        vertex_equivalences=[VertexEquivalence(left="Firm", right=["Org", "Branch"])],
        allow_merges=True,
        canonical_maps={"left": extended},
    )
    preview = preview_compose(left, right, op)

    refusal = next(f for f in preview.findings if f.severity == "refusal")
    assert refusal.kind == "incomplete"
    assert refusal.completion is not None
    assert refusal.completion["kind"] == "extend_cluster"
    assert set(refusal.nodes) == {"left:Shop", "composed:Company"}


def test_overlapping_clusters_report_every_problem_not_only_the_first(
    left, right, canonical_map
):
    """The point of the preview: compose stops at one, this does not.

    Two declarations claim ``right:Org``. Compose refuses on that and never
    reaches the identity of either composed class; the preview reports all
    three, so one run tells the author everything to fix.
    """
    op = ComposeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(left="Firm", right=["Org", "Branch"]),
            VertexEquivalence(left="Shop", right="Org", into="Party"),
        ],
        allow_merges=True,
        canonical_maps={"left": canonical_map},
    )
    preview = preview_compose(left, right, op)

    assert preview.outcome.error_type == "ClusterConflictError"
    kinds = [f.kind for f in preview.findings]
    assert kinds.count("cluster_overlap") == 1, "the refusal folds into its finding"
    assert kinds.count("identity_disagreement") == 2, (
        "compose never reached either; the preview did"
    )
    overlap = next(f for f in preview.findings if f.kind == "cluster_overlap")
    assert "right:Org" in overlap.nodes


def test_a_shared_name_under_error_is_refused_with_its_equivalences(
    left, right, canonical_map
):
    """Both sides canonicalize a class to ``Outlet`` and no cluster composes it."""
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
    op = ComposeManifestsOp(
        vertex_equivalences=[VertexEquivalence(left="Firm", right="Org")],
        canonical_maps={"left": left_map, "right": right_map},
    )
    preview = preview_compose(left, right, op)

    refusal = next(f for f in preview.findings if f.severity == "refusal")
    assert refusal.kind == "name_collision"
    assert refusal.completion is not None
    assert "composed:Outlet" in refusal.nodes


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
    op = ComposeManifestsOp(
        vertex_equivalences=[VertexEquivalence(left="Firm", right="Org")],
        canonical_maps={"left": left_map, "right": right_map},
        name_conflict="union_right",
    )
    preview = preview_compose(left, right, op)

    assert preview.outcome.status == "composed"
    assert not preview.blocking
    assert any(c.synthesized for c in preview.clusters), (
        "the shared name becomes a real cluster"
    )


def test_an_unknown_member_becomes_a_ghost_with_a_spelling_hint(left, right):
    op = ComposeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(left="Frim", right="Org", into="Company")
        ]
    )
    preview = preview_compose(left, right, op, attempt=False)

    ghost = preview.node("left:Frim")
    assert ghost is not None
    assert ghost.kind == "ghost"
    assert not ghost.exists
    finding = next(f for f in preview.findings if f.kind == "unknown_member")
    assert "left:Frim" in finding.nodes


def test_an_unresolved_identity_is_found_before_compose_reaches_it(
    left, right, canonical_map
):
    preview = preview_compose(left, right, _boundary(canonical_map), attempt=False)

    finding = next(f for f in preview.findings if f.kind == "identity_disagreement")
    assert "composed:Company" in finding.nodes
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
    preview = preview_compose(left, right, op, attempt=False)

    assert "identity_disagreement" not in {f.kind for f in preview.findings}


def test_a_property_equivalence_flagged_as_identity_settles_it(
    left, right, canonical_map
):
    op = ComposeManifestsOp(
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
    preview = preview_compose(left, right, op, attempt=False)

    assert "identity_disagreement" not in {f.kind for f in preview.findings}


# ── the invariant ───────────────────────────────────────────────────────────


def _cases(canonical_map: CanonicalMap) -> dict[str, ComposeManifestsOp]:
    extended = canonical_map.model_copy(
        update={
            "vertices": {**canonical_map.vertices, "Shop": "Company"},
            "allow_merges": True,
        }
    )
    return {
        "clean": _keyed(canonical_map),
        "unnamed": ComposeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(left=["Firm", "Shop"], right=["Org", "Branch"])
            ],
            allow_merges=True,
        ),
        "disagreeing-map": ComposeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(
                    left=["Firm", "Shop"], right=["Org", "Branch"], into="Party"
                )
            ],
            allow_merges=True,
            canonical_maps={"left": canonical_map},
        ),
        "overlap": ComposeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(left="Firm", right=["Org", "Branch"]),
                VertexEquivalence(left="Shop", right="Org", into="Party"),
            ],
            allow_merges=True,
            canonical_maps={"left": canonical_map},
        ),
        "forgotten-member": ComposeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(left="Firm", right=["Org", "Branch"])
            ],
            allow_merges=True,
            canonical_maps={"left": extended},
        ),
        "unknown-member": ComposeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(left="Frim", right="Org", into="Company")
            ]
        ),
        "dangling": ComposeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(left="Firm", right="Org", into="Company")
            ],
            canonical_maps={"left": CanonicalMap(vertices={"Nope": "Whatever"})},
        ),
        "unresolved-identity": _boundary(canonical_map),
    }


@pytest.mark.parametrize("case", sorted(_cases(CanonicalMap())))
def test_whatever_compose_refuses_the_structural_pass_also_found(
    case, left, right, canonical_map
):
    """The one invariant holding the preview and compose together.

    Both call the same checks, so a refusal the structural pass missed means
    the preview stopped somewhere compose did not — and a preview that says
    "nothing wrong" about a pair compose refuses is worse than no preview.
    """
    op = _cases(canonical_map)[case]

    attempted = preview_compose(left, right, op)
    # The structural pass on its own: what the preview reports when compose is
    # never run, which is how `--plot` on a refused run uses it.
    alone = preview_compose(left, right, op, attempt=False)
    structural = {f.kind for f in alone.findings if f.severity == "possible"}

    if attempted.outcome.status == "composed":
        assert not [f for f in attempted.findings if f.severity == "refusal"]
        assert not structural, f"{case} composes but the preview objected"
        return

    wanted = expected_kinds(attempted.outcome)
    if not wanted:
        return  # a refusal the preview is not asked to anticipate
    assert wanted & structural, (
        f"{case}: compose refused with {sorted(wanted)}, "
        f"the structural pass saw {sorted(structural)}"
    )


@pytest.mark.parametrize("case", sorted(_cases(CanonicalMap())))
def test_a_preview_survives_a_round_trip_through_json(case, left, right, canonical_map):
    """It is an API payload as much as a picture, so it has to serialize."""
    preview = preview_compose(left, right, _cases(canonical_map)[case])

    payload = preview.to_dict()
    restored = ComposePreview.model_validate(json.loads(json.dumps(payload)))

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
        ("vertex joining a composed class", "incomplete"),
        ("vertex name collision", "name_collision"),
        # Longest match wins, or this would read as a plain collision.
        ("property rename collision", "property_collision"),
        ("property re-target", "property_retarget"),
        ("unknown property", "unknown_property"),
        ("cluster overlap", "cluster_overlap"),
    ],
)
def test_a_refusals_check_phrase_names_its_finding_kind(check, expected):
    assert kind_for_check(check) == expected


def test_an_unclassifiable_refusal_asserts_nothing():
    outcome = ComposeOutcome(
        status="refused", error_type="ValueError", message="something deeper"
    )
    assert expected_kinds(outcome) == frozenset()
