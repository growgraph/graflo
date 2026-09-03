"""Tests for :mod:`graflo.architecture.evolution.equivalence`."""

from __future__ import annotations

import pytest

from graflo.architecture.evolution import (
    ClusterConflictError,
    VertexEquivalence,
    build_clusters,
    resolve_cluster_labels,
    vertex_rename_maps,
)


def test_single_edge_is_one_cluster() -> None:
    clusters = build_clusters(
        [VertexEquivalence(left="Company", right="Org", into="Company")]
    )
    assert len(clusters) == 1
    assert clusters[0].left_names == frozenset({"Company"})
    assert clusters[0].right_names == frozenset({"Org"})


def test_agreeing_edges_merge_into_one_cluster() -> None:
    """{CA1}~{CB1, CB2} with the same into is one component, one label."""
    edges = [
        VertexEquivalence(left="Company", right="Org", into="Company"),
        VertexEquivalence(left="Company", right="Branch", into="Company"),
    ]
    clusters = build_clusters(edges)
    assert len(clusters) == 1
    assert clusters[0].right_names == frozenset({"Org", "Branch"})
    resolved = resolve_cluster_labels(clusters)
    assert len(resolved) == 1
    assert resolved[0].label == "Company"
    left_map, right_map = vertex_rename_maps(resolved)
    assert left_map == {}
    assert right_map == {"Org": "Company", "Branch": "Company"}


def test_overlapping_declared_clusters_with_disagreeing_into_raise() -> None:
    """The prompt's bug: two 'clusters' that share a node but disagree on into.

    cluster1-shaped edges {CA1}~{CB1} into X and {CA1, CA2}~{CB1} into Y share
    CA1 and CB1, so they are one component — and must not silently last-write-
    wins in a rename dict.
    """
    edges = [
        VertexEquivalence(left="CA1", right="CB1", into="X"),
        VertexEquivalence(left="CA1", right="CB2", into="X"),
        VertexEquivalence(left="CA2", right="CB1", into="Y"),
    ]
    clusters = build_clusters(edges)
    assert len(clusters) == 1
    with pytest.raises(ClusterConflictError, match="disagrees"):
        resolve_cluster_labels(clusters)


def test_canonical_label_disagrees_with_into() -> None:
    edges = [VertexEquivalence(left="Company", right="Org", into="Company")]
    clusters = build_clusters(edges)
    with pytest.raises(ClusterConflictError, match="canonical"):
        resolve_cluster_labels(clusters, {("left", "Company"): "Party"})


def test_canonical_label_completes_unmapped_members() -> None:
    edges = [VertexEquivalence(left="Company", right="Org", into="Company")]
    clusters = build_clusters(edges)
    resolved = resolve_cluster_labels(clusters, {("left", "Company"): "Company"})
    assert resolved[0].label == "Company"
    assert ("right", "Org") in resolved[0].unmapped


def test_two_disjoint_clusters_resolve_independently() -> None:
    edges = [
        VertexEquivalence(left="A", right="X", into="A"),
        VertexEquivalence(left="B", right="Y", into="B"),
    ]
    clusters = build_clusters(edges)
    assert len(clusters) == 2
    resolved = resolve_cluster_labels(clusters)
    labels = {item.label for item in resolved}
    assert labels == {"A", "B"}
