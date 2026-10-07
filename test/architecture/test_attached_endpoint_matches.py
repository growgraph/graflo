"""Endpoint selectors composed from edge steps and attached vertex classes.

A class a resource attaches (``find``) is located by that secondary identity,
so every edge endpoint of that class is matched on it -- unless an edge step
states a selector for that endpoint, which wins.
"""

from __future__ import annotations

from graflo.architecture.graph_types.edge_derivation import (
    EdgeDerivationRegistry,
    EndpointMatch,
    EndpointRule,
)

EDGE = ("C", "D", "cd")


def test_nothing_registered_leaves_both_endpoints_on_the_primary() -> None:
    registry = EdgeDerivationRegistry()
    assert registry.endpoint_match_for(EDGE) is None
    assert not registry.has_endpoint_matches()


def test_an_attached_class_fills_a_primary_endpoint() -> None:
    registry = EdgeDerivationRegistry()
    registry.attach("C", "by_k")
    assert registry.attached_selector("C") == "by_k"
    assert registry.attached_selector("D") is None
    assert registry.endpoint_match_for(EDGE) == EndpointMatch(source="by_k")
    assert registry.endpoint_match_for(("D", "C", "dc")) == EndpointMatch(target="by_k")
    assert registry.endpoint_match_for(("D", "E", "de")) is None
    assert registry.has_endpoint_matches()


def test_both_endpoints_attached() -> None:
    registry = EdgeDerivationRegistry()
    registry.attach("C", "by_k")
    registry.attach("D", "by_code")
    assert registry.endpoint_match_for(EDGE) == EndpointMatch(
        source="by_k", target="by_code"
    )


def test_an_explicit_match_wins_per_endpoint() -> None:
    registry = EdgeDerivationRegistry()
    registry.set_endpoint_match(
        EDGE, EndpointMatch(source="by_other", on_ambiguous="first")
    )
    registry.attach("C", "by_k")
    registry.attach("D", "by_code")
    assert registry.endpoint_match_for(EDGE) == EndpointMatch(
        source="by_other", target="by_code", on_ambiguous="first"
    )


def test_a_rule_wins_per_endpoint() -> None:
    registry = EdgeDerivationRegistry()
    registry.add_endpoint_rule(
        EndpointRule(source="C", target=None, relation=None, target_match="by_ref")
    )
    registry.attach("C", "by_k")
    registry.attach("D", "by_code")
    assert registry.endpoint_match_for(EDGE) == EndpointMatch(
        source="by_k", target="by_ref"
    )


def test_copy_and_merge_carry_attached_classes() -> None:
    registry = EdgeDerivationRegistry()
    registry.attach("C", "by_k")
    assert registry.copy().attached_selector("C") == "by_k"
    merged = EdgeDerivationRegistry()
    merged.merge_from(registry)
    assert merged.endpoint_match_for(EDGE) == EndpointMatch(source="by_k")
