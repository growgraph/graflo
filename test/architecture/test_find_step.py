"""``find``: a step that finds the existing vertex by a secondary identity.

The step-level contract and the static predicates over a pipeline; the runtime
and writer behaviour are tested where they live.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from graflo.architecture.contract.ingestion.resource import (
    pipeline_find_selectors,
    step_finds,
)
from graflo.architecture.contract.ingestion.steps.models import (
    VertexActorConfig,
    VertexRouterActorConfig,
)
from graflo.architecture.contract.ingestion.steps.parse import (
    as_actor_step,
    canonical_actor_step,
)

KNOWN = {"Machine", "Line", "Sensor"}


def _steps(pipeline):
    return [as_actor_step(step) for step in pipeline]


def test_find_defaults_to_none_and_is_omitted_from_canonical_step():
    assert VertexActorConfig(vertex="Machine").find is None
    assert VertexRouterActorConfig(type_field="kind").find is None
    assert "find" not in canonical_actor_step({"vertex": "Machine"})
    assert "find" not in canonical_actor_step({"type_field": "kind"})


def test_find_survives_canonicalisation_when_set():
    assert (
        canonical_actor_step({"vertex": "Machine", "find": "maintenance"})["find"]
        == "maintenance"
    )
    step = canonical_actor_step(
        {"type_field": "kind", "find": {"Machine": "maintenance"}}
    )
    assert step["find"] == {"Machine": "maintenance"}


def test_router_find_for_reads_the_per_class_selector():
    router = VertexRouterActorConfig(
        type_field="kind",
        vertex_types=["Machine", "Line"],
        find={"Machine": "maintenance"},
    )
    assert router.find_for("Machine") == "maintenance"
    assert router.find_for("Line") is None
    assert VertexRouterActorConfig(type_field="kind").find_for("Machine") is None


def test_router_find_outside_vertex_types_is_refused():
    with pytest.raises(ValidationError, match="find"):
        VertexRouterActorConfig(
            type_field="kind", vertex_types=["Machine"], find={"Line": "maintenance"}
        )


def test_step_finds_for_vertex_step():
    step = {"vertex": "Machine", "find": "maintenance"}
    assert step_finds(as_actor_step(step), "Machine") == "maintenance"
    assert step_finds(as_actor_step(step), "Line") is None
    assert step_finds(as_actor_step({"vertex": "Machine"}), "Machine") is None


def test_step_finds_for_router_only_within_reach():
    router = {
        "type_field": "kind",
        "vertex_types": ["Machine", "Line"],
        "find": {"Machine": "maintenance", "Line": "maintenance"},
    }
    assert step_finds(as_actor_step(router), "Machine") == "maintenance"
    assert step_finds(as_actor_step(router), "Sensor") is None
    closed = {
        "type_field": "kind",
        "type_map_only": True,
        "type_map": {"h": "Machine"},
        "find": {"Machine": "maintenance", "Line": "maintenance"},
    }
    assert step_finds(as_actor_step(closed), "Machine") == "maintenance"
    assert step_finds(as_actor_step(closed), "Line") is None


def test_step_finds_for_open_router_only_within_known_vertices():
    """An open router reaches the declared classes, not every name in its map."""
    router = {"type_field": "kind", "find": {"Ghost": "x", "Machine": "maintenance"}}
    assert step_finds(as_actor_step(router), "Ghost", known_vertices=KNOWN) is None
    assert (
        step_finds(as_actor_step(router), "Machine", known_vertices=KNOWN)
        == "maintenance"
    )


def test_step_finds_ignores_non_vertex_steps():
    assert (
        step_finds(as_actor_step({"transform": {"call": {"use": "x"}}}), "Machine")
        is None
    )


def test_pipeline_find_selectors_collects_across_levels_and_routers():
    pipeline = [
        {"vertex": "Machine", "find": "maintenance"},
        {
            "descend": {
                "key": "lines",
                "pipeline": [{"vertex": "Line", "find": "maintenance"}],
            }
        },
        {"type_field": "kind", "vertex_types": ["Sensor"], "find": {"Sensor": "ref"}},
    ]
    assert pipeline_find_selectors(_steps(pipeline), known_vertices=KNOWN) == {
        "Machine": "maintenance",
        "Line": "maintenance",
        "Sensor": "ref",
    }


def test_pipeline_find_selectors_empty_without_find():
    assert (
        pipeline_find_selectors(_steps([{"vertex": "Machine"}]), known_vertices=KNOWN)
        == {}
    )


def test_pipeline_find_selectors_allows_lookup_only_with_same_selector():
    pipeline = [
        {"vertex": "Machine", "find": "maintenance"},
        {"vertex": "Machine", "find": "maintenance", "lookup_only": True},
    ]
    assert pipeline_find_selectors(_steps(pipeline), known_vertices=KNOWN) == {
        "Machine": "maintenance"
    }


def test_pipeline_find_selectors_refuses_two_selectors_for_one_class():
    pipeline = [
        {"vertex": "Machine", "find": "maintenance"},
        {"descend": {"key": "x", "pipeline": [{"vertex": "Machine", "find": "ref"}]}},
    ]
    with pytest.raises(
        ValueError, match="Machine.*maintenance.*ref|Machine.*ref.*maintenance"
    ):
        pipeline_find_selectors(_steps(pipeline), known_vertices=KNOWN)


@pytest.mark.parametrize(
    "plain",
    [{"vertex": "Machine"}, {"vertex": "Machine", "lookup_only": True}],
    ids=["written-by-primary", "lookup-only-without-find"],
)
def test_pipeline_find_selectors_refuses_find_mixed_with_plain_step(plain):
    pipeline = [{"vertex": "Machine", "find": "maintenance"}, plain]
    with pytest.raises(ValueError, match="Machine.*explicit edge selectors"):
        pipeline_find_selectors(_steps(pipeline), known_vertices=KNOWN)


def test_pipeline_find_selectors_refuses_router_class_without_find_beside_find_step():
    pipeline = [
        {"vertex": "Machine", "find": "maintenance"},
        {"type_field": "kind", "vertex_types": ["Machine", "Line"]},
    ]
    with pytest.raises(ValueError, match="Machine"):
        pipeline_find_selectors(_steps(pipeline), known_vertices=KNOWN)
