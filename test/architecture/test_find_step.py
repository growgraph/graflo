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
from graflo.architecture.contract.ingestion.steps.parse import canonical_actor_step

KNOWN = {"Host", "Rack", "App"}


def test_find_defaults_to_none_and_is_omitted_from_canonical_step():
    assert VertexActorConfig(vertex="Host").find is None
    assert VertexRouterActorConfig(type_field="kind").find is None
    assert "find" not in canonical_actor_step({"vertex": "Host"})
    assert "find" not in canonical_actor_step({"type_field": "kind"})


def test_find_survives_canonicalisation_when_set():
    assert canonical_actor_step({"vertex": "Host", "find": "cmdb"})["find"] == "cmdb"
    step = canonical_actor_step({"type_field": "kind", "find": {"Host": "cmdb"}})
    assert step["find"] == {"Host": "cmdb"}


def test_router_find_for_reads_the_per_class_selector():
    router = VertexRouterActorConfig(
        type_field="kind", vertex_types=["Host", "Rack"], find={"Host": "cmdb"}
    )
    assert router.find_for("Host") == "cmdb"
    assert router.find_for("Rack") is None
    assert VertexRouterActorConfig(type_field="kind").find_for("Host") is None


def test_router_find_outside_vertex_types_is_refused():
    with pytest.raises(ValidationError, match="find"):
        VertexRouterActorConfig(
            type_field="kind", vertex_types=["Host"], find={"Rack": "cmdb"}
        )


def test_step_finds_for_vertex_step():
    step = {"vertex": "Host", "find": "cmdb"}
    assert step_finds(step, "Host") == "cmdb"
    assert step_finds(step, "Rack") is None
    assert step_finds({"vertex": "Host"}, "Host") is None


def test_step_finds_for_router_only_within_reach():
    router = {
        "type_field": "kind",
        "vertex_types": ["Host", "Rack"],
        "find": {"Host": "cmdb", "Rack": "cmdb"},
    }
    assert step_finds(router, "Host") == "cmdb"
    assert step_finds(router, "App") is None
    closed = {
        "type_field": "kind",
        "type_map_only": True,
        "type_map": {"h": "Host"},
        "find": {"Host": "cmdb", "Rack": "cmdb"},
    }
    assert step_finds(closed, "Host") == "cmdb"
    assert step_finds(closed, "Rack") is None


def test_step_finds_ignores_non_vertex_steps():
    assert step_finds({"transform": {"call": {"use": "x"}}}, "Host") is None


def test_pipeline_find_selectors_collects_across_levels_and_routers():
    pipeline = [
        {"vertex": "Host", "find": "cmdb"},
        {
            "descend": {
                "key": "racks",
                "pipeline": [{"vertex": "Rack", "find": "cmdb"}],
            }
        },
        {"type_field": "kind", "vertex_types": ["App"], "find": {"App": "ref"}},
    ]
    assert pipeline_find_selectors(pipeline, known_vertices=KNOWN) == {
        "Host": "cmdb",
        "Rack": "cmdb",
        "App": "ref",
    }


def test_pipeline_find_selectors_empty_without_find():
    assert pipeline_find_selectors([{"vertex": "Host"}], known_vertices=KNOWN) == {}


def test_pipeline_find_selectors_allows_lookup_only_with_same_selector():
    pipeline = [
        {"vertex": "Host", "find": "cmdb"},
        {"vertex": "Host", "find": "cmdb", "lookup_only": True},
    ]
    assert pipeline_find_selectors(pipeline, known_vertices=KNOWN) == {"Host": "cmdb"}


def test_pipeline_find_selectors_refuses_two_selectors_for_one_class():
    pipeline = [
        {"vertex": "Host", "find": "cmdb"},
        {"descend": {"key": "x", "pipeline": [{"vertex": "Host", "find": "ref"}]}},
    ]
    with pytest.raises(ValueError, match="Host.*cmdb.*ref|Host.*ref.*cmdb"):
        pipeline_find_selectors(pipeline, known_vertices=KNOWN)


@pytest.mark.parametrize(
    "plain",
    [{"vertex": "Host"}, {"vertex": "Host", "lookup_only": True}],
    ids=["written-by-primary", "lookup-only-without-find"],
)
def test_pipeline_find_selectors_refuses_find_mixed_with_plain_step(plain):
    pipeline = [{"vertex": "Host", "find": "cmdb"}, plain]
    with pytest.raises(ValueError, match="Host.*explicit edge selectors"):
        pipeline_find_selectors(pipeline, known_vertices=KNOWN)


def test_pipeline_find_selectors_refuses_router_class_without_find_beside_find_step():
    pipeline = [
        {"vertex": "Host", "find": "cmdb"},
        {"type_field": "kind", "vertex_types": ["Host", "Rack"]},
    ]
    with pytest.raises(ValueError, match="Host"):
        pipeline_find_selectors(pipeline, known_vertices=KNOWN)
