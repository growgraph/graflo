"""Tests for the pipeline producer helpers in ``contract.ingestion.resource``."""

from __future__ import annotations

from graflo.architecture.contract.ingestion.resource import (
    find_vertex_producing_levels,
    step_produces_vertices,
)

_KNOWN = {"Company", "Shop", "Person"}
_BARE = {"vertex_router": {"type_field": "kind"}}
_MAPPED = {"vertex_router": {"type_field": "kind", "type_map": {"firm": "Company"}}}


def _nested(*steps: dict) -> dict:
    return {"descend": {"key": "records", "apply": list(steps)}}


class TestStepProducesVertices:
    def test_a_router_names_its_table_targets(self) -> None:
        assert step_produces_vertices(_MAPPED) == {"Company"}

    def test_a_bare_router_names_nothing_without_the_schema(self) -> None:
        assert step_produces_vertices(_BARE) == set()

    def test_with_the_schema_a_router_produces_every_declared_class(self) -> None:
        """An unmapped discriminator value routes as the class name."""
        assert step_produces_vertices(_BARE, known_vertices=_KNOWN) == _KNOWN
        assert step_produces_vertices(_MAPPED, known_vertices=_KNOWN) == _KNOWN

    def test_a_vertex_step_is_unaffected_by_the_schema(self) -> None:
        step = {"vertex": "Company"}
        assert step_produces_vertices(step, known_vertices=_KNOWN) == {"Company"}

    def test_an_edge_step_produces_nothing(self) -> None:
        step = {"edge": {"source": "Company", "target": "Person"}}
        assert step_produces_vertices(step, known_vertices=_KNOWN) == set()


class TestFindVertexProducingLevels:
    def test_an_explicit_producer_decides(self) -> None:
        levels = find_vertex_producing_levels(
            [_BARE, _nested(_MAPPED)], "Company", known_vertices=_KNOWN
        )
        assert levels == [[1]]

    def test_pass_through_levels_count_only_when_nothing_is_explicit(self) -> None:
        levels = find_vertex_producing_levels(
            [_BARE, _nested(_BARE)], "Shop", known_vertices=_KNOWN
        )
        assert levels == [[], [1]]

    def test_pass_through_needs_the_class_declared(self) -> None:
        assert (
            find_vertex_producing_levels([_BARE], "Ghost", known_vertices=_KNOWN) == []
        )
        assert find_vertex_producing_levels([_BARE], "Shop") == []

    def test_a_level_without_a_router_never_passes_through(self) -> None:
        levels = find_vertex_producing_levels(
            [{"vertex": "Person"}], "Shop", known_vertices=_KNOWN
        )
        assert levels == []
