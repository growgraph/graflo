"""``vertex_types``: the classes a ``vertex_router`` may produce.

The list bounds the router's output after ``type_map`` resolves a value, so
two routers on one discriminator column can split its classes between them.
Every evolution op keeps what a router routes: each raw value reaches its old
class renamed, or nothing if that class went. Where the bounded form cannot
say that, the router is closed over the values it accepted.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from graflo.architecture.contract.ingestion.resource import (
    find_vertex_producing_levels,
    is_pass_through_router,
    is_unbounded_router,
    pipeline_has_unbounded_router,
    role_reach,
    route_discriminator,
    router_reach,
    step_looks_up,
    step_produces_vertices,
)
from graflo.architecture.contract.ingestion.steps.models import (
    VertexRouterActorConfig,
)
from graflo.architecture.contract.ingestion.steps.parse import canonical_actor_step
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import (
    CanonicalizeOp,
    MergeVerticesOp,
    ProjectManifestOp,
    RemoveVerticesOp,
    RenameVerticesOp,
    apply_evolution,
)
from graflo.architecture.evolution.ingestion import _widen_router_projection
from graflo.architecture.evolution.rewrite import (
    close_routers_in_pipeline,
    evolve_router,
    mark_lookup_only_in_pipeline,
)
from graflo.hq.document_caster import DocumentCaster
from graflo.hq.ingestion_parameters import IngestionParams

CLASSES = ("Machine", "Line", "Sensor")

#: One column, two routers: the plant router takes machines and lines, the
#: probe router sensors -- spelled ``S`` or by name.
PLANT = {
    "type": "vertex_router",
    "type_field": "kind",
    "role": "plant",
    "from": {"id": "obj"},
    "vertex_types": ["Machine", "Line"],
}
PROBE = {
    "type": "vertex_router",
    "type_field": "kind",
    "role": "probe",
    "from": {"id": "obj"},
    "type_map": {"S": "Sensor"},
    "vertex_types": ["Sensor"],
}
MONITORS = {
    "type": "edge",
    "source_role": "probe",
    "target_role": "plant",
    "relation": "monitors",
}


def _manifest(*steps: dict[str, Any]) -> GraphManifest:
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "plant", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {"name": name, "properties": ["id"], "identity": ["id"]}
                            for name in CLASSES
                        ]
                    },
                    "edge_config": {
                        "edges": [
                            {
                                "source": "Sensor",
                                "target": "Machine",
                                "relation": "monitors",
                            },
                            {"source": "Machine", "target": "Line"},
                        ]
                    },
                },
            },
            "ingestion_model": {
                "resources": [{"name": "objects", "pipeline": [*steps]}]
            },
        }
    )
    manifest.finish_init()
    return manifest


def _cast(manifest: GraphManifest, rows: list[dict[str, Any]]) -> dict[str, list]:
    caster = DocumentCaster(manifest.require_ingestion_model())
    graph = asyncio.run(
        caster.cast_batch(rows, "objects", params=IngestionParams())
    ).graph
    return {
        name: sorted(dict(doc)["id"] for doc in graph.vertices.get(name, []))
        for name in graph.vertices
    }


def _pipeline(manifest: GraphManifest) -> list[dict[str, Any]]:
    return list(manifest.require_ingestion_model().resources[0].pipeline)


def _router(manifest: GraphManifest, role: str) -> dict[str, Any]:
    return next(s for s in _pipeline(manifest) if s.get("role") == role)


def _rows(*kinds: str) -> list[dict[str, Any]]:
    return [{"kind": kind, "obj": f"{kind.lower()}{i}"} for i, kind in enumerate(kinds)]


class TestRouting:
    def test_two_routers_split_one_column_between_them(self) -> None:
        cast = _cast(
            _manifest(PLANT, PROBE), _rows("Machine", "Line", "S", "Sensor", "Ghost")
        )
        assert cast == {
            "Machine": ["machine0"],
            "Line": ["line1"],
            "Sensor": ["s2", "sensor3"],
        }

    def test_a_table_entry_outside_the_list_is_skipped(self) -> None:
        router = {**PLANT, "type_map": {"S": "Sensor"}}
        assert _cast(_manifest(router), _rows("S", "Line")) == {"Line": ["line1"]}

    def test_with_type_map_only_a_row_passes_both(self) -> None:
        router = {
            **PLANT,
            "type_map": {"M": "Machine", "L": "Line", "S": "Sensor"},
            "type_map_only": True,
            "vertex_types": ["Machine", "Sensor"],
        }
        cast = _cast(_manifest(router), _rows("M", "L", "S", "Machine"))
        assert cast == {"Machine": ["m0"], "Sensor": ["s2"]}

    @pytest.mark.parametrize(
        "router",
        [
            PLANT,
            PROBE,
            {**PROBE, "type_map_only": True},
            {**PLANT, "vertex_types": None, "type_map": {"S": "Sensor"}},
        ],
    )
    def test_the_static_rule_is_the_runtime_rule(self, router) -> None:
        for kind in ("Machine", "Line", "Sensor", "S", "Ghost"):
            routed = route_discriminator(router, kind, CLASSES)
            cast = _cast(_manifest(router), _rows(kind))
            assert list(cast) == ([routed] if routed else [])


class TestValidation:
    def test_the_list_is_a_sorted_set(self) -> None:
        config = VertexRouterActorConfig.model_validate(
            {**PLANT, "vertex_types": ["Line", "Machine", "Line"]}
        )
        assert config.vertex_types == ["Line", "Machine"]

    def test_two_orders_canonicalize_alike(self) -> None:
        forward = canonical_actor_step({**PLANT, "vertex_types": ["Machine", "Line"]})
        backward = canonical_actor_step({**PLANT, "vertex_types": ["Line", "Machine"]})
        assert forward == backward

    def test_an_empty_list_is_refused(self) -> None:
        with pytest.raises(ValueError, match="at least one class"):
            VertexRouterActorConfig.model_validate({**PLANT, "vertex_types": []})

    @pytest.mark.parametrize(
        "setting",
        [
            {"lookup_only": ["Sensor"]},
            {"vertex_from_map": {"Sensor": {"id": "obj"}}},
        ],
    )
    def test_a_setting_for_an_unlisted_class_is_refused(self, setting) -> None:
        with pytest.raises(ValueError, match="excludes"):
            VertexRouterActorConfig.model_validate({**PLANT, **setting})

    def test_an_undeclared_class_is_refused(self) -> None:
        with pytest.raises(ValueError, match="Ghost"):
            _manifest({**PLANT, "vertex_types": ["Machine", "Ghost"]})

    def test_a_per_class_selector_the_role_cannot_hold_is_refused(self) -> None:
        edge = {**MONITORS, "target_match": {"Sensor": "identity"}}
        with pytest.raises(ValueError, match="can never hold"):
            _manifest(PLANT, PROBE, edge)


class TestReach:
    @pytest.mark.parametrize(
        ("router", "reach"),
        [
            (PLANT, {"Machine", "Line"}),
            ({**PROBE, "type_map_only": True}, {"Sensor"}),
            (
                {**PLANT, "type_map": {"S": "Sensor"}, "type_map_only": True},
                set(),
            ),
            (
                {"vertex_router": {"type_field": "kind", "vertex_types": ["Machine"]}},
                {"Machine"},
            ),
        ],
    )
    def test_a_bounded_router_reaches_its_list(self, router, reach) -> None:
        assert router_reach(router) == reach
        assert router_reach(router, known_vertices=CLASSES) == reach
        assert step_produces_vertices(router, known_vertices=CLASSES) == reach
        assert not is_unbounded_router(router)

    def test_an_unbounded_router_reaches_every_declared_class(self) -> None:
        router = {"type": "vertex_router", "type_field": "kind"}
        assert router_reach(router) is None
        assert router_reach(router, known_vertices=CLASSES) == set(CLASSES)
        assert is_unbounded_router(router) and is_pass_through_router(router)

    def test_a_bounded_router_is_an_explicit_producer(self) -> None:
        bare = {"type": "vertex_router", "type_field": "kind"}
        pipeline = [bare, {"descend": {"key": "parts", "pipeline": [PROBE]}}]
        assert find_vertex_producing_levels(
            pipeline, "Sensor", known_vertices=CLASSES
        ) == [[1]]
        assert find_vertex_producing_levels(
            pipeline, "Line", known_vertices=CLASSES
        ) == [[]]

    def test_a_pipeline_of_bounded_routers_does_not_widen(self) -> None:
        assert not pipeline_has_unbounded_router([PLANT, PROBE])

    def test_roles_hold_their_routers_reach(self) -> None:
        assert role_reach([PLANT, PROBE]) == {
            "plant": {"Machine", "Line"},
            "probe": {"Sensor"},
        }

    def test_lookup_only_true_covers_only_the_list(self) -> None:
        router = {**PLANT, "lookup_only": True}
        assert step_looks_up(router, "Machine")
        assert not step_looks_up(router, "Sensor")


# --- the evolution invariant --------------------------------------------------

DECLARED = frozenset({"A", "B", "C", "D"})
RAW = ("a", "b", "c", "A", "B", "C", "D", "N")

_classes = st.sampled_from(sorted(DECLARED))


@st.composite
def _routers(draw) -> dict[str, Any]:
    router: dict[str, Any] = {"type_field": "kind"}
    table = draw(st.dictionaries(st.sampled_from(RAW[:5]), _classes, max_size=4))
    if table:
        router["type_map"] = table
    if draw(st.booleans()):
        router["type_map_only"] = True
    if draw(st.booleans()):
        router["vertex_types"] = sorted(draw(st.sets(_classes, min_size=1, max_size=3)))
    projections = st.sampled_from([{"id": "x"}, {"id": "y"}, {"id": "x", "n": "z"}])
    if draw(st.booleans()):
        router["from"] = draw(projections)
    projected = draw(
        st.sets(st.sampled_from(router.get("vertex_types") or sorted(DECLARED)))
    )
    if projected:
        router["vertex_from_map"] = {c: draw(projections) for c in sorted(projected)}
    return router


@st.composite
def _class_maps(draw) -> dict[str, str | None]:
    kind = draw(st.sampled_from(["rename", "merge", "remove"]))
    if kind == "rename":
        return {draw(_classes): "N"}
    members = draw(st.sets(_classes, min_size=1, max_size=3))
    if kind == "remove":
        return dict.fromkeys(members)
    target = draw(st.sampled_from([*sorted(members), "N"]))
    return dict.fromkeys(members, target)


def _projection(routers: list[dict[str, Any]], value: str, declared) -> Any:
    """The ``from`` map the router routing *value* applies to its class."""
    for router in routers:
        routed = route_discriminator(
            {"type": "vertex_router", **router}, value, declared
        )
        if routed:
            return (router.get("vertex_from_map") or {}).get(routed, router.get("from"))
    return None


def _routes(routers: list[dict[str, Any]], value: str, declared) -> str | None:
    """The class *routers* send *value* to; at most one of them may."""
    routed = [
        r
        for router in routers
        if (
            r := route_discriminator(
                {"type": "vertex_router", **router}, value, declared
            )
        )
    ]
    assert len(routed) <= 1, (value, routers)
    return routed[0] if routed else None


def _after(declared: frozenset[str], mapping: dict[str, str | None]) -> set[str]:
    return (set(declared) - set(mapping)) | {v for v in mapping.values() if v}


class TestEvolutionInvariant:
    @settings(max_examples=400, deadline=None)
    @given(router=_routers(), mapping=_class_maps())
    def test_every_value_routes_to_its_class_renamed(self, router, mapping) -> None:
        evolved = evolve_router(router, mapping, declared=DECLARED)
        for piece in evolved:
            VertexRouterActorConfig.model_validate(piece)
        reach = router_reach(
            {"type": "vertex_router", **router}, known_vertices=DECLARED
        )
        after = _after(DECLARED, mapping)
        for value in RAW:
            was = _routes([router], value, DECLARED)
            now = _routes(evolved, value, after)
            expected = None if was is None else mapping.get(was, was)
            if now == expected:
                # ...and a row still routed keeps the projection it had.
                if now is not None:
                    assert _projection(evolved, value, after) == _projection(
                        [router], value, DECLARED
                    )
                continue
            # A new name may come to route as itself, when the router owned
            # every class renamed onto it.
            owners = {old for old, new in mapping.items() if new == value}
            assert was is None and now == value and value not in DECLARED
            assert reach is not None and owners <= reach

    @settings(max_examples=300, deadline=None)
    @given(
        split=st.sets(_classes, min_size=1, max_size=3),
        mapping=_class_maps(),
    )
    def test_disjoint_routers_stay_disjoint(self, split, mapping) -> None:
        rest = sorted(DECLARED - split)
        if not rest:
            return
        first = {"type_field": "kind", "vertex_types": sorted(split)}
        second = {"type_field": "kind", "vertex_types": rest}
        evolved = [
            evolve_router(r, mapping, declared=DECLARED) for r in (first, second)
        ]
        after = _after(DECLARED, mapping)
        for value in RAW:
            routed = [_routes(r, value, after) for r in evolved]
            assert None in routed, (value, evolved)


class TestOps:
    def test_a_rename_renames_the_list_and_keeps_the_old_value(self) -> None:
        renamed = apply_evolution(
            _manifest(PLANT, PROBE), [RenameVerticesOp(renames={"Machine": "Asset"})]
        )
        plant = _router(renamed, "plant")
        assert plant["vertex_types"] == ["Asset", "Line"]
        assert plant["type_map"] == {"Machine": "Asset"}
        assert "Machine" not in (_router(renamed, "probe").get("type_map") or {})
        assert _cast(renamed, _rows("Machine")) == {"Asset": ["machine0"]}

    def test_the_shorthand_spelling_is_renamed_in_place(self) -> None:
        shorthand = {
            "vertex_router": {
                "type_field": "kind",
                "role": "plant",
                "vertex_types": ["Machine"],
                "vertex_from_map": {"Machine": {"id": "obj"}},
            }
        }
        renamed = apply_evolution(
            _manifest(shorthand), [RenameVerticesOp(renames={"Machine": "Asset"})]
        )
        payload = _pipeline(renamed)[0]["vertex_router"]
        assert payload["vertex_types"] == ["Asset"]
        assert payload["vertex_from_map"] == {"Asset": {"id": "obj"}}

    def test_merging_a_listed_class_with_an_unlisted_one_closes_the_router(
        self,
    ) -> None:
        plant = {**PLANT, "vertex_types": ["Machine"]}
        line = {**PLANT, "role": "line", "vertex_types": ["Line"]}
        merged = apply_evolution(
            _manifest(plant, line),
            [
                CanonicalizeOp(
                    vertices={"Line": "Machine", "Machine": "Machine"},
                    allow_merges=True,
                    allow_self_relations=True,
                )
            ],
        )
        assert _router(merged, "plant")["vertex_types"] == ["Machine"]
        assert not _router(merged, "plant").get("type_map_only")
        closed = _router(merged, "line")
        assert closed["type_map"] == {"Line": "Machine"}
        assert closed["type_map_only"] is True
        # Each row is produced once: the routers still split the column.
        cast = _cast(merged, _rows("Machine", "Line"))
        assert cast == {"Machine": ["line1", "machine0"]}

    def test_merging_classes_the_router_owns_keeps_it_open(self) -> None:
        merged = apply_evolution(
            _manifest(PLANT, PROBE),
            [
                MergeVerticesOp(
                    sources=["Machine", "Line"], into="Unit", allow_self_relations=True
                )
            ],
        )
        plant = _router(merged, "plant")
        assert plant["vertex_types"] == ["Unit"]
        assert plant["type_map"] == {"Machine": "Unit", "Line": "Unit"}
        assert not plant.get("type_map_only")

    def test_merging_a_looked_up_class_with_a_written_one_is_refused(self) -> None:
        plant = {**PLANT, "lookup_only": ["Machine"]}
        with pytest.raises(ValueError, match="only looks up"):
            apply_evolution(
                _manifest(plant),
                [
                    MergeVerticesOp(
                        sources=["Machine", "Line"],
                        into="Unit",
                        allow_self_relations=True,
                    )
                ],
            )

    def test_a_removal_trims_the_list(self) -> None:
        trimmed = apply_evolution(
            _manifest(PLANT, PROBE, MONITORS), [RemoveVerticesOp(names=["Line"])]
        )
        assert _router(trimmed, "plant")["vertex_types"] == ["Machine"]

    def test_a_router_left_with_nothing_goes_with_the_edges_on_its_role(
        self,
    ) -> None:
        trimmed = apply_evolution(
            _manifest(PLANT, PROBE, MONITORS), [RemoveVerticesOp(names=["Sensor"])]
        )
        assert [step.get("role") for step in _pipeline(trimmed)] == ["plant"]

    def test_a_removed_table_entry_does_not_pass_its_key_through(self) -> None:
        router = {
            "type": "vertex_router",
            "type_field": "kind",
            "role": "any",
            "from": {"id": "obj"},
            "type_map": {"Line": "Machine"},
        }
        trimmed = apply_evolution(
            _manifest(router), [RemoveVerticesOp(names=["Machine"])]
        )
        assert _cast(trimmed, _rows("Line", "Sensor")) == {"Sensor": ["sensor1"]}

    def test_projection_keeps_a_bounded_resource_only_when_its_list_survives(
        self,
    ) -> None:
        kept = apply_evolution(
            _manifest(PROBE), [ProjectManifestOp(keep_vertices=["Sensor", "Machine"])]
        )
        assert _router(kept, "probe")["vertex_types"] == ["Sensor"]
        with pytest.raises(ValueError, match="resources empty"):
            apply_evolution(
                _manifest(PROBE), [ProjectManifestOp(keep_vertices=["Machine", "Line"])]
            )

    def test_field_projection_leaves_a_router_that_cannot_produce_the_class(
        self,
    ) -> None:
        router = {**PROBE, "keep_fields": ["id"]}
        assert _widen_router_projection(router, "Machine", ["key"]) is None
        widened = _widen_router_projection(router, "Sensor", ["key"])
        assert widened is not None and widened["keep_fields"] == ["id", "key"]

    def test_lookup_marking_skips_a_router_that_cannot_produce_the_class(
        self,
    ) -> None:
        marked = mark_lookup_only_in_pipeline([PLANT, PROBE], "Sensor")
        assert "lookup_only" not in marked[0]
        assert marked[1]["lookup_only"] == ["Sensor"]

    def test_merge_closes_a_bounded_router_over_its_listed_classes(self) -> None:
        closed = close_routers_in_pipeline([PLANT], ["Machine", "Line", "Sensor"])[0]
        assert closed["type_map"] == {"Machine": "Machine", "Line": "Line"}
        assert closed["type_map_only"] is True

    def test_merging_classes_projected_differently_splits_the_router(self) -> None:
        router = {
            "type": "vertex_router",
            "type_field": "kind",
            "role": "plant",
            "vertex_from_map": {"Machine": {"id": "obj"}, "Line": {"id": "line_no"}},
        }
        merged = apply_evolution(
            _manifest(router),
            [
                MergeVerticesOp(
                    sources=["Machine", "Line"], into="Unit", allow_self_relations=True
                )
            ],
        )
        routers = _pipeline(merged)
        assert [(r["type_map"], r["vertex_from_map"]) for r in routers] == [
            ({"Line": "Unit", "Sensor": "Sensor"}, {"Unit": {"id": "line_no"}}),
            ({"Machine": "Unit"}, {"Unit": {"id": "obj"}}),
        ]
        assert {r["role"] for r in routers} == {"plant"}
        rows = [
            {"kind": "Machine", "obj": "m1", "line_no": "x"},
            {"kind": "Line", "obj": "x", "line_no": "l1"},
        ]
        assert _cast(merged, rows) == {"Unit": ["l1", "m1"]}
