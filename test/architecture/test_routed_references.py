"""Edge endpoints a document decides, matched on a secondary identity.

An edge step whose endpoint comes from a ``vertex_router`` role -- or whose
relation comes from the data -- names no single edge at load time. Its
``source_match`` / ``target_match`` must still reach the stages that read
them per edge: the cast that keeps or prunes an edge, and the writer that
resolves the endpoint. The per-class form (``{Class: selector}``) serves a
router that sends rows to classes keyed differently, and a router's
``lookup_only`` says which of the classes it routes to are only referenced.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.hq.document_caster import DocumentCaster
from graflo.hq.ingestion_parameters import IngestionParams

TARGETS_A = ("WorkOrder", "ClassA", "targets")
TARGETS_C = ("WorkOrder", "ClassC", "targets")

#: ``ClassA`` is referenced by its ``code``; ``ClassC`` by its primary ``x_id``.
PER_CLASS_ROUTER = {
    "type": "vertex_router",
    "type_field": "assetType",
    "vertex_from_map": {"ClassA": {"code": "blaId"}, "ClassC": {"x_id": "blaId"}},
}
ROWS = [
    {"blaId": "K1", "assetType": "ClassA", "work_order_id": "w1"},
    {"blaId": "k9", "assetType": "ClassC", "work_order_id": "w1"},
]


def _manifest(*steps: dict[str, Any]) -> GraphManifest:
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "maintenance", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "ClassA",
                                "properties": ["x_id", "code"],
                                "identity": ["x_id"],
                                "secondary_identities": [
                                    {"name": "by_code", "fields": ["code"]}
                                ],
                            },
                            {
                                "name": "ClassC",
                                "properties": ["x_id"],
                                "identity": ["x_id"],
                            },
                            {
                                "name": "WorkOrder",
                                "properties": ["work_order_id"],
                                "identity": ["work_order_id"],
                            },
                        ]
                    },
                    "edge_config": {
                        "edges": [
                            {
                                "source": "WorkOrder",
                                "target": "ClassA",
                                "relation": "targets",
                            },
                            {
                                "source": "WorkOrder",
                                "target": "ClassC",
                                "relation": "targets",
                            },
                        ]
                    },
                },
            },
            "ingestion_model": {
                "resources": [
                    {
                        "name": "RA",
                        "pipeline": [*steps],
                    }
                ]
            },
        }
    )
    manifest.finish_init()
    return manifest


def _routed(router: dict[str, Any], **edge: Any) -> GraphManifest:
    return _manifest(
        router,
        {"role": "work_order", "vertex": "WorkOrder"},
        {
            "type": "edge",
            "source": "WorkOrder",
            "target_role": "assetType",
            "relation": "targets",
            **edge,
        },
    )


def _cast(manifest: GraphManifest, rows: list[dict[str, Any]] = ROWS):
    caster = DocumentCaster(manifest.require_ingestion_model())
    return asyncio.run(caster.cast_batch(rows, "RA", params=IngestionParams())).graph


def _targets(graph, edge_id) -> list[dict[str, Any]]:
    return [dict(target) for _source, target, *_ in graph.edges.get(edge_id, [])]


def _match(manifest: GraphManifest, edge_id):
    registry = manifest.require_ingestion_model().fetch_resource("RA").edge_derivation
    return registry.endpoint_match_for(edge_id)


class TestDataDrivenEdgesKeepTheirSelectors:
    def test_role_endpoint_matches_on_the_selected_identity(self) -> None:
        manifest = _routed(
            {
                "type": "vertex_router",
                "type_field": "assetType",
                "from": {"code": "blaId"},
            },
            target_match="by_code",
        )
        graph = _cast(manifest, ROWS[:1])
        assert _targets(graph, TARGETS_A) == [{"code": "K1"}]
        match = _match(manifest, TARGETS_A)
        assert match is not None and match.target == "by_code"

    def test_relation_from_the_data_keeps_the_selector(self) -> None:
        manifest = _manifest(
            {"vertex": "ClassA", "from": {"code": "blaId"}, "lookup_only": True},
            {"vertex": "WorkOrder"},
            {
                "source": "WorkOrder",
                "target": "ClassA",
                "relation_field": "rel",
                "target_match": "by_code",
            },
        )
        graph = _cast(
            manifest, [{"blaId": "K1", "work_order_id": "w1", "rel": "targets"}]
        )
        assert _targets(graph, TARGETS_A) == [{"code": "K1"}]


class TestPerClassSelectors:
    def test_each_routed_class_matches_on_its_own_identity(self) -> None:
        manifest = _routed(PER_CLASS_ROUTER, target_match={"ClassA": "by_code"})
        graph = _cast(manifest)
        assert _targets(graph, TARGETS_A) == [{"code": "K1"}]
        assert _targets(graph, TARGETS_C) == [{"x_id": "k9"}]
        assert _match(manifest, TARGETS_C) is None

    def test_a_class_the_schema_does_not_declare_is_refused(self) -> None:
        with pytest.raises(ValueError, match="Ghost"):
            _routed(PER_CLASS_ROUTER, target_match={"Ghost": "by_code"})

    def test_a_selector_its_class_does_not_declare_is_refused(self) -> None:
        with pytest.raises(ValueError, match="ClassC"):
            _routed(PER_CLASS_ROUTER, target_match={"ClassC": "by_code"})

    def test_a_static_endpoint_takes_its_own_class(self) -> None:
        manifest = _manifest(
            {"vertex": "ClassA", "from": {"code": "blaId"}, "lookup_only": True},
            {"vertex": "WorkOrder"},
            {
                "source": "WorkOrder",
                "target": "ClassA",
                "relation": "targets",
                "target_match": {"ClassA": "by_code"},
            },
        )
        graph = _cast(manifest, ROWS[:1])
        assert _targets(graph, TARGETS_A) == [{"code": "K1"}]

    def test_a_static_endpoint_naming_another_class_is_refused(self) -> None:
        with pytest.raises(ValueError, match="ClassC"):
            _manifest(
                {"vertex": "ClassA", "lookup_only": True},
                {"vertex": "WorkOrder"},
                {
                    "source": "WorkOrder",
                    "target": "ClassA",
                    "relation": "targets",
                    "target_match": {"ClassC": "identity"},
                },
            )


class TestRouterLookupOnly:
    def test_every_routed_class_is_only_looked_up(self) -> None:
        manifest = _routed(
            {**PER_CLASS_ROUTER, "lookup_only": True},
            target_match={"ClassA": "by_code"},
        )
        graph = _cast(manifest)
        assert not graph.vertices.get("ClassA")
        assert not graph.vertices.get("ClassC")
        assert _targets(graph, TARGETS_A) == [{"code": "K1"}]
        assert _targets(graph, TARGETS_C) == [{"x_id": "k9"}]

    def test_listed_classes_are_looked_up_the_rest_written(self) -> None:
        manifest = _routed(
            {**PER_CLASS_ROUTER, "lookup_only": ["ClassA"]},
            target_match={"ClassA": "by_code"},
        )
        graph = _cast(manifest)
        assert not graph.vertices.get("ClassA")
        assert [dict(d) for d in graph.vertices["ClassC"]] == [{"x_id": "k9"}]
        assert _targets(graph, TARGETS_A) == [{"code": "K1"}]

    def test_a_listed_class_the_schema_does_not_declare_is_refused(self) -> None:
        with pytest.raises(ValueError, match="Ghost"):
            _routed({**PER_CLASS_ROUTER, "lookup_only": ["Ghost"]})


class TestRouterFind:
    def test_found_classes_attach_and_the_rest_are_written(self) -> None:
        manifest = _routed({**PER_CLASS_ROUTER, "find": {"ClassA": "by_code"}})
        graph = _cast(manifest)
        assert graph.attached["ClassA"] == [{"code": "K1"}]
        assert graph.attached_by == {"ClassA": "by_code"}
        assert not graph.vertices.get("ClassA")
        assert [dict(d) for d in graph.vertices["ClassC"]] == [{"x_id": "k9"}]
        assert _targets(graph, TARGETS_A) == [{"code": "K1"}]
        assert _targets(graph, TARGETS_C) == [{"x_id": "k9"}]

    def test_a_found_class_the_schema_does_not_declare_is_refused(self) -> None:
        with pytest.raises(ValueError, match="Ghost"):
            _routed({**PER_CLASS_ROUTER, "find": {"Ghost": "by_code"}})

    def test_a_selector_its_class_does_not_declare_is_refused(self) -> None:
        with pytest.raises(ValueError, match="ClassC.*by_code|by_code.*ClassC"):
            _routed({**PER_CLASS_ROUTER, "find": {"ClassC": "by_code"}})


def test_cast_logs_what_it_prunes_for_want_of_an_identity(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Pruning a keyless vertex drops data; it must not do so silently."""
    manifest = _routed(
        {"type": "vertex_router", "type_field": "assetType", "from": {"code": "blaId"}}
    )
    with caplog.at_level(logging.WARNING):
        graph = _cast(manifest, ROWS[:1])
    assert not graph.vertices.get("ClassA")
    assert "'ClassA'" in caplog.text and "['x_id']" in caplog.text


def _router_and_edge(manifest: GraphManifest) -> tuple[dict[str, Any], dict[str, Any]]:
    steps = [
        s
        for s in manifest.require_ingestion_model().resources[0].pipeline
        if isinstance(s, dict)
    ]
    router = next(s for s in steps if s.get("type") == "vertex_router")
    edge = next(s for s in steps if s.get("type") == "edge")
    return router, edge


class TestEvolutionKeepsRoutedReferences:
    """Renames, removals and identity changes see both new forms."""

    @staticmethod
    def _referencing() -> GraphManifest:
        return _routed(
            {**PER_CLASS_ROUTER, "lookup_only": ["ClassA"]},
            target_match={"ClassA": "by_code"},
        )

    def test_a_rename_reaches_lookup_only_and_per_class_selectors(self) -> None:
        from graflo.architecture.evolution import RenameVerticesOp, apply_evolution

        renamed = apply_evolution(
            self._referencing(), [RenameVerticesOp(renames={"ClassA": "Asset"})]
        )
        router, edge = _router_and_edge(renamed)
        assert router["lookup_only"] == ["Asset"]
        assert edge["target_match"] == {"Asset": "by_code"}

    def test_a_removal_drops_the_class_from_both(self) -> None:
        from graflo.architecture.evolution import RemoveVerticesOp, apply_evolution

        trimmed = apply_evolution(
            self._referencing(), [RemoveVerticesOp(names=["ClassA"])]
        )
        router, edge = _router_and_edge(trimmed)
        assert not router.get("lookup_only")
        assert not edge.get("target_match")

    def test_a_secondary_a_per_class_selector_uses_is_not_removed(self) -> None:
        from graflo.architecture.evolution import (
            RemoveSecondaryIdentitiesOp,
            apply_evolution,
        )

        with pytest.raises(ValueError, match="still selected by an edge step"):
            apply_evolution(
                self._referencing(),
                [RemoveSecondaryIdentitiesOp(removals={"ClassA": ["by_code"]})],
            )

    @staticmethod
    def _finding() -> GraphManifest:
        return _routed({**PER_CLASS_ROUTER, "find": {"ClassA": "by_code"}})

    @staticmethod
    def _vertex_finding() -> GraphManifest:
        return _manifest(
            {"vertex": "ClassA", "find": "by_code"},
            {"vertex": "WorkOrder"},
            {"source": "WorkOrder", "target": "ClassA", "relation": "targets"},
        )

    def test_a_rename_reaches_a_routers_find(self) -> None:
        from graflo.architecture.evolution import RenameVerticesOp, apply_evolution

        renamed = apply_evolution(
            self._finding(), [RenameVerticesOp(renames={"ClassA": "Asset"})]
        )
        router, _edge = _router_and_edge(renamed)
        assert router["find"] == {"Asset": "by_code"}

    def test_a_rename_keeps_a_vertex_steps_find(self) -> None:
        from graflo.architecture.evolution import RenameVerticesOp, apply_evolution

        renamed = apply_evolution(
            self._vertex_finding(), [RenameVerticesOp(renames={"ClassA": "Asset"})]
        )
        step = renamed.require_ingestion_model().resources[0].pipeline[0]
        assert step == {"vertex": "Asset", "find": "by_code"}

    def test_renaming_the_secondarys_field_keeps_find_working(self) -> None:
        from graflo.architecture.evolution import (
            RenameVertexPropertiesOp,
            apply_evolution,
        )

        renamed = apply_evolution(
            self._finding(),
            [RenameVertexPropertiesOp(renames={"ClassA": {"code": "asset_code"}})],
        )
        router, _edge = _router_and_edge(renamed)
        assert router["find"] == {"ClassA": "by_code"}
        assert _cast(renamed).attached["ClassA"] == [{"asset_code": "K1"}]

    def test_a_removal_drops_the_class_from_find(self) -> None:
        from graflo.architecture.evolution import RemoveVerticesOp, apply_evolution

        trimmed = apply_evolution(self._finding(), [RemoveVerticesOp(names=["ClassA"])])
        router, _edge = _router_and_edge(trimmed)
        assert not router.get("find")

    @pytest.mark.parametrize("manifest", ["_finding", "_vertex_finding"])
    def test_a_secondary_a_find_uses_is_not_removed(self, manifest: str) -> None:
        from graflo.architecture.evolution import (
            RemoveSecondaryIdentitiesOp,
            apply_evolution,
        )

        with pytest.raises(ValueError, match="still selected"):
            apply_evolution(
                getattr(self, manifest)(),
                [RemoveSecondaryIdentitiesOp(removals={"ClassA": ["by_code"]})],
            )

    def test_merging_a_found_class_with_a_written_one_is_refused(self) -> None:
        from graflo.architecture.evolution import MergeVerticesOp, apply_evolution

        with pytest.raises(ValueError, match="finds by a secondary identity"):
            apply_evolution(
                self._finding(),
                [
                    MergeVerticesOp(
                        sources=["ClassA", "ClassC"],
                        into="Asset",
                        allow_observation_fusion=True,
                    )
                ],
            )

    def test_pin_to_retired_reaches_a_role_endpoint_for_that_class_only(self) -> None:
        from graflo.architecture.evolution import (
            IdentityReplacement,
            NaturalIdentityTarget,
            ReplaceIdentityOp,
            apply_evolution,
        )

        replaced = apply_evolution(
            _routed(PER_CLASS_ROUTER),
            [
                ReplaceIdentityOp(
                    replacements={
                        "ClassA": IdentityReplacement(
                            to=NaturalIdentityTarget(identity=["code"]),
                            retire_as="by_x_id",
                            endpoints="pin_to_retired",
                        )
                    }
                )
            ],
        )
        _router, edge = _router_and_edge(replaced)
        assert edge["target_match"] == {"ClassA": "by_x_id"}


#: Maps ``blaId`` into every routed class's key, so a routed row is writable.
KEYED_ROUTER = {
    "type": "vertex_router",
    "type_field": "assetType",
    "from": {"x_id": "blaId"},
}


class TestClosedRouters:
    """``type_map_only``: a router routes the values its table lists, and no other."""

    def test_a_value_missing_from_the_table_is_skipped(self) -> None:
        rows = [
            {"blaId": "x1", "assetType": "ClassA", "work_order_id": "w1"},
            {"blaId": "k9", "assetType": "ClassC", "work_order_id": "w1"},
        ]
        opened = _cast(
            _routed({**KEYED_ROUTER, "type_map": {"ClassC": "ClassC"}}), rows
        )
        closed = _cast(
            _routed(
                {
                    **KEYED_ROUTER,
                    "type_map": {"ClassC": "ClassC"},
                    "type_map_only": True,
                }
            ),
            rows,
        )
        assert [dict(d) for d in opened.vertices["ClassA"]] == [{"x_id": "x1"}]
        assert not closed.vertices.get("ClassA")
        assert [dict(d) for d in closed.vertices["ClassC"]] == [{"x_id": "k9"}]

    def test_static_analysis_sees_only_the_table(self) -> None:
        from graflo.architecture.contract.ingestion.resource import (
            find_vertex_producing_levels,
            step_produces_vertices,
        )

        known = {"ClassA", "ClassC", "WorkOrder"}
        opened = {**KEYED_ROUTER, "type_map": {"c": "ClassC"}}
        closed = {**opened, "type_map_only": True}
        assert step_produces_vertices(opened, known_vertices=known) == known
        assert step_produces_vertices(closed, known_vertices=known) == {"ClassC"}
        assert find_vertex_producing_levels(
            [opened], "ClassA", known_vertices=known
        ) == [[]]
        assert (
            find_vertex_producing_levels([closed], "ClassA", known_vertices=known) == []
        )

    def test_a_rename_rewrites_the_table_and_opens_no_raw_value(self) -> None:
        """Renaming a class writes ``{old: new}`` into an open router, not a closed one."""
        from graflo.architecture.evolution import RenameVerticesOp, apply_evolution

        closed = _routed(
            {
                **KEYED_ROUTER,
                "type_map": {"a": "ClassA", "c": "ClassC"},
                "type_map_only": True,
            }
        )
        renamed = apply_evolution(
            closed, [RenameVerticesOp(renames={"ClassA": "Asset"})]
        )
        router, _edge = _router_and_edge(renamed)
        assert router["type_map"] == {"a": "Asset", "c": "ClassC"}

    def test_a_removal_that_empties_the_table_drops_the_router_and_its_edges(
        self,
    ) -> None:
        """A closed router left routing nothing goes, with the edges on its role."""
        from graflo.architecture.evolution import RemoveVerticesOp, apply_evolution

        closed = _routed(
            {**KEYED_ROUTER, "type_map": {"a": "ClassA"}, "type_map_only": True}
        )
        trimmed = apply_evolution(closed, [RemoveVerticesOp(names=["ClassA"])])
        steps = trimmed.require_ingestion_model().resources[0].pipeline
        assert [s.get("vertex") for s in steps] == ["WorkOrder"]
