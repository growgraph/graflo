"""Evolution reaches router projections and multi-link edge steps.

A router's ``from`` and ``keep_fields`` are shared by every class it routes,
so a property op changes a class's projection in that class's own
``vertex_from_map`` entry. An edge step's ``links`` name endpoints too, and a
class op rewrites or drops them the way it does a single edge.
"""

from __future__ import annotations

import asyncio
from typing import Any

from graflo.architecture.contract.ingestion.resource import (
    find_vertex_producing_levels,
)
from graflo.architecture.contract.ingestion.steps.normalize import (
    normalize_actor_step,
)
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import (
    MergeVerticesOp,
    RemoveEdgesOp,
    RemoveVertexPropertiesOp,
    RemoveVerticesOp,
    RenameVertexPropertiesOp,
    apply_evolution,
)
from graflo.hq.document_caster import DocumentCaster
from graflo.hq.ingestion_parameters import IngestionParams

ROUTER = {
    "type": "vertex_router",
    "type_field": "kind",
    "role": "plant",
    "from": {"id": "obj", "serial": "sn"},
}
LINKS = {
    "type": "edge",
    "links": [
        {"from": "Sensor", "to": "Machine", "relation": "monitors"},
        {"from": "Sensor", "to": "Line", "relation": "monitors"},
    ],
}


def _manifest(*resources: list[dict[str, Any]]) -> GraphManifest:
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "plant", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": name,
                                "properties": ["id", "serial"],
                                "identity": ["id"],
                            }
                            for name in ("Machine", "Line", "Sensor")
                        ]
                    },
                    "edge_config": {
                        "edges": [
                            {
                                "source": "Sensor",
                                "target": target,
                                "relation": "monitors",
                            }
                            for target in ("Machine", "Line")
                        ]
                        + [{"source": "Line", "target": "Machine", "relation": "feeds"}]
                    },
                },
            },
            "ingestion_model": {
                "resources": [
                    {"name": f"r{index}", "pipeline": pipeline}
                    for index, pipeline in enumerate(resources)
                ]
            },
        }
    )
    manifest.finish_init()
    return manifest


def _pipeline(manifest: GraphManifest, index: int = 0) -> list[dict[str, Any]]:
    resource = manifest.require_ingestion_model().resources[index]
    return list(resource.pipeline)


def _cast(manifest: GraphManifest, rows: list[dict[str, Any]]) -> dict[str, list]:
    caster = DocumentCaster(manifest.require_ingestion_model())
    graph = asyncio.run(caster.cast_batch(rows, "r0", params=IngestionParams())).graph
    return {
        name: sorted((dict(doc) for doc in docs), key=lambda d: d["id"])
        for name, docs in graph.vertices.items()
    }


ROWS = [
    {"kind": "Machine", "obj": "m1", "sn": "S-1"},
    {"kind": "Line", "obj": "l1", "sn": "S-2"},
]


class TestRouterProjections:
    def test_a_rename_gives_the_class_its_own_projection(self) -> None:
        renamed = apply_evolution(
            _manifest([ROUTER]),
            [RenameVertexPropertiesOp(renames={"Machine": {"serial": "tag"}})],
        )
        (router,) = _pipeline(renamed)
        assert router["vertex_from_map"] == {"Machine": {"id": "obj", "tag": "sn"}}
        assert router["from"] == {"id": "obj", "serial": "sn"}
        assert _cast(renamed, ROWS) == {
            "Machine": [{"id": "m1", "tag": "S-1"}],
            "Line": [{"id": "l1", "serial": "S-2"}],
        }

    def test_an_unbounded_router_without_from_gets_the_old_name_mapped(self) -> None:
        bare = {"type": "vertex_router", "type_field": "kind"}
        renamed = apply_evolution(
            _manifest([bare]),
            [RenameVertexPropertiesOp(renames={"Machine": {"serial": "tag"}})],
        )
        (router,) = _pipeline(renamed)
        assert router["vertex_from_map"] == {"Machine": {"tag": "serial"}}
        cast = _cast(renamed, [{"kind": "Machine", "id": "m1", "serial": "S-1"}])
        assert cast == {"Machine": [{"id": "m1", "tag": "S-1"}]}

    def test_a_shorthand_projection_is_renamed_in_place(self) -> None:
        shorthand = {
            "vertex_router": {
                "type_field": "kind",
                "vertex_from_map": {"Machine": {"id": "obj", "serial": "sn"}},
            }
        }
        renamed = apply_evolution(
            _manifest([shorthand]),
            [RenameVertexPropertiesOp(renames={"Machine": {"serial": "tag"}})],
        )
        (step,) = _pipeline(renamed)
        assert step["vertex_router"]["vertex_from_map"] == {
            "Machine": {"id": "obj", "tag": "sn"}
        }

    def test_a_router_that_cannot_produce_the_class_is_untouched(self) -> None:
        closed = {**ROUTER, "type_map": {"L": "Line"}, "type_map_only": True}
        renamed = apply_evolution(
            _manifest([closed]),
            [RenameVertexPropertiesOp(renames={"Machine": {"serial": "tag"}})],
        )
        assert "vertex_from_map" not in _pipeline(renamed)[0]

    def test_a_removal_splits_the_shared_from(self) -> None:
        trimmed = apply_evolution(
            _manifest([ROUTER]),
            [RemoveVertexPropertiesOp(removals={"Machine": ["serial"]})],
        )
        (router,) = _pipeline(trimmed)
        assert router["vertex_from_map"] == {"Machine": {"id": "obj"}}
        assert _cast(trimmed, ROWS) == {
            "Machine": [{"id": "m1"}],
            "Line": [{"id": "l1", "serial": "S-2"}],
        }

    def test_a_transform_feeding_a_pass_through_class_is_renamed(self) -> None:
        rename = {"type": "transform", "rename": {"raw": "serial"}}
        bare = {"type": "vertex_router", "type_field": "kind"}
        renamed = apply_evolution(
            _manifest([rename, bare]),
            [RenameVertexPropertiesOp(renames={"Machine": {"serial": "tag"}})],
        )
        transform = normalize_actor_step(dict(_pipeline(renamed)[0]))
        assert transform["rename"] == {"raw": "tag"}

    def test_a_projection_does_not_change_which_level_produces(self) -> None:
        pipeline = [
            {"vertex": "Machine"},
            {"descend": {"key": "parts", "pipeline": [ROUTER]}},
        ]
        renamed = apply_evolution(
            _manifest(pipeline),
            [RenameVertexPropertiesOp(renames={"Machine": {"serial": "tag"}})],
        )
        known = {"Machine", "Line", "Sensor"}
        assert find_vertex_producing_levels(
            _pipeline(renamed), "Machine", known_vertices=known
        ) == find_vertex_producing_levels(pipeline, "Machine", known_vertices=known)


class TestEdgeLinks:
    VERTICES = [{"vertex": "Sensor"}, {"vertex": "Machine"}, {"vertex": "Line"}]

    def _links(self, manifest: GraphManifest) -> list[dict[str, Any]] | None:
        edges = [s for s in _pipeline(manifest) if s.get("type") == "edge"]
        return edges[0]["links"] if edges else None

    def test_a_merge_renames_the_link_endpoints(self) -> None:
        merged = apply_evolution(
            _manifest([*self.VERTICES, LINKS]),
            [
                MergeVerticesOp(
                    sources=["Machine", "Line"],
                    into="Unit",
                    allow_self_relations=True,
                    allow_observation_fusion=True,
                )
            ],
        )
        assert [link["to"] for link in self._links(merged) or []] == ["Unit", "Unit"]

    def test_removing_a_class_drops_its_link(self) -> None:
        trimmed = apply_evolution(
            _manifest([*self.VERTICES, LINKS]), [RemoveVerticesOp(names=["Line"])]
        )
        assert [link["to"] for link in self._links(trimmed) or []] == ["Machine"]

    def test_a_step_left_without_links_goes(self) -> None:
        trimmed = apply_evolution(
            _manifest([*self.VERTICES, LINKS]),
            [RemoveVerticesOp(names=["Machine", "Line"])],
        )
        assert self._links(trimmed) is None

    def test_a_link_on_a_role_nothing_fills_goes(self) -> None:
        probe = {
            "type": "vertex_router",
            "type_field": "kind",
            "role": "probe",
            "vertex_types": ["Sensor"],
        }
        links = {
            "type": "edge",
            "links": [
                {"source_role": "probe", "to": "Machine", "relation": "monitors"},
                {"from": "Line", "to": "Machine", "relation": "feeds"},
            ],
        }
        trimmed = apply_evolution(
            _manifest([probe, {"vertex": "Machine"}, {"vertex": "Line"}, links]),
            [RemoveVerticesOp(names=["Sensor"])],
        )
        assert [link.get("relation") for link in self._links(trimmed) or []] == [
            "feeds"
        ]

    def test_removing_every_linked_relation_drops_the_step(self) -> None:
        trimmed = apply_evolution(
            _manifest([*self.VERTICES, LINKS]),
            [RemoveEdgesOp(relations=["monitors"])],
        )
        assert self._links(trimmed) is None

    def test_a_links_only_resource_survives_an_unrelated_removal(self) -> None:
        links = {"type": "edge", "links": [LINKS["links"][0]]}
        trimmed = apply_evolution(
            _manifest([{"vertex": "Line"}], [links]),
            [RemoveVerticesOp(names=["Line"])],
        )
        assert [r.name for r in trimmed.require_ingestion_model().resources] == ["r1"]
