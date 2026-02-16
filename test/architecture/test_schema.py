import logging

from graflo.architecture.actor import EdgeActor, VertexActor
from graflo.architecture.resource import Resource
from graflo.architecture.schema import Schema

logger = logging.getLogger(__name__)


def test_init_simple(vertex_config_kg, edge_config_kg):
    schema = {
        "vertex_config": vertex_config_kg,
        "edge_config": edge_config_kg,
        "resources": {},
        "general": {"name": "abc"},
    }
    sch = Schema.from_dict(schema)
    assert len(sch.vertex_config.vertices) == 3
    assert len(list(sch.edge_config.edges_items())) == 3


def test_schema_load(schema):
    sch = schema("kg")
    schema_obj = Schema.from_dict(sch)
    assert len(schema_obj.resources) == 2


def test_resource(schema):
    sd = schema("ibes")
    sr = Resource.from_dict(sd["resources"][0])
    assert len(sr.root.actor.descendants) == 10


def test_s(schema):
    sd = schema("ibes")
    sr = Schema.from_dict(sd)
    assert sr.general.name == "ibes"


def test_remove_disconnected_vertices(vertex_config_kg, edge_config_kg):
    """remove_disconnected_vertices drops vertices not in any edge and related actors."""
    # vertex_config_kg has publication, entity, mention
    # edge_config_kg has edges: entity-entity, entity-entity (aux), mention-entity
    # So connected = {entity, mention}; publication is disconnected
    schema_dict = {
        "vertex_config": vertex_config_kg,
        "edge_config": edge_config_kg,
        "resources": [
            {
                "resource_name": "r1",
                "apply": [
                    {"vertex": "publication"},
                    {"vertex": "entity"},
                    {"source": "mention", "target": "entity"},
                ],
            },
        ],
        "general": {"name": "kg"},
    }
    sch = Schema.from_dict(schema_dict)
    assert sch.vertex_config.vertex_set == {"publication", "entity", "mention"}
    assert len(sch.resources) == 1
    root = sch.resources[0].root
    assert len(root.find_descendants(actor_type=VertexActor, name={"publication"})) == 1

    sch.remove_disconnected_vertices()

    assert sch.vertex_config.vertex_set == {"entity", "mention"}
    assert "publication" not in sch.vertex_config.vertex_set
    # Resource r1 should still exist but without the VertexActor(publication)
    assert len(sch.resources) == 1
    assert len(root.find_descendants(actor_type=VertexActor, name={"publication"})) == 0


def test_remove_disconnected_vertices_drops_resource(vertex_config_kg, edge_config_kg):
    """A resource that only references a disconnected vertex should be removed entirely."""
    # publication is disconnected (not in any edge)
    # r_only_pub pipeline has a single step targeting only the disconnected vertex
    schema_dict = {
        "vertex_config": vertex_config_kg,
        "edge_config": edge_config_kg,
        "resources": [
            {
                "resource_name": "r_connected",
                "apply": [
                    {"vertex": "entity"},
                    {"source": "mention", "target": "entity"},
                ],
            },
            {
                "resource_name": "r_only_pub",
                "apply": [
                    {"vertex": "publication"},
                ],
            },
        ],
        "general": {"name": "kg"},
    }
    sch = Schema.from_dict(schema_dict)
    assert len(sch.resources) == 2

    sch.remove_disconnected_vertices()

    assert sch.vertex_config.vertex_set == {"entity", "mention"}
    # r_only_pub should be gone — its only actor referenced a disconnected vertex
    assert len(sch.resources) == 1
    assert sch.resources[0].name == "r_connected"


def test_remove_disconnected_vertices_nested_resource(vertex_config_kg, edge_config_kg):
    """Nested descend blocks are pruned correctly.

    Covers three scenarios:
    1. Mixed nested descend — disconnected actors inside are removed,
       connected actors and the descend wrapper survive.
    2. Nested descend becomes empty — the descend wrapper itself is
       dropped, but the resource survives because of other actors.
    3. Resource whose only content is a nested descend with disconnected
       actors — the resource is removed entirely.
    """
    # vertex_config_kg: publication, entity, mention
    # edge_config_kg edges: entity→entity, entity→entity(aux), mention→entity
    # connected = {entity, mention}; publication is disconnected
    schema_dict = {
        "vertex_config": vertex_config_kg,
        "edge_config": edge_config_kg,
        "resources": [
            {
                "resource_name": "r_mixed",
                "apply": [
                    {"vertex": "entity"},
                    {
                        "key": "items",
                        "apply": [
                            {"vertex": "publication"},
                            {"vertex": "mention"},
                        ],
                    },
                    {"source": "mention", "target": "entity"},
                ],
            },
            {
                "resource_name": "r_nested_empty",
                "apply": [
                    {"vertex": "entity"},
                    {
                        "key": "items",
                        "apply": [
                            {"vertex": "publication"},
                        ],
                    },
                ],
            },
            {
                "resource_name": "r_all_disconnected",
                "apply": [
                    {
                        "key": "items",
                        "apply": [
                            {"vertex": "publication"},
                        ],
                    },
                ],
            },
        ],
        "general": {"name": "kg"},
    }
    sch = Schema.from_dict(schema_dict)

    # -- preconditions --
    assert len(sch.resources) == 3
    r_mixed = sch.fetch_resource("r_mixed")
    r_nested_empty = sch.fetch_resource("r_nested_empty")
    assert (
        len(r_mixed.root.find_descendants(actor_type=VertexActor, name={"publication"}))
        == 1
    )
    assert (
        len(
            r_nested_empty.root.find_descendants(
                actor_type=VertexActor, name={"publication"}
            )
        )
        == 1
    )

    sch.remove_disconnected_vertices()

    assert sch.vertex_config.vertex_set == {"entity", "mention"}

    # r_mixed: publication removed from nested descend; mention and edge survive
    assert (
        len(r_mixed.root.find_descendants(actor_type=VertexActor, name={"publication"}))
        == 0
    )
    assert (
        len(r_mixed.root.find_descendants(actor_type=VertexActor, name={"mention"}))
        == 1
    )
    assert len(r_mixed.root.find_descendants(actor_type=EdgeActor)) == 1

    # r_nested_empty: the nested descend was emptied and dropped;
    # only vertex: entity remains
    assert (
        len(
            r_nested_empty.root.find_descendants(
                actor_type=VertexActor, name={"publication"}
            )
        )
        == 0
    )
    assert (
        len(
            r_nested_empty.root.find_descendants(
                actor_type=VertexActor, name={"entity"}
            )
        )
        == 1
    )
    assert r_nested_empty.count() == 1

    # r_all_disconnected: removed entirely — its only content was disconnected
    assert len(sch.resources) == 2
    resource_names = {r.name for r in sch.resources}
    assert "r_all_disconnected" not in resource_names
