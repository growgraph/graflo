import logging

from graflo.architecture.actor import VertexActor
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
