import logging

from graflo.architecture.actor import (
    ActorWrapper,
    DescendActor,
    EdgeActor,
    TransformActor,
)
from graflo.architecture.edge import EdgeConfig
from graflo.architecture.onto import ActionContext, LocationIndex, VertexRep

logger = logging.getLogger(__name__)


def test_descend(resource_descend, schema_vc_openalex):
    anw = ActorWrapper(**resource_descend)
    anw.finish_init(vertex_config=schema_vc_openalex)
    assert isinstance(anw.actor, DescendActor)
    assert len(anw.actor.descendants) == 2
    assert isinstance(anw.actor.descendants[0].actor, DescendActor)
    level, cname, label, edges = anw.fetch_actors(0, [])
    assert len(edges) == 3


def test_edge(action_node_edge, schema_vc_openalex):
    anw = ActorWrapper(**action_node_edge)
    anw.finish_init(
        transforms={}, vertex_config=schema_vc_openalex, edge_config=EdgeConfig()
    )
    assert isinstance(anw.actor, EdgeActor)
    assert anw.actor.edge.target == "work"


def test_transform(action_node_transform, schema_vc_openalex):
    anw = ActorWrapper(**action_node_transform)
    anw.finish_init(vertex_config=schema_vc_openalex)
    assert isinstance(anw.actor, TransformActor)


def test_mapper_value(resource_concept, schema_vc_openalex):
    test_doc = [{"wikidata": "https://www.wikidata.org/wiki/Q123", "mag": 105794591}]
    anw = ActorWrapper(*resource_concept)
    anw.finish_init(vertex_config=schema_vc_openalex, transforms={})
    ctx = ActionContext()
    ctx = anw(ctx, doc=test_doc)
    assert len(ctx.acc_vertex) == 1
    assert ctx.acc_vertex["concept"][LocationIndex(path=(0,))] == [
        VertexRep(
            vertex={"wikidata": "Q123", "mag": 105794591},
            ctx={"wikidata": "https://www.wikidata.org/wiki/Q123"},
        )
    ]


def test_transform_shortcut(resource_openalex_works, schema_vc_openalex):
    doc = {
        "doi": "https://doi.org/10.1007/978-3-123",
        "id": "https://openalex.org/A123",
    }
    anw = ActorWrapper(*resource_openalex_works)
    transforms = {}
    anw.finish_init(vertex_config=schema_vc_openalex, transforms=transforms)
    ctx = ActionContext()
    ctx = anw(ctx, doc=doc)
    assert ctx.acc_vertex["work"][LocationIndex(path=(0,))] == [
        VertexRep(
            vertex={"_key": "A123", "doi": "10.1007/978-3-123"},
            ctx={
                "doi": "https://doi.org/10.1007/978-3-123",
                "id": "https://openalex.org/A123",
            },
        )
    ]


def test_edge_between_levels(
    resource_openalex_works, schema_vc_openalex, sample_openalex
):
    ctx = ActionContext()
    anw = ActorWrapper(*resource_openalex_works)
    ec = EdgeConfig()
    anw.finish_init(vertex_config=schema_vc_openalex, transforms={}, edge_config=ec)
    ctx = anw(ctx, doc=sample_openalex)
    lindexes = list(ctx.acc_vertex["work"])

    assert len(lindexes) == 6
    assert len([li for li in lindexes if len(li) == 1]) == 1
    assert len([li for li in lindexes if len(li) > 1]) == 5
    assert len(ctx.acc_global[("work", "work", None)]) == 5


def test_relation_from_key(resource_deb, data_deb, schema_vc_deb):
    anw = ActorWrapper(*resource_deb)
    anw.finish_init(vertex_config=schema_vc_deb, transforms={})
    ctx = ActionContext()
    ctx = anw(ctx, doc=data_deb)
    relevant_keys = [
        (u, v, r)
        for u, v, r in ctx.acc_global.keys()
        if v == "package" and u == "package"
    ]
    assert len(relevant_keys) == 4
    assert {k: len(ctx.acc_global[k]) for k in relevant_keys} == {
        ("package", "package", "depends"): 29,
        ("package", "package", "pre_depends"): 3,
        ("package", "package", "suggests"): 2,
        ("package", "package", "breaks"): 1,
    }


def test_relation_exclude_target(resource_deb, data_deb, schema_vc_deb):
    anw = ActorWrapper(*resource_deb)
    anw.finish_init(vertex_config=schema_vc_deb, transforms={})
    ctx = ActionContext()
    ctx = anw(ctx, doc=data_deb)
    assert len(ctx.acc_global[("maintainer", "package", None)]) == 3


def test_resource_deb_compact(resource_deb_compact, data_deb, schema_vc_deb):
    anw = ActorWrapper(*resource_deb_compact)
    anw.finish_init(vertex_config=schema_vc_deb, transforms={})
    ctx = ActionContext()
    ctx = anw(ctx, doc=data_deb)
    relevant_keys = [
        (u, v, r)
        for u, v, r in ctx.acc_global.keys()
        if v == "package" and u == "package"
    ]
    assert len(relevant_keys) == 4
    assert {k: len(ctx.acc_global[k]) for k in relevant_keys} == {
        ("package", "package", "depends"): 29,
        ("package", "package", "pre_depends"): 3,
        ("package", "package", "suggests"): 2,
        ("package", "package", "breaks"): 1,
    }
