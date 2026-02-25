import logging

from graflo.architecture.actor import (
    ActorWrapper,
    DescendActor,
    EdgeActor,
    TransformActor,
    VertexActor,
)
from graflo.architecture.edge import EdgeConfig
from graflo.architecture.onto import ActionContext, LocationIndex, VertexRep
from graflo.architecture.actor_config import (
    TransformActorConfig,
    normalize_actor_step,
    validate_actor_step,
)
from graflo.architecture.vertex import VertexConfig

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


def test_find_descendants_by_vertex_name(resource_descend, schema_vc_openalex):
    """find_descendants returns VertexActors whose name is in the given set."""
    anw = ActorWrapper(**resource_descend)
    anw.finish_init(vertex_config=schema_vc_openalex)
    # Tree: DescendActor -> [DescendActor(apply with name "a"), VertexActor("work")]
    by_name_work = anw.find_descendants(actor_type=VertexActor, name={"work"})
    assert len(by_name_work) == 1
    assert (
        isinstance(by_name_work[0].actor, VertexActor)
        and by_name_work[0].actor.name == "work"
    )
    by_name_empty = anw.find_descendants(actor_type=VertexActor, name={"nonexistent"})
    assert len(by_name_empty) == 0


def test_find_descendants_by_type_and_predicate(
    resource_openalex_works, schema_vc_openalex
):
    """find_descendants with actor_type and custom predicate works on nested tree."""
    anw = ActorWrapper(*resource_openalex_works)
    anw.finish_init(vertex_config=schema_vc_openalex, transforms={})
    all_vertex_work = anw.find_descendants(actor_type=VertexActor, name={"work"})
    assert len(all_vertex_work) == 2  # top-level and under referenced_works
    assert all(
        isinstance(w.actor, VertexActor) and w.actor.name == "work"
        for w in all_vertex_work
    )
    all_transform = anw.find_descendants(actor_type=TransformActor)
    assert len(all_transform) == 3  # keep_suffix_id variants and under referenced_works
    by_predicate = anw.find_descendants(
        predicate=lambda w: isinstance(w.actor, VertexActor) and w.actor.name == "work"
    )
    assert by_predicate == all_vertex_work


def test_find_descendants_transform_by_target_vertex(
    resource_collision, vertex_config_collision
):
    """find_descendants returns TransformActors whose target_vertex is in the given set."""
    anw = ActorWrapper(*resource_collision)
    anw.finish_init(vertex_config=vertex_config_collision, transforms={})
    by_vertex = anw.find_descendants(actor_type=TransformActor, vertex={"person"})
    assert len(by_vertex) == 1
    assert (
        isinstance(by_vertex[0].actor, TransformActor)
        and by_vertex[0].actor.vertex == "person"
    )
    by_vertex_empty = anw.find_descendants(
        actor_type=TransformActor, vertex={"nonexistent"}
    )
    assert len(by_vertex_empty) == 0


def test_explicit_format_pipeline_transform_create_edge():
    """New explicit format: pipeline with transform (map, to_vertex) and create_edge (from, to)."""

    vc = VertexConfig.from_dict({"vertices": [{"name": "users", "fields": ["id"]}]})
    pipeline = [
        {"transform": {"map": {"follower_id": "id"}, "to_vertex": "users"}},
        {"transform": {"map": {"followed_id": "id"}, "to_vertex": "users"}},
        {"create_edge": {"from": "users", "to": "users"}},
    ]
    anw = ActorWrapper(pipeline=pipeline)
    anw.finish_init(vertex_config=vc, transforms={})
    # Root is DescendActor with at least 3 descendants: 2 TransformActors, 1 EdgeActor
    # (finish_init may auto-add a VertexActor for "users")
    assert isinstance(anw.actor, DescendActor)
    assert len(anw.actor.descendants) >= 3
    transform_count = sum(
        1 for d in anw.actor.descendants if isinstance(d.actor, TransformActor)
    )
    edge_count = sum(1 for d in anw.actor.descendants if isinstance(d.actor, EdgeActor))
    assert transform_count >= 2 and edge_count >= 1
    # Explicit format parses via Pydantic config
    step = {"transform": {"map": {"x": "y"}, "to_vertex": "users"}}
    config = validate_actor_step(normalize_actor_step(step))
    assert config.type == "transform"
    assert isinstance(config, TransformActorConfig)
    assert config.map == {"x": "y"}
    assert config.to_vertex == "users"


def test_multi_edges_from_row(resource_ticker, vc_ticker, ec_ticker, sample_ticker):
    ctx = ActionContext()
    anw = ActorWrapper(**resource_ticker)
    anw.finish_init(vertex_config=vc_ticker, transforms={}, edge_config=ec_ticker)
    ctx = anw(ctx, doc=sample_ticker[0])

    acc = anw.normalize_ctx(ctx)

    assert len(acc[("ticker", "feature", None)]) == 3
    assert (
        len(set(w["feature@name"] for _, _, w in acc[("ticker", "feature", None)])) == 3
    )


def test_multi_edges_from_row_filtered(
    resource_ticker, vc_ticker_filtered, ec_ticker, sample_ticker
):
    ctx = ActionContext()
    anw = ActorWrapper(**resource_ticker)
    anw.finish_init(
        vertex_config=vc_ticker_filtered, transforms={}, edge_config=ec_ticker
    )
    ctx = anw(ctx, doc=sample_ticker[0])

    acc = anw.normalize_ctx(ctx)

    assert len(acc[("ticker", "feature", None)]) == 2
