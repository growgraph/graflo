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
    VertexActorConfig,
    normalize_actor_step,
    validate_actor_step,
)
from graflo.architecture.transform import ProtoTransform
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
    acc = anw.assemble(ctx)
    lindexes = list(ctx.acc_vertex["work"])

    assert len(lindexes) == 6
    assert len([li for li in lindexes if len(li) == 1]) == 1
    assert len([li for li in lindexes if len(li) > 1]) == 5
    assert len(acc[("work", "work", None)]) == 5


def test_relation_from_key(resource_deb, data_deb, schema_vc_deb):
    anw = ActorWrapper(*resource_deb)
    anw.finish_init(vertex_config=schema_vc_deb, transforms={})
    ctx = ActionContext()
    ctx = anw(ctx, doc=data_deb)
    acc = anw.assemble(ctx)
    relevant_keys = [
        (u, v, r)
        for u, v, r in (k for k in acc.keys() if isinstance(k, tuple))
        if v == "package" and u == "package"
    ]
    assert len(relevant_keys) == 4
    assert {k: len(acc[k]) for k in relevant_keys} == {
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
    acc = anw.assemble(ctx)
    assert len(acc[("maintainer", "package", None)]) == 3


def test_resource_deb_compact(resource_deb_compact, data_deb, schema_vc_deb):
    anw = ActorWrapper(*resource_deb_compact)
    anw.finish_init(vertex_config=schema_vc_deb, transforms={})
    ctx = ActionContext()
    ctx = anw(ctx, doc=data_deb)
    acc = anw.assemble(ctx)
    relevant_keys = [
        (u, v, r)
        for u, v, r in (k for k in acc.keys() if isinstance(k, tuple))
        if v == "package" and u == "package"
    ]
    assert len(relevant_keys) == 4
    assert {k: len(acc[k]) for k in relevant_keys} == {
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


def test_find_descendants_vertex_by_from_doc(
    resource_collision, vertex_config_collision
):
    """find_descendants returns VertexActors whose from_doc matches."""
    anw = ActorWrapper(*resource_collision)
    anw.finish_init(vertex_config=vertex_config_collision, transforms={})
    by_vertex = anw.find_descendants(actor_type=VertexActor, name={"person"})
    assert len(by_vertex) >= 1
    with_from = [w for w in by_vertex if w.actor.from_doc]
    assert len(with_from) == 1
    assert with_from[0].actor.from_doc == {"id": "name"}


def test_explicit_format_pipeline_vertex_from_create_edge():
    """Pipeline with vertex(from)+create_edge."""

    vc = VertexConfig.from_dict({"vertices": [{"name": "users", "fields": ["id"]}]})
    pipeline = [
        {"vertex": "users", "from": {"id": "follower_id"}},
        {"vertex": "users", "from": {"id": "followed_id"}},
        {"create_edge": {"from": "users", "to": "users"}},
    ]
    anw = ActorWrapper(pipeline=pipeline)
    anw.finish_init(vertex_config=vc, transforms={}, edge_config=EdgeConfig())
    assert isinstance(anw.actor, DescendActor)
    vertex_count = sum(
        1 for d in anw.actor.descendants if isinstance(d.actor, VertexActor)
    )
    edge_count = sum(1 for d in anw.actor.descendants if isinstance(d.actor, EdgeActor))
    assert vertex_count >= 2 and edge_count >= 1
    # Vertex with from parses via Pydantic config
    step = {"vertex": "users", "from": {"id": "x"}}
    config = validate_actor_step(normalize_actor_step(step))
    assert config.type == "vertex"
    assert isinstance(config, VertexActorConfig)
    assert config.from_doc == {"id": "x"}


def test_normalize_actor_step_nested_descend_apply_create_edge_shape():
    step = {
        "descend": {
            "key": "deps",
            "apply": {"create_edge": {"from": "package", "to": "package"}},
        }
    }
    normalized = normalize_actor_step(step)
    assert normalized["type"] == "descend"
    assert normalized["pipeline"][0]["type"] == "edge"
    assert normalized["pipeline"][0]["from"] == "package"
    assert normalized["pipeline"][0]["to"] == "package"

    config = validate_actor_step(normalized)
    assert config.type == "descend"
    assert config.pipeline[0].type == "edge"  # type: ignore[arg-type]


def test_transform_tuple_output_maps_to_vertex_index_fields_in_order():
    vc = VertexConfig.from_dict(
        {
            "vertices": [
                {
                    "name": "pair",
                    "fields": ["left", "right"],
                    "indexes": [{"fields": ["left", "right"]}],
                }
            ]
        }
    )
    pipeline = [{"map": {"unused": "unused"}}, {"vertex": "pair"}]
    anw = ActorWrapper(pipeline=pipeline)
    anw.finish_init(vertex_config=vc, transforms={})

    transform_wrappers = anw.find_descendants(actor_type=TransformActor)
    assert len(transform_wrappers) == 1
    transform_wrappers[0].actor.t = lambda doc: ("L", "R")

    ctx = ActionContext()
    ctx = anw(ctx, doc={"unused": "value"})
    assert ctx.acc_vertex["pair"][LocationIndex(path=(0,))] == [
        VertexRep(vertex={"left": "L", "right": "R"}, ctx={"unused": "value"}),
    ]


def test_transform_named_proto_binding_executes_with_registered_transform():
    anw = ActorWrapper(
        pipeline=[{"name": "to_int", "input": ["value"], "output": ["v"]}]
    )
    transforms = {
        "to_int": ProtoTransform(name="to_int", module="builtins", foo="int", params={})
    }
    anw.finish_init(vertex_config=VertexConfig(vertices=[]), transforms=transforms)

    transform_wrappers = anw.find_descendants(actor_type=TransformActor)
    assert len(transform_wrappers) == 1

    ctx = ActionContext()
    ctx = anw(ctx, doc={"value": "7"})
    payload = ctx.buffer_transforms[LocationIndex(path=(0,))][0]
    assert payload.named == {"v": 7}
    assert payload.positional == ()


def test_multi_edges_from_row(resource_ticker, vc_ticker, ec_ticker, sample_ticker):
    ctx = ActionContext()
    anw = ActorWrapper(**resource_ticker)
    anw.finish_init(vertex_config=vc_ticker, transforms={}, edge_config=ec_ticker)
    ctx = anw(ctx, doc=sample_ticker[0])

    acc = anw.assemble(ctx)

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

    acc = anw.assemble(ctx)

    assert len(acc[("ticker", "feature", None)]) == 2


def test_infer_edges_are_emitted_only_during_assemble(
    resource_ticker, vc_ticker, ec_ticker, sample_ticker
):
    ctx = ActionContext()
    anw = ActorWrapper(**resource_ticker)
    anw.finish_init(vertex_config=vc_ticker, transforms={}, edge_config=ec_ticker)
    ctx = anw(ctx, doc=sample_ticker[0])

    assert isinstance(ctx, ActionContext)
    assert all(not isinstance(k, tuple) for k in ctx.acc_global.keys())

    acc = anw.assemble(ctx)
    assert len(acc[("ticker", "feature", None)]) == 3


def test_transform_payload_consumption_avoids_cross_vertex_self_edge():
    doc = {
        "author_id": "309238221625",
        "FullName": "Guillaume Lemaître",
        "HIndex": "10",
        "research_sector": "32057259",
    }
    vc = VertexConfig.from_dict(
        {
            "vertices": [
                {
                    "name": "author",
                    "fields": ["id", "full_name", "hindex"],
                    "identity": ["id"],
                },
                {
                    "name": "researchField",
                    "fields": ["id", "name", "level"],
                    "identity": ["id"],
                },
            ]
        }
    )
    ec = EdgeConfig.from_dict(
        {
            "edges": [
                {
                    "source": "author",
                    "target": "researchField",
                    "relation": "belongsTo",
                }
            ]
        }
    )
    pipeline = [
        {
            "vertex": "author",
            "from": {"id": "author_id", "full_name": "FullName", "hindex": "HIndex"},
        },
        {"vertex": "researchField", "from": {"id": "research_sector"}},
    ]
    anw = ActorWrapper(pipeline=pipeline)
    anw.finish_init(vertex_config=vc, edge_config=ec, transforms={})

    ctx = ActionContext()
    ctx = anw(ctx, doc=doc)
    acc = anw.assemble(ctx)

    assert acc[("author", "researchField", "belongsTo")] == [
        ({"id": "309238221625"}, {"id": "32057259"}, {})
    ]


def test_infer_edge_only_filters_greedy_edges():
    vc = VertexConfig.from_dict(
        {
            "vertices": [
                {"name": "a", "fields": ["id"], "identity": ["id"]},
                {"name": "b", "fields": ["id"], "identity": ["id"]},
                {"name": "c", "fields": ["id"], "identity": ["id"]},
            ]
        }
    )
    ec = EdgeConfig.from_dict(
        {
            "edges": [
                {"source": "a", "target": "b", "relation": "ab"},
                {"source": "a", "target": "c", "relation": "ac"},
            ]
        }
    )
    pipeline = [
        {"vertex": "a", "from": {"id": "a"}},
        {"vertex": "b", "from": {"id": "b"}},
        {"vertex": "c", "from": {"id": "c"}},
    ]

    anw = ActorWrapper(pipeline=pipeline)
    anw.finish_init(
        vertex_config=vc,
        edge_config=ec,
        transforms={},
        infer_edge_only={("a", "b", None)},
    )
    ctx = ActionContext()
    ctx = anw(ctx, doc={"a": "1", "b": "2", "c": "3"})
    acc = anw.assemble(ctx)

    assert len(acc[("a", "b", "ab")]) == 1
    assert ("a", "c", "ac") not in acc


def test_infer_edge_except_filters_greedy_edges():
    vc = VertexConfig.from_dict(
        {
            "vertices": [
                {"name": "a", "fields": ["id"], "identity": ["id"]},
                {"name": "b", "fields": ["id"], "identity": ["id"]},
                {"name": "c", "fields": ["id"], "identity": ["id"]},
            ]
        }
    )
    ec = EdgeConfig.from_dict(
        {
            "edges": [
                {"source": "a", "target": "b", "relation": "ab"},
                {"source": "a", "target": "c", "relation": "ac"},
            ]
        }
    )
    pipeline = [
        {"vertex": "a", "from": {"id": "a"}},
        {"vertex": "b", "from": {"id": "b"}},
        {"vertex": "c", "from": {"id": "c"}},
    ]

    anw = ActorWrapper(pipeline=pipeline)
    anw.finish_init(
        vertex_config=vc,
        edge_config=ec,
        transforms={},
        infer_edge_except={("a", "c", None)},
    )
    ctx = ActionContext()
    ctx = anw(ctx, doc={"a": "1", "b": "2", "c": "3"})
    acc = anw.assemble(ctx)

    assert len(acc[("a", "b", "ab")]) == 1
    assert ("a", "c", "ac") not in acc


def test_extraction_context_records_observations(
    resource_openalex_works, schema_vc_openalex
):
    doc = {
        "id": "https://openalex.org/A123",
        "doi": "https://doi.org/10.1007/978-3-123",
    }
    anw = ActorWrapper(*resource_openalex_works)
    anw.finish_init(
        vertex_config=schema_vc_openalex, transforms={}, edge_config=EdgeConfig()
    )

    ctx = ActionContext()
    ctx = anw(ctx, doc=doc)

    assert len(ctx.vertex_observations) > 0
    assert len(ctx.transform_observations) > 0
    assert len(ctx.edge_intents) > 0
    assert all(
        obs.provenance.path == obs.location.path for obs in ctx.vertex_observations
    )
    assert all(
        obs.provenance.path == obs.location.path for obs in ctx.transform_observations
    )
