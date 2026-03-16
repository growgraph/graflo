import logging

import pytest

from graflo.architecture.edge import EdgeConfig
from graflo.architecture.resource import Resource, _resolve_type_caster
from graflo.architecture.vertex import VertexConfig

logger = logging.getLogger(__name__)


def test_schema_tree(schema):
    sch = schema("kg")
    mn = Resource.from_dict(sch["ingestion_model"]["resources"][0])
    assert mn.count() == 14


def test_resolve_type_caster_allowlist():
    assert _resolve_type_caster("int") is int
    assert _resolve_type_caster("float") is float
    assert _resolve_type_caster("builtins.str") is str


def test_resolve_type_caster_rejects_expressions():
    assert _resolve_type_caster("__import__('os').system") is None


def test_resource_types_uses_safe_caster_resolution():
    resource = Resource.from_dict(
        {
            "name": "typed_resource",
            "pipeline": [{"vertex": "person"}],
            "types": {"age": "int", "unsafe": "__import__('os').system"},
        }
    )
    assert resource._types["age"] is int
    assert "unsafe" not in resource._types


def test_resource_infer_edge_selectors_are_mutually_exclusive():
    with pytest.raises(ValueError, match="mutually exclusive"):
        Resource.from_dict(
            {
                "name": "typed_resource",
                "pipeline": [{"vertex": "person"}],
                "infer_edge_only": [{"source": "a", "target": "b"}],
                "infer_edge_except": [{"source": "a", "target": "c"}],
            }
        )


def test_resource_infer_edge_selector_references_unknown_edge():
    resource = Resource.from_dict(
        {
            "name": "typed_resource",
            "pipeline": [{"vertex": "person"}],
            "infer_edge_only": [{"source": "a", "target": "b"}],
        }
    )
    vc = VertexConfig.from_dict(
        {"vertices": [{"name": "person", "fields": ["id"], "identity": ["id"]}]}
    )
    ec = EdgeConfig.from_dict({"edges": [{"source": "person", "target": "person"}]})
    with pytest.raises(ValueError, match="undefined vertices"):
        resource.finish_init(vertex_config=vc, edge_config=ec, transforms={})


def test_resource_dynamic_edge_vertices_must_be_declared():
    resource = Resource.from_dict(
        {
            "name": "dynamic_edges",
            "pipeline": [
                {"vertex": "person"},
                {"edge": {"from": "person", "to": "company"}},
            ],
        }
    )
    vc = VertexConfig.from_dict(
        {"vertices": [{"name": "person", "fields": ["id"], "identity": ["id"]}]}
    )
    ec = EdgeConfig.from_dict({"edges": []})

    with pytest.raises(ValueError, match="undefined vertices"):
        resource.finish_init(vertex_config=vc, edge_config=ec, transforms={})


def test_resource_auto_adds_edge_actor_types_to_infer_edge_except():
    """When a Resource has EdgeActors for (s,t), (s,t, None) is auto-added to infer_edge_except."""
    resource = Resource.from_dict(
        {
            "name": "test",
            "pipeline": [
                {"vertex": "a", "from": {"id": "a"}},
                {"vertex": "b", "from": {"id": "b"}},
                {"vertex": "c", "from": {"id": "c"}},
                {"edge": {"from": "a", "to": "b"}},
            ],
        }
    )
    ids = resource._edge_ids_from_edge_actors()
    assert ids == {("a", "b", None)}


def test_resource_infer_edge_except_excludes_edges_handled_by_edge_actors():
    """Resource with EdgeActor for (a,b) does not infer (a,b); (a,c) is still inferred."""
    from graflo.architecture.onto import ActionContext

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
    # EdgeActor for (a,b) is inside a descend that never runs (doc has no "nested" key)
    resource = Resource.from_dict(
        {
            "name": "test",
            "pipeline": [
                {"vertex": "a", "from": {"id": "a"}},
                {"vertex": "b", "from": {"id": "b"}},
                {"vertex": "c", "from": {"id": "c"}},
                {
                    "key": "nested",
                    "apply": [{"edge": {"from": "a", "to": "b"}}],
                },
            ],
        }
    )
    resource.finish_init(vertex_config=vc, edge_config=ec, transforms={})
    anw = resource.root
    ctx = ActionContext()
    ctx = anw(ctx, doc={"a": "1", "b": "2", "c": "3"})
    acc = anw.assemble(ctx)
    # (a,b) has EdgeActor so it's in infer_edge_except - not inferred
    assert ("a", "b", "ab") not in acc
    # (a,c) has no EdgeActor - inferred
    assert len(acc[("a", "c", "ac")]) == 1
