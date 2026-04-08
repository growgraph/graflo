import logging
from typing import Any

import pytest

from graflo.architecture.graph_types import ExtractionContext
from graflo.architecture.schema.edge import EdgeConfig
from graflo.architecture.contract.declarations.resource import (
    Resource,
    _resolve_type_caster,
)
from graflo.architecture.schema.vertex import VertexConfig

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


def test_resource_drop_trivial_input_fields_strips_none_and_empty_string():
    from graflo.architecture.contract.declarations.resource import (
        _strip_trivial_top_level_fields,
    )

    assert _strip_trivial_top_level_fields(
        {"a": 1, "b": None, "c": "", "d": "x", "nested": {"e": None}}
    ) == {"a": 1, "d": "x", "nested": {"e": None}}


def test_resource_drop_trivial_input_fields_passes_stripped_doc_to_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resource = Resource.from_dict(
        {
            "name": "wide_row",
            "pipeline": [{"vertex": "person"}],
            "drop_trivial_input_fields": True,
        }
    )
    vc = VertexConfig.from_dict(
        {
            "vertices": [
                {
                    "name": "person",
                    "properties": ["id", "note"],
                    "identity": ["id"],
                }
            ]
        }
    )
    ec = EdgeConfig.from_dict({"edges": []})
    resource.finish_init(vertex_config=vc, edge_config=ec, transforms={})
    doc = {"id": "1", "note": "hi", "empty": "", "nullish": None, "keep": 0}
    real_extract = resource._executor.extract
    snapshots: list[dict[str, Any]] = []

    def capturing_extract(work: dict[str, Any]) -> ExtractionContext:
        snapshots.append(dict(work))
        return real_extract(work)

    monkeypatch.setattr(resource._executor, "extract", capturing_extract)

    resource(doc)

    assert snapshots == [{"id": "1", "note": "hi", "keep": 0}]


def test_resource_drop_trivial_input_fields_false_passes_doc_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resource = Resource.from_dict(
        {
            "name": "wide_row",
            "pipeline": [{"vertex": "person"}],
            "drop_trivial_input_fields": False,
        }
    )
    vc = VertexConfig.from_dict(
        {
            "vertices": [
                {
                    "name": "person",
                    "properties": ["id"],
                    "identity": ["id"],
                }
            ]
        }
    )
    ec = EdgeConfig.from_dict({"edges": []})
    resource.finish_init(vertex_config=vc, edge_config=ec, transforms={})
    doc = {"id": "1", "empty": ""}
    expected_at_extract_entry = dict(doc)
    real_extract = resource._executor.extract
    snapshots: list[dict[str, Any]] = []

    def capturing_extract(work: dict[str, Any]) -> ExtractionContext:
        snapshots.append(dict(work))
        return real_extract(work)

    monkeypatch.setattr(resource._executor, "extract", capturing_extract)

    resource(doc)

    assert snapshots == [expected_at_extract_entry]


def test_resource_skip_actors_on_missing_input_keys_true_skips_missing_transform() -> (
    None
):
    resource = Resource.from_dict(
        {
            "name": "skip_missing_transform",
            "pipeline": [
                {
                    "transform": {
                        "call": {
                            "module": "builtins",
                            "foo": "int",
                            "input": ["missing_age"],
                            "output": ["age"],
                        }
                    }
                },
                {"vertex": "person", "from": {"id": "id"}},
            ],
            "skip_actors_on_missing_input_keys": True,
        }
    )
    vc = VertexConfig.from_dict(
        {
            "vertices": [
                {
                    "name": "person",
                    "properties": ["id", "age"],
                    "identity": ["id"],
                }
            ]
        }
    )
    ec = EdgeConfig.from_dict({"edges": []})
    resource.finish_init(vertex_config=vc, edge_config=ec, transforms={})

    # missing_age is absent, transform should be skipped (not raise KeyError)
    entities = resource({"id": "u-1"})
    assert entities["person"] == [{"id": "u-1"}]


def test_resource_drop_trivial_input_fields_large_doc_auto_skips_missing_transform(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resource = Resource.from_dict(
        {
            "name": "wide_row",
            "pipeline": [
                {
                    "transform": {
                        "call": {
                            "module": "builtins",
                            "foo": "int",
                            "input": ["age_raw"],
                            "output": ["age"],
                        }
                    }
                },
                {"vertex": "person", "from": {"id": "id"}},
            ],
            "drop_trivial_input_fields": True,
        }
    )
    vc = VertexConfig.from_dict(
        {
            "vertices": [
                {
                    "name": "person",
                    "properties": ["id", "age"],
                    "identity": ["id"],
                }
            ]
        }
    )
    ec = EdgeConfig.from_dict({"edges": []})
    resource.finish_init(vertex_config=vc, edge_config=ec, transforms={})

    large_doc: dict[str, Any] = {f"empty_{i}": "" for i in range(1000)}
    large_doc.update({"id": "u-2", "age_raw": "", "keep_zero": 0})
    real_extract = resource._executor.extract
    snapshots: list[dict[str, Any]] = []

    def capturing_extract(work: dict[str, Any]) -> ExtractionContext:
        snapshots.append(dict(work))
        return real_extract(work)

    monkeypatch.setattr(resource._executor, "extract", capturing_extract)

    # age_raw is stripped as trivial; auto-enabled missing-key skip should prevent failure.
    entities = resource(large_doc)
    assert entities["person"] == [{"id": "u-2"}]
    assert snapshots and "age_raw" not in snapshots[0]
    assert "keep_zero" in snapshots[0]


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
        {"vertices": [{"name": "person", "properties": ["id"], "identity": ["id"]}]}
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
        {"vertices": [{"name": "person", "properties": ["id"], "identity": ["id"]}]}
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
    from graflo.architecture.graph_types import ActionContext

    vc = VertexConfig.from_dict(
        {
            "vertices": [
                {"name": "a", "properties": ["id"], "identity": ["id"]},
                {"name": "b", "properties": ["id"], "identity": ["id"]},
                {"name": "c", "properties": ["id"], "identity": ["id"]},
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
