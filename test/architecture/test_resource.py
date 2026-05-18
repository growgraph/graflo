import logging
from typing import Any

import pytest

from graflo.architecture.graph_types import ExtractionContext
from graflo.architecture.schema.edge import EdgeConfig
from graflo.architecture.contract.declarations.resource import (
    Resource,
    ResourceRuntime,
    build_resource_runtime,
)
from graflo.architecture.schema.vertex import VertexConfig
from graflo.util.casting import resolve_type_caster

logger = logging.getLogger(__name__)


def _runtime(
    data: dict[str, Any],
    vertex_config: VertexConfig,
    edge_config: EdgeConfig,
    transforms: dict | None = None,
    **kwargs: Any,
) -> ResourceRuntime:
    config = Resource.from_dict(data)
    return build_resource_runtime(
        config,
        vertex_config,
        edge_config,
        transforms or {},
        **kwargs,
    )


def test_schema_tree(schema):
    sch = schema("kg")
    mn = Resource.from_dict(sch["ingestion_model"]["resources"][0])
    assert mn.pipeline_actor_count() == 14


def test_resolve_type_caster_allowlist():
    assert resolve_type_caster("int") is int
    assert resolve_type_caster("float") is float
    assert resolve_type_caster("builtins.str") is str


def test_resolve_type_caster_rejects_expressions():
    assert resolve_type_caster("__import__('os').system") is None


def test_resource_drop_trivial_input_fields_strips_none_and_empty_string():
    from graflo.architecture.contract.declarations.resource_runtime import (
        strip_trivial_top_level_fields,
    )

    assert strip_trivial_top_level_fields(
        {"a": 1, "b": None, "c": "", "d": "x", "nested": {"e": None}}
    ) == {"a": 1, "d": "x", "nested": {"e": None}}


def test_resource_drop_trivial_input_fields_passes_stripped_doc_to_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    resource = _runtime(
        {
            "name": "wide_row",
            "pipeline": [{"vertex": "person"}],
            "drop_trivial_input_fields": True,
        },
        vc,
        ec,
    )
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
    resource = _runtime(
        {
            "name": "wide_row",
            "pipeline": [{"vertex": "person"}],
            "drop_trivial_input_fields": False,
        },
        vc,
        ec,
    )
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
    resource = _runtime(
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
        },
        vc,
        ec,
    )

    entities = resource({"id": "u-1"})
    assert entities["person"] == [{"id": "u-1"}]


def test_resource_drop_trivial_input_fields_large_doc_auto_skips_missing_transform(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    resource = _runtime(
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
        },
        vc,
        ec,
    )

    large_doc: dict[str, Any] = {f"empty_{i}": "" for i in range(1000)}
    large_doc.update({"id": "u-2", "age_raw": "", "keep_zero": 0})
    real_extract = resource._executor.extract
    snapshots: list[dict[str, Any]] = []

    def capturing_extract(work: dict[str, Any]) -> ExtractionContext:
        snapshots.append(dict(work))
        return real_extract(work)

    monkeypatch.setattr(resource._executor, "extract", capturing_extract)

    entities = resource(large_doc)
    assert entities["person"] == [{"id": "u-2"}]
    assert snapshots and "age_raw" not in snapshots[0]
    assert "keep_zero" in snapshots[0]


def test_resource_types_uses_safe_caster_resolution():
    config = Resource.from_dict(
        {
            "name": "typed_resource",
            "pipeline": [{"vertex": "person"}],
            "types": {"age": "int", "unsafe": "__import__('os').system"},
        }
    )
    runtime = build_resource_runtime(
        config,
        VertexConfig.from_dict(
            {"vertices": [{"name": "person", "properties": ["id"], "identity": ["id"]}]}
        ),
        EdgeConfig.from_dict({"edges": []}),
    )
    assert runtime.type_casters["age"] is int
    assert "unsafe" not in runtime.type_casters


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
    vc = VertexConfig.from_dict(
        {"vertices": [{"name": "person", "properties": ["id"], "identity": ["id"]}]}
    )
    ec = EdgeConfig.from_dict({"edges": [{"source": "person", "target": "person"}]})
    with pytest.raises(ValueError, match="undefined vertices"):
        _runtime(
            {
                "name": "typed_resource",
                "pipeline": [{"vertex": "person"}],
                "infer_edge_only": [{"source": "a", "target": "b"}],
            },
            vc,
            ec,
        )


def test_resource_dynamic_edge_vertices_must_be_declared():
    vc = VertexConfig.from_dict(
        {"vertices": [{"name": "person", "properties": ["id"], "identity": ["id"]}]}
    )
    ec = EdgeConfig.from_dict({"edges": []})

    with pytest.raises(ValueError, match="undefined vertices"):
        _runtime(
            {
                "name": "dynamic_edges",
                "pipeline": [
                    {"vertex": "person"},
                    {"edge": {"from": "person", "to": "company"}},
                ],
            },
            vc,
            ec,
        )


def test_resource_auto_adds_edge_actor_types_to_infer_edge_except():
    config = Resource.from_dict(
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
    ids = ResourceRuntime.edge_ids_from_pipeline(config.pipeline)
    assert ids == {("a", "b", None)}


def test_resource_infer_edge_except_excludes_edges_handled_by_edge_actors():
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
    resource = _runtime(
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
        },
        vc,
        ec,
    )
    anw = resource.root
    ctx = ActionContext()
    ctx = anw(ctx, doc={"a": "1", "b": "2", "c": "3"})
    acc = anw.assemble(ctx)
    assert ("a", "b", "ab") not in acc
    assert len(acc[("a", "c", "ac")]) == 1


def _person_vertex_config() -> VertexConfig:
    return VertexConfig.from_dict(
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


def test_resource_tolerate_transform_errors_continues_pipeline() -> None:
    ec = EdgeConfig.from_dict({"edges": []})
    resource = _runtime(
        {
            "name": "tolerant",
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
            "tolerate_transform_errors": True,
        },
        _person_vertex_config(),
        ec,
    )

    result = resource.cast_document({"id": "u-1", "age_raw": "not-a-number"})
    person = result.entities["person"][0]
    assert person["id"] == "u-1"
    assert person.get("age") is None
    assert len(result.transform_failures) == 1
    assert result.transform_failures[0].nulled_fields == ("age",)
    assert result.transform_failures[0].exception_type == "ValueError"


def test_resource_tolerate_transform_errors_false_raises() -> None:
    ec = EdgeConfig.from_dict({"edges": []})
    resource = _runtime(
        {
            "name": "strict",
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
            "tolerate_transform_errors": False,
        },
        _person_vertex_config(),
        ec,
    )

    with pytest.raises(ValueError):
        resource.cast_document({"id": "u-1", "age_raw": "not-a-number"})


def test_resource_tolerate_transform_errors_defaults_true() -> None:
    resource = Resource.from_dict(
        {
            "name": "default_tolerant",
            "pipeline": [{"vertex": "person"}],
        }
    )
    assert resource.tolerate_transform_errors is True
