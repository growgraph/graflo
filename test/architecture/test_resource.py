import logging

from graflo.architecture.resource import Resource, _resolve_type_caster

logger = logging.getLogger(__name__)


def test_schema_tree(schema):
    sch = schema("kg")
    mn = Resource.from_dict(sch["resources"][0])
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
            "resource_name": "typed_resource",
            "pipeline": [{"vertex": "person"}],
            "types": {"age": "int", "unsafe": "__import__('os').system"},
        }
    )
    assert resource._types["age"] is int
    assert "unsafe" not in resource._types
