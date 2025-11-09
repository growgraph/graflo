"""Tests for Vertex and Field classes with typed fields."""

import logging
from typing import cast

import pytest

from graflo.architecture.onto import Index
from graflo.architecture.vertex import Field, FieldType, Vertex, VertexConfig
from graflo.onto import DBFlavor

# Type aliases for flexible input types that Vertex accepts
_FieldsInput = list[str] | list[Field] | list[dict]
_IndexesInput = list[Index] | list[dict]

logger = logging.getLogger(__name__)


def test_field_with_none_type():
    """Test Field creation with None type (default, for ArangoDB etc)."""
    field = Field(name="name")
    assert field.name == "name"
    assert field.type is None


def test_field_with_explicit_type():
    """Test Field creation with explicit types."""
    for field_type in FieldType:
        field = Field(name="test", type=field_type.value)
        assert field.name == "test"
        assert field.type == field_type.value

    # Test case insensitive
    field = Field(name="test", type="int")
    assert field.type == "INT"

    field = Field(name="test", type="string")
    assert field.type == "STRING"


def test_field_type_validation():
    """Test that invalid field types raise errors."""
    with pytest.raises(ValueError, match="not allowed"):
        Field(name="test", type="INVALID_TYPE")

    with pytest.raises(ValueError, match="not allowed"):
        Field(name="test", type="invalid")


def test_field_string_behavior():
    """Test that Field objects behave like strings."""
    field = Field(name="test_field", type="INT")

    # String conversion
    assert str(field) == "test_field"

    # Equality with strings (Field compares to string)
    assert field == "test_field"
    # Note: reverse comparison ("string" == Field) doesn't work because
    # strings don't know about Field objects, but Field.__eq__ handles the comparison

    # Hashable for sets and dict keys
    field_set = {field, Field(name="other")}
    assert len(field_set) == 2

    # Can work as dict key (by hash)
    field_dict = {field: "value"}
    assert field_dict[field] == "value"


def test_field_dict_membership():
    """Test Field objects work in dict membership checks when converted to strings."""
    field = Field(name="id")
    test_dict = {"id": 1, "name": "test"}

    # Field objects need to be converted to strings for dict lookups
    # because dict keys use identity, not equality
    field_str = str(field)
    assert field_str in test_dict
    assert test_dict[field_str] == 1

    # Field equality with strings works (Field compares to string)
    assert field == "id"
    # Note: reverse comparison doesn't work because strings don't implement
    # reverse comparison for Field objects


def test_vertex_with_string_fields_backward_compatible():
    """Test Vertex creation with list of strings (backward compatible)."""
    vertex = Vertex(name="user", fields=cast(_FieldsInput, ["id", "name", "email"]))

    assert len(vertex.fields) == 3
    assert all(isinstance(f, Field) for f in vertex.fields)
    assert vertex.fields[0].name == "id"
    assert vertex.fields[0].type is None  # Defaults to None
    assert vertex.fields[1].name == "name"
    assert vertex.fields[2].name == "email"

    # field_names property
    assert vertex.field_names == ["id", "name", "email"]

    # Fields work in sets
    fields_set = set(vertex.fields)
    assert len(fields_set) == 3


def test_vertex_with_string_fields_dict_compatibility():
    """Test that field_names property works for dict lookups (critical for backward compatibility)."""
    vertex = Vertex(name="user", fields=cast(_FieldsInput, ["id", "name"]))
    test_dict = {"id": 1, "name": "John", "other": "ignored"}

    # This is the clean usage pattern from actor_util.py
    # Use field_names property directly - much cleaner than str(f)
    result = {f: test_dict[f] for f in vertex.field_names if f in test_dict}
    assert result == {"id": 1, "name": "John"}


def test_vertex_with_field_objects():
    """Test Vertex creation with list of Field objects."""
    fields = [
        Field(name="id", type="INT"),
        Field(name="name", type="STRING"),
        Field(name="age", type="INT"),
        Field(name="active", type="BOOL"),
    ]
    vertex = Vertex(name="user", fields=fields)

    assert len(vertex.fields) == 4
    assert vertex.fields[0].name == "id"
    assert vertex.fields[0].type == FieldType.INT
    assert vertex.fields[1].type == FieldType.STRING
    assert vertex.fields[3].type == FieldType.BOOL


def test_vertex_with_dict_fields():
    """Test Vertex creation with list of dicts (from YAML/JSON)."""
    fields = [
        {"name": "id", "type": "INT"},
        {"name": "name", "type": "STRING"},
        {"name": "email"},  # No type specified, defaults to None
    ]
    vertex = Vertex(name="user", fields=cast(_FieldsInput, fields))

    assert len(vertex.fields) == 3
    assert vertex.fields[0].name == "id"
    assert vertex.fields[0].type == FieldType.INT
    assert vertex.fields[1].type == FieldType.STRING
    assert vertex.fields[2].name == "email"
    assert vertex.fields[2].type is None


def test_vertex_mixed_field_inputs():
    """Test Vertex creation with mixed field types."""
    fields = [
        "id",  # string
        Field(name="name", type="STRING"),  # Field object
        {"name": "email", "type": "STRING"},  # dict
    ]
    vertex = Vertex(name="user", fields=cast(_FieldsInput, fields))

    assert len(vertex.fields) == 3
    assert all(isinstance(f, Field) for f in vertex.fields)
    assert vertex.fields[0].name == "id"
    assert vertex.fields[0].type is None
    assert vertex.fields[1].name == "name"
    assert vertex.fields[1].type == FieldType.STRING
    assert vertex.fields[2].name == "email"
    assert vertex.fields[2].type == FieldType.STRING


def test_vertex_fields_all_includes_aux():
    """Test fields_all property includes auxiliary fields."""
    vertex = Vertex(
        name="user",
        fields=cast(_FieldsInput, ["id", "name"]),
        fields_aux=["weight", "score"],
    )

    all_fields = vertex.fields_all
    assert len(all_fields) == 4
    assert all(isinstance(f, Field) for f in all_fields)

    field_names = [f.name for f in all_fields]
    assert "id" in field_names
    assert "name" in field_names
    assert "weight" in field_names
    assert "score" in field_names

    # Auxiliary fields should have None type
    weight_field = next(f for f in all_fields if f.name == "weight")
    assert weight_field.type is None


def test_vertex_config_fields_backward_compatible():
    """Test VertexConfig.fields() method returns names by default (backward compatible)."""
    vertex = Vertex(name="user", fields=cast(_FieldsInput, ["id", "name", "email"]))
    config = VertexConfig(vertices=[vertex])

    # Default behavior: returns names (strings) for backward compatibility
    # Order may vary, so check membership and length
    fields = config.fields("user")
    assert len(fields) == 3
    assert all(isinstance(f, str) for f in fields)
    assert set(fields) == {"id", "name", "email"}
    # Check that order is preserved from original fields
    assert fields == ["id", "name", "email"]


def test_vertex_config_fields_with_objects():
    """Test VertexConfig.fields() can return Field objects."""
    vertex = Vertex(
        name="user",
        fields=[
            Field(name="id", type="INT"),
            Field(name="name", type="STRING"),
        ],
    )
    config = VertexConfig(vertices=[vertex])

    # Return Field objects when as_names=False
    fields = config.fields("user", as_names=False)
    assert len(fields) == 2
    assert all(isinstance(f, Field) for f in fields)
    assert fields[0].type == FieldType.INT
    assert fields[1].type == FieldType.STRING

    # Still works with as_names=True
    field_names = config.fields("user", as_names=True)
    assert field_names == ["id", "name"]


def test_vertex_config_fields_with_aux():
    """Test VertexConfig.fields() with auxiliary fields."""
    vertex = Vertex(
        name="user", fields=cast(_FieldsInput, ["id", "name"]), fields_aux=["weight"]
    )
    config = VertexConfig(vertices=[vertex])

    fields_without_aux = config.fields("user", with_aux=False)
    assert fields_without_aux == ["id", "name"]

    fields_with_aux = config.fields("user", with_aux=True)
    assert set(fields_with_aux) == {"id", "name", "weight"}


def test_vertex_from_dict_with_string_fields():
    """Test Vertex.from_dict() with string fields (backward compatible)."""
    vertex_dict = {"name": "user", "fields": ["id", "name", "email"]}
    vertex = Vertex.from_dict(vertex_dict)

    assert vertex.name == "user"
    assert len(vertex.fields) == 3
    assert all(isinstance(f, Field) for f in vertex.fields)
    assert all(f.type is None for f in vertex.fields)


def test_vertex_from_dict_with_typed_fields():
    """Test Vertex.from_dict() with typed fields in dict."""
    vertex_dict = {
        "name": "user",
        "fields": [
            {"name": "id", "type": "INT"},
            {"name": "name", "type": "STRING"},
            {"name": "email"},
        ],
    }
    vertex = Vertex.from_dict(vertex_dict)

    assert vertex.name == "user"
    assert len(vertex.fields) == 3
    assert vertex.fields[0].type == FieldType.INT
    assert vertex.fields[1].type == FieldType.STRING
    assert vertex.fields[2].type is None


def test_vertex_indexes_work_with_field_objects():
    """Test that indexes work correctly with Field objects."""
    vertex = Vertex(
        name="user",
        fields=[
            Field(name="id", type="INT"),
            Field(name="email", type="STRING"),
        ],
        indexes=[],  # Will create default index
    )

    # Default index should be created with field names
    assert len(vertex.indexes) == 1
    assert vertex.indexes[0].fields == ["id", "email"]

    # Field objects should still be accessible
    assert len(vertex.fields) == 2
    assert vertex.fields[0].type == FieldType.INT


def test_vertex_with_custom_indexes():
    """Test vertex with custom indexes that reference fields."""
    vertex = Vertex(
        name="user",
        fields=cast(_FieldsInput, ["id", "name", "email"]),
        indexes=cast(
            _IndexesInput,
            [
                {"fields": ["id", "email"]},
            ],
        ),
    )

    # Index fields should be added to vertex fields if missing
    field_names = vertex.field_names
    assert "id" in field_names
    assert "name" in field_names
    assert "email" in field_names


def test_field_all_types():
    """Test all allowed field types."""
    for field_type in FieldType:
        field = Field(name=f"field_{field_type.value.lower()}", type=field_type.value)
        assert field.type == field_type.value


def test_vertex_update_aux_fields():
    """Test update_aux_fields() method."""
    vertex = Vertex(name="user", fields=cast(_FieldsInput, ["id", "name"]))
    vertex.update_aux_fields(["weight", "score"])

    assert "weight" in vertex.fields_aux
    assert "score" in vertex.fields_aux

    # Test adding more
    vertex.update_aux_fields(["rank"])
    assert "rank" in vertex.fields_aux


def test_invalid_field_type_in_dict():
    """Test that invalid field types in dict raise error."""
    with pytest.raises(ValueError, match="not allowed"):
        Vertex(
            name="user",
            fields=cast(_FieldsInput, [{"name": "test", "type": "INVALID"}]),
        )


def test_init(vertex_pub):
    """Original test: Test Vertex.from_dict() with existing fixture."""
    vc = Vertex.from_dict(vertex_pub)
    assert len(vc.indexes) == 3
    # Fields are now Field objects, so check count
    assert len(vc.fields) == 4
    # Verify they're Field objects
    assert all(isinstance(f, Field) for f in vc.fields)


def test_get_fields_with_defaults_tigergraph():
    """Test get_fields_with_defaults() defaults None types to STRING for TigerGraph."""
    # Create vertex with some fields that have None type
    vertex = Vertex(
        name="user",
        fields=cast(
            _FieldsInput,
            [
                Field(name="id", type="INT"),  # Already has type
                Field(name="name"),  # None type
                Field(name="email", type="STRING"),  # Already has type
                "address",  # String field (will be Field with None type)
            ],
        ),
    )

    # For TigerGraph, None types should default to STRING
    fields = vertex.get_fields_with_defaults(DBFlavor.TIGERGRAPH)
    assert len(fields) == 4
    assert fields[0].name == "id"
    assert fields[0].type == "INT"
    assert fields[1].name == "name"
    assert fields[1].type == "STRING"  # Default applied
    assert fields[2].name == "email"
    assert fields[2].type == "STRING"
    assert fields[3].name == "address"
    assert fields[3].type == "STRING"  # Default applied


def test_get_fields_with_defaults_other_db():
    """Test get_fields_with_defaults() preserves None types for other databases."""
    vertex = Vertex(
        name="user",
        fields=[
            Field(name="id", type="INT"),
            Field(name="name"),  # None type
        ],
    )

    # For ArangoDB, None types should remain None
    fields = vertex.get_fields_with_defaults(DBFlavor.ARANGO)
    assert len(fields) == 2
    assert fields[0].type == "INT"
    assert fields[1].name == "name"
    assert fields[1].type is None  # Preserved

    # For Neo4j, None types should also remain None
    fields = vertex.get_fields_with_defaults(DBFlavor.NEO4J)
    assert fields[1].type is None  # Preserved


def test_get_fields_with_defaults_none():
    """Test get_fields_with_defaults() with None db_flavor returns fields as-is."""
    vertex = Vertex(
        name="user",
        fields=[
            Field(name="id", type="INT"),
            Field(name="name"),  # None type
        ],
    )

    # With None db_flavor, should return fields as-is
    fields = vertex.get_fields_with_defaults(None)
    assert len(fields) == 2
    assert fields[0].type == "INT"
    assert fields[1].type is None  # Preserved


def test_get_fields_with_defaults_with_aux():
    """Test get_fields_with_defaults() includes auxiliary fields when requested."""
    vertex = Vertex(
        name="user",
        fields=cast(_FieldsInput, ["id", "name"]),
        fields_aux=["weight", "score"],
    )

    # For TigerGraph with aux fields
    fields = vertex.get_fields_with_defaults(DBFlavor.TIGERGRAPH, with_aux=True)
    assert len(fields) == 4
    field_names = [f.name for f in fields]
    assert "id" in field_names
    assert "name" in field_names
    assert "weight" in field_names
    assert "score" in field_names
    # All should have STRING type (default)
    assert all(f.type == "STRING" for f in fields)


def test_vertex_config_fields_with_db_flavor():
    """Test VertexConfig.fields() with db_flavor parameter."""
    vertex = Vertex(
        name="user",
        fields=[
            Field(name="id", type="INT"),
            Field(name="name"),  # None type
        ],
    )
    config = VertexConfig(vertices=[vertex])

    # With TigerGraph, should get fields with defaults applied
    fields = config.fields("user", db_flavor=DBFlavor.TIGERGRAPH, as_names=False)
    assert len(fields) == 2
    assert fields[0].type == "INT"
    assert fields[1].type == "STRING"  # Default applied

    # With ArangoDB, None types should remain None
    fields = config.fields("user", db_flavor=DBFlavor.ARANGO, as_names=False)
    assert fields[1].type is None  # Preserved

    # As names (backward compatible)
    field_names = config.fields("user", db_flavor=DBFlavor.TIGERGRAPH, as_names=True)
    assert field_names == ["id", "name"]
