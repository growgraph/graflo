"""Tests for Vertex and Field classes with typed fields."""

import logging

import pytest

from graflo.architecture.database_features import DatabaseProfile
from graflo.architecture.schema import VertexConfigDBAware
from graflo.architecture.schema.vertex import Field, FieldType, Vertex, VertexConfig
from graflo.onto import DBType

logger = logging.getLogger(__name__)


def test_field_with_none_type():
    """Test Field creation with None type (default, for ArangoDB etc)."""
    field = Field(name="name")
    assert field.name == "name"
    assert field.type is None


def test_field_with_explicit_type():
    """Test Field creation with explicit types."""
    for field_type in FieldType:
        field = Field(name="test", type=field_type)
        assert field.name == "test"
        assert field.type == field_type.value

    # Test case insensitive
    field = Field(name="test", type=FieldType.INT)
    assert field.type == "INT"

    field = Field(name="test", type=FieldType.STRING)
    assert field.type == "STRING"


def test_field_type_validation():
    """Test that invalid field types raise errors."""
    with pytest.raises(ValueError, match="not allowed"):
        Field.from_dict({"name": "test", "type": "INVALID_TYPE"})

    with pytest.raises(ValueError, match="not allowed"):
        Field.from_dict({"name": "test", "type": "invalid"})


def test_field_string_behavior():
    """Test that Field objects behave like strings."""
    field = Field(name="test_field", type=FieldType.INT)

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
    vertex = Vertex(name="user", properties=["id", "name", "email"])  # type: ignore[arg-type]

    assert len(vertex.properties) == 3
    assert all(isinstance(f, Field) for f in vertex.properties)
    assert vertex.properties[0].name == "id"
    assert vertex.properties[0].type is None  # Defaults to None
    assert vertex.properties[1].name == "name"
    assert vertex.properties[2].name == "email"

    assert vertex.property_names == ["id", "name", "email"]

    # Fields work in sets
    fields_set = set(vertex.properties)
    assert len(fields_set) == 3


def test_vertex_with_string_fields_dict_compatibility():
    """Test that property_names works for dict lookups."""
    vertex = Vertex(name="user", properties=["id", "name"])  # type: ignore[arg-type]
    test_dict = {"id": 1, "name": "John", "other": "ignored"}

    result = {f: test_dict[f] for f in vertex.property_names if f in test_dict}
    assert result == {"id": 1, "name": "John"}


def test_vertex_with_field_objects():
    """Test Vertex creation with list of Field objects."""
    fields = [
        Field(name="id", type=FieldType.INT),
        Field(name="name", type=FieldType.STRING),
        Field(name="age", type=FieldType.INT),
        Field(name="active", type=FieldType.BOOL),
    ]
    vertex = Vertex(name="user", properties=fields)

    assert len(vertex.properties) == 4
    assert vertex.properties[0].name == "id"
    assert vertex.properties[0].type == FieldType.INT
    assert vertex.properties[1].type == FieldType.STRING
    assert vertex.properties[3].type == FieldType.BOOL


def test_vertex_with_dict_fields():
    """Test Vertex creation with list of dicts (from YAML/JSON)."""
    fields = [
        {"name": "id", "type": "INT"},
        {"name": "name", "type": "STRING"},
        {"name": "email"},  # No type specified, defaults to None
    ]
    vertex = Vertex(name="user", properties=fields)  # type: ignore[arg-type]

    assert len(vertex.properties) == 3
    assert vertex.properties[0].name == "id"
    assert vertex.properties[0].type == FieldType.INT
    assert vertex.properties[1].type == FieldType.STRING
    assert vertex.properties[2].name == "email"
    assert vertex.properties[2].type is None


def test_vertex_mixed_field_inputs():
    """Test Vertex creation with mixed field types."""
    fields = [
        "id",  # string
        Field(name="name", type=FieldType.STRING),  # Field object
        {"name": "email", "type": "STRING"},  # dict
    ]
    vertex = Vertex(name="user", properties=fields)  # type: ignore[arg-type]

    assert len(vertex.properties) == 3
    assert all(isinstance(f, Field) for f in vertex.properties)
    assert vertex.properties[0].name == "id"
    assert vertex.properties[0].type is None
    assert vertex.properties[1].name == "name"
    assert vertex.properties[1].type == FieldType.STRING
    assert vertex.properties[2].name == "email"
    assert vertex.properties[2].type == FieldType.STRING


def test_vertex_config_property_names():
    """Test VertexConfig.property_names() returns string names."""
    vertex = Vertex(name="user", properties=["id", "name", "email"])  # type: ignore[arg-type]
    config = VertexConfig(vertices=[vertex])

    names = config.property_names("user")
    assert len(names) == 3
    assert all(isinstance(f, str) for f in names)
    assert set(names) == {"id", "name", "email"}
    assert names == ["id", "name", "email"]


def test_vertex_config_properties_with_objects():
    """Test VertexConfig.properties() returns Field objects; property_names() returns strings."""
    vertex = Vertex(
        name="user",
        properties=[
            Field(name="id", type=FieldType.INT),
            Field(name="name", type=FieldType.STRING),
        ],
    )
    config = VertexConfig(vertices=[vertex])

    props = config.properties("user")
    assert len(props) == 2
    assert all(isinstance(f, Field) for f in props)
    assert props[0].type == FieldType.INT
    assert props[1].type == FieldType.STRING

    assert config.property_names("user") == ["id", "name"]


def test_vertex_from_dict_with_string_fields():
    """Test Vertex.from_dict() with string fields (backward compatible)."""
    vertex_dict = {"name": "user", "properties": ["id", "name", "email"]}
    vertex = Vertex.from_dict(vertex_dict)

    assert vertex.name == "user"
    assert len(vertex.properties) == 3
    assert all(isinstance(f, Field) for f in vertex.properties)
    assert all(f.type is None for f in vertex.properties)


def test_vertex_from_dict_with_typed_fields():
    """Test Vertex.from_dict() with typed fields in dict."""
    vertex_dict = {
        "name": "user",
        "properties": [
            {"name": "id", "type": "INT"},
            {"name": "name", "type": "STRING"},
            {"name": "email"},
        ],
    }
    vertex = Vertex.from_dict(vertex_dict)

    assert vertex.name == "user"
    assert len(vertex.properties) == 3
    assert vertex.properties[0].type == FieldType.INT
    assert vertex.properties[1].type == FieldType.STRING
    assert vertex.properties[2].type is None


def test_vertex_identity_defaults_to_fields():
    """Test that identity defaults to all fields when not specified."""
    vertex = Vertex(
        name="user",
        properties=[
            Field(name="id", type=FieldType.INT),
            Field(name="email", type=FieldType.STRING),
        ],
    )

    assert vertex.identity == ["id", "email"]

    # Field objects should still be accessible
    assert len(vertex.properties) == 2
    assert vertex.properties[0].type == FieldType.INT


def test_vertex_with_explicit_identity():
    """Test vertex with explicit identity fields."""
    vertex = Vertex(
        name="user",
        properties=["id", "name", "email"],  # type: ignore[arg-type]
        identity=["id", "email"],
    )

    assert vertex.identity == ["id", "email"]
    names = vertex.property_names
    assert "id" in names
    assert "name" in names
    assert "email" in names


def test_field_all_types():
    """Test all allowed field types."""
    for field_type in FieldType:
        field = Field(name=f"field_{field_type.value.lower()}", type=field_type)
        assert field.type == field_type.value


def test_invalid_field_type_in_dict():
    """Test that invalid field types in dict raise error."""
    with pytest.raises(ValueError, match="not allowed"):
        Vertex(
            name="user",
            properties=[{"name": "test", "type": "INVALID"}],  # type: ignore[arg-type]
        )


def test_init(vertex_pub):
    """Original test: Test Vertex.from_dict() with existing fixture."""
    vc = Vertex.from_dict(vertex_pub)
    assert vc.identity == ["arxiv", "doi", "created", "data_source"]
    # Fields are now Field objects, so check count
    assert len(vc.properties) == 4
    # Verify they're Field objects
    assert all(isinstance(f, Field) for f in vc.properties)


def test_get_properties_with_defaults_tigergraph():
    """DB-aware vertex properties default None types to STRING for TigerGraph."""
    # Create vertex with some fields that have None type
    vertex = Vertex(
        name="user",
        properties=[  # type: ignore[arg-type]
            Field(name="id", type=FieldType.INT),  # Already has type
            Field(name="name"),  # None type
            Field(name="email", type=FieldType.STRING),  # Already has type
            "address",  # String field (will be Field with None type)
        ],
    )

    config = VertexConfig(vertices=[vertex])
    db_cfg = VertexConfigDBAware(
        logical=config,
        database_features=DatabaseProfile(db_flavor=DBType.TIGERGRAPH),
    )
    props = db_cfg.properties("user")
    assert len(props) == 4
    assert props[0].name == "id"
    assert props[0].type == "INT"
    assert props[1].name == "name"
    assert props[1].type == "STRING"  # Default applied
    assert props[2].name == "email"
    assert props[2].type == "STRING"
    assert props[3].name == "address"
    assert props[3].type == "STRING"  # Default applied


def test_get_properties_with_defaults_other_db():
    """DB-aware vertex properties preserve None types for non-TigerGraph DBs."""
    vertex = Vertex(
        name="user",
        properties=[
            Field(name="id", type=FieldType.INT),
            Field(name="name"),  # None type
        ],
    )

    config = VertexConfig(vertices=[vertex])
    db_cfg = VertexConfigDBAware(
        logical=config,
        database_features=DatabaseProfile(db_flavor=DBType.ARANGO),
    )
    props = db_cfg.properties("user")
    assert len(props) == 2
    assert props[0].type == "INT"
    assert props[1].name == "name"
    assert props[1].type is None  # Preserved

    db_cfg = VertexConfigDBAware(
        logical=config,
        database_features=DatabaseProfile(db_flavor=DBType.NEO4J),
    )
    props = db_cfg.properties("user")
    assert props[1].type is None  # Preserved


def test_get_properties_with_defaults_none():
    """Logical vertex properties are preserved by default."""
    vertex = Vertex(
        name="user",
        properties=[
            Field(name="id", type=FieldType.INT),
            Field(name="name"),  # None type
        ],
    )

    props = vertex.get_properties()
    assert len(props) == 2
    assert props[0].type == "INT"
    assert props[1].type is None  # Preserved


def test_vertex_config_properties_with_db_flavor():
    """DB-aware VertexConfig wrapper applies DB-specific defaults."""
    vertex = Vertex(
        name="user",
        properties=[
            Field(name="id", type=FieldType.INT),
            Field(name="name"),  # None type
        ],
    )
    config = VertexConfig(vertices=[vertex])

    props = config.properties("user")
    assert props[1].type is None  # Preserved

    db_cfg = VertexConfigDBAware(
        logical=config,
        database_features=DatabaseProfile(db_flavor=DBType.TIGERGRAPH),
    )
    props = db_cfg.properties("user")
    assert len(props) == 2
    assert props[0].type == "INT"
    assert props[1].type == "STRING"  # Default applied

    assert config.property_names("user") == ["id", "name"]


def test_vertex_config_remove_vertices():
    """Test VertexConfig.remove_vertices removes vertices and updates blank_vertices."""
    v1 = Vertex.from_dict({"name": "a", "properties": ["id"]})
    v2 = Vertex.from_dict({"name": "b", "properties": ["id"]})
    v3 = Vertex.from_dict({"name": "c", "properties": ["id"]})
    config = VertexConfig(
        vertices=[v1, v2, v3],
        blank_vertices=["b"],
    )
    assert config.vertex_set == {"a", "b", "c"}
    config.remove_vertices({"b", "c"})
    assert config.vertex_set == {"a"}
    assert config.vertices[0].name == "a"
    assert config.blank_vertices == []
