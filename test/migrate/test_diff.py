from graflo.architecture.schema import Schema
from graflo.migrate.diff import SchemaDiff
from graflo.migrate.models import OperationType


def _schema_v1() -> Schema:
    return Schema.from_dict(
        {
            "metadata": {"name": "kg", "version": "1.0.0"},
            "core_schema": {
                "vertex_config": {
                    "vertices": [
                        {
                            "name": "person",
                            "properties": [{"name": "id", "type": "STRING"}, "name"],
                            "identity": ["id"],
                        },
                        {
                            "name": "company",
                            "properties": [{"name": "id", "type": "STRING"}, "name"],
                            "identity": ["id"],
                        },
                    ]
                },
                "edge_config": {
                    "edges": [
                        {
                            "source": "person",
                            "target": "company",
                            "relation": "works_at",
                        }
                    ]
                },
            },
            "db_profile": {
                "vertex_indexes": {"person": [{"fields": ["name"], "unique": False}]}
            },
        }
    )


def _schema_v2() -> Schema:
    return Schema.from_dict(
        {
            "metadata": {"name": "kg", "version": "1.1.0"},
            "core_schema": {
                "vertex_config": {
                    "vertices": [
                        {
                            "name": "person",
                            "properties": [
                                {"name": "id", "type": "STRING"},
                                {"name": "full_name", "type": "STRING"},
                                {"name": "age", "type": "INT"},
                            ],
                            "identity": ["id"],
                        },
                        {
                            "name": "company",
                            "properties": [{"name": "id", "type": "STRING"}, "name"],
                            "identity": ["id"],
                        },
                        {
                            "name": "country",
                            "properties": [{"name": "code", "type": "STRING"}],
                            "identity": ["code"],
                        },
                    ]
                },
                "edge_config": {
                    "edges": [
                        {
                            "source": "person",
                            "target": "company",
                            "relation": "works_at",
                        },
                        {
                            "source": "company",
                            "target": "country",
                            "relation": "located_in",
                        },
                    ]
                },
            },
            "db_profile": {
                "vertex_indexes": {
                    "person": [
                        {"fields": ["full_name"], "unique": False},
                        {"fields": ["age"], "unique": False},
                    ]
                }
            },
        }
    )


def test_schema_diff_detects_structural_changes():
    diff = SchemaDiff(schema_old=_schema_v1(), schema_new=_schema_v2())
    result = diff.compare()
    op_types = {op.op_type for op in result.operations}
    targets = {op.target for op in result.operations}

    assert OperationType.ADD_VERTEX in op_types
    assert OperationType.ADD_EDGE in op_types
    assert OperationType.ADD_VERTEX_FIELD in op_types
    assert OperationType.REMOVE_VERTEX_FIELD in op_types
    assert any("vertex:country" in target for target in targets)
    assert any("full_name" in target for target in targets)


def test_schema_diff_backward_compatibility_false_on_removal():
    diff = SchemaDiff(schema_old=_schema_v1(), schema_new=_schema_v2())
    diff.compare()
    assert diff.is_backward_compatible() is False


def test_schema_diff_risk_assessment_is_populated():
    diff = SchemaDiff(schema_old=_schema_v1(), schema_new=_schema_v2())
    diff.compare()
    risk_map = diff.risk_assessment()
    assert risk_map
    assert all(":" in key for key in risk_map)


def _list_schema(item_type: str | None, *, on_edge: bool = False) -> Schema:
    """A schema whose ``tags`` property is a LIST of *item_type*, or absent."""
    tags = (
        []
        if item_type is None
        else [{"name": "tags", "type": "LIST", "item_type": item_type}]
    )
    return Schema.from_dict(
        {
            "metadata": {"name": "kg", "version": "1.0.0"},
            "core_schema": {
                "vertex_config": {
                    "vertices": [
                        {
                            "name": "person",
                            "properties": [{"name": "id", "type": "STRING"}]
                            + ([] if on_edge else tags),
                            "identity": ["id"],
                        },
                        {
                            "name": "company",
                            "properties": [{"name": "id", "type": "STRING"}],
                            "identity": ["id"],
                        },
                    ]
                },
                "edge_config": {
                    "edges": [
                        {
                            "source": "person",
                            "target": "company",
                            "relation": "works_at",
                            "properties": tags if on_edge else [],
                        }
                    ]
                },
            },
        }
    )


class TestListItemTypeIsVisible:
    """A LIST whose element type changed.

    ``_field_map`` keyed on ``type`` alone, so ``LIST<STRING>`` -> ``LIST<INT>``
    produced no operation at all: the migration plan came back empty for a
    change that rewrites every stored value.
    """

    def test_a_vertex_list_item_type_change_emits_a_field_type_operation(self) -> None:
        result = SchemaDiff(_list_schema("STRING"), _list_schema("INT")).compare()
        changes = [
            op
            for op in result.operations
            if op.op_type == OperationType.CHANGE_VERTEX_FIELD_TYPE
        ]
        assert len(changes) == 1
        assert changes[0].old_value == {"type": "LIST", "item_type": "STRING"}
        assert changes[0].new_value == {"type": "LIST", "item_type": "INT"}

    def test_an_edge_list_item_type_change_emits_a_field_type_operation(self) -> None:
        result = SchemaDiff(
            _list_schema("STRING", on_edge=True), _list_schema("INT", on_edge=True)
        ).compare()
        changes = [
            op
            for op in result.operations
            if op.op_type == OperationType.CHANGE_EDGE_FIELD_TYPE
        ]
        assert len(changes) == 1
        assert changes[0].new_value == {"type": "LIST", "item_type": "INT"}

    def test_an_unchanged_list_emits_nothing(self) -> None:
        result = SchemaDiff(_list_schema("STRING"), _list_schema("STRING")).compare()
        assert result.operations == []

    def test_an_added_field_payload_carries_item_type(self) -> None:
        result = SchemaDiff(_list_schema(None), _list_schema("STRING")).compare()
        added = [
            op
            for op in result.operations
            if op.op_type == OperationType.ADD_VERTEX_FIELD
        ]
        assert len(added) == 1
        assert added[0].new_value == {
            "name": "tags",
            "type": "LIST",
            "item_type": "STRING",
        }
