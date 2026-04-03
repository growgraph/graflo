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
    assert all(":" in key for key in risk_map.keys())
