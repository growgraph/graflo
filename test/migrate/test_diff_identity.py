"""Identity-aware schema diffing.

Before this, the differ compared only ``Vertex.identity``, so an identity *mode*
change produced an empty diff and ``REKEY_VERTEX`` had no producer at all.
"""

from graflo.architecture.schema import Schema
from graflo.migrate.diff import SchemaDiff
from graflo.migrate.models import OperationType, RiskLevel
from graflo.migrate.planner import MigrationPlanner


def _schema(vertex: dict) -> Schema:
    return Schema.model_validate(
        {
            "metadata": {"name": "diff-demo", "version": "1.0.0"},
            "graph": {
                "vertex_config": {"vertices": [vertex]},
                "edge_config": {"edges": []},
            },
        }
    )


def _op_types(old: dict, new: dict) -> list[OperationType]:
    return [op.op_type for op in SchemaDiff(_schema(old), _schema(new)).operations()]


NATURAL = {"name": "party", "properties": ["id", "uid", "email"], "identity": ["id"]}


class TestIdentityModeIsVisible:
    def test_natural_to_hash_is_no_longer_an_empty_diff(self):
        new = {
            "name": "party",
            "properties": ["id", "uid", "email"],
            "hash_identity_properties": ["email"],
        }

        assert OperationType.CHANGE_VERTEX_IDENTITY in _op_types(NATURAL, new)

    def test_natural_to_assigned_is_detected(self):
        new = {"name": "party", "properties": ["id", "uid", "email"], "assigned": True}

        assert OperationType.CHANGE_VERTEX_IDENTITY in _op_types(NATURAL, new)

    def test_flat_hash_to_funnel_is_detected(self):
        """Both resolve to mode ``hash``, so only the funnel itself distinguishes them."""
        old = {
            "name": "party",
            "properties": ["id", "email", "phone"],
            "hash_identity_properties": ["email"],
        }
        new = {
            "name": "party",
            "properties": ["id", "email", "phone"],
            "identity_funnel": {"branches": [{"id": "email", "fields": ["email"]}]},
        }

        ops = _op_types(old, new)
        assert OperationType.CHANGE_VERTEX_IDENTITY in ops
        assert OperationType.REKEY_VERTEX in ops

    def test_branch_reorder_rekeys_because_the_key_changes(self):
        def _with_branches(branches: list[dict]) -> dict:
            return {
                "name": "party",
                "properties": ["id", "email", "phone"],
                "identity_funnel": {"branches": branches},
            }

        email = {"id": "email", "fields": ["email"]}
        phone = {"id": "phone", "fields": ["phone"]}

        ops = _op_types(_with_branches([email, phone]), _with_branches([phone, email]))
        assert OperationType.REKEY_VERTEX in ops

    def test_identical_funnel_is_an_empty_identity_diff(self):
        vertex = {
            "name": "party",
            "properties": ["id", "email"],
            "identity_funnel": {"branches": [{"id": "email", "fields": ["email"]}]},
        }

        ops = _op_types(vertex, dict(vertex))
        assert OperationType.CHANGE_VERTEX_IDENTITY not in ops
        assert OperationType.REKEY_VERTEX not in ops

    def test_hash_source_change_is_detected(self):
        old = {
            "name": "party",
            "properties": ["id", "email", "phone"],
            "hash_identity_properties": ["email"],
        }
        new = {
            "name": "party",
            "properties": ["id", "email", "phone"],
            "hash_identity_properties": ["email", "phone"],
        }

        assert OperationType.CHANGE_VERTEX_IDENTITY in _op_types(old, new)

    def test_identical_schemas_emit_nothing(self):
        assert _op_types(NATURAL, NATURAL) == []

    def test_payload_carries_the_mode(self):
        new = {
            "name": "party",
            "properties": ["id", "uid", "email"],
            "hash_identity_properties": ["email"],
        }
        ops = SchemaDiff(_schema(NATURAL), _schema(new)).operations()
        change = next(
            op for op in ops if op.op_type == OperationType.CHANGE_VERTEX_IDENTITY
        )

        assert change.old_value["mode"] == "natural"
        assert change.new_value["mode"] == "hash"


class TestRekeyEmission:
    def test_mode_change_requires_a_rekey(self):
        new = {
            "name": "party",
            "properties": ["id", "uid", "email"],
            "hash_identity_properties": ["email"],
        }

        assert OperationType.REKEY_VERTEX in _op_types(NATURAL, new)

    def test_swapping_the_natural_key_requires_a_rekey(self):
        new = {
            "name": "party",
            "properties": ["id", "uid", "email"],
            "identity": ["uid"],
        }

        assert OperationType.REKEY_VERTEX in _op_types(NATURAL, new)

    def test_widening_a_composite_key_does_not_require_a_rekey(self):
        """Every stored key is still addressable, so only the identity op is emitted."""
        new = {
            "name": "party",
            "properties": ["id", "uid", "email"],
            "identity": ["id", "uid"],
        }
        op_types = _op_types(NATURAL, new)

        assert OperationType.CHANGE_VERTEX_IDENTITY in op_types
        assert OperationType.REKEY_VERTEX not in op_types

    def test_rekey_is_critical_and_blocked_by_default(self):
        new = {
            "name": "party",
            "properties": ["id", "uid", "email"],
            "identity": ["uid"],
        }
        diff = SchemaDiff(_schema(NATURAL), _schema(new)).compare()
        plan = MigrationPlanner().build(diff)

        blocked = {op.op_type for op in plan.blocked_operations}
        assert OperationType.REKEY_VERTEX in blocked
        assert all(
            op.risk == RiskLevel.CRITICAL
            for op in plan.blocked_operations
            if op.op_type == OperationType.REKEY_VERTEX
        )

    def test_a_rekey_conflict_is_reported(self):
        new = {
            "name": "party",
            "properties": ["id", "uid", "email"],
            "identity": ["uid"],
        }
        result = SchemaDiff(_schema(NATURAL), _schema(new)).compare()

        assert any(
            conflict.key == "vertex:party:rekey" for conflict in result.conflicts
        )


class TestSecondaryIdentityDiff:
    def test_adding_one_is_detected(self):
        new = {
            "name": "party",
            "properties": ["id", "uid", "email"],
            "identity": ["id"],
            "secondary_identities": [{"name": "by_email", "fields": ["email"]}],
        }

        assert OperationType.CHANGE_SECONDARY_IDENTITY in _op_types(NATURAL, new)

    def test_it_is_medium_risk_not_critical(self):
        """Lookup-plane only: it adds or drops a derived index, it does not rekey."""
        new = {
            "name": "party",
            "properties": ["id", "uid", "email"],
            "identity": ["id"],
            "secondary_identities": [{"name": "by_email", "fields": ["email"]}],
        }
        ops = SchemaDiff(_schema(NATURAL), _schema(new)).operations()
        change = next(
            op for op in ops if op.op_type == OperationType.CHANGE_SECONDARY_IDENTITY
        )

        assert change.risk == RiskLevel.MEDIUM

    def test_removing_one_is_detected(self):
        old = {
            "name": "party",
            "properties": ["id", "uid", "email"],
            "identity": ["id"],
            "secondary_identities": [{"name": "by_email", "fields": ["email"]}],
        }

        assert OperationType.CHANGE_SECONDARY_IDENTITY in _op_types(old, NATURAL)

    def test_it_does_not_fire_when_the_primary_identity_changes_alone(self):
        new = {
            "name": "party",
            "properties": ["id", "uid", "email"],
            "identity": ["uid"],
        }

        assert OperationType.CHANGE_SECONDARY_IDENTITY not in _op_types(NATURAL, new)
