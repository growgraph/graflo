"""Schema diff engine for migration planning."""

from __future__ import annotations

from typing import Any

from graflo.architecture.edge import Edge
from graflo.architecture.schema import Schema
from graflo.architecture.vertex import Field
from graflo.migrate.models import (
    MigrationOperation,
    OperationType,
    SchemaConflict,
    SchemaDiffResult,
)
from graflo.migrate.risk import classify_operation, is_backward_compatible_operations


def _field_map(fields: list[Field]) -> dict[str, str | None]:
    return {field.name: field.type for field in fields}


def _vertex_index_tuples(schema: Schema, vertex_name: str) -> set[tuple]:
    indexes = schema.db_profile.vertex_secondary_indexes(vertex_name)
    return {
        (
            tuple(index.fields),
            bool(index.unique),
            str(index.type),
            bool(index.sparse),
        )
        for index in indexes
    }


def _edge_index_tuples(schema: Schema, edge: Edge) -> set[tuple]:
    indexes = schema.db_profile.edge_secondary_indexes(edge.edge_id)
    return {
        (
            tuple(index.fields),
            bool(index.unique),
            str(index.type),
            bool(index.sparse),
        )
        for index in indexes
    }


class SchemaDiff:
    """Compute a typed structural diff between two schemas."""

    def __init__(self, schema_old: Schema, schema_new: Schema):
        self.schema_old = schema_old
        self.schema_new = schema_new
        self.schema_old.finish_init()
        self.schema_new.finish_init()
        self._result: SchemaDiffResult | None = None

    def compare(self) -> SchemaDiffResult:
        """Build a full diff result including operations/conflicts/warnings."""
        operations: list[MigrationOperation] = []
        conflicts: list[SchemaConflict] = []
        warnings: list[str] = []

        operations.extend(self._diff_vertices(conflicts))
        operations.extend(self._diff_edges(conflicts))
        operations.extend(self._diff_database_features())

        self._result = SchemaDiffResult(
            operations=operations, conflicts=conflicts, warnings=warnings
        )
        return self._result

    def operations(self) -> list[MigrationOperation]:
        """Return diff operations, calculating if needed."""
        if self._result is None:
            self.compare()
        if self._result is None:
            return []
        return self._result.operations

    def is_backward_compatible(self) -> bool:
        """True when schema_new is additive compared to schema_old."""
        return is_backward_compatible_operations(self.operations())

    def risk_assessment(self) -> dict[str, str]:
        """Map operation keys to risk labels."""
        risk_map: dict[str, str] = {}
        for op in self.operations():
            risk_map[f"{op.op_type}:{op.target}"] = op.risk.value
        return risk_map

    def validate_union_safety(self) -> list[SchemaConflict]:
        """Return conflicts from latest compare call."""
        if self._result is None:
            self.compare()
        if self._result is None:
            return []
        return self._result.conflicts

    def _diff_vertices(
        self, conflicts: list[SchemaConflict]
    ) -> list[MigrationOperation]:
        old_vertices = {
            vertex.name: vertex
            for vertex in self.schema_old.graph.vertex_config.vertices
        }
        new_vertices = {
            vertex.name: vertex
            for vertex in self.schema_new.graph.vertex_config.vertices
        }
        old_names = set(old_vertices)
        new_names = set(new_vertices)
        operations: list[MigrationOperation] = []

        for name in sorted(new_names - old_names):
            operations.append(
                self._op(
                    OperationType.ADD_VERTEX,
                    f"vertex:{name}",
                    None,
                    new_vertices[name].to_dict(),
                )
            )
        for name in sorted(old_names - new_names):
            operations.append(
                self._op(
                    OperationType.REMOVE_VERTEX,
                    f"vertex:{name}",
                    old_vertices[name].to_dict(),
                    None,
                    reversible=False,
                )
            )

        for name in sorted(old_names & new_names):
            old_vertex = old_vertices[name]
            new_vertex = new_vertices[name]

            if list(old_vertex.identity) != list(new_vertex.identity):
                operations.append(
                    self._op(
                        OperationType.CHANGE_VERTEX_IDENTITY,
                        f"vertex:{name}:identity",
                        list(old_vertex.identity),
                        list(new_vertex.identity),
                        reversible=False,
                    )
                )
                conflicts.append(
                    SchemaConflict(
                        key=f"vertex:{name}:identity",
                        message="Vertex identity changed; requires explicit rekey strategy.",
                        risk=classify_operation(OperationType.CHANGE_VERTEX_IDENTITY),
                    )
                )

            old_fields = _field_map(old_vertex.fields)
            new_fields = _field_map(new_vertex.fields)
            old_field_names = set(old_fields)
            new_field_names = set(new_fields)

            for field_name in sorted(new_field_names - old_field_names):
                operations.append(
                    self._op(
                        OperationType.ADD_VERTEX_FIELD,
                        f"vertex:{name}:field:{field_name}",
                        None,
                        {"name": field_name, "type": new_fields[field_name]},
                    )
                )
            for field_name in sorted(old_field_names - new_field_names):
                operations.append(
                    self._op(
                        OperationType.REMOVE_VERTEX_FIELD,
                        f"vertex:{name}:field:{field_name}",
                        {"name": field_name, "type": old_fields[field_name]},
                        None,
                        reversible=False,
                    )
                )
            for field_name in sorted(old_field_names & new_field_names):
                if old_fields[field_name] != new_fields[field_name]:
                    operations.append(
                        self._op(
                            OperationType.CHANGE_VERTEX_FIELD_TYPE,
                            f"vertex:{name}:field:{field_name}:type",
                            old_fields[field_name],
                            new_fields[field_name],
                            reversible=False,
                        )
                    )

        return operations

    def _diff_edges(self, conflicts: list[SchemaConflict]) -> list[MigrationOperation]:
        old_edges = {
            edge.edge_id: edge for edge in self.schema_old.graph.edge_config.edges
        }
        new_edges = {
            edge.edge_id: edge for edge in self.schema_new.graph.edge_config.edges
        }
        old_ids = set(old_edges)
        new_ids = set(new_edges)
        operations: list[MigrationOperation] = []

        for edge_id in sorted(new_ids - old_ids):
            edge = new_edges[edge_id]
            operations.append(
                self._op(
                    OperationType.ADD_EDGE, f"edge:{edge_id}", None, edge.to_dict()
                )
            )
        for edge_id in sorted(old_ids - new_ids):
            edge = old_edges[edge_id]
            operations.append(
                self._op(
                    OperationType.REMOVE_EDGE,
                    f"edge:{edge_id}",
                    edge.to_dict(),
                    None,
                    reversible=False,
                )
            )

        for edge_id in sorted(old_ids & new_ids):
            old_edge = old_edges[edge_id]
            new_edge = new_edges[edge_id]

            if old_edge.identities != new_edge.identities:
                operations.append(
                    self._op(
                        OperationType.CHANGE_EDGE_IDENTITY,
                        f"edge:{edge_id}:identity",
                        old_edge.identities,
                        new_edge.identities,
                        reversible=False,
                    )
                )
                conflicts.append(
                    SchemaConflict(
                        key=f"edge:{edge_id}:identity",
                        message="Edge identity changed; may impact deduplication semantics.",
                        risk=classify_operation(OperationType.CHANGE_EDGE_IDENTITY),
                    )
                )

            old_direct = _field_map(old_edge.weights.direct if old_edge.weights else [])
            new_direct = _field_map(new_edge.weights.direct if new_edge.weights else [])
            old_names = set(old_direct)
            new_names = set(new_direct)

            for field_name in sorted(new_names - old_names):
                operations.append(
                    self._op(
                        OperationType.ADD_EDGE_FIELD,
                        f"edge:{edge_id}:field:{field_name}",
                        None,
                        {"name": field_name, "type": new_direct[field_name]},
                    )
                )
            for field_name in sorted(old_names - new_names):
                operations.append(
                    self._op(
                        OperationType.REMOVE_EDGE_FIELD,
                        f"edge:{edge_id}:field:{field_name}",
                        {"name": field_name, "type": old_direct[field_name]},
                        None,
                        reversible=False,
                    )
                )
            for field_name in sorted(old_names & new_names):
                if old_direct[field_name] != new_direct[field_name]:
                    operations.append(
                        self._op(
                            OperationType.CHANGE_EDGE_FIELD_TYPE,
                            f"edge:{edge_id}:field:{field_name}:type",
                            old_direct[field_name],
                            new_direct[field_name],
                            reversible=False,
                        )
                    )

        return operations

    def _diff_database_features(self) -> list[MigrationOperation]:
        operations: list[MigrationOperation] = []
        all_vertices = (
            self.schema_old.graph.vertex_config.vertex_set
            | self.schema_new.graph.vertex_config.vertex_set
        )

        for vertex_name in sorted(all_vertices):
            old_ix = (
                _vertex_index_tuples(self.schema_old, vertex_name)
                if vertex_name in self.schema_old.graph.vertex_config.vertex_set
                else set()
            )
            new_ix = (
                _vertex_index_tuples(self.schema_new, vertex_name)
                if vertex_name in self.schema_new.graph.vertex_config.vertex_set
                else set()
            )
            for ix in sorted(new_ix - old_ix):
                operations.append(
                    self._op(
                        OperationType.ADD_VERTEX_INDEX,
                        f"vertex:{vertex_name}:index:{ix}",
                        None,
                        ix,
                    )
                )
            for ix in sorted(old_ix - new_ix):
                operations.append(
                    self._op(
                        OperationType.REMOVE_VERTEX_INDEX,
                        f"vertex:{vertex_name}:index:{ix}",
                        ix,
                        None,
                    )
                )

        old_edges = {
            edge.edge_id: edge for edge in self.schema_old.graph.edge_config.edges
        }
        new_edges = {
            edge.edge_id: edge for edge in self.schema_new.graph.edge_config.edges
        }
        all_edge_ids = set(old_edges) | set(new_edges)
        for edge_id in sorted(all_edge_ids):
            old_ix = (
                _edge_index_tuples(self.schema_old, old_edges[edge_id])
                if edge_id in old_edges
                else set()
            )
            new_ix = (
                _edge_index_tuples(self.schema_new, new_edges[edge_id])
                if edge_id in new_edges
                else set()
            )
            for ix in sorted(new_ix - old_ix):
                operations.append(
                    self._op(
                        OperationType.ADD_EDGE_INDEX,
                        f"edge:{edge_id}:index:{ix}",
                        None,
                        ix,
                    )
                )
            for ix in sorted(old_ix - new_ix):
                operations.append(
                    self._op(
                        OperationType.REMOVE_EDGE_INDEX,
                        f"edge:{edge_id}:index:{ix}",
                        ix,
                        None,
                    )
                )

        return operations

    @staticmethod
    def _op(
        op_type: OperationType,
        target: str,
        old_value: Any,
        new_value: Any,
        reversible: bool = True,
    ) -> MigrationOperation:
        return MigrationOperation(
            op_type=op_type,
            target=target,
            old_value=old_value,
            new_value=new_value,
            risk=classify_operation(op_type),
            reversible=reversible,
        )
