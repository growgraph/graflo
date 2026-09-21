"""Live schema drift: what a database holds that its declared schema does not.

A database drifts from its schema when something writes to it outside the
declared contract -- a hand-run ``SET``, another loader, a half-applied
migration. Drift is found by introspecting the live database and comparing the
result with the declared :class:`~graflo.architecture.schema.Schema`.

The comparison is deliberately **presence-only**: which vertex types, edge
types and property names exist on one side and not the other. It does not
compare property types, identities or indexes, because introspection cannot
report them faithfully -- Cypher backends return untyped properties, identity
is guessed from property names, and secondary indexes are not read back.
Comparing those would report differences introspection invented, not ones the
database has.

Introspection of most backends samples a bounded number of rows per type. A
declared property no sampled row carries is then *not seen*, which is not proof
that it is absent; :attr:`LiveSchemaDrift.sampled` says which case applies.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, computed_field

from graflo.architecture.schema import Schema
from graflo.onto import DBType

#: (source vertex, relation, target vertex), in logical names where known.
EdgeKey = tuple[str, str, str]


class LiveSchemaDrift(BaseModel):
    """Presence-only differences between a live database and its schema.

    Names are logical where the declared schema maps them, and the raw storage
    name where it does not (anything undeclared has no logical name).
    """

    undeclared_vertices: list[str] = Field(
        default_factory=list,
        description="Vertex types in the database that the schema does not declare.",
    )
    missing_vertices: list[str] = Field(
        default_factory=list,
        description="Declared vertex types with no node in the database (or sample).",
    )
    undeclared_properties: dict[str, list[str]] = Field(
        default_factory=dict,
        description="Per declared vertex type, properties present but not declared.",
    )
    missing_properties: dict[str, list[str]] = Field(
        default_factory=dict,
        description=(
            "Per declared vertex type present in the database, declared "
            "properties no examined node carries."
        ),
    )
    undeclared_edges: list[EdgeKey] = Field(
        default_factory=list,
        description="(source, relation, target) patterns in the data, not declared.",
    )
    missing_edges: list[EdgeKey] = Field(
        default_factory=list,
        description="Declared (source, relation, target) patterns with no edge.",
    )
    sampled: bool = Field(
        default=True,
        description=(
            "True when introspection examined a sample of rows per type, so a "
            "missing entry means 'not seen in the sample', not 'absent'."
        ),
    )

    @computed_field
    @property
    def has_drift(self) -> bool:
        """True when the database holds anything the schema does not declare."""
        return bool(
            self.undeclared_vertices
            or self.undeclared_properties
            or self.undeclared_edges
        )


def _edge_storage_name(db_aware: Any, edge: Any, flavor: DBType) -> str:
    """The relation name an introspected edge of *edge*'s type reports."""
    from graflo.db.traversal import edge_query_name

    return edge_query_name(db_aware, edge, flavor) or edge.relation


def compare_live_schema(
    declared: Schema,
    observed: Schema,
    *,
    db_flavor: DBType,
    sampled: bool = True,
) -> LiveSchemaDrift:
    """Compare an introspected schema with the declared one, by presence only.

    Args:
        declared: The schema the database is supposed to follow.
        observed: What ``Connection.introspect_graph_schema`` recovered. Its
            vertex and relation names are storage names.
        db_flavor: The database's flavor, for mapping logical names to storage
            names.
        sampled: Whether introspection sampled rows rather than reading a
            complete catalogue.

    Returns:
        LiveSchemaDrift: every difference, with lists sorted for stable output.
    """
    db_aware = declared.resolve_db_aware(db_flavor)
    declared_vertices = declared.core_schema.vertex_config
    logical_of = {
        db_aware.vertex_config.vertex_dbname(name): name
        for name in declared_vertices.vertex_set
    }

    def logical(storage_name: str) -> str:
        return logical_of.get(storage_name, storage_name)

    observed_props: dict[str, set[str]] = {
        logical(v.name): set(v.property_names)
        for v in observed.core_schema.vertex_config.vertices
    }

    drift = LiveSchemaDrift(sampled=sampled)
    drift.undeclared_vertices = sorted(
        name for name in observed_props if name not in declared_vertices.vertex_set
    )
    drift.missing_vertices = sorted(
        name for name in declared_vertices.vertex_set if name not in observed_props
    )
    for name in sorted(declared_vertices.vertex_set):
        present = observed_props.get(name)
        if present is None:
            continue
        declared_names = set(declared_vertices[name].property_names) | set(
            db_aware.vertex_config.identity_fields(name)
        )
        extra = sorted(present - declared_names)
        absent = sorted(set(declared_vertices[name].property_names) - present)
        if extra:
            drift.undeclared_properties[name] = extra
        if absent:
            drift.missing_properties[name] = absent

    declared_edges: set[EdgeKey] = {
        (
            edge.source,
            _edge_storage_name(db_aware, edge, db_flavor),
            edge.target,
        )
        for edge in declared.core_schema.edge_config.edges
    }
    observed_edges: set[EdgeKey] = set()
    # A backend that stores edges by collection may report no relation name;
    # such an edge is matched on its endpoints alone, to every declared edge
    # between them, rather than reported as drift it cannot be shown to be.
    matched_by_endpoints: set[EdgeKey] = set()
    for edge in observed.core_schema.edge_config.edges:
        source, target = logical(edge.source), logical(edge.target)
        if edge.relation:
            observed_edges.add((source, edge.relation, target))
            continue
        candidates = {
            key for key in declared_edges if (key[0], key[2]) == (source, target)
        }
        if candidates:
            matched_by_endpoints |= candidates
        else:
            observed_edges.add((source, "", target))
    drift.undeclared_edges = sorted(observed_edges - declared_edges)
    drift.missing_edges = sorted(declared_edges - observed_edges - matched_by_endpoints)
    return drift
