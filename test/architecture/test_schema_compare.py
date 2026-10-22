"""Two versions side by side: rows aligned through renames, before/after per field.

The fixture is two branches off one ancestor: one adds fields and rewrites a
description, the other renames a class, adds a clashing field and an edge
property, and rewrites the same description. Every row the comparison must get
right appears once.
"""

from __future__ import annotations

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution.apply import apply_evolution
from graflo.architecture.evolution.autogenerate import RenameHints
from graflo.architecture.evolution.codec import ops_from_dicts
from graflo.architecture.evolution.compare import (
    RelationRow,
    SchemaComparison,
    VertexRow,
    compare_schemas,
)
from graflo.architecture.evolution.merge3 import merge_three_way

BASE = {
    "schema": {
        "metadata": {"name": "commerce"},
        "graph": {
            "vertex_config": {
                "vertices": [
                    {
                        "name": "Customer",
                        "properties": [
                            {"name": "email", "type": "STRING"},
                            {"name": "name", "type": "STRING"},
                        ],
                        "identity": ["email"],
                    },
                    {
                        "name": "Company",
                        "description": "An organisation",
                        "properties": [{"name": "id", "type": "STRING"}],
                        "identity": ["id"],
                    },
                    {
                        "name": "Product",
                        "properties": [{"name": "id", "type": "STRING"}],
                        "identity": ["id"],
                    },
                ]
            },
            "edge_config": {
                "edges": [
                    {"source": "Customer", "target": "Company", "relation": "works_at"},
                    {
                        "source": "Customer",
                        "target": "Product",
                        "relation": "purchases",
                    },
                ]
            },
        },
    }
}

TEAM_LEFT = [
    {
        "op": "add_vertex_properties",
        "additions": {
            "Customer": [{"name": "tier", "type": "STRING"}],
            "Company": [{"name": "industry", "type": "STRING"}],
            "Product": [{"name": "category", "type": "STRING"}],
        },
    },
    {
        "op": "set_vertex_descriptions",
        "descriptions": {"Company": "Who customers work for"},
    },
]

TEAM_RIGHT = [
    {"op": "rename_vertices", "renames": {"Customer": "Account"}},
    {
        "op": "add_vertex_properties",
        "additions": {"Product": [{"name": "category", "type": "INT"}]},
    },
    {
        "op": "add_edge_properties",
        "additions": {"purchases": [{"name": "channel", "type": "STRING"}]},
    },
    {
        "op": "set_vertex_descriptions",
        "descriptions": {"Company": "A business customer"},
    },
]


def _branches() -> tuple[
    GraphManifest, GraphManifest, GraphManifest, RenameHints, RenameHints
]:
    base = GraphManifest.from_dict(BASE)
    left_ops = ops_from_dicts(TEAM_LEFT)
    right_ops = ops_from_dicts(TEAM_RIGHT)
    left = apply_evolution(base, left_ops)
    right = apply_evolution(base, right_ops)
    return (
        base,
        left,
        right,
        RenameHints.from_ops(left_ops, base=base),
        RenameHints.from_ops(right_ops, base=base),
    )


def _compare() -> SchemaComparison:
    base, left, right, lh, rh = _branches()
    return compare_schemas(
        left,
        right,
        base=base,
        left_hints=lh,
        right_hints=rh,
        left_label="1.1.0",
        right_label="1.0.1",
        base_label="1.0.0",
    )


def _vertex(comparison: SchemaComparison, row_id: str) -> VertexRow:
    return next(r for r in comparison.vertices if r.id == row_id)


def _relation(comparison: SchemaComparison, relation: str) -> RelationRow:
    return next(
        r
        for r in comparison.relations
        if (r.left and r.left.relation == relation)
        or (r.right and r.right.relation == relation)
    )


def test_a_renamed_class_stays_on_one_row() -> None:
    comparison = _compare()
    row = _vertex(comparison, "vertex/customer")
    assert row.left is not None and row.right is not None
    assert (row.left.name, row.right.name) == ("Customer", "Account")
    assert row.status == "renamed"
    assert row.changed_on == ["left", "right"]
    assert "name" in row.aspects and "fields" in row.aspects


def test_a_field_one_side_added_is_on_that_side_only() -> None:
    row = _vertex(_compare(), "vertex/customer")
    tier = next(f for f in row.fields if f.id == "vertex/customer/field/tier")
    assert tier.left is not None and tier.right is None
    assert tier.status == "removed"
    assert tier.changed_on == ["left"]
    email = next(f for f in row.fields if f.id == "vertex/customer/field/email")
    assert email.status == "unchanged" and email.changed_on == []
    assert email.left is not None and email.left.in_identity


def test_one_field_added_on_both_sides_with_two_types_is_one_changed_row() -> None:
    row = _vertex(_compare(), "vertex/product")
    category = next(f for f in row.fields if f.id == "vertex/product/field/category")
    assert category.left is not None and category.right is not None
    assert (category.left.type, category.right.type) == ("STRING", "INT")
    assert category.status == "changed"
    assert category.aspects == ["type"]
    assert category.changed_on == ["left", "right"]


def test_a_description_both_sides_rewrote_is_changed_on_both() -> None:
    row = _vertex(_compare(), "vertex/company")
    assert row.status == "changed"
    assert "description" in row.aspects
    assert row.changed_on == ["left", "right"]
    industry = next(f for f in row.fields if f.left and f.left.name == "industry")
    assert industry.right is None and industry.changed_on == ["left"]


def test_an_edge_onto_a_renamed_class_stays_on_one_row() -> None:
    comparison = _compare()
    purchases = _relation(comparison, "purchases")
    assert purchases.left is not None and purchases.right is not None
    assert (purchases.left.source, purchases.right.source) == ("Customer", "Account")
    assert purchases.left.source_row == purchases.right.source_row == "vertex/customer"
    assert purchases.status == "changed" and purchases.aspects == ["fields"]
    channel = purchases.fields[0]
    assert channel.left is None and channel.right is not None
    assert channel.status == "added" and channel.changed_on == ["right"]
    works_at = _relation(comparison, "works_at")
    assert works_at.status == "unchanged" and works_at.changed_on == []


def test_counts_cover_class_and_relation_rows() -> None:
    comparison = _compare()
    assert sum(comparison.counts.values()) == len(comparison.vertices) + len(
        comparison.relations
    )
    assert comparison.aligned_on_base and comparison.base_label == "1.0.0"
    assert comparison.warnings == []


def test_without_an_ancestor_a_rename_reads_as_removed_and_added() -> None:
    _base, left, right, _lh, _rh = _branches()
    comparison = compare_schemas(left, right)
    statuses = {
        (r.left.name if r.left else None, r.right.name if r.right else None): r.status
        for r in comparison.vertices
    }
    assert statuses[("Customer", None)] == "removed"
    assert statuses[(None, "Account")] == "added"
    assert comparison.warnings and not comparison.aligned_on_base
    assert all(r.changed_on == [] for r in comparison.vertices)


def test_without_an_ancestor_cross_hints_pair_a_rename() -> None:
    _base, left, right, _lh, _rh = _branches()
    comparison = compare_schemas(
        left, right, hints=RenameHints(vertices={"Customer": "Account"})
    )
    row = _vertex(comparison, "vertex/customer")
    assert row.right is not None and row.right.name == "Account"
    assert row.status == "renamed"
    assert comparison.warnings == []


def test_every_merge_conflict_locates_a_row() -> None:
    base, left, right, lh, rh = _branches()
    _merged, result = merge_three_way(base, left, right, left_hints=lh, right_hints=rh)
    comparison = _compare()
    assert result.conflicts
    for conflict in result.conflicts:
        assert comparison.locate(conflict.slot) is not None, conflict.slot


def test_locate_names_the_field_row_or_the_aspect() -> None:
    comparison = _compare()
    category = comparison.locate(("vertex", "product", "field", "category"))
    assert category is not None
    assert category.row_id == "vertex/product"
    assert category.field_row_id == "vertex/product/field/category"
    description = comparison.locate(("vertex", "company", "description"))
    assert description is not None
    assert (description.row_id, description.aspect) == ("vertex/company", "description")
    account = comparison.locate(("vertex", "account"))
    assert account is not None and account.row_id == "vertex/customer"
    channel = comparison.locate(("relation", "purchase", "field"))
    assert channel is not None and channel.aspect == "fields"
    assert comparison.locate(("vertex", "nowhere")) is None


def test_the_comparison_is_deterministic() -> None:
    assert _compare().model_dump() == _compare().model_dump()
