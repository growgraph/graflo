"""Declaring a merged property's type on the merge, and the refusal without it.

A vocabulary that folds several classes into one, and a right side declaring
that class too, must agree on every property type. ``field_types`` on the merge
states the merged type once, keyed by the merged names; the merge retypes every
member that carries the property before folding it. Without it, the refusal
names each member, on each side, that carries each type.
"""

from __future__ import annotations

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import (
    CanonicalMap,
    MergeManifestsOp,
    VertexEquivalence,
    merge_manifests,
)
from graflo.architecture.evolution.merge3 import build_merge_recipe
from graflo.architecture.evolution.merge_commit import build_merge_commit
from graflo.architecture.evolution.ops import MergeFieldTypes
from graflo.architecture.evolution.preview import preview_merge
from graflo.architecture.schema.vertex import FieldMergeError


def _manifest(name: str, vertices: list[dict], edges: list[dict] | None = None):
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": name, "version": "1.0.0"},
                "graph": {
                    "vertex_config": {"vertices": vertices},
                    "edge_config": {"edges": edges or []},
                },
            }
        }
    )
    manifest.finish_init()
    return manifest


def _vertex(name: str, *props: dict) -> dict:
    return {
        "name": name,
        "properties": [{"name": "asset_id", "type": "STRING"}, *props],
        "identity": ["asset_id"],
    }


def _ram(type_: str, *, name: str = "ram", item_type: str | None = None) -> dict:
    field: dict = {"name": name, "type": type_}
    if item_type is not None:
        field["item_type"] = item_type
    return field


#: Three left classes the vocabulary folds into ``Machine``. ``Desktop`` spells
#: the property ``RAM``; ``Tablet`` does not carry it at all.
_VOCABULARY = CanonicalMap(
    vertices={"Laptop": "Machine", "Desktop": "Machine", "Tablet": "Machine"},
    properties={"Desktop": {"RAM": "ram"}},
    allow_merges=True,
)


def _left(laptop: dict, desktop: dict) -> GraphManifest:
    return _manifest(
        "inventory",
        [
            _vertex("Laptop", laptop),
            _vertex("Desktop", desktop),
            _vertex("Tablet"),
        ],
    )


def _right(*props: dict) -> GraphManifest:
    return _manifest("fleet", [_vertex("Machine", *props)])


def _op(field_types: dict | None = None, **extra) -> MergeManifestsOp:
    return MergeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(left="Machine", right="Machine", into="Machine")
        ],
        canonical_maps={"left": _VOCABULARY},
        field_types=(
            MergeFieldTypes.model_validate(field_types)
            if field_types is not None
            else None
        ),
        **extra,
    )


def _merged_field(manifest: GraphManifest, vertex: str, field: str):
    schema = manifest.graph_schema
    assert schema is not None
    for prop in schema.core_schema.vertex_config[vertex].properties:
        if prop.name == field:
            return prop
    raise AssertionError(f"no {vertex}.{field}")


_INT = {"vertices": {"Machine": {"ram": {"type": "INT"}}}}


class TestRefusal:
    def test_names_every_member_carrying_each_type(self) -> None:
        left = _left(_ram("STRING"), _ram("STRING", name="RAM"))
        right = _right(_ram("INT"))

        with pytest.raises(FieldMergeError) as excinfo:
            merge_manifests(left, right, _op(), bump_version=False)

        message = str(excinfo.value)
        assert "Conflicting field types for vertex 'Machine'" in message
        assert "property 'ram'" in message
        assert "left Laptop.ram" in message
        assert "left Desktop.RAM" in message
        assert "right Machine.ram" in message
        assert "Tablet" not in message
        assert "field_types: {vertices: {Machine: {ram:" in message
        assert excinfo.value.fields == ("ram",)

    def test_a_clash_inside_one_side_is_attributed_too(self) -> None:
        """The vocabulary's own members disagree; the right side agrees with one."""
        left = _left(_ram("STRING"), _ram("INT", name="RAM"))
        right = _right(_ram("INT"))

        with pytest.raises(FieldMergeError) as excinfo:
            merge_manifests(left, right, _op(), bump_version=False)

        message = str(excinfo.value)
        assert "left Laptop.ram" in message
        assert "left Desktop.RAM" in message

    def test_list_item_types_clash(self) -> None:
        left = _left(
            _ram("LIST", item_type="STRING"),
            _ram("LIST", name="RAM", item_type="STRING"),
        )
        right = _right(_ram("LIST", item_type="INT"))

        with pytest.raises(FieldMergeError, match="item_type"):
            merge_manifests(left, right, _op(), bump_version=False)


class TestDeclaredType:
    def test_retypes_every_member_before_the_fold(self) -> None:
        left = _left(_ram("STRING"), _ram("STRING", name="RAM"))
        right = _right(_ram("INT"))

        merged = merge_manifests(left, right, _op(_INT), bump_version=False)

        assert _merged_field(merged, "Machine", "ram").type == "INT"

    def test_members_without_the_property_are_left_alone(self) -> None:
        """``Tablet`` has no ``ram``; retyping it would be refused."""
        left = _left(_ram("STRING"), _ram("INT", name="RAM"))

        merged = merge_manifests(left, _right(), _op(_INT), bump_version=False)

        assert _merged_field(merged, "Machine", "ram").type == "INT"

    def test_a_list_type_is_declared_with_its_item_type(self) -> None:
        left = _left(
            _ram("LIST", item_type="STRING"),
            _ram("LIST", name="RAM", item_type="STRING"),
        )
        right = _right(_ram("LIST", item_type="INT"))
        declared = {
            "vertices": {"Machine": {"ram": {"type": "LIST", "item_type": "INT"}}}
        }

        merged = merge_manifests(left, right, _op(declared), bump_version=False)

        field = _merged_field(merged, "Machine", "ram")
        assert (field.type, field.item_type) == ("LIST", "INT")

    def test_a_property_that_already_agrees_is_untouched(self) -> None:
        left = _left(_ram("INT"), _ram("INT", name="RAM"))
        right = _right(_ram("INT"))

        merged = merge_manifests(left, right, _op(_INT), bump_version=False)

        assert _merged_field(merged, "Machine", "ram").type == "INT"

    def test_a_name_nothing_reaches_is_refused(self) -> None:
        left = _left(_ram("STRING"), _ram("STRING", name="RAM"))
        declared = {
            "vertices": {
                "Machin": {"ram": {"type": "INT"}},
                "Machine": {"rom": {"type": "INT"}},
            }
        }

        with pytest.raises(ValueError) as excinfo:
            merge_manifests(left, _right(), _op(declared), bump_version=False)

        message = str(excinfo.value)
        assert "'Machin'" in message
        assert "'rom'" in message

    def test_an_unset_declaration_leaves_the_recipe_unchanged(self) -> None:
        """``None`` is dropped on dump, so recipes recorded before stay valid."""
        assert "field_types" not in _op().to_dict()

    def test_the_merge_commit_replays(self) -> None:
        left = _left(_ram("STRING"), _ram("STRING", name="RAM"))
        right = _right(_ram("INT"))
        op = _op(_INT)
        merged = merge_manifests(left, right, op, bump_version=False)

        entry = build_merge_commit(
            left,
            merged,
            parents=["a" * 12, "b" * 12],
            recipe=build_merge_recipe(left, right, op),
            right=right,
        )

        assert entry.ops[0].op == "change_field_types"


def _edge_sides(left_type: str, right_type: str):
    port = {"name": "Port", "properties": ["k"], "identity": ["k"]}
    left = _manifest(
        "inventory",
        [_vertex("Laptop"), port],
        [
            {
                "source": "Laptop",
                "target": "Port",
                "relation": "has",
                "properties": [{"name": "speed", "type": left_type}],
            }
        ],
    )
    right = _manifest(
        "fleet",
        [_vertex("Machine"), port],
        [
            {
                "source": "Machine",
                "target": "Port",
                "relation": "has",
                "properties": [{"name": "speed", "type": right_type}],
            }
        ],
    )
    return left, right


def _edge_op(field_types: dict | None = None) -> MergeManifestsOp:
    return MergeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(left="Laptop", right="Machine", into="Machine")
        ],
        name_conflict="union_right",
        field_types=(
            MergeFieldTypes.model_validate(field_types)
            if field_types is not None
            else None
        ),
    )


class TestEdges:
    def test_an_edge_property_clash_names_both_sides(self) -> None:
        left, right = _edge_sides("STRING", "FLOAT")

        with pytest.raises(FieldMergeError) as excinfo:
            merge_manifests(left, right, _edge_op(), bump_version=False)

        message = str(excinfo.value)
        assert "property 'speed'" in message
        assert "left Laptop-has->Port.speed" in message
        assert "right Machine-has->Port.speed" in message
        assert "field_types: {edges: {has: {speed:" in message

    def test_a_declared_edge_type_merges(self) -> None:
        left, right = _edge_sides("STRING", "FLOAT")
        declared = {"edges": {"has": {"speed": {"type": "FLOAT"}}}}

        merged = merge_manifests(left, right, _edge_op(declared), bump_version=False)

        schema = merged.graph_schema
        assert schema is not None
        (edge,) = schema.core_schema.edge_config.edges
        (speed,) = [p for p in edge.properties if p.name == "speed"]
        assert speed.type == "FLOAT"


class TestPreview:
    def test_a_declared_type_is_no_finding(self) -> None:
        left = _left(_ram("STRING"), _ram("STRING", name="RAM"))
        right = _right(_ram("INT"))

        refused = preview_merge(left, right, _op(), attempt=False)
        declared = preview_merge(left, right, _op(_INT))

        assert "type_conflict" in {f.kind for f in refused.findings}
        assert declared.outcome.status == "merged"
        assert "type_conflict" not in {f.kind for f in declared.findings}

    def test_a_declared_edge_type_is_no_finding(self) -> None:
        left, right = _edge_sides("STRING", "FLOAT")
        declared = {"edges": {"has": {"speed": {"type": "FLOAT"}}}}

        preview = preview_merge(left, right, _edge_op(declared))

        assert preview.outcome.status == "merged"
        assert not [f for f in preview.findings if f.severity == "refusal"]
