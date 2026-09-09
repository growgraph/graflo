"""Pure merge helpers: what a vertex or edge merge keeps, drops, and refuses.

The compose and merge ops both lower onto these helpers, and a silent drop
here surfaces as a manifest that looks merged while an edge step, a filter, or
a physical edge definition quietly lost its meaning.
"""

from __future__ import annotations

import pytest

from graflo.architecture.evolution.merge_core import (
    merge_edge_pair,
    merge_vertex_models,
)
from graflo.architecture.graph_types import EdgeType
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.semantics import FieldSemantics, Semantics
from graflo.architecture.schema.vertex import Field, SecondaryIdentity, Vertex


def _vertex(
    name: str,
    *,
    identity: list[str] | None = None,
    secondary: list[SecondaryIdentity] | None = None,
    filters: list[dict] | None = None,
) -> Vertex:
    return Vertex(
        name=name,
        properties=[Field(name="id"), Field(name="email"), Field(name="phone")],
        identity=identity or ["id"],
        secondary_identities=secondary or [],
        filters=filters or [],
    )


class TestSecondaryIdentityNames:
    def test_a_name_reused_for_another_field_set_raises_even_after_subsumption(
        self,
    ) -> None:
        """One source's `k` equals the *merged* primary and is dropped; another
        source's `k` over a different set must still not slip through."""
        a = _vertex(
            "a", secondary=[SecondaryIdentity(name="k", fields=["id", "phone"])]
        )
        b = _vertex(
            "b",
            identity=["id", "phone"],
            secondary=[SecondaryIdentity(name="k", fields=["email"])],
        )
        with pytest.raises(ValueError, match="name 'k' refers to"):
            merge_vertex_models([a, b], "ab")

    def test_one_field_set_under_two_names_raises(self) -> None:
        """Edge steps select by name; silently dropping the second breaks them."""
        a = _vertex("a", secondary=[SecondaryIdentity(name="k", fields=["email"])])
        b = _vertex("b", secondary=[SecondaryIdentity(name="j", fields=["email"])])
        with pytest.raises(ValueError, match="named 'k' and 'j'"):
            merge_vertex_models([a, b], "ab")

    def test_an_authored_name_wins_over_a_positional_one(self) -> None:
        a = _vertex("a", secondary=[SecondaryIdentity(fields=["email"])])
        b = _vertex("b", secondary=[SecondaryIdentity(name="k", fields=["email"])])
        merged = merge_vertex_models([a, b], "ab")
        assert [(s.name, s.fields) for s in merged.secondary_identities] == [
            ("k", ["email"])
        ]

    def test_positional_names_are_renumbered_rather_than_collided(self) -> None:
        """Two sources each auto-name their first secondary `secondary_0`."""
        a = _vertex("a", secondary=[SecondaryIdentity(fields=["email"])])
        b = _vertex("b", secondary=[SecondaryIdentity(fields=["phone"])])
        merged = merge_vertex_models([a, b], "ab")
        assert [(s.name, s.fields) for s in merged.secondary_identities] == [
            ("secondary_0", ["email"]),
            ("secondary_1", ["phone"]),
        ]


def test_filters_are_deduplicated_like_every_other_list() -> None:
    same = {"kind": "leaf", "field": "email", "cmp_operator": "!=", "value": [""]}
    a = _vertex("a", filters=[same])
    b = _vertex("b", filters=[same, {**same, "field": "phone"}])
    merged = merge_vertex_models([a, b], "ab")
    assert [f.field for f in merged.filters] == ["email", "phone"]


class TestMergeEdgePair:
    def test_disagreeing_type_or_by_raises(self) -> None:
        """`edge_id` leaves them out; keeping one side's would be silent."""
        direct = Edge(source="a", target="b", relation="r")
        indirect = Edge(
            source="a", target="b", relation="r", type=EdgeType.INDIRECT, by="c"
        )
        with pytest.raises(ValueError, match="disagree on type/by"):
            merge_edge_pair(direct, indirect)

    def test_agreeing_edges_merge_their_properties(self) -> None:
        a = Edge(source="a", target="b", relation="r", properties=["x"])
        b = Edge(source="a", target="b", relation="r", properties=["y"])
        assert [f.name for f in merge_edge_pair(a, b).properties] == ["x", "y"]


class TestPropertyMerge:
    """What a merge does to the properties themselves, not to the identity.

    A merge that reconstructs fields from an enumerated constructor drops
    whatever the constructor was not written for. That produced a merge which
    could not fuse two identical LIST properties at all, and which lost every
    grounding block declared on both sides.
    """

    def test_identical_list_properties_merge(self) -> None:
        left = Vertex(
            name="user",
            properties=[
                Field(name="id"),
                Field(name="tags", type="LIST", item_type="STRING"),
            ],
            identity=["id"],
        )
        merged = merge_vertex_models([left, left.model_copy(deep=True)], "user")
        tags = next(f for f in merged.properties if f.name == "tags")
        assert (str(tags.type), str(tags.item_type)) == ("LIST", "STRING")

    def test_conflicting_item_types_name_the_merged_vertex_and_property(self) -> None:
        left = Vertex(
            name="user",
            properties=[
                Field(name="id"),
                Field(name="tags", type="LIST", item_type="STRING"),
            ],
            identity=["id"],
        )
        right = Vertex(
            name="user",
            properties=[
                Field(name="id"),
                Field(name="tags", type="LIST", item_type="INT"),
            ],
            identity=["id"],
        )
        with pytest.raises(ValueError) as excinfo:
            merge_vertex_models([left, right], "party")
        message = str(excinfo.value)
        # Named for the merge target, not the sources: 'user' no longer exists
        # in the merged schema, so pointing at it would name nothing.
        assert "vertex 'party'" in message
        assert "property 'tags'" in message
        assert "LIST<STRING>" in message and "LIST<INT>" in message

    @pytest.mark.parametrize("grounded_first", [True, False])
    def test_field_grounding_survives_a_vertex_merge(
        self, grounded_first: bool
    ) -> None:
        grounded = Vertex(
            name="user",
            properties=[
                Field(name="id"),
                Field(
                    name="email",
                    type="STRING",
                    semantics=FieldSemantics(iri="https://schema.org/email"),
                ),
            ],
            identity=["id"],
        )
        plain = Vertex(
            name="user",
            properties=[Field(name="id"), Field(name="email", type="STRING")],
            identity=["id"],
        )
        sources = [grounded, plain] if grounded_first else [plain, grounded]
        merged = merge_vertex_models(sources, "user")
        email = next(f for f in merged.properties if f.name == "email")
        assert email.semantics is not None
        assert email.semantics.iri == "https://schema.org/email"

    def test_clashing_units_are_refused(self) -> None:
        def _reading(unit: str) -> Vertex:
            return Vertex(
                name="reading",
                properties=[
                    Field(name="id"),
                    Field(name="speed", semantics=FieldSemantics(unit=unit)),
                ],
                identity=["id"],
            )

        with pytest.raises(ValueError, match="units"):
            merge_vertex_models([_reading("m/s"), _reading("km/h")], "reading")

    def test_edge_properties_fuse_like_vertex_properties(self) -> None:
        def _edge(item_type: str) -> Edge:
            return Edge(
                source="a",
                target="b",
                relation="r",
                properties=[Field(name="tags", type="LIST", item_type=item_type)],
            )

        merged = merge_edge_pair(_edge("STRING"), _edge("STRING"))
        assert str(merged.properties[0].item_type) == "STRING"
        with pytest.raises(ValueError, match="LIST<STRING>"):
            merge_edge_pair(_edge("STRING"), _edge("INT"))


class TestGroundingIsCarried:
    """Class-level grounding, which every merge used to drop outright.

    ``semantics`` was absent from both merge constructors, so a composed vertex
    or edge lost its anchors whether or not the sides agreed on them -- while
    the schema's own metadata block was folded carefully one layer up.
    """

    def test_vertex_grounding_is_carried_and_unioned(self) -> None:
        def _party(synonym: str) -> Vertex:
            return Vertex(
                name="party",
                properties=[Field(name="id")],
                identity=["id"],
                semantics=Semantics(
                    iri="https://schema.org/Person", synonyms=[synonym]
                ),
            )

        merged = merge_vertex_models([_party("person"), _party("human")], "party")
        assert merged.semantics is not None
        assert merged.semantics.iri == "https://schema.org/Person"
        assert merged.semantics.synonyms == ["person", "human"]

    def test_a_disputed_vertex_iri_clears_rather_than_electing_one(self) -> None:
        merged = merge_vertex_models(
            [
                Vertex(
                    name="party",
                    properties=[Field(name="id")],
                    identity=["id"],
                    semantics=Semantics(iri="https://schema.org/Person"),
                ),
                Vertex(
                    name="party",
                    properties=[Field(name="id")],
                    identity=["id"],
                    semantics=Semantics(iri="http://xmlns.com/foaf/0.1/Agent"),
                ),
            ],
            "party",
        )
        assert merged.semantics is not None
        assert merged.semantics.iri is None

    def test_edge_grounding_is_carried(self) -> None:
        def _edge() -> Edge:
            return Edge(
                source="a",
                target="b",
                relation="r",
                semantics=Semantics(iri="https://schema.org/knows"),
            )

        merged = merge_edge_pair(_edge(), _edge())
        assert merged.semantics is not None
        assert merged.semantics.iri == "https://schema.org/knows"
