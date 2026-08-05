"""Orientation card: the cheapest useful summary of an unfamiliar schema."""

import pytest

from graflo.architecture.schema.context import SchemaGraph, build_card
from graflo.architecture.schema.core import CoreSchema
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import EdgeConfig
from graflo.architecture.schema.metadata import GraphMetadata
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig


@pytest.fixture()
def card(context_schema):
    return build_card(context_schema)


def test_reports_metadata_and_counts(card):
    assert card.name == "context-fixture"
    assert card.version == "1.0.0"
    assert card.description == "context test schema"
    assert card.vertex_count == 5
    assert card.edge_count == 6
    # 10, not 9: a blank vertex gains an auto-generated identity property.
    assert card.total_property_count == 10


def test_identity_mode_histogram(card):
    assert card.identity_modes == {"blank": 1, "natural": 4}


def test_isolated_and_relation_vocabulary(card):
    assert card.isolated_types == ["orphan"]
    assert card.isolated_type_count == 1
    assert "works_at" in card.relation_vocabulary
    assert card.relation_count == 5


def test_name_lists_are_bounded_but_counts_are_honest():
    """A 400-type schema must not dump 400 names into the card."""
    vertices = [
        Vertex(
            name=f"v{i:03d}",
            properties=[Field(name="p0", type="string")],
            identity=["p0"],
        )
        for i in range(400)
    ]
    schema = Schema(
        metadata=GraphMetadata(name="grow", version="1.0.0"),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(vertices=vertices),
            edge_config=EdgeConfig(edges=[]),
        ),
    )
    card = build_card(schema, max_names=25)
    assert len(card.isolated_types) == 25
    assert card.isolated_type_count == 400


def test_hub_types_are_ranked_most_central_first(card):
    assert card.hub_types[0].name == "person"
    scores = [hub.score for hub in card.hub_types]
    assert scores == sorted(scores, reverse=True)


def test_entry_points_exclude_blank_types_without_indexes(card):
    names = {entry.name for entry in card.entry_points}
    assert "doc" not in names
    assert "person" in names


def test_entry_point_surfaces_identity_and_indexes(card):
    person = next(entry for entry in card.entry_points if entry.name == "person")
    assert person.identity == ["email"]
    assert person.identity_mode == "natural"
    assert person.indexed_fields == [["name"]]


def test_top_n_bounds_both_lists(context_schema):
    card = build_card(context_schema, top_n=2)
    assert len(card.hub_types) == 2
    assert len(card.entry_points) <= 2


def test_estimated_tokens_is_populated(card):
    assert card.estimated_tokens > 0


def test_card_stays_small_as_schema_grows():
    """The point of a card: cost must not scale with schema size."""

    def schema_of(size: int) -> Schema:
        return Schema(
            metadata=GraphMetadata(name="grow", version="1.0.0"),
            core_schema=CoreSchema(
                vertex_config=VertexConfig(
                    vertices=[
                        Vertex(
                            name=f"v{i:03d}",
                            properties=[
                                Field(name=f"p{j}", type="string") for j in range(6)
                            ],
                            identity=["p0"],
                        )
                        for i in range(size)
                    ]
                ),
                edge_config=EdgeConfig(edges=[]),
            ),
        )

    small = build_card(schema_of(10))
    large = build_card(schema_of(400))
    assert large.vertex_count == 400
    assert large.estimated_tokens < 2 * small.estimated_tokens


def test_accepts_a_prebuilt_graph(context_schema):
    graph = SchemaGraph.from_schema(context_schema)
    assert build_card(context_schema, graph=graph) == build_card(context_schema)


def test_does_not_mutate_schema(context_schema):
    before = context_schema.to_dict()
    build_card(context_schema)
    assert context_schema.to_dict() == before
