"""A mention graph keeps its refs consistent."""

import pytest

from graflo.architecture.graph_types.mention import (
    Mention,
    MentionGraph,
    Relation,
    TextSpan,
)

SPAN = TextSpan(start=0, end=2)


def _mention(ref: str) -> Mention:
    return Mention(ref=ref, type="T", introduces=True, quote=ref, span=SPAN)


def test_refs_must_be_unique():
    with pytest.raises(ValueError, match="not unique"):
        MentionGraph(
            source_id="s", schema_ref="x@1", mentions=[_mention("a"), _mention("a")]
        )


def test_a_relation_names_known_mentions():
    relation = Relation(
        edge_id=("T", "T", "R"), source="a", target="b", quote="", span=SPAN
    )
    with pytest.raises(ValueError, match="unknown mention 'b'"):
        MentionGraph(
            source_id="s",
            schema_ref="x@1",
            mentions=[_mention("a")],
            relations=[relation],
        )


def test_a_graph_round_trips_through_json():
    relation = Relation(
        edge_id=("T", "T", None), source="a", target="b", quote="", span=SPAN
    )
    graph = MentionGraph(
        source_id="s",
        schema_ref="x@1",
        mentions=[_mention("a"), _mention("b")],
        relations=[relation],
    )
    assert MentionGraph.model_validate_json(graph.model_dump_json()) == graph
