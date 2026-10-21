"""A record graph keeps its refs consistent."""

import pytest

from graflo.link.records import PartialRecord, RecordGraph, RecordRelation


def _record(ref: str) -> PartialRecord:
    return PartialRecord(ref=ref, type="T", may_create=True, evidence={"at": ref})


def test_refs_must_be_unique():
    with pytest.raises(ValueError, match="not unique"):
        RecordGraph(
            source_id="s", schema_ref="x@1", records=[_record("a"), _record("a")]
        )


def test_a_relation_names_known_records():
    relation = RecordRelation(edge_id=("T", "T", "R"), source="a", target="b")
    with pytest.raises(ValueError, match="unknown record 'b'"):
        RecordGraph(
            source_id="s",
            schema_ref="x@1",
            records=[_record("a")],
            relations=[relation],
        )


def test_a_graph_round_trips_through_json():
    relation = RecordRelation(
        edge_id=("T", "T", None), source="a", target="b", evidence={"line": 3}
    )
    graph = RecordGraph(
        source_id="s",
        schema_ref="x@1",
        records=[_record("a"), _record("b")],
        relations=[relation],
    )
    again = RecordGraph.model_validate_json(graph.model_dump_json())
    assert again == graph
    assert again.records[0].evidence == {"at": "a"}
