from graflo.architecture.onto import VertexRep
from graflo.util.merge import (
    merge_doc_basis,
)


def test_merge_simple(docs_simple):
    r = merge_doc_basis(docs_simple, ("id",))
    assert len(r) == 1
    assert r[0]["a"] == 2
    assert r[0]["b"] == 1


def test_merge_simple_two_doc(docs_simple_two_doc):
    r = merge_doc_basis(docs_simple_two_doc, ("id",))
    assert len(r) == 2


def test_merge_nodiscriminant(merge_input_no_disc, merge_output_no_disc):
    r = merge_doc_basis(
        merge_input_no_disc,
        index_keys=("_key",),
    )
    r = sorted(r, key=lambda x: x["_key"])
    assert r == merge_output_no_disc


def test_merge():
    input = [
        VertexRep(
            vertex={
                "_key": "8de0b4225b0f31db5784c73f3ea9d8ce73954c61",
                "_role": "source",
            },
            ctx={"a": 1},
        ),
        VertexRep(
            vertex={"text": "complex evolutionary history of acochlidia"}, ctx={"b": 2}
        ),
        VertexRep(
            vertex={
                "_key": "4275320fcb2ee3c9bb2711b735b265e847256628",
                "_role": "relation",
            },
            ctx={"c": 3},
        ),
        VertexRep(vertex={"text": "represents"}, ctx={"d": 4}),
        VertexRep(
            vertex={
                "_key": "009c700138c1c718b0be5730ff557f8aa3c13b63",
                "_role": "target",
            },
            ctx={"e": 5},
        ),
        VertexRep(vertex={"text": "small group of panpulmonata"}, ctx={"f": 6}),
    ]

    output_ref = [
        VertexRep(
            vertex={
                "_key": "8de0b4225b0f31db5784c73f3ea9d8ce73954c61",
                "_role": "source",
                "text": "complex evolutionary history of acochlidia",
            },
            ctx={"a": 1, "b": 2},
        ),
        VertexRep(
            vertex={
                "_key": "4275320fcb2ee3c9bb2711b735b265e847256628",
                "_role": "relation",
                "text": "represents",
            },
            ctx={"c": 3, "d": 4},
        ),
        VertexRep(
            vertex={
                "_key": "009c700138c1c718b0be5730ff557f8aa3c13b63",
                "_role": "target",
                "text": "small group of panpulmonata",
            },
            ctx={"e": 5, "f": 6},
        ),
    ]

    output = merge_doc_basis(input, index_keys=("_key",))
    # Compare vertex and ctx dicts for equality
    for o, r in zip(output, output_ref):
        assert o.vertex == r.vertex
        assert o.ctx == r.ctx


def test_merge_no_index_keys_dict():
    """Test that documents without index keys are merged into a single document."""
    input_docs = [
        {"a": 1, "b": 2},
        {"c": 3, "d": 4},
        {"e": 5},
    ]
    output = merge_doc_basis(input_docs, index_keys=("_key",))
    assert len(output) == 1
    assert output[0] == {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}


def test_merge_no_index_keys_vertexrep():
    """Test that VertexReps without index keys are merged into a single VertexRep."""
    input_docs = [
        VertexRep(vertex={"a": 1, "b": 2}, ctx={"x": 10}),
        VertexRep(vertex={"c": 3, "d": 4}, ctx={"y": 20}),
        VertexRep(vertex={"e": 5}, ctx={"z": 30}),
    ]
    output = merge_doc_basis(input_docs, index_keys=("_key",))
    assert len(output) == 1
    assert output[0].vertex == {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}
    assert output[0].ctx == {"x": 10, "y": 20, "z": 30}
