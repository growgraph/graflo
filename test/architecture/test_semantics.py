"""The optional semantic-grounding block, and the extra-key policy around it."""

import pytest
from pydantic import ValidationError

from graflo.architecture.schema.core import CoreSchema
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.metadata import GraphMetadata
from graflo.architecture.schema.semantics import FieldSemantics, Semantics
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig

PERSON_IRI = "https://schema.org/Person"


def _schema_with_semantics() -> Schema:
    return Schema(
        metadata=GraphMetadata(
            name="grounded",
            version="1.0.0",
            semantics=Semantics(iri="https://schema.org/Dataset"),
        ),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(
                vertices=[
                    Vertex(
                        name="person",
                        properties=[
                            Field(name="email", type="string"),
                            Field(
                                name="speed",
                                type="float",
                                semantics=FieldSemantics(unit="m/s"),
                            ),
                        ],
                        identity=["email"],
                        semantics=Semantics(
                            iri=PERSON_IRI,
                            exact_match=["http://xmlns.com/foaf/0.1/Person"],
                            synonyms=["individual", "human"],
                        ),
                    ),
                    Vertex(
                        name="company",
                        properties=[Field(name="tax_id", type="string")],
                        identity=["tax_id"],
                    ),
                ]
            ),
            edge_config=EdgeConfig(
                edges=[
                    Edge(
                        source="person",
                        target="company",
                        relation="works_at",
                        semantics=Semantics(iri="https://schema.org/worksFor"),
                    )
                ]
            ),
        ),
    )


def test_semantics_defaults_to_absent():
    vertex = Vertex(name="plain", properties=[Field(name="a")], identity=["a"])
    assert vertex.semantics is None
    assert "semantics" not in vertex.to_minimal_canonical_dict()


def test_semantics_survives_a_model_round_trip():
    schema = _schema_with_semantics()
    restored = Schema.model_validate(schema.to_dict())
    assert restored == schema
    person = restored.core_schema.vertex_config["person"]
    assert person.semantics is not None
    assert person.semantics.iri == PERSON_IRI
    assert person.semantics.synonyms == ["individual", "human"]


def test_field_semantics_carries_unit():
    schema = _schema_with_semantics()
    speed = next(
        f
        for f in schema.core_schema.vertex_config["person"].properties
        if f.name == "speed"
    )
    assert speed.semantics is not None
    assert speed.semantics.unit == "m/s"


def test_unit_is_rejected_outside_a_field():
    """``unit`` is meaningless on a vertex or edge; the type split enforces that.

    Constructed from a dict rather than a keyword so the static checker sees a
    valid call and the rejection is asserted at runtime, where it happens.
    """
    with pytest.raises(ValidationError):
        Semantics.model_validate({"unit": "m/s"})
    assert FieldSemantics.model_validate({"unit": "m/s"}).unit == "m/s"


def test_unknown_vertex_key_is_rejected_not_dropped():
    """The regression this policy exists for: silent loss of an authored block."""
    with pytest.raises(ValidationError, match="extra_forbidden|Extra inputs"):
        Vertex.from_dict(
            {"name": "person", "properties": ["a"], "identity": ["a"], "semantcs": {}}
        )


def test_relocated_vertex_keys_name_their_new_home():
    for key, home in (
        ("dbname", "vertex_storage_names"),
        ("indexes", "vertex_indexes"),
        ("transforms", "ingestion model"),
    ):
        with pytest.raises(ValidationError, match=home):
            Vertex.from_dict({"name": "person", "properties": ["a"], key: "whatever"})


def test_unknown_vertex_config_key_is_rejected():
    with pytest.raises(ValidationError):
        VertexConfig.from_dict({"vertices": [], "vertex_config": None})
