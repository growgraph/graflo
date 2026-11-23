"""neo4j-graphrag ``GraphSchema``: what each direction keeps, and what it names as lost."""

from __future__ import annotations

import pathlib
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from graflo.architecture.schema.document import Schema
from graflo.interop.neo4j_graphrag import from_graph_schema, to_graph_schema

DATA = pathlib.Path(__file__).resolve().parent / "data"

LIBRARY = """
node_types:
  - label: Author
    description: A person who writes books.
    properties:
      - {name: orcid, type: STRING}
      - {name: name, type: STRING, description: Full name.}
      - {name: born, type: DATE}
  - label: Book
    properties:
      - {name: isbn, type: STRING}
      - {name: title, type: STRING}
      - {name: pages, type: INTEGER}
      - {name: location, type: POINT}
  - label: Review
    properties:
      - {name: text, type: STRING}
      - {name: url, type: STRING}
  - Genre
relationship_types:
  - label: WROTE
    description: Authorship.
    properties:
      - {name: year, type: INTEGER}
  - label: ABOUT
  - label: SHELVED
    properties:
      - {name: at, type: ZONED_DATETIME}
  - label: CITES
patterns:
  - [Author, WROTE, Book]
  - [Review, ABOUT, Book]
  - {source: Book, relationship: SHELVED, target: Genre}
constraints:
  - {type: KEY, node_type: Book, property_names: [isbn]}
  - {type: KEY, node_type: Author, property_names: [orcid]}
  - {type: UNIQUENESS, node_type: Author, property_names: [name, born]}
  - {type: UNIQUENESS, node_type: Review, property_names: [url]}
  - {type: EXISTENCE, node_type: Book, property_name: title}
  - {type: KEY, relationship_type: SHELVED, property_names: [at]}
additional_node_types: false
"""


@pytest.fixture
def library() -> tuple[Schema, list[str]]:
    return from_graph_schema(yaml.safe_load(LIBRARY), "library")


@pytest.fixture
def support() -> Schema:
    return Schema.model_validate(
        yaml.safe_load((DATA / "support_schema.yaml").read_text())
    )


def _vertex(schema: Schema, name: str) -> Any:
    (vertex,) = [v for v in schema.core_schema.vertex_config.vertices if v.name == name]
    return vertex


def _has(losses: list[str], *words: str) -> bool:
    return any(all(w in loss for w in words) for loss in losses)


# -- GraphSchema -> graflo ---------------------------------------------------


def test_keys_become_identities_and_uniqueness_alternate_keys(library) -> None:
    schema, _ = library
    author = _vertex(schema, "Author")
    assert author.identity == ["orcid"]
    assert [(s.name, s.fields) for s in author.secondary_identities] == [
        ("by_name_born", ["name", "born"])
    ]
    assert author.description == "A person who writes books."
    assert _vertex(schema, "Book").identity == ["isbn"]


def test_a_node_type_without_a_key_is_blank_and_says_so(library) -> None:
    schema, losses = library
    review = _vertex(schema, "Review")
    assert review.blank
    assert _has(losses, "`Review`", "no KEY", "new entity")
    assert _has(losses, "`Review`", "UNIQUENESS", "url")


def test_a_label_only_node_type_is_one_name_field(library) -> None:
    schema, losses = library
    genre = _vertex(schema, "Genre")
    assert [f.name for f in genre.properties if f.type is not None] == ["name"]
    assert genre.blank
    assert _has(losses, "`Genre`", "no properties")


def test_property_types_map_and_the_rest_are_named(library) -> None:
    schema, losses = library
    book = {f.name: f.type for f in _vertex(schema, "Book").properties}
    assert book["pages"] == "INT"
    assert book["location"] is None
    assert {f.name: f.type for f in _vertex(schema, "Author").properties}["born"] == (
        "DATETIME"
    )
    assert _has(losses, "`Book.location`", "POINT")
    assert _has(losses, "`Author.born`", "DATE")
    assert _has(losses, "`Book.title`", "EXISTENCE")


def test_each_pattern_is_an_edge_with_its_relationship_properties(library) -> None:
    schema, losses = library
    edges = {e.relation: e for e in schema.core_schema.edge_config.edges}
    assert set(edges) == {"WROTE", "ABOUT", "SHELVED"}
    assert edges["WROTE"].description == "Authorship."
    assert [p.name for p in edges["WROTE"].properties] == ["year"]
    assert edges["SHELVED"].identities == [["at"]]
    assert _has(losses, "`SHELVED`", "KEY", "pair of endpoints")
    assert _has(losses, "`CITES`", "no pattern")


def test_open_world_switches_are_named(library) -> None:
    _, losses = library
    assert not _has(losses, "additional_node_types")
    assert _has(losses, "`ABOUT`", "undeclared") is False
    data = yaml.safe_load(LIBRARY) | {"additional_patterns": True}
    _, losses = from_graph_schema(data, "library")
    assert _has(losses, "additional_patterns")


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"patterns": [["Author", "WROTE", "Shelf"]]}, "undeclared types: Shelf"),
        ({"colour": "red"}, "colour"),
        (
            {
                "constraints": [
                    {"type": "KEY", "node_type": "Book", "property_names": ["x"]}
                ]
            },
            "undeclared properties: x",
        ),
        (
            {"constraints": [{"type": "KEY", "property_names": ["isbn"]}]},
            "one node_type or relationship_type",
        ),
    ],
)
def test_a_bad_document_is_refused_by_name(
    change: dict[str, Any], message: str
) -> None:
    with pytest.raises(ValidationError, match=message):
        from_graph_schema(yaml.safe_load(LIBRARY) | change, "library")


# -- graflo -> GraphSchema ---------------------------------------------------


def test_a_schema_exports_types_patterns_and_keys(support: Schema) -> None:
    doc, _ = to_graph_schema(support)
    assert [n["label"] for n in doc["node_types"]] == [
        "Customer",
        "Agent",
        "Ticket",
        "Product",
        "Tag",
        "Comment",
    ]
    assert {"source": "Customer", "relationship": "RAISED", "target": "Ticket"} in doc[
        "patterns"
    ]
    assert {
        "type": "UNIQUENESS",
        "node_type": "Customer",
        "property_names": ["email"],
    } in doc["constraints"]
    keyed = {c["node_type"] for c in doc["constraints"] if c["type"] == "KEY"}
    assert "Comment" not in keyed
    comment = next(n for n in doc["node_types"] if n["label"] == "Comment")
    assert [p["name"] for p in comment["properties"]] == ["body"]
    assert all(doc[k] is False for k in doc if k.startswith("additional_"))


def test_what_graphschema_cannot_say_is_named(support: Schema) -> None:
    _, losses = to_graph_schema(support)
    assert _has(losses, "`Ticket.wait`", "unit")
    assert _has(losses, "`Customer`", "synonyms")
    assert _has(losses, "`Tag`", "digest")
    assert not _has(losses, "db_profile")


def test_export_then_import_keeps_types_edges_keys_and_descriptions(
    support: Schema,
) -> None:
    doc, _ = to_graph_schema(support)
    back, _ = from_graph_schema(doc, "support")
    for vertex in support.core_schema.vertex_config.vertices:
        again = _vertex(back, vertex.name)
        assert again.description == vertex.description
        assert again.blank == vertex.blank
        if not vertex.blank:
            assert again.identity == (
                vertex.hash_identity_properties or vertex.identity
            )
        typed = {f.name: f.type for f in vertex.properties if f.type is not None}
        assert {f.name: f.type for f in again.properties if f.type is not None} == typed
    triples = {
        (e.source, e.relation, e.target) for e in support.core_schema.edge_config.edges
    }
    assert {
        (e.source, e.relation, e.target) for e in back.core_schema.edge_config.edges
    } == triples


def _two_edges(**second: Any) -> Schema:
    vertex = {
        "name": "A",
        "properties": [{"name": "k", "type": "STRING"}],
        "identity": ["k"],
    }
    return Schema.model_validate(
        {
            "metadata": {"name": "s"},
            "core_schema": {
                "vertex_config": {"vertices": [vertex, {**vertex, "name": "B"}]},
                "edge_config": {
                    "edges": [
                        {
                            "source": "A",
                            "target": "B",
                            "relation": "LINKS",
                            "properties": [{"name": "w", "type": "FLOAT"}],
                        },
                        {"source": "B", "target": "A", "relation": "LINKS", **second},
                    ]
                },
            },
        }
    )


def test_edges_sharing_a_relation_share_one_relationship_type() -> None:
    doc, losses = to_graph_schema(_two_edges(properties=[{"name": "w", "type": "INT"}]))
    (links,) = doc["relationship_types"]
    assert links["properties"] == [{"name": "w", "type": "FLOAT"}]
    assert len(doc["patterns"]) == 2
    assert _has(losses, "`B LINKS A.w`", "type")


def test_edge_identities_are_named_not_written_as_a_key() -> None:
    doc, losses = to_graph_schema(
        _two_edges(properties=[{"name": "at", "type": "DATETIME"}], identities=[["at"]])
    )
    assert not [c for c in doc["constraints"] if "relationship_type" in c]
    assert _has(losses, "`B LINKS A`", "identities")


def test_the_export_loads_in_neo4j_graphrag(support: Schema) -> None:
    schema_module = pytest.importorskip("neo4j_graphrag.components.schema")
    doc, _ = to_graph_schema(support)
    loaded = schema_module.GraphSchema.model_validate(doc)
    assert len(loaded.node_types) == len(doc["node_types"])
