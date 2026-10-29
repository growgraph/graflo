"""Field ``match`` and ``derived``: declared structure, graded comparison, derived identity."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from graflo.architecture.schema import CoreSchema, GraphMetadata, Schema
from graflo.architecture.schema.compare import compare_value, is_exclusive
from graflo.architecture.schema.context.element_text import (
    extractable_properties,
    identity_label,
)
from graflo.architecture.schema.derive import derive_fields, value_keys, value_parts
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.identity_digest import (
    compute_hash_identity,
    compute_vertex_identity,
)
from graflo.architecture.schema.key_contract import key_contract, primary_identity
from graflo.architecture.schema.vertex import (
    Field,
    FieldMergeConflict,
    Vertex,
    VertexConfig,
    merge_fields,
)
from graflo.hq.identify import identify
from graflo.rdf import ManifestRdfDeserializer, ManifestRdfSerializer

LABEL: dict[str, Any] = {"normalizer": "label_v1"}
SITE: dict[str, Any] = {
    "normalizer": "label_v1",
    "parts": {"split": [",", "/", ";"], "order": "general_first"},
}
HOST: dict[str, Any] = {
    "normalizer": "label_v1",
    "parts": {"split": ["."], "order": "specific_first"},
}
WORDS: dict[str, Any] = {
    "normalizer": "label_v1",
    "parts": {"split": "words", "order": "unordered"},
}


def _field(name: str = "f", **kwargs: Any) -> Field:
    return Field.model_validate({"name": name, **kwargs})


def _unit() -> Vertex:
    return Vertex.model_validate(
        {
            "name": "Unit",
            "identity": ["serial"],
            "properties": [
                "serial",
                {"name": "site", "type": "STRING", "match": SITE},
                {
                    "name": "site_keys",
                    "type": "LIST",
                    "item_type": "STRING",
                    "derived": {"source": "site", "form": "keys"},
                },
            ],
        }
    )


def _alias() -> Vertex:
    return Vertex.model_validate(
        {
            "name": "Alias",
            "identity": ["id"],
            "hash_identity_properties": ["normalized_label"],
            "properties": [
                {"name": "label", "match": LABEL},
                {
                    "name": "normalized_label",
                    "derived": {"source": "label", "form": "normal"},
                },
            ],
        }
    )


# -- declarations -------------------------------------------------------------


def test_a_field_without_match_or_derived_dumps_as_before():
    assert Field(name="f").model_dump() == Field(name="f").model_dump(
        exclude={"match", "derived"}
    )
    assert "match" not in Field(name="f").model_dump()


@pytest.mark.parametrize(
    ("declaration", "message"),
    [
        ({"normalizer": "label_v9"}, "unknown normalizer"),
        ({"parts": {"split": [], "order": "general_first"}}, "separators"),
        ({"parts": {"split": "words"}}, "order"),
        ({"parts": {"order": "general_first"}}, "split"),
    ],
)
def test_a_match_states_its_structure_completely(declaration, message):
    with pytest.raises(ValidationError, match=message):
        _field(match=declaration)


@pytest.mark.parametrize(
    ("field", "message"),
    [
        ({"derived": {"source": "a", "form": "normal"}, "match": LABEL}, "match"),
        (
            {
                "derived": {"source": "a", "form": "keys"},
                "type": "LIST",
                "item_type": "STRING",
                "reserved": True,
            },
            "reserved",
        ),
        ({"derived": {"source": "a", "form": "keys"}}, "LIST"),
        ({"derived": {"source": "a", "form": "normal"}, "type": "INT"}, "STRING"),
        ({"derived": {"source": "f", "form": "normal"}}, "itself"),
    ],
)
def test_a_derived_field_is_computed_never_compared_or_stamped(field, message):
    with pytest.raises(ValidationError, match=message):
        _field(**field)


@pytest.mark.parametrize(
    ("properties", "message"),
    [
        (
            ["k", {"name": "d", "derived": {"source": "x", "form": "normal"}}],
            "not a property",
        ),
        (
            ["k", {"name": "d", "derived": {"source": "k", "form": "normal"}}],
            "normalizer",
        ),
        (
            [
                "k",
                {"name": "s", "match": WORDS},
                {
                    "name": "d",
                    "type": "LIST",
                    "item_type": "STRING",
                    "derived": {"source": "s", "form": "keys"},
                },
            ],
            "ordered match.parts",
        ),
        (
            [
                "k",
                {"name": "s", "match": LABEL},
                {"name": "d", "derived": {"source": "s", "form": "normal"}},
                {"name": "e", "derived": {"source": "d", "form": "normal"}},
            ],
            "derived or reserved",
        ),
    ],
)
def test_a_derived_field_reads_a_plain_field_that_says_how(properties, message):
    with pytest.raises(ValidationError, match=message):
        Vertex.model_validate(
            {"name": "V", "identity": ["k"], "properties": properties}
        )


def test_an_edge_field_declares_neither():
    with pytest.raises(ValidationError, match="vertex field declarations"):
        Edge.model_validate(
            {
                "source": "a",
                "target": "b",
                "properties": [{"name": "w", "match": LABEL}],
            }
        )


def test_a_merge_carries_one_sides_declaration_and_refuses_two():
    plain, declared = _field(name="site"), _field(name="site", match=SITE)
    assert merge_fields(plain, declared, owner="vertex 'Unit'").match == declared.match
    with pytest.raises(FieldMergeConflict, match="match"):
        merge_fields(declared, _field(name="site", match=LABEL), owner="vertex 'Unit'")


# -- parts and keys -----------------------------------------------------------


@pytest.mark.parametrize(
    ("match", "value", "keys"),
    [
        (SITE, "Berlin, pump hall 2", ["berlin", "berlin pump hall 2"]),
        (
            SITE,
            "Berlin / Hall 2; Bay 3",
            ["berlin", "berlin hall 2", "berlin hall 2 bay 3"],
        ),
        (HOST, "eu.example.com", ["com", "example com", "eu example com"]),
        (
            {**LABEL, "parts": {"split": "words", "order": "general_first"}},
            "Nordvik Paper Mill",
            ["nordvik", "nordvik paper", "nordvik paper mill"],
        ),
        (WORDS, "Nordvik Paper Mill", []),
        (SITE, None, []),
    ],
)
def test_keys_are_the_runs_that_keep_the_general_end(match, value, keys):
    assert value_keys(_field(match=match), value) == keys


def test_without_a_normalizer_parts_are_kept_as_written():
    field = _field(match={"parts": {"split": ["/"], "order": "general_first"}})
    assert value_parts(field, "Lab/ Bay 3") == ["Lab", "Bay 3"]


def test_derivation_overwrites_a_stated_value_and_drops_one_without_its_source():
    unit = _unit()
    assert derive_fields(unit, {"site": "Berlin, hall 2", "site_keys": ["x"]}) == {
        "site": "Berlin, hall 2",
        "site_keys": ["berlin", "berlin hall 2"],
    }
    assert derive_fields(unit, {"serial": "S", "site_keys": ["x"]}) == {"serial": "S"}


# -- comparison ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("match", "stated", "stored", "keyed", "level"),
    [
        (None, "a", "a", True, "exact"),
        (None, "a", "b", True, "conflict"),
        (None, "a", "b", False, "differs"),
        (None, None, "b", True, "unknown"),
        ({**LABEL, "exclusive": True}, "Open", "OPEN", False, "exact"),
        ({**LABEL, "exclusive": True}, "open", "closed", False, "conflict"),
        (SITE, "Berlin", "Berlin, pump hall 2", False, "compatible"),
        (SITE, "Berlin, pump hall 2", "Berlin", False, "compatible"),
        (SITE, "pump hall 2", "Berlin, pump hall 2", False, "differs"),
        (SITE, "Lyon", "Berlin, pump hall 2", True, "conflict"),
        (HOST, "example.com", "eu.example.com", True, "compatible"),
        (HOST, "eu.example", "eu.example.com", True, "conflict"),
        (WORDS, "controller", "P-200 controller", False, "compatible"),
        (
            {
                **LABEL,
                "parts": {"split": "words", "order": "general_first"},
                "exclusive": False,
            },
            "Brookmere",
            "Brookmere Waterworks",
            True,
            "compatible",
        ),
    ],
)
def test_a_level_follows_the_declared_structure(match, stated, stored, keyed, level):
    field = _field(match=match) if match is not None else _field()
    assert compare_value(field, stated, stored, keyed=keyed) == level


def test_a_list_takes_its_best_members_level():
    field = _field(type="LIST", item_type="STRING", match=SITE)
    assert compare_value(field, "Berlin", ["Lyon", "Berlin, hall 2"], keyed=False) == (
        "compatible"
    )
    assert not is_exclusive(_field(type="LIST", item_type="STRING"), keyed=True)


# -- derived identity -----------------------------------------------------------


def test_a_derived_identity_names_its_normalizer_and_a_plain_one_is_unchanged():
    alias = _alias()
    one = compute_vertex_identity({"label": "Berlin pump"}, alias)
    assert one == compute_vertex_identity({"label": " BERLIN-pump! "}, alias)
    assert one != compute_hash_identity(
        {"normalized_label": "berlin pump"}, ["normalized_label"]
    )
    plain = Vertex(
        name="P", identity=["id"], hash_identity_properties=["a"], properties=["a"]
    )
    assert compute_vertex_identity({"a": "x"}, plain) == compute_hash_identity(
        {"a": "x"}, ["a"]
    )


def test_a_cast_alias_stating_only_its_label_is_identified():
    alias = _alias()
    schema = Schema(
        metadata=GraphMetadata(name="s"),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(vertices=[alias]),
            edge_config=EdgeConfig(edges=[]),
        ),
    )
    found = identify(schema, "Alias", {"label": "Berlin pump"})
    assert found.outcome == "identified"
    contract = key_contract(alias)
    assert found.identity == primary_identity(
        alias, contract, derive_fields(alias, {"label": "berlin  PUMP"})
    )


# -- what the cast sees -----------------------------------------------------------


def test_the_cast_never_sees_a_derived_field_and_reads_identity_by_its_source():
    alias, unit = _alias(), _unit()
    assert [f.name for f in extractable_properties(alias)] == ["label"]
    assert identity_label(alias) == "hash(label)"
    assert [f.name for f in extractable_properties(unit)] == ["serial", "site"]


# -- RDF ----------------------------------------------------------------------


def test_match_and_derived_round_trip_through_rdf():
    from graflo.architecture.contract.manifest import GraphManifest

    manifest = GraphManifest.model_validate(
        {
            "schema": {
                "metadata": {"name": "s", "version": "1.0.0"},
                "core_schema": {
                    "vertex_config": {
                        "vertices": [
                            _unit().model_dump(),
                            {
                                "name": "Host",
                                "identity": ["name"],
                                "properties": [
                                    {
                                        "name": "name",
                                        "match": {**HOST, "exclusive": False},
                                    },
                                    {"name": "tags", "match": {}},
                                ],
                            },
                        ]
                    },
                    "edge_config": {"edges": []},
                },
            }
        }
    )
    base = "https://example.org/manifest/"
    ttl = ManifestRdfSerializer(include_ontology=False).to_turtle(manifest, base)
    back = ManifestRdfDeserializer().from_turtle(ttl, base.rstrip("/"))
    assert back.graph_schema is not None and manifest.graph_schema is not None
    vertices = {v.name: v for v in back.graph_schema.core_schema.vertex_config.vertices}
    original = {
        v.name: v for v in manifest.graph_schema.core_schema.vertex_config.vertices
    }
    for name, vertex in original.items():
        got = {f.name: (f.match, f.derived) for f in vertices[name].properties}
        want = {f.name: (f.match, f.derived) for f in vertex.properties}
        assert got == want


def test_a_stored_normal_form_still_addresses_its_vertex():
    alias = _alias()
    schema = Schema(
        metadata=GraphMetadata(name="s"),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(vertices=[alias]),
            edge_config=EdgeConfig(edges=[]),
        ),
    )
    by_label = identify(schema, "Alias", {"label": "Berlin  PUMP"})
    by_normal = identify(schema, "Alias", {"normalized_label": "berlin pump"})
    assert by_normal.outcome == "identified"
    assert by_normal.identity == by_label.identity
