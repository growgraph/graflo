"""The compact schema notation: what it stands for, the way back, and what it
refuses."""

from __future__ import annotations

import pathlib
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from graflo.architecture.schema.document import Schema
from graflo.interop.compact import (
    CompactUnsupported,
    from_compact,
    is_compact,
    to_compact,
)

DATA = pathlib.Path(__file__).resolve().parent / "data"

# ``data/support_schema.yaml``, in the compact notation.
SUPPORT = """
name: support
version: 1.0.0
types:
  Customer:
    about: A customer account.
    aka: [client]
    key: account
    alt_keys: [email]
    fields: {account: str, email: str, name: str}
  Agent:
    about: A support agent.
    key: handle
    fields: {handle: str, name: str}
  Ticket:
    about: A support ticket.
    aka: [case, issue]
    key: number
    fields:
      number: str
      title: {type: str, about: A one-line summary of the problem.}
      severity: int
      opened: datetime
      wait:
        type: float
        about: Hours the customer waited for a fix.
        aka: [waited, waiting time]
        unit: h
  Product:
    about: A product the company sells.
    key: sku
    fields: {sku: str, name: str}
  Tag:
    about: A label put on tickets.
    key: label
    hashed: true
    fields: {label: str}
  Comment:
    about: A remark written on a ticket.
    fields: {body: str}
relations:
  - Customer RAISED Ticket
  - {from: Agent, name: HANDLES, to: Ticket, fields: {since: datetime}}
  - Ticket ABOUT Product
  - Ticket DUPLICATE_OF Ticket
  - Ticket TAGGED Tag
  - Comment ON Ticket
"""


def _schema(data: dict[str, Any]) -> Schema:
    return Schema.model_validate(data)


@pytest.fixture
def support() -> Schema:
    return _schema(yaml.safe_load((DATA / "support_schema.yaml").read_text()))


def test_a_compact_schema_is_the_graflo_schema_it_stands_for(support: Schema) -> None:
    compact = from_compact(yaml.safe_load(SUPPORT))
    assert compact.to_minimal_canonical_dict() == support.to_minimal_canonical_dict()


def test_a_graflo_schema_comes_back_in_the_compact_notation(support: Schema) -> None:
    assert to_compact(support) == yaml.safe_load(SUPPORT)


def test_the_compact_form_is_recognised_by_its_types_key(support: Schema) -> None:
    assert is_compact(yaml.safe_load(SUPPORT))
    assert not is_compact(support.to_minimal_canonical_dict())


def test_named_alternate_keys_keep_their_names() -> None:
    data = {
        "name": "crm",
        "types": {
            "Person": {
                "key": "id",
                "alt_keys": {"crm": "crm_id", "by_name_city": ["name", "city"]},
                "fields": {"id": "str", "crm_id": "str", "name": "str", "city": "str"},
            }
        },
    }
    schema = from_compact(data)
    (person,) = schema.core_schema.vertex_config.vertices
    assert [(s.name, s.fields) for s in person.secondary_identities] == [
        ("crm", ["crm_id"]),
        ("by_name_city", ["name", "city"]),
    ]
    assert to_compact(schema) == data


def _graflo(**vertex: Any) -> dict[str, Any]:
    return {
        "metadata": {"name": "s"},
        "core_schema": {
            "vertex_config": {
                "vertices": [
                    {"name": "A", "properties": [], "identity": ["k"], **vertex}
                ]
            },
            "edge_config": {"edges": []},
        },
    }


@pytest.mark.parametrize(
    ("vertex", "lost"),
    [
        ({"properties": [{"name": "k", "type": "UINT"}]}, "A.k.type=UINT"),
        (
            {
                "properties": [{"name": "k", "type": "STRING"}],
                "semantics": {"iri": "https://example.org/A"},
            },
            "semantics.iri",
        ),
    ],
)
def test_what_the_notation_cannot_say_is_refused_by_name(
    vertex: dict[str, Any], lost: str
) -> None:
    with pytest.raises(CompactUnsupported) as caught:
        to_compact(_schema(_graflo(**vertex)))
    assert any(lost in feature for feature in caught.value.features)


def test_edge_identities_are_refused() -> None:
    data = _graflo(properties=[{"name": "k", "type": "STRING"}])
    data["core_schema"]["edge_config"]["edges"] = [
        {
            "source": "A",
            "target": "A",
            "relation": "LINKS",
            "properties": [{"name": "at", "type": "DATETIME"}],
            "identities": [["at"]],
        }
    ]
    with pytest.raises(CompactUnsupported) as caught:
        to_compact(_schema(data))
    assert any("identities" in feature for feature in caught.value.features)


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"key": "missing"}, "key fields not in fields: missing"),
        ({"hashed": True, "key": None}, "hashed needs a key"),
        ({"colour": "red"}, "colour"),
        ({"fields": {"k": "string"}}, "fields.k"),
    ],
)
def test_a_bad_type_is_named(change: dict[str, Any], message: str) -> None:
    data = {"name": "s", "types": {"A": {"key": "k", "fields": {"k": "str"}, **change}}}
    with pytest.raises(ValidationError) as caught:
        from_compact(data)
    assert message in str(caught.value)
    assert "types.A" in str(caught.value)


def test_a_relation_names_known_types_in_three_words() -> None:
    data = {"name": "s", "types": {"A": {"key": "k", "fields": {"k": "str"}}}}
    with pytest.raises(ValidationError, match="unknown types: B"):
        from_compact({**data, "relations": ["A LINKS B"]})
    with pytest.raises(ValidationError, match="'Source RELATION Target'"):
        from_compact({**data, "relations": ["A LINKS"]})
