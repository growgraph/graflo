"""identify: what a set of property values identifies under the key contract."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from graflo.architecture.graph_types.container import GraphContainer
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.identity_digest import compute_vertex_identity
from graflo.connections.graflo_backend import GraFloBackendConfig
from graflo.db.manager import ConnectionManager
from graflo.filter.onto import ComparisonOperator, FilterExpression
from graflo.hq.container_write import write_container
from graflo.hq.identify import identify, identify_many

SCHEMA: dict[str, Any] = {
    "metadata": {"name": "support", "version": "1.0.0"},
    "core_schema": {
        "vertex_config": {
            "vertices": [
                {
                    "name": "Customer",
                    "properties": [
                        "account",
                        "email",
                        "phone",
                        {
                            "name": "access_zones",
                            "type": "LIST",
                            "item_type": "INT",
                            "reserved": True,
                        },
                    ],
                    "identity": ["account"],
                    "secondary_identities": [
                        {"name": "by_email", "fields": ["email"]},
                        {"name": "by_phone", "fields": ["phone"]},
                    ],
                },
                {
                    "name": "Part",
                    "properties": ["maker", "code"],
                    "hash_identity_properties": ["maker", "code"],
                },
                {"name": "Comment", "properties": ["body"], "blank": True},
            ]
        },
        "edge_config": {"edges": []},
    },
}
CUSTOMERS = [
    ({"account": "ACC-1", "email": "jo@x.com", "phone": "+1"}, [1]),
    ({"account": "ACC-2", "email": "desk@x.com"}, [1]),
    ({"account": "ACC-3", "email": "desk@x.com"}, [1]),
    ({"account": "ACC-4", "email": "ann@y.com"}, [2]),
]


@pytest.fixture
def schema() -> Schema:
    return Schema.model_validate(SCHEMA)


@pytest.fixture
def db(
    tmp_path: Path, schema: Schema, monkeypatch: pytest.MonkeyPatch
) -> Iterator[Any]:
    from graflo.db.graflo_backend.connection import GraFloBackendConnection

    # The file backend evaluates a guard on vertex reads in memory.
    monkeypatch.setattr(GraFloBackendConnection, "supports_read_guard", True)
    conf = GraFloBackendConfig(output_dir=tmp_path)
    for doc, zones in CUSTOMERS:
        asyncio.run(
            write_container(
                schema,
                GraphContainer(vertices={"Customer": [doc]}),
                conf,
                stamp={"access_zones": zones},
            )
        )
    with ConnectionManager(connection_config=conf) as connection:
        yield connection


def _opens(*zones: int) -> FilterExpression:
    return FilterExpression(
        kind="leaf",
        field="access_zones",
        cmp_operator=ComparisonOperator.ANY_IN,
        value=[list(zones)],
    )


def test_a_complete_primary_key_identifies_without_a_lookup(schema):
    found = identify(schema, "Customer", {"account": "ACC-9", "email": "z@x.com"})
    assert found.outcome == "identified"
    assert found.identity == {"account": "ACC-9"}
    assert found.by == "key"
    assert found.found is None


def test_a_complete_digest_key_identifies_by_its_digest(schema):
    values = {"maker": "Acme", "code": "X1"}
    found = identify(schema, "Part", values)
    vertex = schema.core_schema.vertex_config["Part"]
    assert found.identity == {"id": compute_vertex_identity(values, vertex)}


def test_a_digest_key_with_a_missing_field_is_incomplete(schema):
    found = identify(schema, "Part", {"maker": "Acme"})
    assert found.outcome == "incomplete"
    assert found.identity is None
    assert found.state == "partial"
    assert found.missing == ("code",)


def test_a_keyless_type_is_incomplete_with_nothing_missing(schema):
    found = identify(schema, "Comment", {"body": "shipped"})
    assert (found.outcome, found.state, found.missing) == ("incomplete", "keyless", ())


def test_without_a_database_only_the_primary_key_decides(schema):
    found = identify(schema, "Customer", {"email": "jo@x.com"})
    assert found.outcome == "incomplete"
    assert found.state == "alternate"
    assert found.alternates == ("by_email",)
    assert found.missing == ("account",)


def test_a_secondary_identity_naming_one_entity_identifies_it(schema, db):
    found = identify(schema, "Customer", {"email": "jo@x.com"}, db=db)
    assert found.outcome == "identified"
    assert found.identity == {"account": "ACC-1"}
    assert (found.by, found.alternate, found.found) == ("alternate", "by_email", True)


def test_a_secondary_identity_naming_two_entities_is_ambiguous(schema, db):
    found = identify(schema, "Customer", {"email": "desk@x.com"}, db=db)
    assert found.outcome == "ambiguous"
    assert found.identity is None
    assert sorted(i["account"] for i in found.identities) == ["ACC-2", "ACC-3"]


def test_a_secondary_identity_naming_nothing_is_unmatched(schema, db):
    found = identify(schema, "Customer", {"email": "nobody@x.com"}, db=db)
    assert (found.outcome, found.found) == ("unmatched", False)


def test_a_later_secondary_identity_settles_an_ambiguous_one(schema, db):
    found = identify(schema, "Customer", {"email": "desk@x.com", "phone": "+1"}, db=db)
    assert found.outcome == "identified"
    assert (found.identity, found.alternate) == ({"account": "ACC-1"}, "by_phone")


def test_an_entity_the_guard_hides_is_unmatched(schema, db):
    values = {"email": "ann@y.com"}
    assert identify(schema, "Customer", values, db=db, guard=_opens(1)).outcome == (
        "unmatched"
    )
    shown = identify(schema, "Customer", values, db=db, guard=_opens(2))
    assert shown.identity == {"account": "ACC-4"}


def test_with_a_database_a_primary_key_says_whether_the_graph_holds_it(schema, db):
    held, absent, hidden = identify_many(
        schema,
        "Customer",
        [{"account": "ACC-1"}, {"account": "ACC-9"}, {"account": "ACC-4"}],
        db=db,
        guard=_opens(1),
    )
    assert all(i.outcome == "identified" for i in (held, absent, hidden))
    assert [held.found, absent.found, hidden.found] == [True, False, False]


def test_a_guard_on_fields_a_type_lacks_is_not_applied(schema, db):
    guard = FilterExpression.from_dict(["==", "x", "missing_field"])
    found = identify(schema, "Customer", {"email": "jo@x.com"}, db=db, guard=guard)
    assert found.identity == {"account": "ACC-1"}


def test_many_value_sets_are_answered_in_order(schema, db):
    found = identify_many(
        schema,
        "Customer",
        [{"email": "nobody@x.com"}, {"account": "ACC-7"}, {"email": "jo@x.com"}],
        db=db,
    )
    assert [i.outcome for i in found] == ["unmatched", "identified", "identified"]


def test_an_unknown_type_is_refused(schema):
    with pytest.raises(ValueError, match="unknown vertex type"):
        identify(schema, "Robot", {})
