"""classify_anchor: an anchor is a complete key, never a property match."""

from __future__ import annotations

from typing import Any

import pytest

from graflo.architecture.schema.anchor import (
    AnchorIncompleteError,
    AnchorNotAKeyError,
    check_anchor_shape,
    classify_anchor,
)
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.key_contract import KeyContract, key_contract

SCHEMA: dict[str, Any] = {
    "metadata": {"name": "anchors", "version": "1.0.0"},
    "core_schema": {
        "vertex_config": {
            "vertices": [
                {
                    "name": "Customer",
                    "properties": ["account", "email", "name"],
                    "identity": ["account"],
                    "secondary_identities": [{"name": "by_email", "fields": ["email"]}],
                },
                {
                    "name": "Site",
                    "properties": ["region", "code", "name"],
                    "identity": ["region", "code"],
                },
                {
                    "name": "Alias",
                    "properties": ["label", "normalized_label"],
                    "hash_identity_properties": ["normalized_label"],
                },
            ]
        },
        "edge_config": {"edges": []},
    },
}


def _contract(vertex_type: str) -> KeyContract:
    schema = Schema.model_validate(SCHEMA)
    return key_contract(schema.core_schema.vertex_config[vertex_type])


def test_a_bare_value_anchors_a_one_field_identity() -> None:
    assert classify_anchor(_contract("Customer"), "ACC-1") == ("bare", None)


def test_a_bare_value_cannot_anchor_a_composite_identity() -> None:
    with pytest.raises(AnchorIncompleteError) as refused:
        classify_anchor(_contract("Site"), "S1")
    assert refused.value.missing == ("region", "code")


def test_the_primary_identity_anchors() -> None:
    assert classify_anchor(_contract("Site"), {"code": "S1", "region": "eu"}) == (
        "identity",
        None,
    )


def test_part_of_a_composite_identity_is_incomplete() -> None:
    with pytest.raises(AnchorIncompleteError) as refused:
        classify_anchor(_contract("Site"), {"region": "eu"})
    assert refused.value.missing == ("code",)
    assert refused.value.code == "anchor_incomplete"


def test_a_secondary_identity_anchors_by_lookup() -> None:
    form, key_set = classify_anchor(_contract("Customer"), {"email": "jo@x.com"})
    assert form == "alternate"
    assert key_set is not None and key_set.name == "by_email"


def test_a_derived_key_anchors_by_its_source_fields() -> None:
    form, key_set = classify_anchor(_contract("Alias"), {"normalized_label": "x"})
    assert form == "key_set"
    assert key_set is not None and key_set.fields == ("normalized_label",)


def test_a_derived_key_anchors_by_its_digest() -> None:
    assert classify_anchor(_contract("Alias"), {"id": "be9d"}) == ("identity", None)


def test_a_field_that_is_no_key_is_refused() -> None:
    with pytest.raises(AnchorNotAKeyError, match="not a key") as refused:
        classify_anchor(_contract("Customer"), {"name": "Jo Park"})
    assert refused.value.code == "anchor_not_a_key"


def test_a_whole_row_is_refused() -> None:
    """A key plus other fields is a match on those fields too, not an anchor."""
    with pytest.raises(AnchorNotAKeyError):
        classify_anchor(
            _contract("Alias"),
            {"id": "be9d", "label": "Berlin pump", "normalized_label": "berlin pump"},
        )


def test_an_undeclared_field_is_refused_before_any_query_is_written() -> None:
    with pytest.raises(AnchorNotAKeyError):
        classify_anchor(_contract("Customer"), {"account` == 1 //": "x"})


@pytest.mark.parametrize("key", [{}, "", {"account": ""}, {"account": None}])
def test_an_empty_anchor_is_incomplete(key: Any) -> None:
    with pytest.raises(AnchorIncompleteError):
        check_anchor_shape("Customer", key)


@pytest.mark.parametrize("key", [None, ["ACC-1"], {"account": ["ACC-1"]}, {1: "x"}])
def test_an_anchor_holds_scalars_only(key: Any) -> None:
    with pytest.raises(AnchorNotAKeyError):
        check_anchor_shape("Customer", key)
