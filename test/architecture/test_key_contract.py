"""A vertex type's key contract, and which key a set of values completes."""

import pytest

from graflo.architecture.schema.identity_digest import compute_vertex_identity
from graflo.architecture.schema.identity_uuid import mint_uuid
from graflo.architecture.schema.key_contract import (
    key_contract,
    key_status,
    primary_identity,
)
from graflo.architecture.schema.vertex import Vertex


def _vertex(**payload) -> Vertex:
    return Vertex.model_validate({"name": "T", **payload})


ROOM = _vertex(
    properties=["building", "number", "code"],
    identity=["building", "number"],
    secondary_identities=[{"name": "by_code", "fields": ["code"]}],
)
TAG = _vertex(
    properties=["label", "group"], hash_identity_properties=["label", "group"]
)
PARTY = _vertex(
    properties=["email", "phone", "country"],
    identity_funnel={
        "branches": [
            {"id": "email", "fields": ["email"]},
            {"id": "phone", "fields": ["phone", "country"]},
        ]
    },
)
NOTE = _vertex(properties=["body"], blank=True)
ORDER = _vertex(
    properties=["ref"],
    assigned=True,
    secondary_identities=[{"name": "by_ref", "fields": ["ref"]}],
)


def test_each_identity_mode_has_its_source():
    assert key_contract(ROOM).source == "stated"
    assert key_contract(TAG).source == "derived"
    assert key_contract(PARTY).source == "derived"
    assert key_contract(NOTE).source == "minted"
    assert key_contract(ORDER).source == "minted"


def test_a_funnel_has_one_key_set_per_branch():
    contract = key_contract(PARTY)
    assert [(k.name, k.fields) for k in contract.keys] == [
        ("email", ("email",)),
        ("phone", ("phone", "country")),
    ]


@pytest.mark.parametrize(
    ("values", "state", "missing"),
    [
        ({"building": "B", "number": "12"}, "primary", ()),
        ({"number": "12"}, "partial", ("building",)),
        ({"number": "12", "building": ""}, "partial", ("building",)),
        ({"code": "B-12"}, "alternate", ("building", "number")),
        ({}, "none", ("building", "number")),
    ],
)
def test_a_composite_key_counts_only_when_complete(values, state, missing):
    status = key_status(key_contract(ROOM), values)
    assert (status.state, status.missing) == (state, missing)


def test_a_partial_flat_hash_is_not_digested():
    contract = key_contract(TAG)
    assert key_status(contract, {"label": "hardware"}).state == "partial"
    assert primary_identity(TAG, contract, {"label": "hardware"}) is None
    full = {"label": "hardware", "group": "ops"}
    assert primary_identity(TAG, contract, full) == {
        "id": compute_vertex_identity(full, TAG)
    }


def test_a_funnel_branch_completes_the_key():
    contract = key_contract(PARTY)
    assert key_status(contract, {"phone": "+1"}).state == "partial"
    status = key_status(contract, {"phone": "+1", "country": "US"})
    assert status.state == "primary"
    assert status.key is not None and status.key.name == "phone"


def test_a_minted_type_is_keyless_and_may_still_be_looked_up():
    status = key_status(key_contract(ORDER), {"ref": "PO-1"})
    assert status.state == "keyless"
    assert [a.name for a in status.alternates] == ["by_ref"]


def test_the_mint_is_stable():
    # Pinned: every key minted before this function moved depends on it.
    assert mint_uuid("plant@2.0.0", "msg-1", "Note", "n1") == (
        "3587db98-ae28-5016-86fb-3d66a549861a"
    )
