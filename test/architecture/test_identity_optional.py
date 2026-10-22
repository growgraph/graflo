"""``identity_optional``: a record without the step's key has no such vertex."""

from __future__ import annotations

from typing import Any

import pytest

from graflo.architecture.contract.manifest import GraphManifest

ACCOUNT = {"name": "Account", "properties": ["account_id"], "identity": ["account_id"]}
PERSON = {"name": "Person", "properties": ["email", "label"], "identity": ["email"]}
ACCOUNT_OF = {"source": "Account", "target": "Person", "relation": "accountOf"}


def _resource(person_step: dict[str, Any], vertices: list[dict] | None = None):
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "people", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {"vertices": vertices or [ACCOUNT, PERSON]},
                    "edge_config": {"edges": [ACCOUNT_OF]},
                },
            },
            "ingestion_model": {
                "resources": [
                    {
                        "name": "users",
                        "pipeline": [
                            {"vertex": "Account", "from": {"account_id": "id"}},
                            person_step,
                            {"edge": {"from": "Account", "to": "Person"}},
                        ],
                    }
                ]
            },
        }
    )
    manifest.finish_init()
    return manifest.require_ingestion_model().fetch_resource("users")


PERSON_STEP = {"vertex": "Person", "from": {"email": "mail", "label": "name"}}
HIDDEN = {"id": "a-2", "name": "Piotr"}
VISIBLE = {"id": "a-1", "mail": "ann@example.org", "name": "Ann"}


def test_a_record_without_the_key_emits_neither_the_vertex_nor_its_edge():
    resource = _resource({**PERSON_STEP, "identity_optional": True})
    entities = dict(resource.cast_document(HIDDEN).entities)
    assert [d["account_id"] for d in entities["Account"]] == ["a-2"]
    assert not entities.get("Person")
    assert not entities.get(("Account", "Person", "accountOf"))


def test_a_record_with_the_key_is_unchanged_by_the_flag():
    plain = dict(_resource(PERSON_STEP).cast_document(VISIBLE).entities)
    flagged = dict(
        _resource({**PERSON_STEP, "identity_optional": True})
        .cast_document(VISIBLE)
        .entities
    )
    assert flagged == plain
    assert [d["email"] for d in flagged["Person"]] == ["ann@example.org"]


def test_without_the_flag_the_keyless_document_is_still_emitted():
    """The caster then drops and reports it, as before."""
    entities = dict(_resource(PERSON_STEP).cast_document(HIDDEN).entities)
    assert entities["Person"]
    assert all(not d.get("email") for d in entities["Person"])


def test_the_flag_is_refused_on_a_minted_identity():
    digest = {
        "name": "Person",
        "properties": ["email", "label", "id"],
        "identity": ["id"],
        "hash_identity_properties": ["email"],
    }
    with pytest.raises(ValueError, match="identity_optional"):
        _resource({**PERSON_STEP, "identity_optional": True}, [ACCOUNT, digest])
