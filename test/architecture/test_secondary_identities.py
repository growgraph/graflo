"""Schema-level tests for secondary identities on vertices."""

from __future__ import annotations

import pytest

from graflo.architecture.database_features import DatabaseProfile
from graflo.architecture.schema.db_aware import VertexConfigDBAware
from graflo.architecture.schema.vertex import (
    SecondaryIdentity,
    Vertex,
    VertexConfig,
)
from graflo.onto import DBType


def _vertex(**overrides) -> Vertex:
    payload: dict = {
        "name": "VertexA",
        "properties": ["IdA", "isin", "org", "code"],
        "identity": ["IdA"],
    }
    payload.update(overrides)
    return Vertex.model_validate(payload)


class TestAuthoredShape:
    def test_mapping_form_keeps_name(self) -> None:
        vertex = _vertex(secondary_identities=[{"name": "by_isin", "fields": ["isin"]}])
        assert vertex.secondary_identities[0].name == "by_isin"
        assert vertex.secondary_identities[0].fields == ["isin"]

    def test_bare_list_is_accepted_and_auto_named(self) -> None:
        """The originally specified list-of-lists shape still loads."""
        vertex = _vertex(secondary_identities=[["isin"], ["org", "code"]])
        assert [e.fields for e in vertex.secondary_identities] == [
            ["isin"],
            ["org", "code"],
        ]
        assert vertex.secondary_identity_names == ["secondary_0", "secondary_1"]

    def test_mixed_forms_coexist(self) -> None:
        vertex = _vertex(
            secondary_identities=[
                {"name": "by_isin", "fields": ["isin"]},
                ["org", "code"],
            ]
        )
        assert vertex.secondary_identity_names == ["by_isin", "secondary_1"]

    def test_bare_string_is_accepted(self) -> None:
        assert SecondaryIdentity.model_validate("isin").fields == ["isin"]

    def test_fields_are_deduped(self) -> None:
        vertex = _vertex(secondary_identities=[["org", "org", "code"]])
        assert vertex.secondary_identities[0].fields == ["org", "code"]

    def test_empty_field_set_is_rejected(self) -> None:
        with pytest.raises(ValueError):
            _vertex(secondary_identities=[[]])

    def test_default_is_empty(self) -> None:
        assert _vertex().secondary_identities == []


class TestValidation:
    def test_unknown_field_is_added_to_properties(self) -> None:
        """Mirrors identity: a declared key implies the property exists."""
        vertex = _vertex(properties=["IdA"], secondary_identities=[["isin"]])
        assert "isin" in vertex.property_names

    def test_list_typed_field_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="LIST-typed"):
            _vertex(
                properties=[
                    "IdA",
                    {"name": "tags", "type": "LIST", "item_type": "STRING"},
                ],
                secondary_identities=[["tags"]],
            )

    def test_blank_vertex_is_rejected(self) -> None:
        """A blank vertex has no source-visible key to match on."""
        with pytest.raises(ValueError, match="blank"):
            Vertex.model_validate(
                {
                    "name": "B",
                    "properties": ["x"],
                    "blank": True,
                    "secondary_identities": [["x"]],
                }
            )

    def test_duplicate_of_primary_identity_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="duplicates the primary identity"):
            _vertex(secondary_identities=[["IdA"]])

    def test_duplicate_field_sets_are_rejected(self) -> None:
        with pytest.raises(ValueError, match="duplicate secondary identity"):
            _vertex(secondary_identities=[["isin"], ["isin"]])

    def test_field_set_equality_ignores_order(self) -> None:
        with pytest.raises(ValueError, match="duplicate secondary identity"):
            _vertex(secondary_identities=[["org", "code"], ["code", "org"]])

    def test_duplicate_names_are_rejected(self) -> None:
        with pytest.raises(ValueError, match="duplicate secondary identity name"):
            _vertex(
                secondary_identities=[
                    {"name": "dup", "fields": ["isin"]},
                    {"name": "dup", "fields": ["org"]},
                ]
            )

    @pytest.mark.parametrize("mode", ["assigned", "hash"])
    def test_allowed_for_assigned_and_hash_identity(self, mode: str) -> None:
        """UUID/digest primary key plus a business key is the canonical case."""
        payload: dict = {
            "name": "V",
            "properties": ["isin", "src"],
            "secondary_identities": [["isin"]],
        }
        if mode == "assigned":
            payload["assigned"] = True
        else:
            payload["hash_identity_properties"] = ["src"]
        vertex = Vertex.model_validate(payload)
        assert vertex.identity_mode == mode
        assert vertex.secondary_identities[0].fields == ["isin"]


class TestSelectorResolution:
    def test_resolve_by_name(self) -> None:
        config = VertexConfig(
            vertices=[
                _vertex(secondary_identities=[{"name": "by_isin", "fields": ["isin"]}])
            ]
        )
        assert config.match_fields("VertexA", "by_isin") == ["isin"]

    def test_resolve_by_explicit_field_list_any_order(self) -> None:
        config = VertexConfig(
            vertices=[_vertex(secondary_identities=[["org", "code"]])]
        )
        assert config.match_fields("VertexA", ["code", "org"]) == ["org", "code"]

    def test_sugar_resolves_when_exactly_one_declared(self) -> None:
        config = VertexConfig(vertices=[_vertex(secondary_identities=[["isin"]])])
        assert config.match_fields("VertexA", "secondary") == ["isin"]

    def test_sugar_is_ambiguous_with_several_declared(self) -> None:
        config = VertexConfig(
            vertices=[_vertex(secondary_identities=[["isin"], ["org", "code"]])]
        )
        with pytest.raises(ValueError, match="no secondary identity matches"):
            config.match_fields("VertexA", "secondary")

    def test_unknown_selector_lists_what_is_declared(self) -> None:
        config = VertexConfig(
            vertices=[
                _vertex(secondary_identities=[{"name": "by_isin", "fields": ["isin"]}])
            ]
        )
        with pytest.raises(ValueError, match="by_isin"):
            config.match_fields("VertexA", "nope")

    @pytest.mark.parametrize("selector", [None, "identity"])
    def test_primary_is_the_default(self, selector) -> None:
        """Every existing edge step keeps matching on the primary identity."""
        config = VertexConfig(vertices=[_vertex(secondary_identities=[["isin"]])])
        assert config.match_fields("VertexA", selector) == ["IdA"]

    def test_db_aware_wrapper_resolves_the_same(self) -> None:
        config = VertexConfig(
            vertices=[
                _vertex(secondary_identities=[{"name": "by_isin", "fields": ["isin"]}])
            ]
        )
        db_aware = VertexConfigDBAware(config, DatabaseProfile(db_flavor=DBType.NEO4J))
        assert db_aware.match_fields("VertexA", "by_isin") == ["isin"]
        assert db_aware.match_fields("VertexA", None) == ["IdA"]
        assert db_aware.secondary_identities("VertexA")[0].fields == ["isin"]


class TestRoundTrip:
    def test_survives_model_dump_and_reload(self) -> None:
        vertex = _vertex(
            secondary_identities=[
                {"name": "by_isin", "fields": ["isin"]},
                ["org", "code"],
            ]
        )
        reloaded = Vertex.model_validate(vertex.model_dump())
        assert [e.fields for e in reloaded.secondary_identities] == [
            ["isin"],
            ["org", "code"],
        ]
        assert reloaded.secondary_identity_names == ["by_isin", "secondary_1"]
