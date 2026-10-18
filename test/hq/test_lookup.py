"""lookup_candidates: which existing vertices a short text may name."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from graflo.architecture.backend import GraFloBackendWriter
from graflo.architecture.schema import CoreSchema, GraphMetadata, Schema
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.semantics import FieldSemantics
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig
from graflo.connections.graflo_backend import GraFloBackendConfig
from graflo.db.manager import ConnectionManager
from graflo.hq.lookup import (
    AliasHop,
    KeyPattern,
    LookupSpec,
    label_fields,
    lookup_candidates,
)
from graflo.util.transform import normalize_label

NAME = "https://schema.org/name"
ALT_LABEL = "http://www.w3.org/2004/02/skos/core#altLabel"


def _schema() -> Schema:
    return Schema(
        metadata=GraphMetadata(name="support"),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(
                vertices=[
                    Vertex(
                        name="Product",
                        properties=[
                            Field(name="sku"),
                            Field(name="name", semantics=FieldSemantics(iri=NAME)),
                            Field(name="name_norm"),
                        ],
                        identity=["sku"],
                    ),
                    Vertex(
                        name="Alias",
                        properties=[
                            Field(
                                name="label", semantics=FieldSemantics(iri=ALT_LABEL)
                            ),
                            Field(name="label_norm"),
                        ],
                        identity=["label"],
                    ),
                    Vertex(
                        name="Ticket",
                        properties=[Field(name="number"), Field(name="title")],
                        identity=["number"],
                    ),
                ]
            ),
            edge_config=EdgeConfig(
                edges=[Edge(source="Alias", target="Product", relation="ALIAS_OF")]
            ),
        ),
    )


def _product(sku: str, name: str) -> dict[str, Any]:
    return {"sku": sku, "name": name, "name_norm": normalize_label(name)}


@pytest.fixture
def conn(tmp_path: Path) -> Iterator[Any]:
    schema = _schema()
    with GraFloBackendWriter(tmp_path) as writer:
        writer.write_schema(schema)
        writer.write_vertex_batch(
            "Product",
            [_product("P-200", "Pump X1"), _product("P-300", "Valve Pro")],
        )
        writer.write_vertex_batch(
            "Alias",
            [{"label": "Berlin pump", "label_norm": normalize_label("Berlin pump")}],
        )
        writer.write_vertex_batch(
            "Ticket", [{"number": "SUP-42", "title": "pump noise"}]
        )
        writer.write_edge_batch(
            ("Alias", "Product", "ALIAS_OF"),
            [[{"label": "Berlin pump"}, {"sku": "P-200"}, {}]],
        )
        writer.flush_index()
    with ConnectionManager(
        connection_config=GraFloBackendConfig(output_dir=tmp_path)
    ) as connection:
        yield connection


SPEC = LookupSpec(
    normalized_fields={"Product": ["name_norm"], "Alias": ["label_norm"]},
    key_patterns=[
        KeyPattern(vertex_type="Ticket", field="number", pattern=r"\bSUP-\d+\b")
    ],
    aliases=[AliasHop(alias_type="Alias", relation="ALIAS_OF", target_type="Product")],
)


def _found(candidates: list[Any]) -> set[tuple[str, str, str]]:
    return {(c.vertex_type, next(iter(c.key.values())), c.lane) for c in candidates}


class TestNormalizeLabel:
    @pytest.mark.parametrize(
        ("raw", "normal"),
        [
            ("Pump X1", "pump x1"),
            ("  PUMP   x1 ", "pump x1"),
            ("Pump-X1!", "pump x1"),
            ("Ｐｕｍｐ", "pump"),
        ],
    )
    def test_case_punctuation_width_and_spacing_fold(
        self, raw: str, normal: str
    ) -> None:
        assert normalize_label(raw) == normal

    def test_empty_is_none(self) -> None:
        assert normalize_label(" -- ") is None
        assert normalize_label(None) is None


class TestLabelFields:
    def test_fields_with_a_label_iri(self) -> None:
        schema = _schema()
        vertices = {v.name: v for v in schema.core_schema.vertex_config.vertices}
        assert label_fields(vertices["Product"]) == ["name"]
        assert label_fields(vertices["Alias"]) == ["label"]
        assert label_fields(vertices["Ticket"]) == []


class TestLookup:
    def test_an_exact_label(self, conn: Any) -> None:
        found = lookup_candidates(conn, _schema(), "is the Pump X1 down?", SPEC)
        assert ("Product", "P-200", "exact") in _found(found)

    def test_a_normalized_label(self, conn: Any) -> None:
        found = lookup_candidates(conn, _schema(), "the PUMP-x1 is down", SPEC)
        assert ("Product", "P-200", "normalized") in _found(found)

    def test_a_key_pattern(self, conn: Any) -> None:
        found = lookup_candidates(conn, _schema(), "see SUP-42 again", SPEC)
        assert ("Ticket", "SUP-42", "key") in _found(found)

    def test_an_alias_hops_to_its_target(self, conn: Any) -> None:
        found = lookup_candidates(
            conn, _schema(), "the berlin pump is down again", SPEC
        )
        targets = [c for c in found if c.vertex_type == "Product"]
        assert [(c.key, c.lane) for c in targets] == [({"sku": "P-200"}, "alias")]
        assert targets[0].mention.lower() == "berlin pump"

    def test_a_candidate_carries_its_label_and_mention(self, conn: Any) -> None:
        (candidate,) = [
            c
            for c in lookup_candidates(conn, _schema(), "Valve Pro leaks", SPEC)
            if c.vertex_type == "Product"
        ]
        assert candidate.label == "Valve Pro"
        assert candidate.mention == "Valve Pro"

    def test_one_candidate_per_vertex_best_lane_first(self, conn: Any) -> None:
        found = lookup_candidates(conn, _schema(), "Pump X1 or pump x1", SPEC)
        products = [c for c in found if c.vertex_type == "Product"]
        assert [(c.key["sku"], c.lane) for c in products] == [("P-200", "exact")]

    def test_the_cap_holds(self, conn: Any) -> None:
        spec = SPEC.model_copy(update={"max_seeds": 1})
        found = lookup_candidates(
            conn, _schema(), "Pump X1 and Valve Pro and SUP-42", spec
        )
        assert len(found) == 1

    def test_types_restrict_the_search(self, conn: Any) -> None:
        spec = SPEC.model_copy(update={"vertex_types": ["Ticket"]})
        found = lookup_candidates(conn, _schema(), "Pump X1 and SUP-42", spec)
        assert _found(found) == {("Ticket", "SUP-42", "key")}

    def test_nothing_named_finds_nothing(self, conn: Any) -> None:
        assert lookup_candidates(conn, _schema(), "all quiet today", SPEC) == []
