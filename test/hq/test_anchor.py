"""Traversal anchors on a live (file) backend: complete keys, looked up when secondary."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from graflo.architecture.graph_types.container import GraphContainer
from graflo.architecture.query.models import TraverseQuery
from graflo.architecture.schema.anchor import (
    AnchorAmbiguousError,
    AnchorIncompleteError,
    AnchorNotAKeyError,
    AnchorUnmatchedError,
)
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.identity_digest import compute_vertex_identity
from graflo.connections.graflo_backend import GraFloBackendConfig
from graflo.db.manager import ConnectionManager
from graflo.hq.container_write import write_container

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
                {"name": "Ticket", "properties": ["number"], "identity": ["number"]},
                {
                    "name": "Alias",
                    "properties": ["label", "normalized_label"],
                    "hash_identity_properties": ["normalized_label"],
                },
                {"name": "Product", "properties": ["sku"], "identity": ["sku"]},
            ]
        },
        "edge_config": {
            "edges": [
                {"source": "Customer", "target": "Ticket", "relation": "RAISED"},
                {"source": "Alias", "target": "Product", "relation": "aliasOf"},
            ]
        },
    },
}
CUSTOMERS = [
    {"account": "ACC-1", "email": "jo@x.com", "name": "Jo Park"},
    {"account": "ACC-4", "email": "desk@x.com", "name": "Front Desk"},
    {"account": "ACC-5", "email": "desk@x.com", "name": "Back Desk"},
]


def _schema() -> Schema:
    return Schema.model_validate(SCHEMA)


def _alias_id(normalized: str) -> str:
    vertex = _schema().core_schema.vertex_config["Alias"]
    digest = compute_vertex_identity({"normalized_label": normalized}, vertex)
    assert digest is not None
    return digest


@pytest.fixture
def db(tmp_path: Path) -> Iterator[Any]:
    conf = GraFloBackendConfig(output_dir=tmp_path)
    berlin = {
        "id": _alias_id("berlin pump"),
        "label": "Berlin pump",
        "normalized_label": "berlin pump",
    }
    asyncio.run(
        write_container(
            _schema(),
            GraphContainer(
                vertices={
                    "Customer": CUSTOMERS,
                    "Ticket": [{"number": "HD-1"}, {"number": "HD-4"}],
                    "Alias": [berlin],
                    "Product": [{"sku": "P-200"}],
                },
                edges={
                    ("Customer", "Ticket", "RAISED"): [
                        ({"account": "ACC-1"}, {"number": "HD-1"}, {}),
                        ({"account": "ACC-4"}, {"number": "HD-4"}, {}),
                    ],
                    ("Alias", "Product", "aliasOf"): [
                        ({"id": berlin["id"]}, {"sku": "P-200"}, {})
                    ],
                },
            ),
            conf,
        )
    )
    with ConnectionManager(connection_config=conf) as connection:
        yield connection


def _reached(container: GraphContainer, vertex_type: str, field: str) -> list[str]:
    return sorted(doc[field] for doc in container.vertices.get(vertex_type, []))


def test_a_secondary_identity_anchors_the_walk(db: Any) -> None:
    reached = db.graph_neighbors("Customer", {"email": "jo@x.com"}, schema=_schema())
    assert _reached(reached, "Ticket", "number") == ["HD-1"]


def test_a_secondary_identity_naming_several_vertices_is_ambiguous(db: Any) -> None:
    with pytest.raises(AnchorAmbiguousError) as refused:
        db.graph_neighbors("Customer", {"email": "desk@x.com"}, schema=_schema())
    assert refused.value.matches == 2
    assert refused.value.code == "anchor_ambiguous"


def test_a_secondary_identity_naming_nothing_is_unmatched(db: Any) -> None:
    with pytest.raises(AnchorUnmatchedError):
        db.graph_neighbors("Customer", {"email": "none@x.com"}, schema=_schema())


def test_a_primary_identity_naming_nothing_has_no_neighbours(db: Any) -> None:
    reached = db.graph_neighbors("Customer", {"account": "ACC-9"}, schema=_schema())
    assert not reached.vertices and not reached.edges


def test_a_field_that_is_no_key_is_refused(db: Any) -> None:
    with pytest.raises(AnchorNotAKeyError):
        db.graph_neighbors("Customer", {"name": "Jo Park"}, schema=_schema())


def test_a_whole_row_is_refused(db: Any) -> None:
    """The shape a client gets from ``nodes``: a key plus every other property."""
    with pytest.raises(AnchorNotAKeyError):
        db.graph_neighbors("Customer", dict(CUSTOMERS[0]), schema=_schema())


def test_a_derived_key_anchors_by_its_digest_and_by_its_source(db: Any) -> None:
    digest = _alias_id("berlin pump")
    by_digest = db.graph_neighbors("Alias", {"id": digest}, schema=_schema())
    by_source = db.graph_neighbors(
        "Alias", {"normalized_label": "berlin pump"}, schema=_schema()
    )
    assert _reached(by_digest, "Product", "sku") == ["P-200"]
    assert _reached(by_source, "Product", "sku") == ["P-200"]


def test_a_mapping_without_a_schema_is_refused(db: Any) -> None:
    with pytest.raises(AnchorNotAKeyError, match="needs the schema"):
        db.graph_neighbors("Customer", {"account": "ACC-1"})


def test_a_traversal_names_the_seed_that_failed(db: Any) -> None:
    query = TraverseQuery(
        seeds=[
            {"vertex_type": "Customer", "key": "ACC-1"},
            {"vertex_type": "Customer", "key": {"email": "desk@x.com"}},
        ]
    )
    with pytest.raises(AnchorAmbiguousError) as refused:
        db.traverse(query, schema=_schema())
    assert refused.value.seed == 1


def test_a_traversal_walks_from_secondary_and_primary_seeds(db: Any) -> None:
    query = TraverseQuery(
        seeds=[
            {"vertex_type": "Customer", "key": {"email": "jo@x.com"}},
            {"vertex_type": "Customer", "key": "ACC-4"},
        ]
    )
    reached = db.traverse(query, schema=_schema())
    assert _reached(reached, "Ticket", "number") == ["HD-1", "HD-4"]


def test_an_incomplete_anchor_is_refused_before_any_read(db: Any) -> None:
    with pytest.raises(AnchorIncompleteError):
        db.graph_neighbors("Customer", {"account": ""}, schema=_schema())
