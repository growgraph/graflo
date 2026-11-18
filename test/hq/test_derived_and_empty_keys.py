"""Keys over derived fields resolve, and an empty value keys like a missing one."""

from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from graflo.architecture.graph_types.container import GraphContainer
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.identity_digest import compute_vertex_identity
from graflo.connections.graflo_backend import GraFloBackendConfig
from graflo.db.manager import ConnectionManager
from graflo.hq.container_write import write_container
from graflo.hq.document_caster import filter_graph_container_drop_empty_identity_inplace
from graflo.hq.identify import identify_many
from graflo.link.records import PartialRecord, RecordGraph, StatedProp
from graflo.link.resolve import resolve_records

LABEL = {"name": "label", "match": {"normalizer": "label_v1"}}
NORMAL = {"name": "label_n", "derived": {"source": "label", "form": "normal"}}

SCHEMA: dict[str, Any] = {
    "metadata": {"name": "parts", "version": "1.0.0"},
    "core_schema": {
        "vertex_config": {
            "vertices": [
                # A natural key on a derived field.
                {
                    "name": "Brand",
                    "properties": [LABEL, NORMAL],
                    "identity": ["label_n"],
                },
                # A secondary identity on a derived field.
                {
                    "name": "Maker",
                    "properties": ["code", LABEL, NORMAL],
                    "identity": ["code"],
                    "secondary_identities": [
                        {"name": "by_name", "fields": ["label_n"]}
                    ],
                },
                {
                    "name": "Part",
                    "properties": ["maker", "code"],
                    "hash_identity_properties": ["maker", "code"],
                },
            ]
        },
        "edge_config": {"edges": []},
    },
}


@pytest.fixture
def schema() -> Schema:
    return Schema.model_validate(SCHEMA)


def test_a_key_on_a_derived_field_survives_the_cast(schema: Schema) -> None:
    gc = GraphContainer(vertices={"Brand": [{"label": "Acme  Ltd"}]})

    filter_graph_container_drop_empty_identity_inplace(
        gc, vertex_config=schema.core_schema.vertex_config
    )

    (doc,) = gc.vertices["Brand"]
    assert doc["label_n"]


def test_a_secondary_identity_on_a_derived_field_is_read_from_its_source(
    schema: Schema, tmp_path: Path
) -> None:
    conf = GraFloBackendConfig(output_dir=tmp_path)
    asyncio.run(
        write_container(
            schema,
            GraphContainer(vertices={"Maker": [{"code": "M-1", "label": "Acme Ltd"}]}),
            conf,
        )
    )

    with ConnectionManager(connection_config=conf) as db:
        (found,) = identify_many(schema, "Maker", [{"label": "Acme Ltd"}], db=db)

    assert (found.outcome, found.identity, found.alternate) == (
        "identified",
        {"code": "M-1"},
        "by_name",
    )


def test_link_resolves_a_derived_secondary_identity_within_the_batch(
    schema: Schema,
) -> None:
    def record(ref: str, **props: Any) -> PartialRecord:
        return PartialRecord(
            ref=ref,
            type="Maker",
            may_create=True,
            props={k: StatedProp(value=v) for k, v in props.items()},
        )

    graphs = [
        RecordGraph(
            source_id=source,
            schema_ref="parts@1.0.0",
            records=[rec],
        )
        for source, rec in (
            ("a", record("m", code="M-1", label="Acme Ltd")),
            ("b", record("m", label="Acme Ltd")),
        )
    ]

    resolution = resolve_records(schema, graphs)

    resolved = {(r.source_id, r.ref): r for r in resolution.resolved}
    assert resolved[("b", "m")].identity == {"code": "M-1"}


@pytest.mark.parametrize("empty", [None, ""], ids=["none", "empty-string"])
def test_an_empty_hash_field_keys_like_a_missing_one(
    schema: Schema, empty: str | None
) -> None:
    part = schema.core_schema.vertex_config["Part"]

    assert compute_vertex_identity(
        {"maker": "acme", "code": empty}, part
    ) == compute_vertex_identity({"maker": "acme"}, part)


def test_a_filled_hash_key_keeps_its_digest(schema: Schema) -> None:
    part = schema.core_schema.vertex_config["Part"]
    payload = {"maker": "acme", "code": "X-1"}
    legacy = hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode()
    ).hexdigest()

    assert compute_vertex_identity(dict(payload), part) == legacy
