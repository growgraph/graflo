"""The writer sets derived fields before every vertex upsert, overwriting what was stated."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from graflo.architecture.graph_types.container import GraphContainer
from graflo.architecture.schema.document import Schema
from graflo.connections.graflo_backend import GraFloBackendConfig
from graflo.db.manager import ConnectionManager
from graflo.hq.container_write import write_container

SCHEMA: dict[str, Any] = {
    "metadata": {"name": "sites", "version": "1.0.0"},
    "core_schema": {
        "vertex_config": {
            "vertices": [
                {
                    "name": "Unit",
                    "properties": [
                        "serial",
                        {
                            "name": "site",
                            "match": {
                                "normalizer": "label_v1",
                                "parts": {"split": [","], "order": "general_first"},
                            },
                        },
                        {
                            "name": "site_keys",
                            "type": "LIST",
                            "item_type": "STRING",
                            "derived": {"source": "site", "form": "keys"},
                        },
                    ],
                    "identity": ["serial"],
                },
                {
                    "name": "Alias",
                    "properties": [
                        {"name": "label", "match": {"normalizer": "label_v1"}},
                        {
                            "name": "normalized_label",
                            "derived": {"source": "label", "form": "normal"},
                        },
                    ],
                    "identity": ["id"],
                    "hash_identity_properties": ["normalized_label"],
                },
            ]
        },
        "edge_config": {"edges": []},
    },
}


def test_a_stated_derived_value_is_overwritten_and_one_alias_per_normal_form(
    tmp_path: Path,
):
    conf = GraFloBackendConfig(output_dir=tmp_path)
    container = GraphContainer(
        vertices={
            "Unit": [{"serial": "KP-1", "site": "Berlin, hall 2", "site_keys": ["x"]}],
            "Alias": [
                {"label": "Berlin pump", "normalized_label": "berlin pumps"},
                {"label": "BERLIN-pump"},
            ],
        }
    )
    asyncio.run(write_container(Schema.model_validate(SCHEMA), container, conf))

    with ConnectionManager(connection_config=conf) as db:
        (unit,) = db.fetch_docs("Unit")
        aliases = db.fetch_docs("Alias")
    assert unit["site_keys"] == ["berlin", "berlin hall 2"]
    assert [a["normalized_label"] for a in aliases] == ["berlin pump"]
