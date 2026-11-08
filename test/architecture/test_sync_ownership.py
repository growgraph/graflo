"""Record sync ownership: a resource's ``sync`` block, checked when the model initialises."""

from __future__ import annotations

import copy
from typing import Any

import pytest

from graflo.architecture.contract.manifest import GraphManifest

MANIFEST: dict[str, Any] = {
    "schema": {
        "metadata": {"name": "sync_ownership", "version": "1.0.0"},
        "core_schema": {
            "vertex_config": {
                "vertices": [
                    {
                        "name": "Work",
                        "properties": ["wid", "title"],
                        "identity": ["wid"],
                    },
                    {
                        "name": "Author",
                        "properties": ["aid", "name"],
                        "identity": ["aid"],
                    },
                    {"name": "Place", "properties": ["pid"], "identity": ["pid"]},
                ]
            },
            "edge_config": {
                "edges": [
                    {"source": "Work", "target": "Work", "relation": "cites"},
                    {"source": "Author", "target": "Work", "relation": "wrote"},
                    {
                        "source": "Author",
                        "target": "Place",
                        "relation": "at",
                        "properties": ["work"],
                        "identities": [["source", "target", "relation", "work"]],
                    },
                ]
            },
        },
    },
    "ingestion_model": {
        "resources": [
            {
                "name": "works",
                "infer_edges": False,
                "apply": [
                    {"vertex": "Work", "from": {"wid": "wid", "title": "title"}},
                    {"vertex": "Author", "from": {"aid": "aid"}},
                    {"vertex": "Place", "from": {"pid": "pid"}},
                    {"edge": {"from": "Work", "to": "Work", "relation": "cites"}},
                    {"edge": {"from": "Author", "to": "Work", "relation": "wrote"}},
                    {
                        "edge": {
                            "from": "Author",
                            "to": "Place",
                            "relation": "at",
                            "properties": ["work"],
                        }
                    },
                ],
                "sync": {
                    "owns": "Work",
                    "edges": [
                        {"source": "Work", "target": "Work", "relation": "cites"},
                        {"source": "Author", "target": "Work", "relation": "wrote"},
                        {
                            "source": "Author",
                            "target": "Place",
                            "relation": "at",
                            "owner_field": "work",
                        },
                    ],
                },
            },
            {
                "name": "authors",
                "infer_edges": False,
                "apply": [{"vertex": "Author", "from": {"aid": "aid", "name": "name"}}],
                "sync": {"owns": "Author"},
            },
        ],
        "transforms": [],
    },
}


def _init(config: dict[str, Any]) -> GraphManifest:
    manifest = GraphManifest.from_config(config)
    manifest.finish_init()
    return manifest


def _refused(config: dict[str, Any], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        _init(config)


def _works(config: dict[str, Any]) -> dict[str, Any]:
    return config["ingestion_model"]["resources"][0]


def test_a_consistent_declaration_is_accepted() -> None:
    manifest = _init(copy.deepcopy(MANIFEST))
    sync = manifest.require_ingestion_model().fetch_resource_config("works").sync

    assert sync is not None
    assert [sync.owner_side(edge) for edge in sync.edges] == [
        "source",
        "target",
        "field",
    ]


def test_an_undeclared_type_cannot_be_owned() -> None:
    config = copy.deepcopy(MANIFEST)
    _works(config)["sync"]["owns"] = "Book"
    _refused(config, "owns 'Book', which the schema does not declare")


def test_a_type_has_one_owner() -> None:
    config = copy.deepcopy(MANIFEST)
    config["ingestion_model"]["resources"][1]["sync"]["owns"] = "Work"
    config["ingestion_model"]["resources"][1]["apply"].append(
        {"vertex": "Work", "from": {"wid": "wid"}}
    )
    _refused(config, "owns 'Work', already owned by 'works'")


def test_a_resource_owns_only_what_it_produces() -> None:
    config = copy.deepcopy(MANIFEST)
    config["ingestion_model"]["resources"][1]["sync"]["owns"] = "Place"
    _refused(config, "owns 'Place', which it does not produce")


def test_an_owned_edge_is_declared_and_written_by_its_owner() -> None:
    config = copy.deepcopy(MANIFEST)
    _works(config)["sync"]["edges"].append(
        {"source": "Work", "target": "Author", "relation": "by"}
    )
    _refused(config, r"owns edge \('Work', 'Author', 'by'\), which the schema")

    config = copy.deepcopy(MANIFEST)
    _works(config)["apply"].pop(3)
    _refused(config, r"owns edge \('Work', 'Work', 'cites'\), which it does not write")


def test_an_edge_away_from_the_owned_type_names_its_owner_field() -> None:
    config = copy.deepcopy(MANIFEST)
    del _works(config)["sync"]["edges"][2]["owner_field"]
    _refused(config, "touches no 'Work'; name the identity field")

    config = copy.deepcopy(MANIFEST)
    edge = config["schema"]["core_schema"]["edge_config"]["edges"][2]
    edge["identities"] = [["source", "target", "relation"]]
    _refused(config, "owner_field 'work' .* is not an identity field")


def test_an_edge_touching_the_owned_type_takes_no_owner_field() -> None:
    config = copy.deepcopy(MANIFEST)
    _works(config)["sync"]["edges"][1]["owner_field"] = "work"
    _refused(config, "is owned by its target; it takes no owner_field")


def test_an_owned_edge_has_one_writer() -> None:
    config = copy.deepcopy(MANIFEST)
    authors = config["ingestion_model"]["resources"][1]
    authors["apply"] += [
        {"vertex": "Work", "from": {"wid": "wid"}},
        {"edge": {"from": "Author", "to": "Work", "relation": "wrote"}},
    ]
    _refused(config, r"owned by 'works' but also written by \['authors'\]")


def test_an_owned_edge_written_by_inference_elsewhere_is_refused() -> None:
    config = copy.deepcopy(MANIFEST)
    authors = config["ingestion_model"]["resources"][1]
    authors["infer_edges"] = True
    authors["apply"].append({"vertex": "Work", "from": {"wid": "wid"}})
    _refused(config, r"owned by 'works' but also written by \['authors'\]")


def test_the_block_survives_the_canonical_form() -> None:
    manifest = _init(copy.deepcopy(MANIFEST))
    canonical = manifest.to_minimal_canonical_dict()
    resources = canonical["ingestion_model"]["resources"]

    assert resources[1]["sync"] == {"owns": "Author"}
    assert resources[0]["sync"]["edges"][2]["owner_field"] == "work"


def test_the_block_survives_rdf() -> None:
    from graflo.rdf.deserializer import ManifestRdfDeserializer
    from graflo.rdf.serializer import ManifestRdfSerializer

    base = "http://example.org/sync/"
    manifest = _init(copy.deepcopy(MANIFEST))
    turtle = ManifestRdfSerializer(include_ontology=False).to_turtle(manifest, base)
    back = ManifestRdfDeserializer().from_turtle(turtle, base.rstrip("/"))

    model = back.require_ingestion_model()
    assert model.fetch_resource_config("works").sync == (
        manifest.require_ingestion_model().fetch_resource_config("works").sync
    )
