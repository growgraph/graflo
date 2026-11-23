"""Apache Ossie export: every document validates, and what is left out is listed."""

from __future__ import annotations

import json
import pathlib

import jsonschema
import pytest
import yaml
from jsonschema.protocols import Validator
from referencing import Registry, Resource

from graflo.architecture.contract.manifest import GraphManifest
from graflo.interop.ossie import OSSIE_VERSION, OssieExport, manifest_to_ossie

ROOT = pathlib.Path(__file__).resolve().parents[2]
EXAMPLES = ROOT / "examples"
SPEC = ROOT / "test" / "spec" / "ossie"
MANIFESTS = [
    p
    for p in sorted(EXAMPLES.glob("**/manifest*.yaml"))
    if "schema" in yaml.safe_load(p.read_text())
]
SHOP = EXAMPLES / "11-connection-proxy" / "manifest_shop.yaml"


def _load(path: pathlib.Path) -> GraphManifest:
    manifest = GraphManifest.from_dict(yaml.safe_load(path.read_text()))
    manifest.finish_init()
    return manifest


#: Upstream path of each vendored schema; their cross-references name these.
UPSTREAM = {
    "ossie-schema.json": "core-spec/ossie-schema.json",
    "ontology.json": "ontology/ontology.json",
    "mapping.json": "ontology/mapping.json",
}


def _registry() -> Registry:
    """The vendored schemas under every URI they are referenced by, so no
    validation ever fetches (an upstream reference names the moving `main`)."""
    resources = []
    for name, path in UPSTREAM.items():
        resource = Resource.from_contents(json.loads((SPEC / name).read_text()))
        for uri in (
            f"https://raw.githubusercontent.com/apache/ossie/main/{path}",
            f"https://github.com/apache/ossie/{path}",
        ):
            resources.append((uri, resource))
    return Registry().with_resources(resources)


@pytest.fixture(scope="module")
def validators() -> dict[str, Validator]:
    registry = _registry()
    files = {
        "ontology": "ontology.json",
        "semantic_model": "ossie-schema.json",
        "mapping": "mapping.json",
    }
    return {
        key: jsonschema.Draft202012Validator(
            json.loads((SPEC / name).read_text()), registry=registry
        )
        for key, name in files.items()
    }


@pytest.fixture(scope="module")
def shop() -> OssieExport:
    return manifest_to_ossie(_load(SHOP))


@pytest.mark.parametrize("path", MANIFESTS, ids=lambda p: str(p.relative_to(EXAMPLES)))
def test_every_document_validates(
    path: pathlib.Path, validators: dict[str, Validator]
) -> None:
    export = manifest_to_ossie(_load(path))
    for key, validator in validators.items():
        document = getattr(export, key)
        if document is not None:
            assert [e.message for e in validator.iter_errors(document)] == []


def test_documents_declare_the_pinned_version(shop: OssieExport) -> None:
    assert shop.ontology["version"] == OSSIE_VERSION
    assert shop.semantic_model is not None and shop.mapping is not None
    assert shop.semantic_model["version"] == shop.mapping["version"] == OSSIE_VERSION
    assert shop.mapping["ontology_ref"]["name"] == shop.ontology["name"]
    assert shop.mapping["semantic_model_ref"]["name"] == shop.semantic_model["name"]


def _concept(export: OssieExport, name: str) -> dict:
    return next(c for c in export.ontology["ontology"] if c["concept"] == name)


def test_identity_becomes_identify_by(shop: OssieExport) -> None:
    users = _concept(shop, "users")
    assert users["type"] == "EntityType"
    assert users["identify_by"] == ["id"]
    ident = next(r for r in users["relationships"] if r["name"] == "id")
    assert ident["multiplicity"] == "OneToOne"


def test_an_edge_with_properties_is_reified(shop: OssieExport) -> None:
    purchase = _concept(shop, "users_purchases_products")
    assert purchase["identify_by"] == ["source", "target"]
    names = [r["name"] for r in purchase["relationships"]]
    assert names[:2] == ["source", "target"]
    assert {"purchase_date", "quantity", "total_amount"} <= set(names)
    assert purchase["annotations"]["graflo"]["relation"] == "purchases"


def test_sql_resources_map_identity_from_columns(shop: OssieExport) -> None:
    assert shop.semantic_model is not None and shop.mapping is not None
    sources = {d["name"]: d["source"] for d in shop.semantic_model["datasets"]}
    assert sources["purchases"] == "purchases"
    follows = next(
        c
        for c in shop.mapping["concept_mappings"]
        if c["concept"] == "users_follows_users"
    )
    [om] = follows["object_mappings"]
    ends = {r["relationship"]: r["referent_mappings"] for r in om["referent_mappings"]}
    assert ends["source"] == [
        {"relationship": "id", "expression": "follows.follower_id"}
    ]
    assert ends["target"] == [
        {"relationship": "id", "expression": "follows.followed_id"}
    ]
    assert shop.losses == []


def test_a_plain_edge_is_a_relationship_with_its_inverse() -> None:
    export = manifest_to_ossie(_load(EXAMPLES / "19-edge-inverses" / "manifest.yaml"))
    person = _concept(export, "person")
    employed = next(r for r in person["relationships"] if r["name"] == "employed_by")
    assert employed["roles"] == [{"concept": "institution"}]
    assert employed["verbalizes"] == [
        "{person} employed_by {institution}",
        "{institution} employs {person}",
    ]
    assert any("undirected" in loss for loss in export.losses)


def test_file_resources_are_losses_and_no_mapping_is_written() -> None:
    export = manifest_to_ossie(
        _load(EXAMPLES / "01-csv-two-resources" / "manifest.yaml")
    )
    assert export.semantic_model is None and export.mapping is None
    assert any("FileConnector" in loss for loss in export.losses)
    assert set(export.files()) == {"hr.ontology.yaml", "losses.md"}


def test_files_are_plain_yaml(shop: OssieExport) -> None:
    files = shop.files()
    assert set(files) == {
        "shop.ontology.yaml",
        "shop.semantic_model.yaml",
        "shop.mapping.yaml",
        "losses.md",
    }
    assert "&id" not in files["shop.mapping.yaml"]
    assert yaml.safe_load(files["shop.mapping.yaml"]) == shop.mapping


def test_a_manifest_without_a_schema_is_refused() -> None:
    with pytest.raises(ValueError, match="schema"):
        manifest_to_ossie(GraphManifest.from_dict({"bindings": {"connectors": []}}))
