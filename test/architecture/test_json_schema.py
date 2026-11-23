"""The GSTL JSON Schema describes every manifest graflo writes."""

from __future__ import annotations

import copy
import pathlib
from collections.abc import Callable

import jsonschema
import pytest
import yaml
from jsonschema.protocols import Validator

from graflo.architecture.contract.json_schema import (
    GSTL_SCHEMA_ID,
    manifest_json_schema,
)
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.contract.provenance import GSTL_VERSION

EXAMPLES = pathlib.Path(__file__).resolve().parents[2] / "examples"
MANIFESTS = sorted(EXAMPLES.glob("**/manifest*.yaml"))


@pytest.fixture(scope="module")
def validator() -> Validator:
    schema = manifest_json_schema()
    jsonschema.Draft202012Validator.check_schema(schema)
    return jsonschema.Draft202012Validator(schema)


def _canonical(path: pathlib.Path) -> dict:
    manifest = GraphManifest.from_dict(yaml.safe_load(path.read_text()))
    return manifest.to_minimal_canonical_dict()


def test_schema_declares_its_identity() -> None:
    schema = manifest_json_schema()
    assert schema["$id"] == GSTL_SCHEMA_ID
    assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert schema["x-gstl-version"] == GSTL_VERSION


def test_schema_is_stable() -> None:
    assert manifest_json_schema() == manifest_json_schema()


def test_examples_exist() -> None:
    assert len(MANIFESTS) > 10


@pytest.mark.parametrize("path", MANIFESTS, ids=lambda p: str(p.relative_to(EXAMPLES)))
def test_every_written_manifest_validates(
    path: pathlib.Path, validator: Validator
) -> None:
    errors = [e.message for e in validator.iter_errors(_canonical(path))]
    assert errors == []


def _first_vertex(document: dict) -> dict:
    return document["schema"]["core_schema"]["vertex_config"]["vertices"][0]


def _pipeline(document: dict) -> list:
    return document["ingestion_model"]["resources"][0]["pipeline"]


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda d: d.update(unknown_block={}), id="unknown top-level key"),
        pytest.param(
            lambda d: _first_vertex(d).update(colour="red"), id="unknown vertex key"
        ),
        pytest.param(
            lambda d: _first_vertex(d)["properties"].append(
                {"name": "x", "type": "TEXT"}
            ),
            id="unknown field type",
        ),
        pytest.param(
            lambda d: _pipeline(d).append(
                {"edge": {"from": "person", "to": "department", "flavour": 1}}
            ),
            id="unknown edge step key",
        ),
        pytest.param(
            lambda d: _pipeline(d).append({"type": "vertex", "vertex": "person"}),
            id="internal step form",
        ),
    ],
)
def test_malformed_manifests_fail(
    mutate: Callable[[dict], None], validator: Validator
) -> None:
    document = copy.deepcopy(
        _canonical(EXAMPLES / "01-csv-two-resources" / "manifest.yaml")
    )
    mutate(document)
    assert list(validator.iter_errors(document))
