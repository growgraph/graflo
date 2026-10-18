"""Push connectors: a resource that accepts records handed to it, with no address."""

from __future__ import annotations

import pytest
import yaml

from graflo.architecture.contract.bindings import (
    Bindings,
    BoundSourceKind,
    PushConnector,
)
from graflo.architecture.contract.manifest import GraphManifest


def test_a_push_connector_is_its_own_source_kind() -> None:
    assert (
        PushConnector(kind="push", name="groundings").bound_source_kind()
        is BoundSourceKind.PUSH
    )


def test_a_push_connector_needs_a_name() -> None:
    with pytest.raises(ValueError):
        PushConnector(kind="push")  # ty: ignore[missing-argument]


def test_a_push_connector_matches_its_name_and_resource() -> None:
    connector = PushConnector(
        kind="push", name="groundings", resource_name="provenance"
    )
    assert connector.matches("groundings")
    assert connector.matches("provenance")
    assert not connector.matches("other")


def test_the_hash_follows_the_envelope_not_the_name() -> None:
    plain = PushConnector(kind="push", name="a")
    renamed = PushConnector(kind="push", name="b")
    enveloped = PushConnector(kind="push", name="a", records_path="data.items")
    annotated = PushConnector(
        kind="push", name="a", row_annotations={"source": "schemacast"}
    )
    assert plain.hash == renamed.hash
    assert len({plain.hash, enveloped.hash, annotated.hash}) == 3


def test_bindings_wire_a_push_connector_to_its_resource() -> None:
    bindings = Bindings.model_validate(
        {
            "connectors": [{"name": "groundings", "kind": "push"}],
            "resource_connector": [
                {"resource": "provenance", "connector": "groundings"}
            ],
        }
    )
    (connector,) = bindings.get_connectors_for_resource("provenance")
    assert isinstance(connector, PushConnector)


def test_a_manifest_with_a_push_connector_round_trips_through_yaml() -> None:
    manifest = GraphManifest.from_config(
        {
            "bindings": {
                "connectors": [
                    {"name": "groundings", "kind": "push", "records_path": "items"}
                ],
                "resource_connector": [
                    {"resource": "provenance", "connector": "groundings"}
                ],
            }
        }
    )
    dumped = yaml.safe_load(yaml.safe_dump(manifest.model_dump(mode="json")))
    again = GraphManifest.from_config(dumped)
    (connector,) = again.require_bindings().get_connectors_for_resource("provenance")
    assert isinstance(connector, PushConnector)
    assert connector.records_path == "items"


def test_the_kind_survives_the_minimal_canonical_form() -> None:
    bindings = Bindings.model_validate(
        {"connectors": [{"name": "groundings", "kind": "push"}]}
    )
    minimal = bindings.to_minimal_canonical_dict()
    again = Bindings.model_validate(minimal)
    assert isinstance(again.connectors[0], PushConnector)
