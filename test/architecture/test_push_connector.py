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


def test_the_hash_follows_the_name_and_the_envelope() -> None:
    plain = PushConnector(kind="push", name="a")
    renamed = PushConnector(kind="push", name="b")
    enveloped = PushConnector(kind="push", name="a", records_path="data.items")
    annotated = PushConnector(
        kind="push", name="a", row_annotations={"source": "schemacast"}
    )
    assert len({plain.hash, renamed.hash, enveloped.hash, annotated.hash}) == 4
    assert plain.hash == PushConnector(kind="push", name="a").hash


def test_two_push_connectors_load_in_one_manifest() -> None:
    bindings = Bindings.model_validate(
        {
            "connectors": [
                {"name": "slack_source_records", "kind": "push"},
                {"name": "jira_source_records", "kind": "push"},
            ],
            "resource_connector": [
                {"resource": "slack_records", "connector": "slack_source_records"},
                {"resource": "jira_records", "connector": "jira_source_records"},
            ],
        }
    )
    (slack,) = bindings.get_connectors_for_resource("slack_records")
    (jira,) = bindings.get_connectors_for_resource("jira_records")
    assert slack.name == "slack_source_records"
    assert jira.name == "jira_source_records"


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
