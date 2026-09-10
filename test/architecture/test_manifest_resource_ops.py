"""`add_resources` / `remove_resources`: the ingestion block's own add and drop.

Without these a change set could only rename or narrow the resources it
started with, and the differ had to report an added resource as inexpressible.
"""

from __future__ import annotations

import pytest

from graflo.architecture.contract import GraphManifest
from graflo.architecture.contract.bindings import FileConnector
from graflo.architecture.evolution import (
    AddResourcesOp,
    RemoveResourcesOp,
    apply_evolution,
)
from graflo.architecture.evolution.codec import op_from_dict
from graflo.architecture.evolution.hashing import manifest_hash
from graflo.architecture.evolution.inverse import invert_op

PARTY = {"name": "party", "properties": ["id"], "identity": ["id"]}
CRM = {"name": "crm", "pipeline": [{"vertex": "party"}]}
ERP = {"name": "erp", "pipeline": [{"vertex": "party"}]}
WIRED_CRM = {
    "connectors": [
        FileConnector(name="c_crm", regex="crm.*", resource_name="crm").to_dict(
            skip_defaults=False
        )
    ],
    "resource_connector": [{"resource": "crm", "connector": "c_crm"}],
}


def _manifest(
    resources: list[dict] | None, *, bindings: dict | None = None
) -> GraphManifest:
    payload: dict = {
        "schema": {
            "metadata": {"name": "resources-demo", "version": "1.0.0"},
            "graph": {
                "vertex_config": {"vertices": [PARTY]},
                "edge_config": {"edges": []},
            },
        }
    }
    if resources is not None:
        payload["ingestion_model"] = {"resources": resources}
    if bindings is not None:
        payload["bindings"] = bindings
    return GraphManifest.model_validate(payload)


def _names(manifest: GraphManifest) -> list[str]:
    assert manifest.ingestion_model is not None
    return [r.name for r in manifest.ingestion_model.resources]


class TestAddResources:
    def test_appends_to_an_existing_block(self) -> None:
        out = apply_evolution(
            _manifest([CRM]), [AddResourcesOp(resources=[ERP])], bump_version=False
        )
        assert _names(out) == ["crm", "erp"]

    def test_creates_the_block_when_absent(self) -> None:
        out = apply_evolution(
            _manifest(None), [AddResourcesOp(resources=[CRM])], bump_version=False
        )
        assert _names(out) == ["crm"]

    def test_rejects_an_existing_name(self) -> None:
        with pytest.raises(ValueError, match="already exist"):
            apply_evolution(
                _manifest([CRM]), [AddResourcesOp(resources=[CRM])], bump_version=False
            )

    def test_rejects_duplicate_names_at_parse_time(self) -> None:
        with pytest.raises(ValueError, match="unique by name"):
            AddResourcesOp(resources=[CRM, CRM])


class TestRemoveResources:
    def test_drops_the_resource_and_its_binding(self) -> None:
        manifest = _manifest([CRM, ERP], bindings=WIRED_CRM)
        out = apply_evolution(
            manifest, [RemoveResourcesOp(names=["crm"])], bump_version=False
        )
        assert _names(out) == ["erp"]
        assert out.bindings is not None
        assert out.bindings.resource_connector == []

    def test_rejects_an_unknown_name(self) -> None:
        with pytest.raises(ValueError, match="unknown resources"):
            apply_evolution(
                _manifest([CRM]),
                [RemoveResourcesOp(names=["ghost"])],
                bump_version=False,
            )


class TestInverses:
    def test_add_then_remove_restores_the_manifest(self) -> None:
        base = _manifest([CRM])
        op = op_from_dict({"op": "add_resources", "resources": [ERP]})
        inverse = invert_op(op, manifest=base)
        assert isinstance(inverse, RemoveResourcesOp) and inverse.names == ["erp"]
        forward = apply_evolution(base, [op], bump_version=False, finish_init=False)
        restored = apply_evolution(
            forward, [inverse], bump_version=False, finish_init=False
        )
        assert manifest_hash(restored) == manifest_hash(base)

    def test_remove_then_add_restores_the_manifest(self) -> None:
        base = _manifest([CRM, ERP])
        op = op_from_dict({"op": "remove_resources", "names": ["erp"]})
        inverse = invert_op(op, manifest=base)
        assert inverse is not None
        forward = apply_evolution(base, [op], bump_version=False, finish_init=False)
        restored = apply_evolution(
            forward, [inverse], bump_version=False, finish_init=False
        )
        assert manifest_hash(restored) == manifest_hash(base)

    def test_removing_a_wired_resource_has_no_inverse(self) -> None:
        """The pruned `resource_connector` entry is not restorable by any op."""
        base = _manifest([CRM], bindings=WIRED_CRM)
        op = op_from_dict({"op": "remove_resources", "names": ["crm"]})
        assert invert_op(op, manifest=base) is None
