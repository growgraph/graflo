"""Tests for :class:`AddResourceTransformsOp` — the ingestion-side fundamental."""

from __future__ import annotations

import asyncio

import pytest

from graflo.architecture.contract.ingestion.transform import ProtoTransform
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import (
    AddResourceTransformsOp,
    apply_evolution,
)
from graflo.architecture.evolution.codec import op_from_dict, op_to_dict
from graflo.hq.document_caster import DocumentCaster
from graflo.hq.ingestion_parameters import IngestionParams


def _manifest(*, with_ingestion: bool = True) -> GraphManifest:
    payload: dict = {
        "schema": {
            "metadata": {"name": "g", "version": "1.0.0"},
            "graph": {
                "vertex_config": {
                    "vertices": [
                        {
                            "name": "party",
                            "properties": ["party_id", "local_key"],
                            "identity": ["party_id"],
                        }
                    ]
                },
                "edge_config": {"edges": []},
            },
        },
    }
    if with_ingestion:
        payload["ingestion_model"] = {
            "resources": [{"name": "crm", "pipeline": [{"vertex": "party"}]}],
            "transforms": [],
        }
    manifest = GraphManifest.from_config(payload)
    manifest.finish_init()
    return manifest


_INLINE_STEP = {
    "transform": {
        "call": {
            "module": "graflo.util.transform",
            "foo": "tagged_key",
            "params": {"tag": "a"},
            "input": ["party_id"],
            "output": ["local_key"],
        }
    }
}


class TestOpModel:
    def test_non_transform_step_rejected_at_construction(self) -> None:
        with pytest.raises(ValueError, match="not a transform step"):
            AddResourceTransformsOp(additions={"crm": [{"vertex": "party"}]})

    def test_empty_step_list_rejected(self) -> None:
        with pytest.raises(ValueError, match="empty step list"):
            AddResourceTransformsOp(additions={"crm": []})

    def test_unnamed_registry_transform_rejected(self) -> None:
        with pytest.raises((ValueError, TypeError)):
            AddResourceTransformsOp(
                additions={"crm": [_INLINE_STEP]},
                transforms=[
                    ProtoTransform(module="graflo.util.transform", foo="tagged_key")
                ],
            )

    def test_codec_round_trip(self) -> None:
        op = AddResourceTransformsOp(additions={"crm": [_INLINE_STEP]})
        assert op_from_dict(op_to_dict(op)) == op


class TestApply:
    def test_appends_step_and_output_reaches_vertex(self) -> None:
        manifest = apply_evolution(
            _manifest(),
            [AddResourceTransformsOp(additions={"crm": [_INLINE_STEP]})],
        )
        im = manifest.require_ingestion_model()
        pipeline = im.resources[0].pipeline
        assert pipeline[-1] == _INLINE_STEP  # appended at the end of the root level

        caster = DocumentCaster(im)
        result = asyncio.run(
            caster.cast_batch([{"party_id": "p1"}], "crm", params=IngestionParams())
        )
        docs = list(result.graph.vertices["party"])
        assert docs and docs[0]["local_key"] == "a:p1"

    def test_registry_use_resolution(self) -> None:
        proto = ProtoTransform(
            name="tag_local",
            module="graflo.util.transform",
            foo="tagged_key",
            params={"tag": "a"},
            input=("party_id",),
            output=("local_key",),
        )
        manifest = apply_evolution(
            _manifest(),
            [
                AddResourceTransformsOp(
                    additions={"crm": [{"transform": {"call": {"use": "tag_local"}}}]},
                    transforms=[proto],
                )
            ],
        )
        im = manifest.require_ingestion_model()
        assert [t.name for t in im.transforms] == ["tag_local"]
        caster = DocumentCaster(im)
        result = asyncio.run(
            caster.cast_batch([{"party_id": "p1"}], "crm", params=IngestionParams())
        )
        assert next(iter(result.graph.vertices["party"]))["local_key"] == "a:p1"

    def test_unresolved_use_raises(self) -> None:
        op = AddResourceTransformsOp(
            additions={"crm": [{"transform": {"call": {"use": "nope"}}}]}
        )
        with pytest.raises(ValueError, match="unknown transform 'nope'"):
            apply_evolution(_manifest(), [op])

    def test_missing_ingestion_model_raises(self) -> None:
        op = AddResourceTransformsOp(additions={"crm": [_INLINE_STEP]})
        with pytest.raises(ValueError, match="requires ingestion_model"):
            apply_evolution(_manifest(with_ingestion=False), [op])

    def test_unknown_resource_raises(self) -> None:
        op = AddResourceTransformsOp(additions={"ghost": [_INLINE_STEP]})
        with pytest.raises(ValueError, match="unknown resources \\['ghost'\\]"):
            apply_evolution(_manifest(), [op])

    def test_registry_collision_identical_dedupes_divergent_raises(self) -> None:
        proto = ProtoTransform(
            name="tag_local",
            module="graflo.util.transform",
            foo="tagged_key",
            params={"tag": "a"},
            input=("party_id",),
            output=("local_key",),
        )
        base = apply_evolution(
            _manifest(),
            [
                AddResourceTransformsOp(
                    additions={"crm": [{"transform": {"call": {"use": "tag_local"}}}]},
                    transforms=[proto],
                )
            ],
        )
        # Identical body: dedupes silently.
        again = apply_evolution(
            base,
            [
                AddResourceTransformsOp(
                    additions={"crm": [{"transform": {"call": {"use": "tag_local"}}}]},
                    transforms=[proto],
                )
            ],
        )
        assert [t.name for t in again.require_ingestion_model().transforms] == [
            "tag_local"
        ]
        # Divergent body: raises.
        divergent = proto.model_copy(update={"params": {"tag": "b"}})
        with pytest.raises(ValueError, match="incompatible transform definitions"):
            apply_evolution(
                base,
                [
                    AddResourceTransformsOp(
                        additions={
                            "crm": [{"transform": {"call": {"use": "tag_local"}}}]
                        },
                        transforms=[divergent],
                    )
                ],
            )

    def test_bad_module_fails_loudly_at_apply(self) -> None:
        step = {
            "transform": {
                "call": {
                    "module": "graflo.util.no_such_module",
                    "foo": "nope",
                    "input": ["party_id"],
                    "output": ["local_key"],
                }
            }
        }
        op = AddResourceTransformsOp(additions={"crm": [step]})
        with pytest.raises(Exception, match="no_such_module|ModuleNotFound"):
            apply_evolution(_manifest(), [op])
