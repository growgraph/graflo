"""``when`` on a transform step: a guard that skips the step, writing nothing."""

from __future__ import annotations

import asyncio

import pytest

from graflo.architecture.contract.ingestion.steps import (
    TransformActorConfig,
    TransformGuardConfig,
)
from graflo.architecture.contract.manifest import GraphManifest
from graflo.hq.document_caster import DocumentCaster
from graflo.hq.ingestion_parameters import IngestionParams


def _upper_step(when: dict | None, *, input_field: str, output: str) -> dict:
    step: dict = {
        "transform": {
            "call": {
                "module": "graflo.util.transform",
                "foo": "affix_gated_key",
                "input": [input_field],
                "output": [output],
                "params": {"prefix": ""},
            }
        }
    }
    if when is not None:
        step["transform"]["when"] = when
    return step


def _routed_manifest(steps: list[dict]) -> GraphManifest:
    """One router over ``kind`` with two classes, plus *steps* at its level."""
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "g", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {"name": "Firm", "properties": ["firm_id", "key"]},
                            {"name": "Shop", "properties": ["shop_id", "key"]},
                        ]
                    },
                    "edge_config": {"edges": []},
                },
            },
            "ingestion_model": {
                "resources": [
                    {
                        "name": "r",
                        "pipeline": [
                            {
                                "vertex_router": {
                                    "type_field": "kind",
                                    "type_map": {"firm": "Firm", "shop": "Shop"},
                                }
                            },
                            *steps,
                        ],
                    }
                ],
                "transforms": [],
            },
        }
    )
    manifest.finish_init()
    return manifest


def _cast(manifest: GraphManifest, rows: list[dict]) -> dict[str, list[dict]]:
    caster = DocumentCaster(manifest.require_ingestion_model())
    result = asyncio.run(caster.cast_batch(rows, "r", params=IngestionParams()))
    return dict(result.graph.vertices)


class TestGuardModel:
    def test_yaml_spelling_and_python_spelling_agree(self) -> None:
        from_yaml = TransformGuardConfig.model_validate(
            {"field": "kind", "in": ["firm"]}
        )
        from_python = TransformGuardConfig(field="kind", values=["firm"])

        assert from_yaml == from_python
        assert from_yaml.to_dict() == {"field": "kind", "in": ["firm"]}

    def test_the_guard_needs_at_least_one_value(self) -> None:
        with pytest.raises(ValueError):
            TransformGuardConfig(field="kind", values=[])

    def test_exact_match_not_prefix(self) -> None:
        guard = TransformGuardConfig(field="kind", values=["firm"])

        assert guard.passes({"kind": "firm"})
        assert not guard.passes({"kind": "firm_x"})
        assert not guard.passes({"kind": None})
        assert not guard.passes({})
        assert not guard.passes("not a dict")

    def test_when_rides_through_the_step_normalizer(self) -> None:
        step = TransformActorConfig.model_validate(
            _upper_step({"field": "kind", "in": ["shop"]}, input_field="a", output="b")
        )

        assert step.when is not None
        assert step.when.field == "kind"
        assert step.when.values == ["shop"]
        assert step.to_dict()["when"] == {"field": "kind", "in": ["shop"]}


class TestGuardedStep:
    def test_a_passing_guard_runs_the_step(self) -> None:
        manifest = _routed_manifest(
            [
                _upper_step(
                    {"field": "kind", "in": ["firm"]}, input_field="raw", output="key"
                )
            ]
        )

        out = _cast(manifest, [{"kind": "firm", "firm_id": "f1", "raw": "X"}])

        assert out["Firm"][0]["key"] == "x"

    def test_a_failing_guard_writes_nothing(self) -> None:
        manifest = _routed_manifest(
            [
                _upper_step(
                    {"field": "kind", "in": ["firm"]}, input_field="raw", output="key"
                )
            ]
        )

        out = _cast(manifest, [{"kind": "shop", "shop_id": "s1", "raw": "X"}])

        assert "key" not in out["Shop"][0]

    def test_a_missing_gate_field_fails_the_guard(self) -> None:
        manifest = _routed_manifest(
            [
                _upper_step(
                    {"field": "gate", "in": ["yes"]}, input_field="raw", output="key"
                )
            ]
        )

        out = _cast(manifest, [{"kind": "firm", "firm_id": "f1", "raw": "X"}])

        assert "key" not in out["Firm"][0]

    def test_two_guarded_writers_of_one_field_do_not_clobber(self) -> None:
        """The property a per-class derivation behind a router relies on.

        Both rows carry both columns. Without guards the second step would
        overwrite the first's value with ``None`` for the firm row; with them,
        each row runs exactly the step meant for its class.
        """
        manifest = _routed_manifest(
            [
                _upper_step(
                    {"field": "kind", "in": ["firm"]}, input_field="a", output="key"
                ),
                _upper_step(
                    {"field": "kind", "in": ["shop"]}, input_field="b", output="key"
                ),
            ]
        )

        out = _cast(
            manifest,
            [
                {"kind": "firm", "firm_id": "f1", "a": "FromA", "b": ""},
                {"kind": "shop", "shop_id": "s1", "a": "", "b": "FromB"},
            ],
        )

        assert out["Firm"][0]["key"] == "froma"
        assert out["Shop"][0]["key"] == "fromb"

    def test_unguarded_writers_do_clobber(self) -> None:
        """The contrast that motivates the guard."""
        manifest = _routed_manifest(
            [
                _upper_step(None, input_field="a", output="key"),
                _upper_step(None, input_field="b", output="key"),
            ]
        )

        out = _cast(
            manifest, [{"kind": "firm", "firm_id": "f1", "a": "FromA", "b": ""}]
        )

        assert out["Firm"][0].get("key") is None
