"""The ingestion half of a materialized inverse, as ops.

``set_inverse_emission`` flips ``emit_inverse`` on addressed steps. The ops that
change what a flag can write into keep the flags consistent with the schema:
removing an inverse edge clears the flags that fed it, and a pair cannot be
retracted from under a step that still mirrors it.
"""

from __future__ import annotations

import copy
from typing import Any

import pytest

from graflo.architecture.contract import GraphManifest
from graflo.architecture.contract.ingestion.steps.ref import (
    EdgeStepRef,
    iter_edge_steps,
    with_emit_inverse,
)
from graflo.architecture.evolution import apply_evolution, invert_ops
from graflo.architecture.evolution.codec import op_from_dict
from graflo.architecture.evolution.hashing import manifest_hash
from graflo.architecture.evolution.merge3 import op_slots
from graflo.architecture.evolution.ops import (
    INGESTION_REWRITING_OPS,
    SetInverseEmissionOp,
)

FORWARD = {"source": "person", "target": "institution", "relation": "employed_by"}
INVERSE = {"source": "institution", "target": "person", "relation": "employs"}
PAIR = {"relation": "employed_by", "inverse": "employs"}
STEP = {"edge": {"from": "person", "to": "institution", "relation": "employed_by"}}
VERTICES = [{"vertex": "person"}, {"vertex": "institution"}]


def _manifest(
    pipeline: list[dict[str, Any]],
    *,
    edges: list[dict[str, Any]] | None = None,
    inverses: list[dict[str, str]] | None = None,
) -> GraphManifest:
    return GraphManifest.from_dict(
        {
            "schema": {
                "metadata": {"name": "ops", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "person",
                                "identity": ["pid"],
                                "properties": ["pid"],
                            },
                            {
                                "name": "institution",
                                "identity": ["iid"],
                                "properties": ["iid"],
                            },
                        ]
                    },
                    "edge_config": {
                        "edges": copy.deepcopy(
                            edges if edges is not None else [FORWARD, INVERSE]
                        ),
                        "inverses": inverses if inverses is not None else [PAIR],
                    },
                },
            },
            "ingestion_model": {"resources": [{"name": "rows", "pipeline": pipeline}]},
        }
    )


def _apply(manifest: GraphManifest, *ops: dict[str, Any]) -> GraphManifest:
    return apply_evolution(
        manifest, [op_from_dict(op) for op in ops], bump_version=False
    )


def _flags(manifest: GraphManifest) -> dict[str, bool]:
    assert manifest.ingestion_model is not None
    return {
        str(view.ref): view.emit_inverse
        for resource in manifest.ingestion_model.resources
        for view in iter_edge_steps(list(resource.pipeline))
    }


SET = {"op": "set_inverse_emission", "steps": {"rows": [{"step": 2}]}}


class TestSetInverseEmission:
    def test_it_sets_the_flag_on_the_addressed_step(self) -> None:
        out = _apply(_manifest([*VERTICES, STEP]), SET)
        assert _flags(out) == {"2": True}

    def test_it_clears_it_again_and_leaves_no_trace(self) -> None:
        manifest = _manifest([*VERTICES, STEP])
        out = _apply(manifest, SET, {**SET, "enabled": False})
        assert manifest_hash(out) == manifest_hash(manifest)

    def test_it_is_its_own_inverse_over_the_steps_that_changed(self) -> None:
        already = {"edge": {**STEP["edge"], "emit_inverse": True}}
        manifest = _manifest([*VERTICES, already, STEP])
        op = SetInverseEmissionOp(
            steps={"rows": [EdgeStepRef(step=2), EdgeStepRef(step=3)]}
        )
        (undo,), blockers = invert_ops([op], manifest=manifest)
        assert blockers == []
        assert isinstance(undo, SetInverseEmissionOp)
        assert undo.enabled is False
        assert [ref.step for ref in undo.steps["rows"]] == [3]

    def test_a_link_and_a_nested_step_are_addressable(self) -> None:
        nested = {
            "key": "jobs",
            "apply": [{"edge": {"links": [dict(STEP["edge"]), dict(STEP["edge"])]}}],
        }
        out = _apply(
            _manifest([*VERTICES, nested]),
            {
                "op": "set_inverse_emission",
                "steps": {"rows": [{"at": [2], "step": 0, "link": 1}]},
            },
        )
        assert _flags(out) == {"2/0#link0": False, "2/0#link1": True}

    def test_the_step_keeps_its_authored_spelling(self) -> None:
        flat = {"source": "person", "target": "institution", "relation": "employed_by"}
        assert with_emit_inverse([flat], EdgeStepRef(step=0), True) == [
            {**flat, "emit_inverse": True}
        ]
        assert with_emit_inverse([STEP], EdgeStepRef(step=0), True) == [
            {"edge": {**STEP["edge"], "emit_inverse": True}}
        ]

    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            ({"inverses": [], "edges": [FORWARD]}, "has no declared inverse"),
            ({"edges": [FORWARD]}, "is not declared"),
        ],
        ids=["undeclared-pair", "unrealized-pair"],
    )
    def test_a_flag_that_could_never_write_is_refused(
        self, kwargs: dict[str, Any], match: str
    ) -> None:
        with pytest.raises(ValueError, match=match) as refusal:
            _apply(_manifest([*VERTICES, STEP], **kwargs), SET)
        assert str(refusal.value).startswith("set_inverse_emission: resource 'rows'")

    @pytest.mark.parametrize(
        ("steps", "match"),
        [
            ({"other": [{"step": 2}]}, "unknown resources"),
            ({"rows": [{"step": 0}]}, "no edge step at 0"),
            ({"rows": [{"step": 9}]}, "no edge step at 9"),
        ],
    )
    def test_an_address_that_names_no_edge_step_is_refused(
        self, steps: dict[str, Any], match: str
    ) -> None:
        with pytest.raises(ValueError, match=match):
            _apply(
                _manifest([*VERTICES, STEP]),
                {"op": "set_inverse_emission", "steps": steps},
            )

    def test_the_same_step_twice_is_refused(self) -> None:
        with pytest.raises(ValueError, match="must be unique"):
            SetInverseEmissionOp(
                steps={"rows": [EdgeStepRef(step=2), EdgeStepRef(step=2)]}
            )

    def test_it_reaches_ingestion_and_claims_one_slot_per_resource(self) -> None:
        assert "set_inverse_emission" in INGESTION_REWRITING_OPS
        assert op_slots(op_from_dict(SET)) == {("resource", "rows")}


class TestTheFlagsFollowTheSchema:
    def test_removing_the_inverse_edge_clears_the_flags_that_fed_it(self) -> None:
        flagged = {"edge": {**STEP["edge"], "emit_inverse": True}}
        out = _apply(
            _manifest([*VERTICES, flagged]),
            {"op": "remove_edges", "edges": [INVERSE]},
        )
        assert _flags(out) == {"2": False}
        out.finish_init()

    def test_a_flag_that_still_has_something_to_write_into_is_kept(self) -> None:
        """A routed step mirrors several pairs; removing one inverse leaves the rest."""
        funds = {"source": "person", "target": "institution", "relation": "funds"}
        funded = {"source": "institution", "target": "person", "relation": "funded_by"}
        step = {
            "edge": {
                "from": "person",
                "to": "institution",
                "relation_field": "rel",
                "emit_inverse": True,
            }
        }
        out = _apply(
            _manifest(
                [*VERTICES, step],
                edges=[FORWARD, INVERSE, funds, funded],
                inverses=[PAIR, {"relation": "funds", "inverse": "funded_by"}],
            ),
            {"op": "remove_edges", "edges": [funded]},
        )
        assert _flags(out) == {"2": True}

    def test_a_pair_cannot_be_retracted_from_under_a_step_that_mirrors_it(
        self,
    ) -> None:
        flagged = {"edge": {**STEP["edge"], "emit_inverse": True}}
        with pytest.raises(ValueError, match="still mirror these relations") as refusal:
            _apply(
                _manifest([*VERTICES, flagged]),
                {"op": "retract_edge_inverses", "relations": ["employs"]},
            )
        assert "rows:2" in str(refusal.value)

    def test_clearing_the_flag_first_lets_the_retraction_through(self) -> None:
        flagged = {"edge": {**STEP["edge"], "emit_inverse": True}}
        out = _apply(
            _manifest([*VERTICES, flagged]),
            {**SET, "enabled": False},
            {"op": "retract_edge_inverses", "relations": ["employs"]},
        )
        assert out.graph_schema is not None
        assert out.graph_schema.core_schema.edge_config.inverses == []


class TestProjectionKeepsAPairTogether:
    def _kept(self, manifest: GraphManifest, **options: Any) -> list[tuple]:
        out = _apply(
            manifest,
            {"op": "project_manifest", "keep_edges": [FORWARD], **options},
        )
        assert out.graph_schema is not None
        return [e.edge_id for e in out.graph_schema.core_schema.edge_config.edges]

    def test_by_default_only_the_listed_edges_survive(self) -> None:
        assert self._kept(_manifest([*VERTICES, STEP])) == [
            ("person", "institution", "employed_by")
        ]

    def test_keep_inverse_edges_keeps_the_declared_mirror(self) -> None:
        assert self._kept(_manifest([*VERTICES, STEP]), keep_inverse_edges=True) == [
            ("person", "institution", "employed_by"),
            ("institution", "person", "employs"),
        ]

    def test_a_projection_that_drops_the_mirror_leaves_a_loadable_manifest(
        self,
    ) -> None:
        flagged = {"edge": {**STEP["edge"], "emit_inverse": True}}
        out = _apply(
            _manifest([*VERTICES, flagged]),
            {"op": "project_manifest", "keep_edges": [FORWARD]},
        )
        assert _flags(out) == {"2": False}
        out.finish_init()
