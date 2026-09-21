from __future__ import annotations

import copy
from typing import Any

import pytest

from graflo.architecture.contract import GraphManifest
from graflo.architecture.evolution import (
    AddInverseEdgesOp,
    DeclareEdgeInversesOp,
    apply_evolution,
)

FORWARD = {"source": "person", "target": "institution", "relation": "employed_by"}


def _manifest(
    edges: list[dict[str, Any]],
    pipeline: list[dict[str, Any]],
    *,
    db_profile: dict[str, Any] | None = None,
    inverses: list[dict[str, str]] | None = None,
) -> GraphManifest:
    edge_config: dict[str, Any] = {"edges": copy.deepcopy(edges)}
    if inverses is not None:
        edge_config["inverses"] = inverses
    schema: dict[str, Any] = {
        "metadata": {"name": "demo", "version": "1.0.0"},
        "graph": {
            "vertex_config": {
                "vertices": [
                    {"name": "person", "identity": ["id"], "properties": ["id"]},
                    {"name": "institution", "identity": ["id"], "properties": ["id"]},
                ]
            },
            "edge_config": edge_config,
        },
    }
    if db_profile is not None:
        schema["db_profile"] = db_profile
    return GraphManifest.from_dict(
        {
            "schema": schema,
            "ingestion_model": {
                "resources": [{"name": "relations", "pipeline": pipeline}]
            },
        }
    )


def _dynamic_pipeline(relation_map: dict[str, str]) -> list[dict[str, Any]]:
    return [
        {"vertex_router": {"type_field": "source_type", "role": "source"}},
        {"vertex_router": {"type_field": "target_type", "role": "target"}},
        {
            "edge": {
                "source_role": "source",
                "target_role": "target",
                "relation_field": "relation_type",
                "relation_map": relation_map,
            }
        },
    ]


STATIC_STEP = {
    "edge": {"from": "person", "to": "institution", "relation": "employed_by"}
}


def _apply(manifest: GraphManifest, **inverses: str) -> GraphManifest:
    """Declare ``inverses``, then realize them as explicit inverse edges."""
    return apply_evolution(
        manifest,
        [
            DeclareEdgeInversesOp(inverses=inverses),
            AddInverseEdgesOp(relations=list(inverses)),
        ],
        bump_version=False,
    )


def _edge_ids(manifest: GraphManifest) -> list[tuple]:
    assert manifest.graph_schema is not None
    return [e.edge_id for e in manifest.graph_schema.core_schema.edge_config.edges]


def _edge_steps(manifest: GraphManifest) -> list[dict[str, Any]]:
    assert manifest.ingestion_model is not None
    return [
        s["edge"] for s in manifest.ingestion_model.resources[0].pipeline if "edge" in s
    ]


def _cast(manifest: GraphManifest, row: dict[str, Any]) -> dict[Any, list]:
    manifest.finish_init()
    entities = manifest.require_ingestion_model().fetch_resource("relations")(row)
    return {key: docs for key, docs in entities.items() if isinstance(key, tuple)}


ROW = {"source_type": "person", "target_type": "institution", "id": "x"}


def test_inverse_edges_dynamic_edge_actor() -> None:
    """The forward step is flagged to mirror; no second step is generated."""
    manifest = _manifest([FORWARD], _dynamic_pipeline({"EMPLOYED_BY": "employed_by"}))
    out = _apply(manifest, employed_by="employs")

    (step,) = _edge_steps(out)
    assert step["emit_inverse"] is True
    assert step["source_role"] == "source"
    assert step["relation_map"] == {"EMPLOYED_BY": "employed_by"}
    assert ("institution", "person", "employs") in _edge_ids(out)

    edges = _cast(out, {**ROW, "relation_type": "EMPLOYED_BY"})
    assert ("person", "institution", "employed_by") in edges
    assert ("institution", "person", "employs") in edges


def test_an_undeclared_relation_is_refused() -> None:
    """The op realizes the declaration; it never declares."""
    manifest = _manifest([FORWARD], [STATIC_STEP])
    with pytest.raises(ValueError, match="no declared inverse for relations"):
        apply_evolution(
            manifest, [AddInverseEdgesOp(relations=["employed_by"])], bump_version=False
        )
    with pytest.raises(ValueError, match="no inverse pairs declared"):
        apply_evolution(manifest, [AddInverseEdgesOp()], bump_version=False)


def test_omitted_inverses_realize_the_declared_table() -> None:
    manifest = _manifest(
        [FORWARD],
        [STATIC_STEP],
        inverses=[{"relation": "employed_by", "inverse": "employs"}],
    )
    out = apply_evolution(manifest, [AddInverseEdgesOp()], bump_version=False)
    assert ("institution", "person", "employs") in _edge_ids(out)
    assert out.graph_schema is not None
    assert [
        (p.relation, p.inverse)
        for p in out.graph_schema.core_schema.edge_config.inverses
    ] == [("employed_by", "employs")]


def test_the_whole_table_is_realized_both_ways_and_skips_native_edges() -> None:
    """Either side of a pair may be the one authored; a native edge is realized."""
    manifest = _manifest(
        [
            FORWARD,
            {"source": "person", "target": "person", "relation": "mentored_by"},
            {"source": "institution", "target": "institution", "relation": "owns"},
        ],
        [STATIC_STEP],
        db_profile={"db_flavor": "tigergraph", "native_inverses": ["owns"]},
        inverses=[
            {"relation": "employed_by", "inverse": "employs"},
            {"relation": "mentors", "inverse": "mentored_by"},
            {"relation": "owns", "inverse": "owned_by"},
        ],
    )
    out = apply_evolution(manifest, [AddInverseEdgesOp()], bump_version=False)
    ids = _edge_ids(out)
    assert ("institution", "person", "employs") in ids
    assert ("person", "person", "mentors") in ids
    assert ("institution", "institution", "owned_by") not in ids


def test_undirected_edges_are_refused_statically_and_dynamically() -> None:
    undirected = {**FORWARD, "directed": False}
    for pipeline in ([STATIC_STEP], _dynamic_pipeline({"EMPLOYED_BY": "employed_by"})):
        with pytest.raises(ValueError, match="undirected edge has no inverse pair"):
            _apply(_manifest([undirected], pipeline), employed_by="employs")


def test_a_native_inverse_refuses_an_explicit_inverse_edge() -> None:
    """Both would store one fact; neither the schema nor ingestion may get the copy."""
    for pipeline in ([STATIC_STEP], _dynamic_pipeline({"EMPLOYED_BY": "employed_by"})):
        manifest = _manifest(
            [FORWARD],
            pipeline,
            db_profile={"db_flavor": "tigergraph", "native_inverses": ["employed_by"]},
            inverses=[{"relation": "employed_by", "inverse": "employs"}],
        )
        with pytest.raises(ValueError, match="maintained natively"):
            _apply(manifest, employed_by="employs")


def test_an_explicit_inverse_step_of_another_spelling_is_not_duplicated() -> None:
    manifest = _manifest(
        [FORWARD],
        [
            STATIC_STEP,
            {
                "edge": {
                    "source": "institution",
                    "target": "person",
                    "relation": "employs",
                }
            },
        ],
    )
    steps = _edge_steps(_apply(manifest, employed_by="employs"))
    assert len(steps) == 2


def test_an_already_declared_inverse_edge_keeps_its_own_ingestion() -> None:
    """The user modeled it explicitly; the op must not add a second writer."""
    manifest = _manifest(
        [FORWARD, {"source": "institution", "target": "person", "relation": "employs"}],
        [STATIC_STEP],
    )
    out = _apply(manifest, employed_by="employs")
    assert len(_edge_steps(out)) == 1


def test_a_relation_without_a_realized_inverse_is_written_one_way() -> None:
    """The mirror is taken per resolved relation, so an unpaired one is left alone."""
    manifest = _manifest(
        [FORWARD, {"source": "person", "target": "institution", "relation": "funds"}],
        _dynamic_pipeline({"EMPLOYED_BY": "employed_by", "FUNDS": "funds"}),
    )
    out = _apply(manifest, employed_by="employs")

    edges = _cast(out, {**ROW, "relation_type": "FUNDS"})
    assert edges[("person", "institution", "funds")]
    # Nothing is mirrored, and edge inference must not fill the gap with the
    # inverse of a different relation.
    assert [
        key for key, docs in edges.items() if key[0] == "institution" and docs
    ] == []


@pytest.mark.parametrize(
    "step",
    [
        {"from": "person", "to": "institution", "relation_field": "relation_type"},
        {"links": [{"from": "person", "to": "institution", "relation_field": "r"}]},
    ],
    ids=["relation-field-with-fixed-endpoints", "link-reading-a-relation-field"],
)
def test_steps_that_name_their_relation_in_the_data_are_mirrored_too(
    step: dict[str, Any],
) -> None:
    """A generated step could not be restricted to the mapped relations; a flag need not be."""
    manifest = _manifest([FORWARD], [{"edge": step}])
    (flagged,) = _edge_steps(_apply(manifest, employed_by="employs"))
    holder = flagged["links"][0] if "links" in flagged else flagged
    assert holder["emit_inverse"] is True


def test_undoing_the_op_restores_the_pipeline_exactly() -> None:
    from graflo.architecture.evolution import invert_ops
    from graflo.architecture.evolution.hashing import manifest_hash

    manifest = _manifest(
        [FORWARD],
        _dynamic_pipeline({"EMPLOYED_BY": "employed_by"}),
        inverses=[{"relation": "employed_by", "inverse": "employs"}],
    )
    ops = [AddInverseEdgesOp(relations=["employed_by"])]
    applied = apply_evolution(manifest, ops, bump_version=False)
    undo, blockers = invert_ops(ops, manifest=manifest)
    assert blockers == []

    restored = apply_evolution(applied, undo, bump_version=False)
    assert manifest_hash(restored) == manifest_hash(manifest)
    assert "emit_inverse" not in _edge_steps(restored)[0]


def test_the_inverse_spec_does_not_share_the_forward_physical_type() -> None:
    manifest = _manifest(
        [FORWARD],
        [STATIC_STEP],
        db_profile={"edge_specs": [{**FORWARD, "relation_name": "EMPLOYED_BY_T"}]},
    )
    out = _apply(manifest, employed_by="employs")
    assert out.graph_schema is not None
    names = {
        spec.edge_id: spec.relation_name
        for spec in out.graph_schema.db_profile.edge_specs
    }
    assert names[("person", "institution", "employed_by")] == "EMPLOYED_BY_T"
    assert names[("institution", "person", "employs")] is None


def test_the_inverse_edge_is_not_grounded_to_the_forward_semantics() -> None:
    manifest = _manifest(
        [
            {
                **FORWARD,
                "description": "person works at institution",
                "semantics": {"iri": "http://schema.org/worksFor"},
            }
        ],
        [STATIC_STEP],
    )
    out = _apply(manifest, employed_by="employs")
    assert out.graph_schema is not None
    inverse = out.graph_schema.core_schema.edge_config.edge_for(
        ("institution", "person", "employs")
    )
    assert inverse.semantics is None
    assert inverse.description is None


def test_an_unrelated_template_edge_gets_no_twin() -> None:
    manifest = _manifest(
        [FORWARD, {"source": "institution", "target": "person"}], [STATIC_STEP]
    )
    out = _apply(manifest, employed_by="employs")
    assert ("person", "institution", None) not in _edge_ids(out)


@pytest.mark.parametrize(
    "pipeline",
    [[STATIC_STEP], _dynamic_pipeline({"EMPLOYED_BY": "employed_by"})],
    ids=["static", "dynamic"],
)
def test_realizing_twice_changes_nothing(pipeline: list[dict[str, Any]]) -> None:
    from graflo.architecture.evolution.hashing import manifest_hash

    once = _apply(_manifest([FORWARD], pipeline), employed_by="employs")
    twice = apply_evolution(once, [AddInverseEdgesOp()], bump_version=False)
    assert manifest_hash(twice) == manifest_hash(once)


class TestTheDifferReproducesARealization:
    """A revision made with ``add_inverse_edges`` can be re-derived from its two manifests."""

    @staticmethod
    def _declared(pipeline: list[dict[str, Any]]) -> GraphManifest:
        return _manifest(
            [FORWARD],
            pipeline,
            inverses=[{"relation": "employed_by", "inverse": "employs"}],
        )

    @pytest.mark.parametrize(
        "pipeline",
        [
            [STATIC_STEP],
            _dynamic_pipeline({"EMPLOYED_BY": "employed_by"}),
            [
                {
                    "key": "jobs",
                    "pipeline": [
                        {"edge": {"links": [dict(STATIC_STEP["edge"])]}},
                    ],
                }
            ],
        ],
        ids=["fixed", "routed-and-mapped", "link-in-a-nested-pipeline"],
    )
    def test_realizing_and_withdrawing_both_replay(
        self, pipeline: list[dict[str, Any]]
    ) -> None:
        from graflo.architecture.evolution.autogenerate import diff_manifests_verified
        from graflo.architecture.evolution.hashing import manifest_hash

        base = self._declared(pipeline)
        realized = apply_evolution(base, [AddInverseEdgesOp()], bump_version=False)

        forward, warnings = diff_manifests_verified(base, realized)
        assert warnings == []
        assert [op.op for op in forward] == ["add_edges", "set_inverse_emission"]
        assert manifest_hash(
            apply_evolution(base, forward, bump_version=False)
        ) == manifest_hash(realized)

        backward, warnings = diff_manifests_verified(realized, base)
        assert warnings == []
        assert backward[0].op == "set_inverse_emission"
        assert backward[0].enabled is False  # type: ignore[union-attr]
        assert manifest_hash(
            apply_evolution(realized, backward, bump_version=False)
        ) == manifest_hash(base)

    def test_a_flag_flip_next_to_an_inexpressible_edit_is_not_guessed_at(self) -> None:
        """Positions only correspond when nothing else moved; otherwise the differ says so."""
        from graflo.architecture.evolution.autogenerate import diff_manifests

        base = self._declared([STATIC_STEP])
        realized = apply_evolution(base, [AddInverseEdgesOp()], bump_version=False)
        assert realized.ingestion_model is not None
        payload = realized.to_dict(skip_defaults=False)
        resource = payload["ingestion_model"]["resources"][0]
        resource["pipeline"] = [{"vertex": "person"}, *resource["pipeline"]]
        edited = GraphManifest.from_dict(payload)

        ops, warnings = diff_manifests(base, edited)
        assert "set_inverse_emission" not in [op.op for op in ops]
        assert any("differs" in warning for warning in warnings)
