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


def test_inverse_edges_dynamic_edge_actor() -> None:
    manifest = _manifest([FORWARD], _dynamic_pipeline({"EMPLOYED_BY": "employed_by"}))
    out = _apply(manifest, employed_by="employs")

    edge_steps = _edge_steps(out)
    assert len(edge_steps) == 2
    inverse = edge_steps[1]
    assert inverse["source_role"] == "target"
    assert inverse["target_role"] == "source"
    assert inverse["relation_field"] == "relation_type"
    assert inverse["relation_map"] == {"EMPLOYED_BY": "employs"}
    assert inverse["relation_map_only"] is True
    assert ("institution", "person", "employs") in _edge_ids(out)


def test_an_undeclared_relation_is_refused() -> None:
    """The op realizes the declaration; it never declares."""
    manifest = _manifest([FORWARD], [STATIC_STEP])
    with pytest.raises(ValueError, match="no declared inverse for relations"):
        apply_evolution(
            manifest, [AddInverseEdgesOp(relations=["employed_by"])], bump_version=False
        )
    with pytest.raises(ValueError, match="no inverses declared"):
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
        db_profile={
            "db_flavor": "tigergraph",
            "edge_specs": [
                {
                    "source": "institution",
                    "target": "institution",
                    "relation": "owns",
                    "native_inverse": True,
                }
            ],
        },
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
        with pytest.raises(ValueError, match="undirected edge has no inverse"):
            _apply(_manifest([undirected], pipeline), employed_by="employs")


def test_a_native_inverse_refuses_an_explicit_inverse_edge() -> None:
    """Both would store one fact; neither the schema nor ingestion may get the copy."""
    for pipeline in ([STATIC_STEP], _dynamic_pipeline({"EMPLOYED_BY": "employed_by"})):
        manifest = _manifest(
            [FORWARD],
            pipeline,
            db_profile={
                "db_flavor": "tigergraph",
                "edge_specs": [{**FORWARD, "native_inverse": True}],
            },
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


def test_a_partial_relation_map_writes_only_the_mapped_inverse() -> None:
    """Unmapped raw values used to pass through, reversed, under the forward name."""
    manifest = _manifest(
        [FORWARD, {"source": "person", "target": "institution", "relation": "funds"}],
        _dynamic_pipeline({"EMPLOYED_BY": "employed_by", "FUNDS": "funds"}),
    )
    inverse = _edge_steps(_apply(manifest, employed_by="employs"))[1]
    assert inverse["relation_map"] == {"EMPLOYED_BY": "employs"}
    assert inverse["relation_map_only"] is True


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
