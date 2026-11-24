"""``normalize`` / ``validate`` / ``finish_init`` on the contract models.

``normalize`` is the only mutator and is idempotent; ``validate`` is a pure
check of normal form; ``finish_init`` is the two followed by the runtime build.
Construction already leaves a manifest in normal form.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import yaml
from hypothesis import given

from graflo.architecture.contract.ingestion.resource import ResourceConfig
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution.apply import apply_evolution
from graflo.architecture.evolution.hashing import manifest_hash
from graflo.architecture.refusal import OpRefusal
from graflo.architecture.schema.database_features import EdgePhysicalSpec
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.vertex import Vertex, VertexConfig
from test.architecture.test_evolution_laws import (
    LAWS,
    manifest_and_sequence,
    manifests,
)

EXAMPLES = Path(__file__).resolve().parents[2] / "examples"


def _example_paths() -> list[Path]:
    paths = []
    for path in sorted(EXAMPLES.glob("**/*.yaml")):
        data = yaml.safe_load(path.read_text())
        if isinstance(data, dict) and "schema" in data:
            paths.append(path)
    return paths


EXAMPLE_PATHS = _example_paths()


def _load(path: Path) -> GraphManifest:
    return GraphManifest.from_dict(yaml.safe_load(path.read_text()))


def _state(manifest: GraphManifest) -> tuple[str, dict[str, Any]]:
    return manifest_hash(manifest), manifest.to_dict()


def _check_normal_form(manifest: GraphManifest) -> None:
    loaded = _state(manifest)
    manifest.normalize()
    assert _state(manifest) == loaded, "construction did not leave normal form"
    manifest.normalize()
    assert _state(manifest) == loaded, "normalize is not idempotent"
    manifest.validate()
    assert _state(manifest) == loaded, "validate changed the manifest"
    manifest.finish_init()
    assert _state(manifest) == loaded, "finish_init changed a normal manifest"


def test_examples_are_found() -> None:
    assert len(EXAMPLE_PATHS) >= 15


@pytest.mark.parametrize(
    "path", EXAMPLE_PATHS, ids=[str(p.relative_to(EXAMPLES)) for p in EXAMPLE_PATHS]
)
def test_an_example_loads_in_normal_form(path: Path) -> None:
    _check_normal_form(_load(path))


@LAWS
@given(manifests())
def test_a_generated_manifest_loads_in_normal_form(manifest: GraphManifest) -> None:
    _check_normal_form(manifest)


@LAWS
@given(manifest_and_sequence())
def test_applied_ops_leave_normal_form(case) -> None:
    """Replay without ``finish_init`` (a checkout) lands where a rehash does."""
    manifest, ops = case
    try:
        applied = apply_evolution(manifest, ops, bump_version=False, finish_init=False)
    except OpRefusal:
        return
    replayed = _state(applied)
    applied.normalize()
    assert _state(applied) == replayed


# ── finish_init refuses exactly what validate refuses ───────────────────────


def _manifest(**ingestion: Any) -> GraphManifest:
    return GraphManifest.from_dict(
        {
            "schema": {
                "metadata": {"name": "t"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {"name": "a", "properties": ["x", "y"], "identity": ["x"]},
                            {"name": "b", "properties": ["z"], "identity": ["z"]},
                        ]
                    },
                    "edge_config": {"edges": [{"source": "a", "target": "b"}]},
                },
            },
            "ingestion_model": {
                "resources": [
                    {
                        "name": "r",
                        "pipeline": [{"vertex": "a"}, {"vertex": "b"}],
                        **ingestion,
                    }
                ]
            },
        }
    )


def _undeclared_edge_spec(m: GraphManifest) -> None:
    m.require_schema().db_profile.edge_specs.append(
        EdgePhysicalSpec(source="b", target="a")
    )


def _colliding_physical_names(m: GraphManifest) -> None:
    m.require_schema().db_profile.vertex_property_names = {"a": {"x": "y"}}


def _undeclared_edge_endpoint(m: GraphManifest) -> None:
    m.require_schema().core_schema.edge_config.edges.append(
        Edge(source="a", target="nowhere")
    )


def _nothing(m: GraphManifest) -> None:
    pass


CASES: dict[
    str, tuple[Callable[[], GraphManifest], Callable[[GraphManifest], None]]
] = {
    "sound": (_manifest, _nothing),
    "unknown infer selector": (
        lambda: _manifest(infer_edge_only=[{"source": "b", "target": "a"}]),
        _nothing,
    ),
    "edge spec on an undeclared edge": (_manifest, _undeclared_edge_spec),
    "colliding physical names": (_manifest, _colliding_physical_names),
    "edge to an undeclared vertex": (_manifest, _undeclared_edge_endpoint),
    "vertex step to an undeclared vertex": (
        lambda: _manifest(pipeline=[{"vertex": "a"}, {"vertex": "ghost"}]),
        _nothing,
    ),
}


def _refusal(run: Callable[[], None]) -> str | None:
    try:
        run()
    except ValueError as exc:
        return str(exc)
    return None


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("case", list(CASES))
def test_finish_init_refuses_exactly_what_validate_refuses(
    case: str, strict: bool
) -> None:
    build, damage = CASES[case]
    checked, finished = build(), build()
    damage(checked)
    damage(finished)
    before = _state(checked)

    refused = _refusal(lambda: checked.validate(strict_references=strict))
    assert _state(checked) == before, "validate changed the manifest"
    assert refused == _refusal(lambda: finished.finish_init(strict_references=strict))
    if case != "sound" and not (case.startswith("vertex step") and not strict):
        assert refused is not None


def test_dynamic_edge_feedback_is_the_one_runtime_step_that_writes_the_schema() -> None:
    manifest = _manifest(
        pipeline=[{"vertex": "a"}, {"vertex": "b"}, {"edge": {"from": "b", "to": "a"}}]
    )
    loaded = _state(manifest)
    manifest.validate()
    manifest.finish_init()
    assert _state(manifest) == loaded
    manifest.finish_init(dynamic_edge_feedback=True)
    edges = manifest.require_schema().core_schema.edge_config
    assert ("b", "a", None) in edges


# ── extra_weights edges are normal from construction on ─────────────────────


def test_an_extra_weights_edge_declares_its_identity_tokens_at_load() -> None:
    config = ResourceConfig.model_validate(
        {
            "name": "r",
            "pipeline": [{"vertex": "a"}],
            "extra_weights": [
                {
                    "source": "a",
                    "target": "b",
                    "identities": [["source", "target", "day"]],
                }
            ],
        }
    )
    assert config.extra_weights[0].edge.property_names == ["day"]


def test_a_manifest_with_extra_weights_loads_in_normal_form() -> None:
    _check_normal_form(
        _manifest(
            extra_weights=[
                {
                    "source": "a",
                    "target": "b",
                    "identities": [["source", "target", "day"]],
                }
            ]
        )
    )


# ── implicit identity ───────────────────────────────────────────────────────


def _config(**vertex: Any) -> VertexConfig:
    return VertexConfig.model_validate({"vertices": [{"name": "v", **vertex}]})


def test_an_implicit_identity_is_the_declared_properties() -> None:
    vertex = _config(properties=["x"], secondary_identities=[["z"]])["v"]
    assert vertex.identity == ["x"]
    assert vertex.property_names == ["x", "z"]
    assert [entry.fields for entry in vertex.secondary_identities] == [["z"]]


def test_an_implicit_identity_survives_a_round_trip() -> None:
    config = _config(properties=["x", "y"], secondary_identities=[["z"]])
    again = VertexConfig.model_validate(config.to_dict())
    assert again.to_dict() == config.to_dict()
    assert again["v"].identity == ["x", "y"]


def test_a_standalone_vertex_defers_secondary_fields_to_its_identity() -> None:
    vertex = Vertex.model_validate(
        {"name": "v", "properties": ["x"], "secondary_identities": [["z"]]}
    )
    assert vertex.identity == []
    assert vertex.property_names == ["x"]
    explicit = Vertex.model_validate(
        {
            "name": "v",
            "properties": ["x"],
            "identity": ["x"],
            "secondary_identities": [["z"]],
        }
    )
    assert explicit.property_names == ["x", "z"]


@pytest.mark.parametrize(
    "vertex",
    [
        {"properties": ["x"], "secondary_identities": [["x"]]},
        {"properties": ["x", "y"], "secondary_identities": [["y", "x"]]},
        {"assigned": True, "secondary_identities": [["id"]]},
    ],
    ids=["implicit natural", "implicit natural, reordered", "assigned"],
)
def test_a_secondary_identity_equal_to_the_implied_primary_is_refused_everywhere(
    vertex: dict[str, Any],
) -> None:
    with pytest.raises(ValueError, match="duplicates the primary identity"):
        Vertex.model_validate({"name": "v", **vertex})
    with pytest.raises(ValueError, match="duplicates the primary identity"):
        _config(**vertex)


def test_normalize_leaves_an_unfillable_identity_for_validate() -> None:
    config = _config(properties=["x"], identity=["x"])
    vertex = config["v"]
    object.__setattr__(vertex, "identity", [])
    object.__setattr__(config, "identity_from_all_properties", False)
    before = config.to_dict()
    config.normalize()
    assert config.to_dict() == before
    with pytest.raises(ValueError, match="must define identity fields"):
        config.validate()


def test_secondary_identities_need_a_primary_one() -> None:
    vertex = {"secondary_identities": [["z"]]}
    with pytest.raises(ValueError, match="need a primary identity"):
        Vertex.model_validate({"name": "v", **vertex})
    with pytest.raises(ValueError, match="need a primary identity"):
        _config(**vertex)
