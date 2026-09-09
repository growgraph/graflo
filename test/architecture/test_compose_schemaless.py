"""Composing a manifest that carries no ``schema`` block.

A manifest with only an ``ingestion_model`` and/or ``bindings`` is a new source
wired onto an existing type vocabulary. It has always been a valid
:class:`~graflo.architecture.contract.manifest.GraphManifest` -- one block is
enough -- but ``compose_manifests`` used to refuse it outright.

The union is three-way rather than "fill the missing side with an empty
``Schema``", and these tests pin why. An empty ``Schema`` is not neutral:
``DatabaseProfile.db_flavor`` defaults to Arango, ``_merge_db_profiles`` takes
every scalar from the left, and ``_merge_graph_metadata`` takes the left's
version -- so an empty *left* would silently retarget the composed manifest and
drop the right's namespace and schema version.
"""

from __future__ import annotations

import pytest

from graflo.architecture.contract.bindings import FileConnector
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import (
    ComposeManifestsOp,
    VertexEquivalence,
    compose_manifests,
)
from graflo.connections.onto import DBType


def _typed_manifest(
    *,
    name: str,
    vertex: str,
    db_flavor: str = "neo4j",
    target_namespace: str = "estate",
    version: str = "2.3.0",
) -> GraphManifest:
    """A manifest with a schema whose physical profile is *not* the default."""
    m = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": name, "version": version},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": vertex,
                                "properties": [{"name": "id"}, {"name": "label"}],
                                "identity": ["id"],
                            }
                        ]
                    },
                    "edge_config": {"edges": []},
                },
                "db_profile": {
                    "db_flavor": db_flavor,
                    "target_namespace": target_namespace,
                    "vertex_indexes": {vertex: [{"fields": ["label"]}]},
                },
            }
        }
    )
    m.finish_init()
    return m


def _overlay_manifest(*, resource: str = "r_feed", stem: str = "feed") -> GraphManifest:
    """The shape this unblocks: source wiring, no types of its own."""
    m = GraphManifest.from_config(
        {
            "ingestion_model": {
                "resources": [{"name": resource, "apply": []}],
                "transforms": [],
            },
            "bindings": {
                "connectors": [
                    FileConnector(
                        name=f"c_{stem}", regex=f"{stem}.*", resource_name=resource
                    ).to_dict(skip_defaults=False)
                ],
                "resource_connector": [
                    {"resource": resource, "connector": f"c_{stem}"}
                ],
            },
        }
    )
    m.finish_init()
    return m


def test_schemaless_right_overlay_composes_and_keeps_the_left_schema() -> None:
    left = _typed_manifest(name="core", vertex="Asset")
    right = _overlay_manifest()

    out = compose_manifests(left, right, ComposeManifestsOp(), bump_version=False)

    assert out.graph_schema is not None
    assert out.graph_schema.core_schema.vertex_config.vertex_set == {"Asset"}
    assert out.graph_schema.metadata.version == "2.3.0"
    assert out.ingestion_model is not None
    assert {r.name for r in out.ingestion_model.resources} == {"r_feed"}
    assert out.bindings is not None
    assert {c.name for c in out.bindings.connectors} == {"c_feed"}


def test_schemaless_left_keeps_the_right_physical_profile_verbatim() -> None:
    """The regression a fabricated empty ``Schema`` would have introduced.

    Nothing on the left may win by default: not the Arango flavor, not an
    absent namespace, not a ``0.1.0`` version, not an empty index map.
    """
    left = _overlay_manifest()
    right = _typed_manifest(
        name="core",
        vertex="Asset",
        db_flavor="neo4j",
        target_namespace="estate",
        version="2.3.0",
    )

    out = compose_manifests(left, right, ComposeManifestsOp(), bump_version=False)

    assert out.graph_schema is not None
    profile = out.graph_schema.db_profile
    assert profile.db_flavor == DBType.NEO4J
    assert profile.target_namespace == "estate"
    assert list(profile.vertex_indexes) == ["Asset"]
    assert out.graph_schema.metadata.name == "core"
    assert out.graph_schema.metadata.version == "2.3.0"
    assert out.graph_schema.core_schema.vertex_config.vertex_set == {"Asset"}


def test_both_sides_schemaless_compose_to_no_schema() -> None:
    left = _overlay_manifest(resource="r_left", stem="left")
    right = _overlay_manifest(resource="r_right", stem="right")
    out = compose_manifests(
        left,
        right,
        ComposeManifestsOp(name_conflict="prefix_right"),
        bump_version=False,
    )

    assert out.graph_schema is None
    assert out.ingestion_model is not None
    assert {r.name for r in out.ingestion_model.resources} == {"r_left", "r_right"}


def test_bump_version_is_a_no_op_without_a_schema() -> None:
    out = compose_manifests(
        _overlay_manifest(resource="r_a", stem="a"),
        _overlay_manifest(resource="r_b", stem="b"),
        ComposeManifestsOp(name_conflict="prefix_right"),
        bump_version="minor",
    )
    assert out.graph_schema is None


def test_equivalence_naming_a_vertex_on_the_schemaless_side_still_raises() -> None:
    """Relaxing the guard must not turn a broken op into a silent no-op."""
    left = _typed_manifest(name="core", vertex="Asset")
    right = _overlay_manifest()

    with pytest.raises(ValueError, match="right manifest"):
        compose_manifests(
            left,
            right,
            ComposeManifestsOp(
                vertex_equivalences=[
                    VertexEquivalence(left="Asset", right="Device", into="Asset")
                ]
            ),
            bump_version=False,
        )
