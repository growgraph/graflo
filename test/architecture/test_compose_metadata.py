"""Metadata folding across :func:`compose_manifests`.

A composed manifest contains both sides' types, so describing it with only the
left side's prose, anchors and naming convention is wrong in the same way
carrying only the left side's vertices would be. These tests pin the fold, and
pin the two fields that deliberately do *not* fold.
"""

from __future__ import annotations

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.contract.provenance import ManifestMetadata
from graflo.architecture.evolution import ComposeManifestsOp, compose_manifests
from graflo.architecture.schema.core import CoreSchema
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import EdgeConfig
from graflo.architecture.schema.metadata import GraphMetadata
from graflo.architecture.schema.naming import NameCase, NamingConvention
from graflo.architecture.schema.provenance import Provenance
from graflo.architecture.schema.semantics import Semantics
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig
from graflo.migrate.io import manifest_hash


def _manifest(
    *,
    vertex: str,
    schema_metadata: GraphMetadata,
    manifest_metadata: ManifestMetadata | None = None,
) -> GraphManifest:
    schema = Schema(
        metadata=schema_metadata,
        core_schema=CoreSchema(
            vertex_config=VertexConfig(
                vertices=[
                    Vertex(name=vertex, properties=[Field(name="id")], identity=["id"])
                ],
                force_types={},
            ),
            edge_config=EdgeConfig(edges=[]),
        ),
    )
    m = GraphManifest(
        graph_schema=schema,
        metadata=manifest_metadata,
    )
    m.finish_init()
    return m


def _compose(left: GraphManifest, right: GraphManifest) -> GraphManifest:
    return compose_manifests(
        left, right, ComposeManifestsOp(), bump_version=False, finish_init=False
    )


def test_manifest_metadata_is_excluded_from_the_content_hash() -> None:
    """Naming a manifest cannot move its content address."""
    unnamed = _manifest(vertex="A", schema_metadata=GraphMetadata(name="s"))
    named = _manifest(
        vertex="A",
        schema_metadata=GraphMetadata(name="s"),
        manifest_metadata=ManifestMetadata(
            name="estate", description="the whole estate"
        ),
    )
    assert manifest_hash(unnamed) == manifest_hash(named)


def test_a_bindings_only_manifest_can_be_named() -> None:
    """The case the field exists for: no schema to borrow a name from."""
    m = GraphManifest.from_config(
        {
            "bindings": {"connectors": [], "resource_connector": []},
            "metadata": {"name": "helix-discovery", "description": "estate REST API"},
        }
    )
    assert m.graph_schema is None
    assert m.metadata is not None
    assert m.metadata.name == "helix-discovery"
    assert m.metadata.description == "estate REST API"


def test_compose_folds_manifest_name_and_description() -> None:
    left = _manifest(
        vertex="A",
        schema_metadata=GraphMetadata(name="l"),
        manifest_metadata=ManifestMetadata(name="cmdb", description="the CMDB"),
    )
    right = _manifest(
        vertex="B",
        schema_metadata=GraphMetadata(name="r"),
        manifest_metadata=ManifestMetadata(name="discovery", description="the scanner"),
    )
    composed = _compose(left, right)
    assert composed.metadata is not None
    assert composed.metadata.name == "cmdb+discovery"
    assert composed.metadata.description == "the CMDB\n\nthe scanner"


def test_compose_keeps_a_one_sided_manifest_name() -> None:
    left = _manifest(vertex="A", schema_metadata=GraphMetadata(name="l"))
    right = _manifest(
        vertex="B",
        schema_metadata=GraphMetadata(name="r"),
        manifest_metadata=ManifestMetadata(name="discovery"),
    )
    composed = _compose(left, right)
    assert composed.metadata is not None
    assert composed.metadata.name == "discovery"


def test_compose_yields_no_manifest_metadata_when_neither_side_has_any() -> None:
    left = _manifest(vertex="A", schema_metadata=GraphMetadata(name="l"))
    right = _manifest(vertex="B", schema_metadata=GraphMetadata(name="r"))
    assert _compose(left, right).metadata is None


def test_compose_keeps_the_right_schema_description() -> None:
    """The regression: the right side's prose used to be dropped on the floor."""
    left = _manifest(
        vertex="A",
        schema_metadata=GraphMetadata(name="l", description="left says this"),
    )
    right = _manifest(
        vertex="B",
        schema_metadata=GraphMetadata(name="r", description="right says that"),
    )
    meta = _compose(left, right).require_schema().metadata
    assert meta.description == "left says this\n\nright says that"


def test_compose_unions_schema_semantics() -> None:
    left = _manifest(
        vertex="A",
        schema_metadata=GraphMetadata(
            name="l",
            semantics=Semantics(
                iri="https://example.org/Estate",
                exact_match=["https://example.org/E"],
                synonyms=["estate"],
            ),
        ),
    )
    right = _manifest(
        vertex="B",
        schema_metadata=GraphMetadata(
            name="r",
            semantics=Semantics(
                iri="https://example.org/Estate",
                exact_match=["https://example.org/E2"],
                synonyms=["estate", "fleet"],
            ),
        ),
    )
    semantics = _compose(left, right).require_schema().metadata.semantics
    assert semantics is not None
    assert semantics.iri == "https://example.org/Estate"
    assert semantics.exact_match == ["https://example.org/E", "https://example.org/E2"]
    assert semantics.synonyms == ["estate", "fleet"]


def test_disagreeing_semantic_iris_clear_rather_than_electing_the_left() -> None:
    left = _manifest(
        vertex="A",
        schema_metadata=GraphMetadata(
            name="l", semantics=Semantics(iri="https://example.org/Estate")
        ),
    )
    right = _manifest(
        vertex="B",
        schema_metadata=GraphMetadata(
            name="r", semantics=Semantics(iri="https://example.org/Fleet")
        ),
    )
    semantics = _compose(left, right).require_schema().metadata.semantics
    assert semantics is not None
    assert semantics.iri is None


def test_a_one_sided_semantics_block_survives_the_fold() -> None:
    left = _manifest(vertex="A", schema_metadata=GraphMetadata(name="l"))
    right = _manifest(
        vertex="B",
        schema_metadata=GraphMetadata(
            name="r", semantics=Semantics(iri="https://example.org/Fleet")
        ),
    )
    semantics = _compose(left, right).require_schema().metadata.semantics
    assert semantics is not None
    assert semantics.iri == "https://example.org/Fleet"


def test_an_agreed_naming_convention_survives_and_a_disagreement_does_not() -> None:
    pascal = NamingConvention(vertex_case=NameCase.PASCAL)
    snake = NamingConvention(vertex_case=NameCase.SNAKE)

    agreed = _compose(
        _manifest(vertex="A", schema_metadata=GraphMetadata(name="l", naming=pascal)),
        _manifest(vertex="B", schema_metadata=GraphMetadata(name="r", naming=pascal)),
    )
    assert agreed.require_schema().metadata.naming == pascal

    mixed = _compose(
        _manifest(vertex="A", schema_metadata=GraphMetadata(name="l", naming=pascal)),
        _manifest(vertex="B", schema_metadata=GraphMetadata(name="r", naming=snake)),
    )
    assert mixed.require_schema().metadata.naming is None


def test_compose_does_not_inherit_the_left_provenance() -> None:
    """The composed artifact has a new content address; it may not claim the left's."""
    left = _manifest(
        vertex="A",
        schema_metadata=GraphMetadata(
            name="l", provenance=Provenance(content_hash="deadbeef", canon="1")
        ),
        manifest_metadata=ManifestMetadata(
            name="cmdb",
            provenance=Provenance(content_hash="deadbeef", canon="1"),
        ),
    )
    right = _manifest(vertex="B", schema_metadata=GraphMetadata(name="r"))
    composed = _compose(left, right)
    assert composed.require_schema().metadata.provenance is None
    assert composed.metadata is not None
    assert composed.metadata.provenance is None
