"""The three grounding ops, and the widened property add.

Grounding used to be authorable only at the moment a type was written. The
schema models have carried ``semantics`` blocks for a while, but no operation
could attach one to an element that already existed -- so a manifest that
arrived ungrounded (an inferred one, or anything predating the block) could
never be grounded through the op system at all, only rewritten by hand. These
tests pin the ops that close that, and the property-add that can finally carry a
type and a grounding in one replayable step.
"""

from __future__ import annotations

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import (
    AddEdgePropertiesOp,
    AddVertexPropertiesOp,
    EdgeFieldSemanticsTarget,
    FieldSemanticsTarget,
    SetEdgeSemanticsOp,
    SetFieldSemanticsOp,
    SetVertexSemanticsOp,
    apply_evolution,
    invert_ops,
)
from graflo.architecture.schema.semantics import FieldSemantics, Semantics
from graflo.architecture.schema.vertex import Field, FieldType

PROV_ENTITY = "http://www.w3.org/ns/prov#Entity"
SOSA_FOI = "http://www.w3.org/ns/sosa/FeatureOfInterest"
DCT_PART_OF = "http://purl.org/dc/terms/isPartOf"
PROV_GENERATED = "http://www.w3.org/ns/prov#generatedAtTime"


def _manifest() -> GraphManifest:
    """An ungrounded manifest -- the state these ops exist to rescue."""
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "plain", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "Asset",
                                "properties": [{"name": "id"}, {"name": "temp_c"}],
                                "identity": ["id"],
                            },
                            {
                                "name": "Site",
                                "properties": [{"name": "id"}],
                                "identity": ["id"],
                            },
                        ]
                    },
                    "edge_config": {
                        "edges": [
                            {"source": "Asset", "target": "Site", "relation": "partOf"}
                        ]
                    },
                },
            }
        }
    )
    manifest.finish_init()
    return manifest


def _vertex(manifest: GraphManifest, name: str):
    return manifest.require_schema().core_schema.vertex_config[name]


def _field(manifest: GraphManifest, vertex: str, field: str) -> Field:
    return next(f for f in _vertex(manifest, vertex).properties if f.name == field)


# ── grounding a type that already exists ────────────────────────────────────


def test_a_vertex_can_be_grounded_after_the_fact() -> None:
    out = apply_evolution(
        _manifest(),
        [
            SetVertexSemanticsOp(
                semantics={"Asset": Semantics(iri=PROV_ENTITY, exact_match=[SOSA_FOI])}
            )
        ],
    )
    semantics = _vertex(out, "Asset").semantics
    assert semantics is not None
    assert semantics.iri == PROV_ENTITY
    assert semantics.exact_match == [SOSA_FOI]
    # Untouched types keep their (absent) grounding.
    assert _vertex(out, "Site").semantics is None


def test_grounding_an_unknown_vertex_names_it() -> None:
    with pytest.raises(ValueError, match="unknown vertices"):
        apply_evolution(
            _manifest(),
            [SetVertexSemanticsOp(semantics={"Nope": Semantics(iri=PROV_ENTITY)})],
        )


def test_an_edge_can_be_grounded_after_the_fact() -> None:
    out = apply_evolution(
        _manifest(),
        [
            SetEdgeSemanticsOp(
                edges=[{"source": "Asset", "target": "Site", "relation": "partOf"}],
                semantics=Semantics(iri=DCT_PART_OF),
            )
        ],
    )
    edge = out.require_schema().core_schema.edge_config.edges[0]
    assert edge.semantics is not None
    assert edge.semantics.iri == DCT_PART_OF


def test_grounding_an_unknown_edge_names_it() -> None:
    with pytest.raises(ValueError, match="unknown edges"):
        apply_evolution(
            _manifest(),
            [
                SetEdgeSemanticsOp(
                    edges=[{"source": "Asset", "target": "Nope", "relation": "x"}],
                    semantics=Semantics(iri=DCT_PART_OF),
                )
            ],
        )


# ── field grounding, which is why there are three ops and not one ───────────


def test_a_property_carries_a_unit_which_a_type_may_not() -> None:
    """The whole reason ``FieldSemantics`` is a separate model and op.

    ``unit`` is meaningful on a measurement and meaningless on a type, and the
    models are kept apart so the mistake is a validation error rather than a
    silently ignored key.
    """
    out = apply_evolution(
        _manifest(),
        [
            SetFieldSemanticsOp(
                targets=[
                    FieldSemanticsTarget(
                        vertex="Asset",
                        field="temp_c",
                        semantics=FieldSemantics(unit="Cel"),
                    )
                ]
            )
        ],
    )
    semantics = _field(out, "Asset", "temp_c").semantics
    assert semantics is not None
    assert semantics.unit == "Cel"

    with pytest.raises(ValueError):
        SetVertexSemanticsOp.model_validate({"semantics": {"Asset": {"unit": "Cel"}}})


def test_an_edge_property_can_be_grounded_and_inverted() -> None:
    """Edge properties carry `FieldSemantics` too; the op now reaches them."""
    manifest = apply_evolution(
        _manifest(),
        [AddEdgePropertiesOp(additions={"partOf": ["since"]})],
        bump_version=False,
    )
    target = EdgeFieldSemanticsTarget(
        source="Asset",
        target="Site",
        relation="partOf",
        field="since",
        semantics=FieldSemantics(iri=PROV_GENERATED, unit="s"),
    )
    op = SetFieldSemanticsOp(targets=[target])
    out = apply_evolution(manifest, [op], bump_version=False)
    edge = out.require_schema().core_schema.edge_config.edges[0]
    since = next(f for f in edge.properties if f.name == "since")
    assert since.semantics is not None and since.semantics.unit == "s"

    inverses, blockers = invert_ops([op], manifest=manifest)
    assert not blockers
    restored = apply_evolution(out, inverses, bump_version=False)
    edge = restored.require_schema().core_schema.edge_config.edges[0]
    assert next(f for f in edge.properties if f.name == "since").semantics is None


def test_grounding_an_unknown_edge_property_names_the_edge() -> None:
    op = SetFieldSemanticsOp(
        targets=[
            EdgeFieldSemanticsTarget(
                source="Asset", target="Site", relation="partOf", field="ghost"
            )
        ]
    )
    with pytest.raises(
        ValueError,
        match=r"unknown properties: \[\"\('Asset', 'Site', 'partOf'\).ghost\"\]",
    ):
        apply_evolution(_manifest(), [op], bump_version=False)


def test_grounding_a_missing_property_names_the_property_not_the_vertex() -> None:
    """The likely mistake is a stale field name after a rename."""
    with pytest.raises(ValueError, match=r"unknown properties.*Asset\.gone"):
        apply_evolution(
            _manifest(),
            [
                SetFieldSemanticsOp(
                    targets=[
                        FieldSemanticsTarget(
                            vertex="Asset",
                            field="gone",
                            semantics=FieldSemantics(unit="Cel"),
                        )
                    ]
                )
            ],
        )


def test_the_same_property_cannot_be_targeted_twice() -> None:
    with pytest.raises(ValueError, match="unique by"):
        SetFieldSemanticsOp(
            targets=[
                FieldSemanticsTarget(vertex="Asset", field="temp_c"),
                FieldSemanticsTarget(vertex="Asset", field="temp_c"),
            ]
        )


# ── the widened property add ────────────────────────────────────────────────


def test_a_property_can_arrive_typed_and_grounded_in_one_op() -> None:
    """Without this, a temporal or measured property needs three ops and still
    cannot be grounded, because nothing else writes ``semantics`` on a field."""
    out = apply_evolution(
        _manifest(),
        [
            AddVertexPropertiesOp(
                additions={
                    "Asset": [
                        Field(
                            name="valid_from",
                            type=FieldType.DATETIME,
                            semantics=FieldSemantics(iri=PROV_GENERATED),
                        )
                    ]
                }
            )
        ],
    )
    field = _field(out, "Asset", "valid_from")
    assert field.type == FieldType.DATETIME
    assert field.semantics is not None
    assert field.semantics.iri == PROV_GENERATED


def test_a_bare_name_still_means_an_untyped_property() -> None:
    """The original shape, unchanged -- the widening is additive."""
    out = apply_evolution(
        _manifest(), [AddVertexPropertiesOp(additions={"Asset": ["note"]})]
    )
    assert _field(out, "Asset", "note").type is None


def test_both_shapes_mix_in_one_op() -> None:
    out = apply_evolution(
        _manifest(),
        [
            AddVertexPropertiesOp(
                additions={
                    "Asset": ["note", Field(name="reading", type=FieldType.FLOAT)]
                }
            )
        ],
    )
    assert _field(out, "Asset", "note").type is None
    assert _field(out, "Asset", "reading").type == FieldType.FLOAT


# ── reversibility ───────────────────────────────────────────────────────────


def test_grounding_inverts_back_to_ungrounded() -> None:
    """Clearing is expressible, so the inverse of grounding a bare type is real."""
    manifest = _manifest()
    ops = [
        SetVertexSemanticsOp(semantics={"Asset": Semantics(iri=PROV_ENTITY)}),
        SetEdgeSemanticsOp(
            edges=[{"source": "Asset", "target": "Site", "relation": "partOf"}],
            semantics=Semantics(iri=DCT_PART_OF),
        ),
        SetFieldSemanticsOp(
            targets=[
                FieldSemanticsTarget(
                    vertex="Asset", field="temp_c", semantics=FieldSemantics(unit="Cel")
                )
            ]
        ),
    ]
    grounded = apply_evolution(manifest, ops)
    inverses, reasons = invert_ops(ops, manifest=manifest)
    assert reasons == []

    back = apply_evolution(grounded, inverses)
    assert _vertex(back, "Asset").semantics is None
    assert back.require_schema().core_schema.edge_config.edges[0].semantics is None
    assert _field(back, "Asset", "temp_c").semantics is None


def test_an_edge_selection_with_differing_priors_has_no_single_inverse() -> None:
    """One payload value for the whole selection, so a mixed prior is not
    expressible as one op. Reported as irreversible rather than flattened."""
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "plain", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "Asset",
                                "properties": [{"name": "id"}],
                                "identity": ["id"],
                            },
                            {
                                "name": "Site",
                                "properties": [{"name": "id"}],
                                "identity": ["id"],
                            },
                        ]
                    },
                    "edge_config": {
                        "edges": [
                            {
                                "source": "Asset",
                                "target": "Site",
                                "relation": "partOf",
                                "semantics": {"iri": DCT_PART_OF},
                            },
                            {"source": "Asset", "target": "Site", "relation": "at"},
                        ]
                    },
                },
            }
        }
    )
    manifest.finish_init()

    ops = [
        SetEdgeSemanticsOp(
            edges=[
                {"source": "Asset", "target": "Site", "relation": "partOf"},
                {"source": "Asset", "target": "Site", "relation": "at"},
            ],
            semantics=Semantics(iri="https://schema.org/location"),
        )
    ]
    _, reasons = invert_ops(ops, manifest=manifest)
    assert reasons, "a mixed prior must be reported, not silently flattened"
