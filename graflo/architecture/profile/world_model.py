"""The World Model Profile: six mechanically checkable assertions.

A named conformance level over a manifest. It introduces no semantics and
touches no backend -- every assertion reads fields the contract already has --
so it is a profile rather than a feature, and it runs against manifests nobody
here authored.

**One distinction to keep straight.** Assertion ``provenance`` is about the
*data*: does the model say where a fact came from. It is unrelated to
``ManifestMetadata.provenance`` and ``schema/provenance.py::Provenance``, which
are the *artifact's* content address and lineage. Conflating the two is the
mistake this docstring exists to prevent.
"""

from __future__ import annotations

from collections.abc import Iterable

from graflo.architecture.profile.context import (
    AGENT_TYPE_IRIS,
    PROVENANCE_EDGE_IRIS,
    TEMPORAL_PROPERTY_IRIS,
    UNIT_PROPERTY_IRIS,
    CheckContext,
)
from graflo.architecture.profile.model import AssertionResult, Finding, Status, roll_up
from graflo.architecture.profile.runner import Assertion, Profile

PROFILE_VERSION = "0.1"

_NOT_DECLARABLE = (
    "not verifiable without the authored document; "
    "pass the manifest as written rather than a parsed model"
)


# --- helpers ----------------------------------------------------------------


def _iris(semantics) -> list[str]:
    """Every IRI a ``Semantics`` block asserts, ``iri`` first."""
    if semantics is None:
        return []
    out: list[str] = []
    if getattr(semantics, "iri", None):
        out.append(semantics.iri)
    out.extend(getattr(semantics, "exact_match", []) or [])
    return out


def _field_type(field) -> str:
    """``Field.type`` as a plain string.

    ``ConfigBaseModel`` sets ``use_enum_values``, so this is usually already a
    string -- but a model built in Python may hold the enum, and both must read
    the same here.
    """
    raw = getattr(field, "type", None)
    if raw is None:
        return ""
    return str(getattr(raw, "value", raw))


def _vertices(context: CheckContext) -> list:
    schema = context.manifest.graph_schema
    if schema is None:
        return []
    return list(schema.core_schema.vertex_config.vertices)


def _edges(context: CheckContext) -> list:
    schema = context.manifest.graph_schema
    if schema is None:
        return []
    return list(schema.core_schema.edge_config.edges)


def _edge_address(edge) -> str:
    relation = edge.relation or "?"
    return f"edge:{edge.source}-{relation}->{edge.target}"


def _result(
    assertion_id: str,
    title: str,
    findings: list[Finding],
    checked: int,
    *,
    required: bool = True,
) -> AssertionResult:
    """Roll *findings* up into one assertion outcome.

    Nothing checked is ``not_applicable``, never a pass: an assertion that had
    nothing to look at has not been satisfied, and saying so is what stops a
    schema-less manifest reporting six green ticks.
    """
    if checked == 0 and not findings:
        status: Status = "not_applicable"
    else:
        status = roll_up([f.status for f in findings]) if findings else "pass"
    return AssertionResult(
        id=assertion_id,
        title=title,
        required=required,
        status=status,
        checked=checked,
        findings=findings,
    )


def _any_iri_in(semantics_holders: Iterable, table: frozenset[str]) -> bool:
    return any(
        iri in table for holder in semantics_holders for iri in _iris(holder.semantics)
    )


# --- 1. grounded types ------------------------------------------------------

_A1 = "grounded-types"


def check_grounded_types(context: CheckContext) -> AssertionResult:
    """Every vertex and edge names the concept it denotes, by IRI."""
    findings: list[Finding] = []
    checked = 0
    elements = [(f"vertex:{v.name}", v) for v in _vertices(context)]
    elements += [(_edge_address(e), e) for e in _edges(context)]
    for address, element in elements:
        checked += 1
        iris = _iris(element.semantics)
        if not iris:
            findings.append(
                Finding(
                    assertion=_A1,
                    status="fail",
                    severity="error",
                    target=address,
                    message="no semantics.iri or semantics.exact_match",
                )
            )
            continue
        for iri in iris:
            status = context.resolver.resolve(iri)
            if status == "malformed":
                findings.append(
                    Finding(
                        assertion=_A1,
                        status="fail",
                        severity="error",
                        target=address,
                        message=f"{iri!r} is not an absolute IRI",
                        detail={"iri": iri},
                    )
                )
            elif status == "unknown":
                findings.append(
                    Finding(
                        assertion=_A1,
                        status="warn",
                        severity="warning",
                        target=address,
                        message=(
                            f"{iri} is not in a recognised vocabulary; liveness "
                            "unchecked"
                        ),
                        detail={"iri": iri, "liveness": "unchecked"},
                    )
                )
    return _result(
        _A1, "Types are grounded in an external vocabulary", findings, checked
    )


# --- 2. declared identity ---------------------------------------------------

_A2 = "declared-identity"
_IDENTITY_KEYS = (
    "identity",
    "blank",
    "assigned",
    "hash_identity_properties",
    "identity_funnel",
)


def check_declared_identity(context: CheckContext) -> AssertionResult:
    """Every vertex states how it is identified, rather than falling back.

    ``VertexConfig`` refuses a vertex with no identity *unless*
    ``identity_from_all_properties`` is set, in which case it silently adopts
    every property as the key. That fallback is the failure this looks for, and
    it is only visible in the authored document.
    """
    findings: list[Finding] = []
    vertices = _vertices(context)
    authored = context.authored_vertices()
    for vertex in vertices:
        address = f"vertex:{vertex.name}"
        mode = vertex.identity_mode
        if mode == "blank":
            findings.append(
                Finding(
                    assertion=_A2,
                    status="fail",
                    severity="error",
                    target=address,
                    message="identity mode is blank: instances carry no natural key",
                    detail={"identity_mode": mode},
                )
            )
            continue
        if not context.has_authored:
            findings.append(
                Finding(
                    assertion=_A2,
                    status="warn",
                    severity="warning",
                    target=address,
                    message=f"identity mode {mode!r}; declaration {_NOT_DECLARABLE}",
                    detail={"identity_mode": mode},
                )
            )
            continue
        block = authored.get(vertex.name)
        if block is not None and not any(block.get(key) for key in _IDENTITY_KEYS):
            findings.append(
                Finding(
                    assertion=_A2,
                    status="fail",
                    severity="error",
                    target=address,
                    message=(
                        "declares no identity; it fell back to "
                        "identity_from_all_properties, which keys the vertex on "
                        "every property it happens to carry"
                    ),
                    detail={"identity_mode": mode},
                )
            )
    return _result(
        _A2, "Every vertex declares an identity mode", findings, len(vertices)
    )


# --- 3. declared directionality ---------------------------------------------

_A3 = "declared-directionality"


def check_declared_directionality(context: CheckContext) -> AssertionResult:
    """Every edge states whether source-to-target order is meaningful.

    ``Edge.directed`` defaults to ``True``, so a declared direction and an
    undeclared one are the same model. Only the authored document can tell them
    apart.
    """
    findings: list[Finding] = []
    edges = _edges(context)
    if not context.has_authored:
        if edges:
            findings.append(
                Finding(
                    assertion=_A3,
                    status="warn",
                    severity="warning",
                    target="manifest",
                    message=f"directionality {_NOT_DECLARABLE}",
                )
            )
        return _result(
            _A3, "Every edge declares its directionality", findings, len(edges)
        )

    authored = context.authored_edges()
    undeclared = [block for block in authored if "directed" not in block]
    for block in undeclared:
        relation = block.get("relation") or "?"
        findings.append(
            Finding(
                assertion=_A3,
                status="fail",
                severity="error",
                target=f"edge:{block.get('source')}-{relation}->{block.get('target')}",
                message="does not declare `directed`; it defaulted to True",
            )
        )
    return _result(_A3, "Every edge declares its directionality", findings, len(edges))


# --- 4. declared units ------------------------------------------------------

_A4 = "declared-units"


def _has_row_level_unit(element) -> bool:
    """Whether *element* declares a property that carries the unit as data.

    An abstract type whose instances each measure something different cannot
    name one unit in its contract without lying, so declaring a unit-valued
    property is the honest form and passes this assertion.
    """
    return _any_iri_in(getattr(element, "properties", []) or [], UNIT_PROPERTY_IRIS)


def check_declared_units(context: CheckContext) -> AssertionResult:
    """Every measured property says what it is measured in.

    Measured means a floating-point property that is not part of the key. It
    passes either by carrying ``semantics.unit`` itself, or by its type
    declaring a unit-valued companion property.
    """
    findings: list[Finding] = []
    checked = 0
    for vertex in _vertices(context):
        key_fields = set(vertex.identity) | set(vertex.digest_source_fields)
        row_unit = _has_row_level_unit(vertex)
        for field in vertex.properties:
            if _field_type(field) not in ("FLOAT", "DOUBLE"):
                continue
            if field.name in key_fields:
                continue
            checked += 1
            address = f"vertex:{vertex.name}.{field.name}"
            unit = getattr(field.semantics, "unit", None) if field.semantics else None
            if unit:
                if "://" in unit:
                    findings.append(
                        Finding(
                            assertion=_A4,
                            status="warn",
                            severity="warning",
                            target=address,
                            message=(
                                f"unit {unit!r} is an IRI; this manifest mixes unit "
                                "conventions, so units cannot be compared across it"
                            ),
                            detail={"unit": unit, "unit_source": "schema"},
                        )
                    )
                continue
            if row_unit:
                findings.append(
                    Finding(
                        assertion=_A4,
                        status="pass",
                        severity="info",
                        target=address,
                        message="unit carried per row by a unit-valued property",
                        detail={"unit_source": "row"},
                    )
                )
                continue
            findings.append(
                Finding(
                    assertion=_A4,
                    status="fail",
                    severity="error",
                    target=address,
                    message=(
                        "measured property carries no unit, and its type declares "
                        "no unit-valued property to carry one per row"
                    ),
                )
            )
    return _result(_A4, "Every measured property carries a unit", findings, checked)


# --- 5. temporal validity ---------------------------------------------------

_A5 = "temporal"


def check_temporal(context: CheckContext) -> AssertionResult:
    """The model says when its facts were true, or an operator waives it.

    Declared means at least one datetime property grounded in a validity or
    observation-time vocabulary -- time expressed as modelled state, which is
    what the contract can express today without a temporal primitive.

    A waiver is applied by the runner, not here: this reports the finding, and
    an operator's sidecar decides whether it counts against the manifest.
    """
    carriers: list[str] = []
    for vertex in _vertices(context):
        for field in vertex.properties:
            if _field_type(field) != "DATETIME":
                continue
            if any(iri in TEMPORAL_PROPERTY_IRIS for iri in _iris(field.semantics)):
                carriers.append(f"{vertex.name}.{field.name}")
    if carriers:
        finding = Finding(
            assertion=_A5,
            status="pass",
            severity="info",
            target="manifest",
            message=f"temporal validity modelled by {len(carriers)} property/properties",
            detail={"carriers": carriers},
        )
    else:
        finding = Finding(
            assertion=_A5,
            status="fail",
            severity="error",
            target="manifest",
            message=(
                "no property is grounded in a validity or observation-time "
                "vocabulary, so the model cannot say when a fact was true; "
                "declare one or waive this assertion with a reason"
            ),
        )
    return _result(_A5, "Temporal validity is declared or waived", [finding], 1)


# --- 6. provenance ----------------------------------------------------------

_A6 = "provenance"


def check_provenance(context: CheckContext) -> AssertionResult:
    """The model says where a fact came from, and ingestion attaches it.

    Two halves. The contract half asks whether the schema can express
    provenance at all: an actor type, and an edge grounded in a
    derivation or attribution property. The ingestion half asks whether a
    resource that creates vertices actually writes one of those edges -- and is
    reported ``not_applicable`` when the manifest declares no ingestion model,
    rather than passing on a question it never asked.
    """
    findings: list[Finding] = []
    vertices = _vertices(context)
    edges = _edges(context)
    checked = len(vertices) + len(edges)

    has_agent = _any_iri_in(vertices, AGENT_TYPE_IRIS)
    provenance_edges = [
        edge
        for edge in edges
        if any(iri in PROVENANCE_EDGE_IRIS for iri in _iris(edge.semantics))
    ]
    if not has_agent:
        findings.append(
            Finding(
                assertion=_A6,
                status="fail",
                severity="error",
                target="manifest",
                message=(
                    "no vertex is grounded as an agent, so a fact has nothing to "
                    "be attributed to"
                ),
            )
        )
    if not provenance_edges:
        findings.append(
            Finding(
                assertion=_A6,
                status="fail",
                severity="error",
                target="manifest",
                message=(
                    "no edge is grounded in a derivation or attribution property, "
                    "so the model cannot record where a fact came from"
                ),
            )
        )
    if has_agent and provenance_edges:
        # Recorded rather than left silent: without it the assertion's only
        # finding on a schema-only manifest is the ingestion note, and the
        # roll-up reports the whole assertion `not_applicable` when its
        # contract half was in fact satisfied.
        relations = sorted({e.relation for e in provenance_edges if e.relation})
        findings.append(
            Finding(
                assertion=_A6,
                status="pass",
                severity="info",
                target="manifest",
                message="provenance is expressible: an agent type and "
                f"{len(provenance_edges)} provenance relation(s)",
                detail={"relations": relations},
            )
        )

    if context.manifest.ingestion_model is None:
        findings.append(
            Finding(
                assertion=_A6,
                status="not_applicable",
                severity="info",
                target="manifest",
                message=(
                    "manifest declares no ingestion model, so whether provenance "
                    "is attached at ingest was not checked"
                ),
            )
        )
    elif provenance_edges:
        findings.append(
            Finding(
                assertion=_A6,
                status="pass",
                severity="info",
                target="manifest",
                message="provenance relations available to ingestion",
            )
        )
    return _result(_A6, "Provenance is expressible and attached", findings, checked)


WORLD_MODEL_PROFILE = Profile(
    name="world-model",
    version=PROFILE_VERSION,
    assertions=(
        Assertion(
            _A1,
            "Types are grounded in an external vocabulary",
            True,
            check_grounded_types,
        ),
        Assertion(
            _A2, "Every vertex declares an identity mode", True, check_declared_identity
        ),
        Assertion(
            _A3,
            "Every edge declares its directionality",
            True,
            check_declared_directionality,
        ),
        Assertion(
            _A4, "Every measured property carries a unit", True, check_declared_units
        ),
        Assertion(_A5, "Temporal validity is declared or waived", True, check_temporal),
        Assertion(
            _A6, "Provenance is expressible and attached", True, check_provenance
        ),
    ),
)


__all__ = [
    "PROFILE_VERSION",
    "WORLD_MODEL_PROFILE",
    "check_declared_directionality",
    "check_declared_identity",
    "check_declared_units",
    "check_grounded_types",
    "check_provenance",
    "check_temporal",
]
