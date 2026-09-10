"""Planning the operations that lift a manifest into a twin-ready shape.

A pure planner: it emits :data:`~graflo.architecture.evolution.ops.ManifestOp`
values and applies nothing. ``apply_evolution`` applies them, ``invert_ops``
undoes them, and the op list itself is the reviewable artifact -- an operator
reads what the lift proposes before any of it runs. That is the whole argument
for expressing this as Operations rather than as a bespoke transform: a
transform is a black box that either did the right thing or did not.

**What a lift does not do.** It changes the *contract*, not the data flow. A
lifted manifest declares ``<Type>State`` and ``Evidence``, but no resource
populates them, so a manifest that carries an ``ingestion_model`` will still
report the profile's "provenance materialised at ingest" finding afterwards --
correctly. Scaffolding the pipelines needs resource-level ops and a statement of
which resource feeds which type, and is deliberately out of scope here. This is
the misreading the next reader will have, so it is also in the CLI help.
"""

from __future__ import annotations

from typing import Any

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.schema.semantics import FieldSemantics, Semantics
from graflo.architecture.schema.vertex import Field, Vertex

from ..ops import (
    AddEdgesOp,
    AddVerticesOp,
    FieldSemanticsTarget,
    ManifestOp,
    RemoveVertexPropertiesOp,
    ReplaceIdentityOp,
    SetEdgeDirectedOp,
    SetEdgeSemanticsOp,
    SetFieldSemanticsOp,
    SetVertexSemanticsOp,
)
from . import vocabulary as vocab
from .spec import LiftSpec


class LiftError(ValueError):
    """The lift cannot be planned, and the message says which declaration is missing.

    Raised rather than guessed. Every case here is one where the planner can see
    that something is wrong but not what the right answer is -- an ungrounded
    type, a name it would have to overwrite, a property it was told to move that
    is part of the key.
    """


def _semantics(grounding: Any) -> Semantics:
    return Semantics(
        iri=grounding.iri,
        exact_match=list(grounding.exact_match),
        synonyms=list(grounding.synonyms),
    )


def _key_fields(vertex: Vertex) -> list[Field]:
    """The properties that identify *vertex*, as fields a scaffold can carry."""
    names = list(vertex.identity or vertex.hash_identity_properties or [])
    by_name = {field.name: field for field in vertex.properties}
    return [by_name[name] for name in names if name in by_name]


def plan_lift(
    manifest: GraphManifest,
    spec: LiftSpec,
    *,
    authored: dict[str, Any] | None = None,
) -> list[ManifestOp]:
    """The ops that lift *manifest* into a twin-ready schema.

    Args:
        manifest: the manifest to lift, already ``finish_init()``-ed.
        spec: the semantic declarations the planner cannot infer.
        authored: the manifest as written, when available. Only an authored
            document distinguishes "this vertex declared no identity" from
            "this vertex declared every property as its identity", so without
            it the planner cannot detect the fallback and does not try.

    Returns:
        Ops in application order: grounding first, then structure, then the
        scaffolding, then the removals. Ordering matters -- a property is moved
        onto its ``State`` before it is removed from the entity.

    Raises:
        LiftError: a declaration the planner needs is missing, or a name it
            would mint is already taken.
    """
    schema = manifest.graph_schema
    if schema is None:
        raise LiftError(
            "cannot lift a manifest with no schema block: there are no types to "
            "ground and nothing to attach state to"
        )

    core = schema.core_schema
    vertex_config = core.vertex_config
    existing = set(vertex_config.vertex_set)
    by_name = {vertex.name: vertex for vertex in vertex_config.vertices}

    unknown = sorted(
        (set(spec.grounding) | set(spec.stateful) | set(spec.observed)) - existing
    )
    if unknown:
        raise LiftError(f"spec names types the manifest does not have: {unknown}")

    ops: list[ManifestOp] = []
    ops += _identity_ops(spec, vertex_config, authored)
    ops += _grounding_ops(spec, core)
    ops += _directionality_ops(core)
    ops += _unit_ops(spec, by_name)
    scaffold_ops, scaffolded = _scaffold_ops(spec, by_name, existing)
    ops += scaffold_ops
    ops += _provenance_ops(spec, existing, scaffolded)
    ops += _retire_ops(spec, by_name)
    return ops


# --- 2. declared identity ---------------------------------------------------


def _identity_ops(
    spec: LiftSpec, vertex_config: Any, authored: dict[str, Any] | None
) -> list[ManifestOp]:
    """Pin an identity for any type that only has one by fallback.

    Only detectable from the authored document: ``VertexConfig`` fills an unset
    identity from every property, so by the time there is a model the fallback
    and a deliberate all-properties key are the same thing. Without *authored*
    the planner stays quiet rather than pinning identities nobody asked it to.
    """
    if not spec.identity:
        if authored is None:
            return []
        fallbacks = sorted(_identity_fallbacks(authored))
        if fallbacks:
            raise LiftError(
                f"these types declare no identity and fall back to "
                f"identity_from_all_properties: {fallbacks}. Declare one per type "
                "under `identity:` in the spec -- keying a type on every property "
                "it happens to carry is not an identity the lift can adopt for you"
            )
        return []

    return [
        ReplaceIdentityOp(
            replacements={
                name: {
                    "to": {"mode": "natural", "identity": fields},
                    # The old "identity" was the all-properties fallback, so
                    # there is nothing worth demoting to a lookup key.
                    "retire": "keep",
                }
                for name, fields in sorted(spec.identity.items())
            }
        )
    ]


def _identity_fallbacks(authored: dict[str, Any]) -> set[str]:
    """Vertex names in *authored* that declare no identity of any kind."""
    schema = authored.get("schema") or authored.get("graph_schema") or {}
    core = schema.get("core_schema") or schema.get("graph") or {}
    blocks = (core.get("vertex_config") or {}).get("vertices") or []
    keys = (
        "identity",
        "blank",
        "assigned",
        "hash_identity_properties",
        "identity_funnel",
    )
    return {
        block["name"]
        for block in blocks
        if isinstance(block, dict)
        and "name" in block
        and not any(block.get(key) for key in keys)
    }


# --- 1. grounded types ------------------------------------------------------


def _grounding_ops(spec: LiftSpec, core: Any) -> list[ManifestOp]:
    ops: list[ManifestOp] = []
    if spec.grounding:
        ops.append(
            SetVertexSemanticsOp(
                semantics={
                    name: _semantics(grounding)
                    for name, grounding in sorted(spec.grounding.items())
                }
            )
        )
    # One op per distinct grounding: the payload carries a single value for the
    # whole selection, so grouping unlike edges would flatten them.
    for entry in spec.edge_grounding:
        ops.append(
            SetEdgeSemanticsOp(
                edges=[
                    {
                        "source": entry.source,
                        "target": entry.target,
                        "relation": entry.relation,
                    }
                ],
                semantics=_semantics(entry),
            )
        )
    return ops


# --- 3. declared directionality ---------------------------------------------


def _directionality_ops(core: Any) -> list[ManifestOp]:
    """Restate ``directed`` on every edge, so the writer can emit it explicitly.

    The op is a no-op against the model -- the value it sets is the value that is
    already there. It exists so the *change set* records the decision, which is
    what a replay of this lift needs in order to mean the same thing.
    """
    edges = list(core.edge_config.edges)
    if not edges:
        return []
    ops: list[ManifestOp] = []
    for value in (True, False):
        selected = [
            {"source": e.source, "target": e.target, "relation": e.relation}
            for e in edges
            if bool(e.directed) is value
        ]
        if selected:
            ops.append(SetEdgeDirectedOp(edges=selected, directed=value))
    return ops


# --- 4. declared units ------------------------------------------------------


def _unit_ops(spec: LiftSpec, by_name: dict[str, Vertex]) -> list[ManifestOp]:
    targets: list[FieldSemanticsTarget] = []
    missing: list[str] = []
    for address, unit in sorted(spec.measured.items()):
        vertex_name, field_name = address.split(".", 1)
        vertex = by_name.get(vertex_name)
        if vertex is None or field_name not in {f.name for f in vertex.properties}:
            missing.append(address)
            continue
        existing = next(f for f in vertex.properties if f.name == field_name).semantics
        targets.append(
            FieldSemanticsTarget(
                vertex=vertex_name,
                field=field_name,
                # Preserve any grounding already on the property: a unit is an
                # addition to what a field means, not a replacement for it.
                semantics=FieldSemantics(
                    iri=existing.iri if existing else None,
                    exact_match=list(existing.exact_match) if existing else [],
                    synonyms=list(existing.synonyms) if existing else [],
                    unit=unit,
                ),
            )
        )
    if missing:
        raise LiftError(f"`measured` names properties that do not exist: {missing}")
    return [SetFieldSemanticsOp(targets=targets)] if targets else []


# --- 5. temporal validity, and 6a's attachment points -----------------------


def _scaffold_ops(
    spec: LiftSpec, by_name: dict[str, Vertex], existing: set[str]
) -> tuple[list[ManifestOp], list[str]]:
    """``<Type>State`` / ``<Type>Observation`` types and the edges tying them back."""
    vertices: list[Vertex] = []
    edges: list[Any] = []
    scaffolded: list[str] = []
    collisions: list[str] = []

    for subject in sorted(spec.stateful):
        vertex = by_name[subject]
        key_fields = _key_fields(vertex)
        if not key_fields:
            raise LiftError(
                f"{subject!r} has no identity fields, so its state rows would have "
                "nothing to point back to"
            )
        moved = _moved_fields(subject, spec.stateful[subject], vertex, key_fields)
        name = vocab.state_type_name(subject)
        if name in existing:
            collisions.append(name)
            continue
        vertices.append(vocab.state_vertex(subject, key_fields, moved))
        edges.append(vocab.specialization_edge(subject))
        scaffolded.append(name)

    for subject in sorted(spec.observed):
        vertex = by_name[subject]
        key_fields = _key_fields(vertex)
        if not key_fields:
            raise LiftError(
                f"{subject!r} has no identity fields, so its observations would "
                "have nothing to point back to"
            )
        name = vocab.observation_type_name(subject)
        if name in existing:
            collisions.append(name)
            continue
        vertices.append(vocab.observation_vertex(subject, key_fields))
        edges.append(vocab.feature_of_interest_edge(subject))
        scaffolded.append(name)

    if collisions:
        raise LiftError(
            f"the lift would mint types the manifest already has: {sorted(collisions)}. "
            "Rename them first -- silently merging into an existing type is not a "
            "lift, it is a schema change nobody asked for"
        )

    ops: list[ManifestOp] = []
    if vertices:
        ops.append(AddVerticesOp(vertices=vertices))
    if edges:
        ops.append(AddEdgesOp(edges=edges))
    return ops, scaffolded


def _moved_fields(
    subject: str, names: list[str], vertex: Vertex, key_fields: list[Field]
) -> list[Field]:
    by_field = {field.name: field for field in vertex.properties}
    unknown = sorted(set(names) - set(by_field))
    if unknown:
        raise LiftError(f"{subject!r} has no properties {unknown}")
    key_names = {field.name for field in key_fields}
    keyed = sorted(set(names) & key_names)
    if keyed:
        raise LiftError(
            f"{subject!r}: {keyed} identify the type, so they cannot also be facts "
            "that change over time -- an identity that changes is not an identity"
        )
    return [by_field[name] for name in names]


# --- 6. provenance ----------------------------------------------------------


def _provenance_ops(
    spec: LiftSpec, existing: set[str], scaffolded: list[str]
) -> list[ManifestOp]:
    """``Evidence`` and ``Agent``, plus the edges that make lineage expressible.

    ``Evidence -wasAttributedTo-> Agent`` is emitted even when there is nothing
    else to attach: it is what makes the schema *able* to say where a fact came
    from, which is the question the profile asks.
    """
    if not spec.provenance:
        return []
    taken = sorted({vocab.EVIDENCE, vocab.AGENT} & existing)
    if taken:
        raise LiftError(
            f"the lift would mint types the manifest already has: {taken}. Rename "
            "them first, or set `provenance: false` if they already serve this role"
        )
    edges = [vocab.derived_from_edge(name) for name in scaffolded]
    edges.append(vocab.attributed_to_edge())
    return [
        AddVerticesOp(vertices=[vocab.evidence_vertex(), vocab.agent_vertex()]),
        AddEdgesOp(edges=edges),
    ]


# --- the destructive half, emitted last -------------------------------------


def _retire_ops(spec: LiftSpec, by_name: dict[str, Vertex]) -> list[ManifestOp]:
    """Remove the moved properties from the entities they no longer belong on.

    Last, and separable: everything before this is additive, so a caller who
    wants the scaffolding without the surgery sets ``retire: keep`` and the op
    list simply ends earlier.
    """
    if spec.retire == "keep":
        return []
    removals = {
        subject: list(names)
        for subject, names in sorted(spec.stateful.items())
        if names
    }
    return [RemoveVertexPropertiesOp(removals=removals)] if removals else []


__all__ = ["LiftError", "plan_lift"]
